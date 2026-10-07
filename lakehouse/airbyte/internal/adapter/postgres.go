package adapter

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/datalakehouse/airbyte-config-tool/internal/discovery"
	"github.com/datalakehouse/airbyte-config-tool/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresAdapter struct {
	pool   *pgxpool.Pool
	config model.DBConfig
}

func NewPostgresAdapter() *PostgresAdapter {
	return &PostgresAdapter{}
}

func (p *PostgresAdapter) buildConnString(cfg model.DBConfig) string {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	encodedPass := url.QueryEscape(cfg.Password)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.Username, encodedPass, cfg.Host, cfg.Port, cfg.Database, sslMode)
}

func (p *PostgresAdapter) Connect(ctx context.Context, cfg model.DBConfig) error {
	p.config = cfg
	connStr := p.buildConnString(cfg)
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to create postgres connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping postgres at %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	p.pool = pool
	return nil
}

func (p *PostgresAdapter) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

func (p *PostgresAdapter) Ping(ctx context.Context) (*model.DBInfo, error) {
	if p.pool == nil {
		return nil, fmt.Errorf("not connected to postgres")
	}

	var version string
	if err := p.pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return nil, fmt.Errorf("error reading postgres version: %w", err)
	}

	var isReplica bool
	if err := p.pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&isReplica); err != nil {
		return nil, fmt.Errorf("error checking replica status: %w", err)
	}

	info := &model.DBInfo{
		Engine:    "PostgreSQL",
		Version:   version,
		IsReplica: isReplica,
	}

	var walLevel string
	_ = p.pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel)
	if walLevel != "" && walLevel != "logical" {
		info.Warnings = append(info.Warnings,
			fmt.Sprintf("Current wal_level is '%s'. For CDC (Logical Replication), PostgreSQL requires wal_level = logical (and DB restart).", walLevel))
	}

	if isReplica {
		info.Warnings = append(info.Warnings,
			"Detected Read-Only Replica (Standby Node). Note: DDL statements (CREATE VIEW/USER) must be executed on the Primary Master node.")
	} else {
		info.Warnings = append(info.Warnings,
			"Detected Primary Node. For high-volume production, consider directing Airbyte reads to a standby replica.")
	}

	return info, nil
}

func (p *PostgresAdapter) DiscoverSchemas(ctx context.Context) ([]string, error) {
	query := `
		SELECT schema_name 
		FROM information_schema.schemata 
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') 
		  AND schema_name NOT LIKE 'pg_temp_%'
		ORDER BY schema_name;
	`
	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, nil
}

func (p *PostgresAdapter) DiscoverTables(ctx context.Context, schema string) ([]model.TableMetadata, error) {
	if schema == "" {
		schema = "public"
	}

	tableQuery := `
		SELECT t.table_name
		FROM information_schema.tables t
		WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE'
		ORDER BY t.table_name;
	`
	tRows, err := p.pool.Query(ctx, tableQuery, schema)
	if err != nil {
		return nil, err
	}
	defer tRows.Close()

	var tables []model.TableMetadata
	for tRows.Next() {
		var tName string
		if err := tRows.Scan(&tName); err != nil {
			return nil, err
		}
		tables = append(tables, model.TableMetadata{
			Schema: schema,
			Name:   tName,
		})
	}

	// For each table, query columns and primary keys
	for i := range tables {
		tName := tables[i].Name

		// Check primary keys
		pkMap := make(map[string]bool)
		pkQuery := `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			  AND tc.table_schema = kcu.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_schema = $1
			  AND tc.table_name = $2;
		`
		pkRows, err := p.pool.Query(ctx, pkQuery, schema, tName)
		if err == nil {
			for pkRows.Next() {
				var col string
				if err := pkRows.Scan(&col); err == nil {
					pkMap[col] = true
				}
			}
			pkRows.Close()
		}

		// Query columns
		colQuery := `
			SELECT column_name, data_type, is_nullable
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position;
		`
		cRows, err := p.pool.Query(ctx, colQuery, schema, tName)
		if err != nil {
			return nil, err
		}

		var columns []model.ColumnMetadata
		hasCursor := false
		suggestedCursor := ""

		for cRows.Next() {
			var colName, dataType, isNullStr string
			if err := cRows.Scan(&colName, &dataType, &isNullStr); err != nil {
				cRows.Close()
				return nil, err
			}

			isPK := pkMap[colName]
			isSens, maskType := discovery.DetectSensitivity(colName, dataType)

			if !hasCursor && discovery.DetectCursorColumn(colName, dataType) {
				hasCursor = true
				suggestedCursor = colName
			}

			columns = append(columns, model.ColumnMetadata{
				Name:             colName,
				DataType:         dataType,
				IsNullable:       (isNullStr == "YES"),
				IsPrimaryKey:     isPK,
				IsSensitive:      isSens,
				SuggestedMasking: maskType,
			})
		}
		cRows.Close()

		// Fallback cursor to PK if no timestamp column
		if !hasCursor {
			for _, col := range columns {
				if col.IsPrimaryKey {
					hasCursor = true
					suggestedCursor = col.Name
					break
				}
			}
		}

		tables[i].Columns = columns
		tables[i].HasCursorCol = hasCursor
		tables[i].SuggestedCursor = suggestedCursor
	}

	return tables, nil
}

func (p *PostgresAdapter) GenerateDDL(plan model.ProvisionPlan) (string, error) {
	if plan.SyncStrategy == "" {
		plan.SyncStrategy = model.SyncStrategyStandard
	}
	if plan.ReaderUsername == "" {
		plan.ReaderUsername = "airbyte_reader"
	}
	if plan.StatementTimeoutSec <= 0 {
		plan.StatementTimeoutSec = 30
	}
	if plan.MaxSlotWALKeepSizeGB <= 0 {
		plan.MaxSlotWALKeepSizeGB = 20
	}
	tenantClean := sanitizeIdent(plan.TenantID)
	if plan.PublicationName == "" {
		plan.PublicationName = fmt.Sprintf("airbyte_pub_%s", tenantClean)
	} else {
		plan.PublicationName = sanitizeIdent(plan.PublicationName)
	}
	if plan.ReplicationSlot == "" {
		plan.ReplicationSlot = fmt.Sprintf("airbyte_slot_%s", tenantClean)
	} else {
		plan.ReplicationSlot = sanitizeSlotName(plan.ReplicationSlot)
	}

	if plan.SyncStrategy == model.SyncStrategyCDC {
		return p.generateCDCDDL(plan)
	}

	return p.generateStandardDDL(plan)
}

func (p *PostgresAdapter) generateCDCDDL(plan model.ProvisionPlan) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- Controlled CDC Provisioning Script generated for PostgreSQL\n")
	sb.WriteString("-- Ingestion Mode            : Controlled CDC (Logical Replication)\n")
	sb.WriteString(fmt.Sprintf("-- Publication Name          : %s\n", plan.PublicationName))
	sb.WriteString(fmt.Sprintf("-- Restricted CDC User       : %s\n", plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("-- WAL Max Keep Size         : %d GB (Disk Bloat Protection)\n", plan.MaxSlotWALKeepSizeGB))
	sb.WriteString(fmt.Sprintf("-- Statement Timeout         : %d seconds\n", plan.StatementTimeoutSec))
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Create or Update Restricted User with REPLICATION role
	sb.WriteString("-- 1. Create Restricted CDC Reader User with REPLICATION Role\n")
	escapedPassword := strings.ReplaceAll(plan.ReaderPassword, "'", "''")
	sb.WriteString("DO $$\nBEGIN\n")
	sb.WriteString(fmt.Sprintf("  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN\n", plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("    CREATE ROLE %s WITH LOGIN REPLICATION PASSWORD '%s';\n", plan.ReaderUsername, escapedPassword))
	sb.WriteString("  ELSE\n")
	sb.WriteString(fmt.Sprintf("    ALTER ROLE %s WITH LOGIN REPLICATION PASSWORD '%s';\n", plan.ReaderUsername, escapedPassword))
	sb.WriteString("  END IF;\n")
	sb.WriteString("END\n$$;\n\n")

	// 2. Grant USAGE and SELECT exclusively on chosen physical tables
	sb.WriteString("-- 2. Grant Least Privileges: USAGE and SELECT exclusively on chosen physical tables\n")
	schemaMap := make(map[string][]string)
	for _, v := range plan.Views {
		schema := v.SourceSchema
		if schema == "" {
			schema = "public"
		}
		schemaMap[schema] = append(schemaMap[schema], v.SourceTable)
	}

	var allQualifiedTables []string
	for schema, tables := range schemaMap {
		sb.WriteString(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;\n", schema, plan.ReaderUsername))
		for _, tbl := range tables {
			qualified := fmt.Sprintf("%s.%s", schema, tbl)
			allQualifiedTables = append(allQualifiedTables, qualified)
			sb.WriteString(fmt.Sprintf("GRANT SELECT ON TABLE %s TO %s;\n", qualified, plan.ReaderUsername))
		}
	}
	sb.WriteString("\n")

	// 3. Strict Least Privilege Guardrails: Zero-Write (No INSERT/UPDATE/DELETE/CREATE)
	sb.WriteString("-- 3. Guardrails: Zero-Write Mode (Revoke modifications & enforce read-only)\n")
	for schema := range schemaMap {
		sb.WriteString(fmt.Sprintf("REVOKE CREATE ON SCHEMA %s FROM %s;\n", schema, plan.ReaderUsername))
	}
	sb.WriteString(fmt.Sprintf("ALTER ROLE %s SET statement_timeout = '%ds';\n", plan.ReaderUsername, plan.StatementTimeoutSec))
	sb.WriteString(fmt.Sprintf("ALTER ROLE %s SET default_transaction_read_only = on;\n\n", plan.ReaderUsername))

	// 4. Create Publication for chosen tables
	if len(allQualifiedTables) > 0 {
		tableList := strings.Join(allQualifiedTables, ", ")
		sb.WriteString("-- 4. Create Publication for Logical Replication Stream\n")
		sb.WriteString("DO $$\nBEGIN\n")
		sb.WriteString(fmt.Sprintf("  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = '%s') THEN\n", plan.PublicationName))
		sb.WriteString(fmt.Sprintf("    CREATE PUBLICATION %s FOR TABLE %s;\n", plan.PublicationName, tableList))
		sb.WriteString("  ELSE\n")
		sb.WriteString(fmt.Sprintf("    ALTER PUBLICATION %s SET TABLE %s;\n", plan.PublicationName, tableList))
		sb.WriteString("  END IF;\n")
		sb.WriteString("END\n$$;\n\n")
	}

	// 5. Create Logical Replication Slot for Airbyte CDC (pgoutput plugin)
	sb.WriteString("-- 5. Create Logical Replication Slot for Airbyte CDC (pgoutput plugin)\n")
	sb.WriteString("DO $$\n")
	sb.WriteString("DECLARE\n")
	sb.WriteString("  v_wal_level text;\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("  SELECT setting INTO v_wal_level FROM pg_settings WHERE name = 'wal_level';\n")
	sb.WriteString("  IF v_wal_level != 'logical' THEN\n")
	sb.WriteString(fmt.Sprintf("    RAISE WARNING 'wal_level is currently \"%%\", but logical decoding requires \"logical\". Set wal_level = logical and restart PostgreSQL before connecting Airbyte.', v_wal_level;\n"))
	sb.WriteString("  ELSE\n")
	sb.WriteString(fmt.Sprintf("    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s') THEN\n", plan.ReplicationSlot))
	sb.WriteString(fmt.Sprintf("      PERFORM pg_create_logical_replication_slot('%s', 'pgoutput');\n", plan.ReplicationSlot))
	sb.WriteString("    END IF;\n")
	sb.WriteString("  END IF;\n")
	sb.WriteString("END\n$$;\n\n")

	// 6. Safety Guardrail: Set max_slot_wal_keep_size
	sb.WriteString("-- 6. Safety Guardrail: Prevent Unbounded WAL Accumulation (Protects DB Disk)\n")
	sb.WriteString(fmt.Sprintf("ALTER SYSTEM SET max_slot_wal_keep_size = '%dGB';\n", plan.MaxSlotWALKeepSizeGB))
	sb.WriteString("SELECT pg_reload_conf();\n")

	return sb.String(), nil
}

func (p *PostgresAdapter) generateStandardDDL(plan model.ProvisionPlan) (string, error) {
	if plan.IntegrationSchema == "" {
		plan.IntegrationSchema = "airbyte_vault"
	}

	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- Standalone Provisioning Script generated for PostgreSQL\n")
	sb.WriteString("-- Ingestion Mode            : Standard Incremental (Flat Views + Cursor)\n")
	sb.WriteString(fmt.Sprintf("-- Target Integration Schema : %s\n", plan.IntegrationSchema))
	sb.WriteString(fmt.Sprintf("-- Restricted User           : %s\n", plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("-- Statement Timeout         : %d seconds\n", plan.StatementTimeoutSec))
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Create Schema
	sb.WriteString("-- 1. Create Integration Schema for View Isolation\n")
	sb.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n\n", plan.IntegrationSchema))

	// 2. Create Flat Views with Column Selection & Masking
	sb.WriteString("-- 2. Create Flat Views (1 View = 1 Table, Masking PII, Zero Joins)\n")
	for _, v := range plan.Views {
		var colExpressions []string
		for _, col := range v.Columns {
			switch col.Masking {
			case model.MaskExclude:
				// Skip column completely
				continue
			case model.MaskMD5:
				colExpressions = append(colExpressions, fmt.Sprintf("MD5(%s::text) AS %s", col.ColumnName, col.ColumnName))
			case model.MaskEmail:
				expr := fmt.Sprintf("CASE WHEN %s LIKE '%%@%%' THEN CONCAT(SUBSTRING(%s FROM 1 FOR 2), '***@', SPLIT_PART(%s, '@', 2)) ELSE '***@masked.com' END AS %s",
					col.ColumnName, col.ColumnName, col.ColumnName, col.ColumnName)
				colExpressions = append(colExpressions, expr)
			case model.MaskPhone:
				expr := fmt.Sprintf("CASE WHEN LENGTH(%s) >= 4 THEN CONCAT('***-***-', RIGHT(%s, 4)) ELSE '***' END AS %s",
					col.ColumnName, col.ColumnName, col.ColumnName)
				colExpressions = append(colExpressions, expr)
			case model.MaskCard:
				expr := fmt.Sprintf("CONCAT('****-****-****-', RIGHT(%s, 4)) AS %s", col.ColumnName, col.ColumnName)
				colExpressions = append(colExpressions, expr)
			case model.MaskNull:
				colExpressions = append(colExpressions, fmt.Sprintf("NULL::text AS %s", col.ColumnName))
			default:
				colExpressions = append(colExpressions, col.ColumnName)
			}
		}

		if len(colExpressions) == 0 {
			colExpressions = append(colExpressions, "1 AS placeholder")
		}

		sourceSchema := v.SourceSchema
		if sourceSchema == "" {
			sourceSchema = "public"
		}
		targetView := v.TargetView
		if targetView == "" {
			targetView = "v_" + v.SourceTable
		}

		sb.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS %s.%s CASCADE;\n", plan.IntegrationSchema, targetView))
		sb.WriteString(fmt.Sprintf("CREATE VIEW %s.%s AS\nSELECT\n  %s\nFROM %s.%s;\n\n",
			plan.IntegrationSchema, targetView,
			strings.Join(colExpressions, ",\n  "),
			sourceSchema, v.SourceTable,
		))
	}

	// 3. Create or Update Restricted User
	sb.WriteString("-- 3. Create Restricted Reader User with High-Entropy Password\n")
	escapedPassword := strings.ReplaceAll(plan.ReaderPassword, "'", "''")
	sb.WriteString("DO $$\nBEGIN\n")
	sb.WriteString(fmt.Sprintf("  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN\n", plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("    CREATE ROLE %s WITH LOGIN PASSWORD '%s';\n", plan.ReaderUsername, escapedPassword))
	sb.WriteString("  ELSE\n")
	sb.WriteString(fmt.Sprintf("    ALTER ROLE %s WITH PASSWORD '%s';\n", plan.ReaderUsername, escapedPassword))
	sb.WriteString("  END IF;\n")
	sb.WriteString("END\n$$;\n\n")

	// 4. Grant Least Privileges (Grant SELECT only on Views in Integration Schema)
	sb.WriteString("-- 4. Grant Least Privileges: USAGE and SELECT exclusively on Integration Views\n")
	sb.WriteString(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;\n", plan.IntegrationSchema, plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;\n", plan.IntegrationSchema, plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO %s;\n\n", plan.IntegrationSchema, plan.ReaderUsername))

	// 5. Explicitly Revoke Base Table Access and Enforce Safety Limits
	sb.WriteString("-- 5. Guardrails: Block direct table access and enforce statement timeout\n")
	sb.WriteString(fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM %s;\n", plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("REVOKE CREATE ON SCHEMA %s FROM %s;\n", plan.IntegrationSchema, plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("ALTER ROLE %s SET statement_timeout = '%ds';\n", plan.ReaderUsername, plan.StatementTimeoutSec))
	sb.WriteString(fmt.Sprintf("ALTER ROLE %s SET default_transaction_read_only = on;\n", plan.ReaderUsername))

	return sb.String(), nil
}

// GenerateAuditSQL creates diagnostic queries for the DBA to inspect objects created by the tool
func (p *PostgresAdapter) GenerateAuditSQL(plan model.ProvisionPlan) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- POSTGRESQL DBA AUDIT & VERIFICATION SUITE\n")
	sb.WriteString(fmt.Sprintf("-- Mode: %s | Target User: %s\n", plan.SyncStrategy, plan.ReaderUsername))
	sb.WriteString("-- Run these queries as Database Administrator to inspect all changes.\n")
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Audit User Role & Capabilities
	sb.WriteString("-- 1. AUDIT USER ROLE & REPLICATION PRIVILEGE\n")
	sb.WriteString(fmt.Sprintf("SELECT rolname, rolcanlogin, rolreplication, rolsuper, rolinherit\nFROM pg_roles\nWHERE rolname = '%s';\n\n", plan.ReaderUsername))

	// 2. Audit Table Permissions
	sb.WriteString("-- 2. AUDIT TABLE PERMISSIONS (Verify SELECT only, Zero-Write)\n")
	sb.WriteString(fmt.Sprintf("SELECT grantee, table_schema, table_name, privilege_type\nFROM information_schema.role_table_grants\nWHERE grantee = '%s'\nORDER BY table_schema, table_name, privilege_type;\n\n", plan.ReaderUsername))

	// 3. Test privilege checks explicitly
	sb.WriteString("-- 3. AUDIT INDIVIDUAL TABLE ACCESS RIGHTS FOR READER USER\n")
	for _, v := range plan.Views {
		schema := v.SourceSchema
		if schema == "" {
			schema = "public"
		}
		sb.WriteString(fmt.Sprintf("SELECT '%s.%s' AS table_name,\n       has_table_privilege('%s', '%s.%s', 'SELECT') AS can_select,\n       has_table_privilege('%s', '%s.%s', 'INSERT') AS can_insert,\n       has_table_privilege('%s', '%s.%s', 'UPDATE') AS can_update,\n       has_table_privilege('%s', '%s.%s', 'DELETE') AS can_delete;\n",
			schema, v.SourceTable, plan.ReaderUsername, schema, v.SourceTable,
			plan.ReaderUsername, schema, v.SourceTable, plan.ReaderUsername, schema, v.SourceTable, plan.ReaderUsername, schema, v.SourceTable))
	}
	sb.WriteString("\n")

	if plan.SyncStrategy == model.SyncStrategyCDC {
		// 4. Audit Publication
		sb.WriteString("-- 4. AUDIT PUBLICATION (Check CDC Replication Scope)\n")
		sb.WriteString(fmt.Sprintf("SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate\nFROM pg_publication\nWHERE pubname = '%s';\n\n", plan.PublicationName))

		sb.WriteString("-- 5. AUDIT TABLES INCLUDED IN CDC PUBLICATION\n")
		sb.WriteString(fmt.Sprintf("SELECT pubname, schemaname, tablename\nFROM pg_publication_tables\nWHERE pubname = '%s';\n\n", plan.PublicationName))

		// 6. Audit WAL & System Guardrails
		sb.WriteString("-- 6. AUDIT WAL SETTINGS & SAFETY GUARDRAILS\n")
		sb.WriteString("SELECT name, setting, unit, short_desc\nFROM pg_settings\nWHERE name IN ('wal_level', 'max_slot_wal_keep_size', 'max_replication_slots', 'max_wal_senders');\n\n")

		// 7. Audit Replication Slots
		sb.WriteString("-- 7. AUDIT ACTIVE REPLICATION SLOTS (Inspect Airbyte connection & WAL lag)\n")
		sb.WriteString(fmt.Sprintf("SELECT slot_name, plugin, slot_type, active, database, wal_status\nFROM pg_replication_slots\nWHERE slot_name = '%s';\n", plan.ReplicationSlot))
	} else {
		// Standard Views
		sb.WriteString("-- 4. AUDIT CREATED ISOLATION SCHEMA & FLAT VIEWS\n")
		sb.WriteString(fmt.Sprintf("SELECT table_schema, table_name, view_definition\nFROM information_schema.views\nWHERE table_schema = '%s'\nORDER BY table_name;\n\n", plan.IntegrationSchema))

		sb.WriteString("-- 5. CONFIRM DIRECT ACCESS TO BASE TABLES IS DENIED\n")
		sb.WriteString(fmt.Sprintf("SELECT has_schema_privilege('%s', 'public', 'CREATE') AS can_create_public,\n       has_table_privilege('%s', 'public.customers', 'SELECT') AS can_read_base_customers;\n", plan.ReaderUsername, plan.ReaderUsername))
	}

	return sb.String(), nil
}

func (p *PostgresAdapter) ApplyPlan(ctx context.Context, sqlScript string) error {
	if p.pool == nil {
		return fmt.Errorf("database connection is not open")
	}

	statements := SplitPostgresStatements(sqlScript)
	for _, stmt := range statements {
		stmtTrimmed := strings.TrimSpace(stmt)
		if stmtTrimmed == "" {
			continue
		}

		// Check if statement contains actual executable SQL (not just comments)
		lines := strings.Split(stmtTrimmed, "\n")
		hasExecutableCode := false
		for _, line := range lines {
			l := strings.TrimSpace(line)
			if l != "" && !strings.HasPrefix(l, "--") {
				hasExecutableCode = true
				break
			}
		}
		if !hasExecutableCode {
			continue
		}

		_, err := p.pool.Exec(ctx, stmtTrimmed)
		if err != nil {
			// On managed cloud databases (e.g. AWS RDS/Aurora, GCP Cloud SQL) or environments
			// without superuser rights, ALTER SYSTEM or pg_reload_conf may fail with permission errors.
			// These guardrails are best-effort system settings, so we skip gracefully
			// rather than failing the core reader user and publication provisioning.
			upperStmt := strings.ToUpper(stmtTrimmed)
			if strings.Contains(upperStmt, "ALTER SYSTEM") || strings.Contains(upperStmt, "PG_RELOAD_CONF") {
				continue
			}
			return fmt.Errorf("error executing statement on postgres:\n%s\nError: %w", stmtTrimmed, err)
		}
	}
	return nil
}

// SplitPostgresStatements splits a PostgreSQL script into individual executable statements,
// taking into account single-line comments (--), block comments (/* */),
// string literals ('...'), and dollar quotes ($$...$$ or $<tag>$...$<tag>$).
func SplitPostgresStatements(script string) []string {
	var statements []string
	var cur strings.Builder
	inSingleQuote := false
	inLineComment := false
	inBlockComment := false
	dollarTag := ""

	runes := []rune(script)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]

		// 1. Line comments (-- ...)
		if dollarTag == "" && !inSingleQuote && !inBlockComment {
			if r == '-' && i+1 < n && runes[i+1] == '-' {
				inLineComment = true
			}
		}
		if inLineComment {
			if r == '\n' {
				inLineComment = false
			}
			cur.WriteRune(r)
			continue
		}

		// 2. Block comments (/* ... */)
		if dollarTag == "" && !inSingleQuote && !inLineComment {
			if r == '/' && i+1 < n && runes[i+1] == '*' {
				inBlockComment = true
			}
		}
		if inBlockComment {
			if r == '*' && i+1 < n && runes[i+1] == '/' {
				cur.WriteRune(r)
				cur.WriteRune(runes[i+1])
				i++
				inBlockComment = false
				continue
			}
			cur.WriteRune(r)
			continue
		}

		// 3. Dollar quote handling ($tag$...$tag$)
		if !inSingleQuote {
			if dollarTag == "" {
				// Look for opening dollar tag: $ [a-zA-Z0-9_]* $
				if r == '$' {
					j := i + 1
					for j < n && ((runes[j] >= 'a' && runes[j] <= 'z') || (runes[j] >= 'A' && runes[j] <= 'Z') || (runes[j] >= '0' && runes[j] <= '9') || runes[j] == '_') {
						j++
					}
					if j < n && runes[j] == '$' {
						dollarTag = string(runes[i : j+1])
						for k := i; k <= j; k++ {
							cur.WriteRune(runes[k])
						}
						i = j
						continue
					}
				}
			} else {
				// Check for closing dollar tag
				tagRunes := []rune(dollarTag)
				tagLen := len(tagRunes)
				if i+tagLen <= n && string(runes[i:i+tagLen]) == dollarTag {
					for k := 0; k < tagLen; k++ {
						cur.WriteRune(runes[i+k])
					}
					i += tagLen - 1
					dollarTag = ""
					continue
				}
			}
		}

		// 4. String literals ('...')
		if dollarTag == "" {
			if r == '\'' {
				// Escaped single quote: ''
				if inSingleQuote && i+1 < n && runes[i+1] == '\'' {
					cur.WriteRune(r)
					cur.WriteRune(runes[i+1])
					i++
					continue
				}
				inSingleQuote = !inSingleQuote
			}
		}

		// 5. Statement delimiter
		if r == ';' && dollarTag == "" && !inSingleQuote {
			stmt := strings.TrimSpace(cur.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			cur.Reset()
			continue
		}

		cur.WriteRune(r)
	}

	if stmt := strings.TrimSpace(cur.String()); stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

func (p *PostgresAdapter) VerifyReader(ctx context.Context, cfg model.DBConfig, plan model.ProvisionPlan, sampleTarget string) (string, error) {
	testCfg := cfg
	testCfg.Username = plan.ReaderUsername
	testCfg.Password = plan.ReaderPassword

	connStr := p.buildConnString(testCfg)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return "", fmt.Errorf("failed to authenticate with newly created reader user '%s': %w", plan.ReaderUsername, err)
	}
	defer conn.Close(ctx)

	var sb strings.Builder

	if plan.SyncStrategy == model.SyncStrategyCDC {
		// CDC Verification:
		// 1. Check REPLICATION privilege on pg_roles
		var hasReplication bool
		roleQuery := fmt.Sprintf("SELECT rolreplication FROM pg_roles WHERE rolname = '%s'", plan.ReaderUsername)
		_ = conn.QueryRow(ctx, roleQuery).Scan(&hasReplication)

		// 2. Query authorized physical table
		var count int64
		tableQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", sampleTarget)
		if err := conn.QueryRow(ctx, tableQuery).Scan(&count); err != nil {
			return "", fmt.Errorf("CDC reader user failed to read authorized table (%s): %w", sampleTarget, err)
		}

		sb.WriteString(fmt.Sprintf("✅ Verified read access on physical table '%s': returned %d rows.\n", sampleTarget, count))
		if hasReplication {
			sb.WriteString(fmt.Sprintf("🛡️ REPLICATION Attribute Confirmed: User '%s' has valid replication streaming rights.\n", plan.ReaderUsername))
		} else {
			sb.WriteString(fmt.Sprintf("⚠️ Warning: User '%s' is missing the REPLICATION role attribute.\n", plan.ReaderUsername))
		}

		// 3. Test that write operation is blocked
		blockedInsert := fmt.Sprintf("INSERT INTO %s DEFAULT VALUES;", sampleTarget)
		_, errInsert := conn.Exec(ctx, blockedInsert)
		if errInsert != nil && (strings.Contains(strings.ToLower(errInsert.Error()), "permission denied") || strings.Contains(strings.ToLower(errInsert.Error()), "read-only")) {
			sb.WriteString("🛡️ Zero-Write Confirmed: Write attempt was rejected with 'Permission Denied / Read-Only' as expected.\n")
		} else {
			sb.WriteString("⚠️ Warning: Write operation restriction test did not return expected error.\n")
		}
	} else {
		// Standard View Verification:
		var count int64
		viewQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", sampleTarget)
		if err := conn.QueryRow(ctx, viewQuery).Scan(&count); err != nil {
			return "", fmt.Errorf("reader user failed to read authorized view (%s): %w", sampleTarget, err)
		}

		blockedQuery := "SELECT * FROM public.customers LIMIT 1;"
		var dummy int
		errBlocked := conn.QueryRow(ctx, blockedQuery).Scan(&dummy)
		isBlocked := (errBlocked != nil && strings.Contains(strings.ToLower(errBlocked.Error()), "permission denied"))

		sb.WriteString(fmt.Sprintf("✅ Verified read access on '%s': returned %d rows.\n", sampleTarget, count))
		if isBlocked {
			sb.WriteString("🛡️ Isolation Confirmed: Direct read attempt on 'public.customers' was rejected with 'Permission Denied' as expected.\n")
		} else {
			sb.WriteString("⚠️ Warning: Direct read on base table was not strictly denied. Please review schema privileges.\n")
		}
	}

	return sb.String(), nil
}

func sanitizeSlotName(name string) string {
	name = strings.ToLower(name)
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	res := sb.String()
	for strings.Contains(res, "__") {
		res = strings.ReplaceAll(res, "__", "_")
	}
	res = strings.Trim(res, "_")
	if res == "" {
		res = "airbyte_slot_default"
	}
	if len(res) > 63 {
		res = res[:63]
	}
	return res
}

func sanitizeIdent(name string) string {
	name = strings.ToLower(name)
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	res := sb.String()
	for strings.Contains(res, "__") {
		res = strings.ReplaceAll(res, "__", "_")
	}
	res = strings.Trim(res, "_")
	if res == "" {
		res = "default"
	}
	if len(res) > 63 {
		res = res[:63]
	}
	return res
}

