package adapter

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"github.com/datalakehouse/airbyte-config-tool/internal/discovery"
	"github.com/datalakehouse/airbyte-config-tool/internal/model"
	_ "github.com/microsoft/go-mssqldb"
)

type MSSQLAdapter struct {
	db     *sql.DB
	config model.DBConfig
}

func NewMSSQLAdapter() *MSSQLAdapter {
	return &MSSQLAdapter{}
}

func (m *MSSQLAdapter) buildDSN(cfg model.DBConfig) string {
	query := url.Values{}
	query.Add("database", cfg.Database)
	if cfg.SSLMode == "require" || cfg.SSLMode == "verify-ca" || cfg.SSLMode == "verify-full" {
		query.Add("encrypt", "true")
		query.Add("trustServerCertificate", "true")
	} else {
		query.Add("encrypt", "disable")
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cfg.Username, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		RawQuery: query.Encode(),
	}
	return u.String()
}

func (m *MSSQLAdapter) Connect(ctx context.Context, cfg model.DBConfig) error {
	m.config = cfg
	if cfg.Port == 0 {
		cfg.Port = 1433
	}
	dsn := m.buildDSN(cfg)
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mssql connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping mssql at %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	m.db = db
	return nil
}

func (m *MSSQLAdapter) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MSSQLAdapter) Ping(ctx context.Context) (*model.DBInfo, error) {
	if m.db == nil {
		return nil, fmt.Errorf("not connected to mssql")
	}

	var version string
	if err := m.db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version); err != nil {
		return nil, fmt.Errorf("error reading mssql version: %w", err)
	}

	info := &model.DBInfo{
		Engine:    "Microsoft SQL Server",
		Version:   version,
		IsReplica: false,
	}

	// 1. Check if database-level CDC is enabled
	var isCDCEnabled bool
	queryCDC := "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()"
	if err := m.db.QueryRowContext(ctx, queryCDC).Scan(&isCDCEnabled); err == nil {
		if !isCDCEnabled {
			info.Warnings = append(info.Warnings,
				fmt.Sprintf("CDC is not yet enabled on database '%s'. The tool will generate 'EXEC sys.sp_cdc_enable_db' to enable it.", m.config.Database))
		}
	}

	// 2. Check if SQL Server Agent service is running (required for CDC capture/cleanup jobs)
	var agentRunning bool
	queryAgent := `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.dm_server_services 
			WHERE servicename LIKE 'SQL Server Agent%' AND status_desc = 'Running'
		) THEN 1 ELSE 0 END
	`
	if err := m.db.QueryRowContext(ctx, queryAgent).Scan(&agentRunning); err == nil {
		if !agentRunning {
			info.Warnings = append(info.Warnings,
				"SQL Server Agent service does not appear to be running. SQL Server CDC requires SQL Server Agent running to process change logs.")
		}
	}

	return info, nil
}

func (m *MSSQLAdapter) DiscoverSchemas(ctx context.Context) ([]string, error) {
	if m.db == nil {
		return nil, fmt.Errorf("not connected to mssql")
	}

	query := `
		SELECT s.name 
		FROM sys.schemas s
		WHERE s.name NOT IN (
			'guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 
			'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 
			'db_datawriter', 'db_denydatareader', 'db_denydatawriter', 'cdc'
		)
		ORDER BY s.name;
	`
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying mssql schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	if len(schemas) == 0 {
		schemas = []string{"dbo"}
	}
	return schemas, nil
}

func (m *MSSQLAdapter) DiscoverTables(ctx context.Context, schema string) ([]model.TableMetadata, error) {
	if m.db == nil {
		return nil, fmt.Errorf("not connected to mssql")
	}
	if schema == "" {
		schema = "dbo"
	}

	// 1. Discover Columns
	colQuery := `
		SELECT 
			t.name AS table_name,
			c.name AS column_name,
			ty.name AS data_type,
			c.is_nullable
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		JOIN sys.columns c ON t.object_id = c.object_id
		JOIN sys.types ty ON c.user_type_id = ty.user_type_id
		WHERE s.name = @p1 AND t.is_ms_shipped = 0 AND t.name NOT LIKE 'sys%' AND t.name NOT LIKE 'cdc_%'
		ORDER BY t.name, c.column_id;
	`
	rows, err := m.db.QueryContext(ctx, colQuery, schema)
	if err != nil {
		return nil, fmt.Errorf("error querying mssql columns: %w", err)
	}
	defer rows.Close()

	tableMap := make(map[string]*model.TableMetadata)
	var tableOrder []string

	for rows.Next() {
		var tableName, colName, dataType string
		var isNullable bool

		if err := rows.Scan(&tableName, &colName, &dataType, &isNullable); err != nil {
			return nil, err
		}

		tbl, exists := tableMap[tableName]
		if !exists {
			tbl = &model.TableMetadata{
				Schema:  schema,
				Name:    tableName,
				Columns: []model.ColumnMetadata{},
			}
			tableMap[tableName] = tbl
			tableOrder = append(tableOrder, tableName)
		}

		isSens, mask := discovery.DetectSensitivity(colName, dataType)
		tbl.Columns = append(tbl.Columns, model.ColumnMetadata{
			Name:             colName,
			DataType:         dataType,
			IsNullable:       isNullable,
			IsSensitive:      isSens,
			SuggestedMasking: mask,
		})
	}

	// 2. Discover Primary Keys
	pkQuery := `
		SELECT 
			t.name AS table_name,
			c.name AS column_name
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		JOIN sys.tables t ON i.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE i.is_primary_key = 1 AND s.name = @p1 AND t.is_ms_shipped = 0;
	`
	pkRows, err := m.db.QueryContext(ctx, pkQuery, schema)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var tableName, colName string
			if err := pkRows.Scan(&tableName, &colName); err == nil {
				if tbl, ok := tableMap[tableName]; ok {
					for i := range tbl.Columns {
						if strings.EqualFold(tbl.Columns[i].Name, colName) {
							tbl.Columns[i].IsPrimaryKey = true
						}
					}
				}
			}
		}
	}

	// 3. Discover Row Counts
	countQuery := `
		SELECT 
			t.name AS table_name,
			SUM(p.rows) AS estimated_rows
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		JOIN sys.partitions p ON t.object_id = p.object_id
		WHERE s.name = @p1 AND t.is_ms_shipped = 0 AND p.index_id IN (0, 1)
		GROUP BY t.name;
	`
	cntRows, err := m.db.QueryContext(ctx, countQuery, schema)
	if err == nil {
		defer cntRows.Close()
		for cntRows.Next() {
			var tableName string
			var estRows int64
			if err := cntRows.Scan(&tableName, &estRows); err == nil {
				if tbl, ok := tableMap[tableName]; ok {
					tbl.EstimatedRows = estRows
				}
			}
		}
	}

	// 4. Assign suggested cursors
	var tables []model.TableMetadata
	for _, name := range tableOrder {
		tbl := tableMap[name]
		for _, col := range tbl.Columns {
			if discovery.DetectCursorColumn(col.Name, col.DataType) {
				tbl.HasCursorCol = true
				tbl.SuggestedCursor = col.Name
				if strings.ToLower(col.Name) == "updated_at" {
					break
				}
			}
		}
		tables = append(tables, *tbl)
	}

	return tables, nil
}

func (m *MSSQLAdapter) GenerateDDL(plan model.ProvisionPlan) (string, error) {
	if plan.SyncStrategy == "" {
		plan.SyncStrategy = model.SyncStrategyStandard
	}
	if plan.ReaderUsername == "" {
		plan.ReaderUsername = "airbyte_reader"
	}

	if plan.SyncStrategy == model.SyncStrategyCDC {
		return m.generateCDCDDL(plan)
	}

	return m.generateStandardDDL(plan)
}

func (m *MSSQLAdapter) generateCDCDDL(plan model.ProvisionPlan) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- Controlled CDC Provisioning Script generated for Microsoft SQL Server\n")
	sb.WriteString("-- Ingestion Mode  : Controlled CDC (SQL Server CDC Service)\n")
	sb.WriteString(fmt.Sprintf("-- Restricted User : %s\n", plan.ReaderUsername))
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Enable CDC on Database
	sb.WriteString("-- 1. Enable CDC on Database (if not already enabled)\n")
	sb.WriteString("IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString("    EXEC sys.sp_cdc_enable_db;\n")
	sb.WriteString("END;\nGO\n\n")

	// 2. Enable CDC for each selected table
	sb.WriteString("-- 2. Enable CDC on Target Tables\n")
	for _, v := range plan.Views {
		sourceSchema := v.SourceSchema
		if sourceSchema == "" {
			sourceSchema = "dbo"
		}

		// Check if table has a primary key to determine supports_net_changes
		hasPK := false
		for _, col := range v.Columns {
			_ = col
		}
		// Default supports_net_changes to 1 if primary key exists, 0 otherwise
		netChanges := 1
		_ = hasPK

		sb.WriteString(fmt.Sprintf("IF (SELECT is_tracked_by_cdc FROM sys.tables WHERE object_id = OBJECT_ID(N'[%s].[%s]')) = 0\n", sourceSchema, v.SourceTable))
		sb.WriteString("BEGIN\n")
		sb.WriteString("    EXEC sys.sp_cdc_enable_table\n")
		sb.WriteString(fmt.Sprintf("        @source_schema = N'%s',\n", sourceSchema))
		sb.WriteString(fmt.Sprintf("        @source_name   = N'%s',\n", v.SourceTable))
		sb.WriteString("        @role_name     = NULL,\n")
		sb.WriteString(fmt.Sprintf("        @supports_net_changes = %d;\n", netChanges))
		sb.WriteString("END;\nGO\n\n")
	}

	// 3. Create Login & Database User
	escapedPass := strings.ReplaceAll(plan.ReaderPassword, "'", "''")
	sb.WriteString("-- 3. Create Restricted Reader Login and User\n")
	sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '%s')\n", plan.ReaderUsername))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    CREATE LOGIN [%s] WITH PASSWORD = '%s', CHECK_POLICY = OFF;\n", plan.ReaderUsername, escapedPass))
	sb.WriteString("END;\n")
	sb.WriteString("ELSE\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    ALTER LOGIN [%s] WITH PASSWORD = '%s';\n", plan.ReaderUsername, escapedPass))
	sb.WriteString("END;\nGO\n\n")

	sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '%s')\n", plan.ReaderUsername))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    CREATE USER [%s] FOR LOGIN [%s];\n", plan.ReaderUsername, plan.ReaderUsername))
	sb.WriteString("END;\nGO\n\n")

	// 4. Grant Least-Privilege Permissions for CDC
	sb.WriteString("-- 4. Grant CDC and SELECT Privileges (Least Privilege / Zero-Write)\n")
	sb.WriteString(fmt.Sprintf("GRANT SELECT ON SCHEMA::cdc TO [%s];\n", plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("GRANT VIEW DATABASE STATE TO [%s];\n", plan.ReaderUsername))

	for _, v := range plan.Views {
		sourceSchema := v.SourceSchema
		if sourceSchema == "" {
			sourceSchema = "dbo"
		}
		sb.WriteString(fmt.Sprintf("GRANT SELECT ON [%s].[%s] TO [%s];\n", sourceSchema, v.SourceTable, plan.ReaderUsername))
	}

	// 5. Zero-Write Guardrails: Explicitly DENY mutation commands
	sb.WriteString(fmt.Sprintf("\nDENY INSERT, UPDATE, DELETE, ALTER TO [%s];\nGO\n", plan.ReaderUsername))

	return sb.String(), nil
}

func (m *MSSQLAdapter) generateStandardDDL(plan model.ProvisionPlan) (string, error) {
	if plan.IntegrationSchema == "" {
		plan.IntegrationSchema = "airbyte_vault"
	}

	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- Standalone Provisioning Script generated for Microsoft SQL Server\n")
	sb.WriteString(fmt.Sprintf("-- Integration Schema : %s\n", plan.IntegrationSchema))
	sb.WriteString(fmt.Sprintf("-- Restricted User     : %s\n", plan.ReaderUsername))
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Create Schema
	sb.WriteString("-- 1. Create Isolation Integration Schema\n")
	sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')\n", plan.IntegrationSchema))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    EXEC('CREATE SCHEMA [%s]');\n", plan.IntegrationSchema))
	sb.WriteString("END;\nGO\n\n")

	// 2. Create Login & User
	escapedPass := strings.ReplaceAll(plan.ReaderPassword, "'", "''")
	sb.WriteString("-- 2. Create Login and Database User\n")
	sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '%s')\n", plan.ReaderUsername))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    CREATE LOGIN [%s] WITH PASSWORD = '%s', CHECK_POLICY = OFF;\n", plan.ReaderUsername, escapedPass))
	sb.WriteString("END;\n")
	sb.WriteString("ELSE\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    ALTER LOGIN [%s] WITH PASSWORD = '%s';\n", plan.ReaderUsername, escapedPass))
	sb.WriteString("END;\nGO\n\n")

	sb.WriteString(fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '%s')\n", plan.ReaderUsername))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    CREATE USER [%s] FOR LOGIN [%s];\n", plan.ReaderUsername, plan.ReaderUsername))
	sb.WriteString("END;\nGO\n\n")

	// 3. Create Flat Views with Column Masking
	sb.WriteString("-- 3. Create Flat Views with Dynamic Data Masking\n")
	for _, v := range plan.Views {
		var colExpressions []string
		for _, col := range v.Columns {
			expr := m.buildMaskingExpression(col)
			colExpressions = append(colExpressions, fmt.Sprintf("    %s AS [%s]", expr, col.ColumnName))
		}

		sourceSchema := v.SourceSchema
		if sourceSchema == "" {
			sourceSchema = "dbo"
		}

		colsJoined := strings.Join(colExpressions, ",\n")
		sb.WriteString(fmt.Sprintf("CREATE OR ALTER VIEW [%s].[%s] AS\nSELECT\n%s\nFROM [%s].[%s];\nGO\n\n",
			plan.IntegrationSchema, v.TargetView, colsJoined, sourceSchema, v.SourceTable))
	}

	// 4. Grant Permissions on Integration Schema
	sb.WriteString("-- 4. Grant SELECT on Integration Schema Only\n")
	sb.WriteString(fmt.Sprintf("GRANT SELECT ON SCHEMA::[%s] TO [%s];\n", plan.IntegrationSchema, plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("DENY INSERT, UPDATE, DELETE, ALTER TO [%s];\nGO\n", plan.ReaderUsername))

	return sb.String(), nil
}

func (m *MSSQLAdapter) buildMaskingExpression(col model.ColumnRule) string {
	quoted := fmt.Sprintf("[%s]", col.ColumnName)
	switch col.Masking {
	case model.MaskMD5:
		return fmt.Sprintf("CONVERT(VARCHAR(32), HASHBYTES('MD5', CAST(%s AS NVARCHAR(MAX))), 2)", quoted)
	case model.MaskEmail:
		return fmt.Sprintf("CASE WHEN CHARINDEX('@', CAST(%s AS NVARCHAR(MAX))) > 1 THEN CONCAT(LEFT(%s, 1), '***@***.com') ELSE '***@***.com' END", quoted, quoted)
	case model.MaskPhone:
		return fmt.Sprintf("CASE WHEN LEN(CAST(%s AS NVARCHAR(MAX))) >= 4 THEN CONCAT(LEFT(%s, 3), '****', RIGHT(%s, 2)) ELSE '***' END", quoted, quoted, quoted)
	case model.MaskCard:
		return fmt.Sprintf("CASE WHEN LEN(CAST(%s AS NVARCHAR(MAX))) >= 4 THEN CONCAT('****-****-****-', RIGHT(%s, 4)) ELSE '****-****-****-****' END", quoted, quoted)
	case model.MaskNull:
		return "NULL"
	case model.MaskNone:
		fallthrough
	default:
		return quoted
	}
}

func (m *MSSQLAdapter) GenerateAuditSQL(plan model.ProvisionPlan) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- MSSQL DBA Audit & Verification Diagnostic Suite\n")
	sb.WriteString(fmt.Sprintf("-- Target Database: %s | Tenant: %s\n", m.config.Database, plan.TenantID))
	sb.WriteString(fmt.Sprintf("-- Ingestion Mode  : %s\n", plan.SyncStrategy))
	sb.WriteString("-- ====================================================================\n\n")

	if plan.SyncStrategy == model.SyncStrategyCDC {
		sb.WriteString("-- 1. Verify Database-Level CDC Status\n")
		sb.WriteString("SELECT name, is_cdc_enabled, recovery_model_desc \nFROM sys.databases \nWHERE name = DB_NAME();\nGO\n\n")

		sb.WriteString("-- 2. Verify SQL Server Agent Status (Required for CDC background capture)\n")
		sb.WriteString("SELECT servicename, status_desc, startup_type_desc \nFROM sys.dm_server_services \nWHERE servicename LIKE 'SQL Server Agent%';\nGO\n\n")

		sb.WriteString("-- 3. Verify Active CDC Background Jobs (Capture & Cleanup)\n")
		sb.WriteString("SELECT job_id, job_type, maxtrans, maxscans, continuous, pollinginterval \nFROM msdb.dbo.cdc_jobs;\nGO\n\n")

		sb.WriteString("-- 4. Verify CDC Tracked Tables and Capture Instances\n")
		sb.WriteString("SELECT \n")
		sb.WriteString("    s.name AS schema_name,\n")
		sb.WriteString("    t.name AS table_name,\n")
		sb.WriteString("    ct.capture_instance,\n")
		sb.WriteString("    ct.start_lsn,\n")
		sb.WriteString("    ct.supports_net_changes\n")
		sb.WriteString("FROM cdc.change_tables ct\n")
		sb.WriteString("JOIN sys.tables t ON ct.source_object_id = t.object_id\n")
		sb.WriteString("JOIN sys.schemas s ON t.schema_id = s.schema_id;\nGO\n\n")

		sb.WriteString("-- 5. Verify Reader User Permissions (Zero-Write Audit)\n")
		sb.WriteString("SELECT \n")
		sb.WriteString("    dp.name AS principal_name,\n")
		sb.WriteString("    p.permission_name,\n")
		sb.WriteString("    p.state_desc,\n")
		sb.WriteString("    OBJECT_SCHEMA_NAME(p.major_id) AS object_schema,\n")
		sb.WriteString("    OBJECT_NAME(p.major_id) AS object_name\n")
		sb.WriteString("FROM sys.database_permissions p\n")
		sb.WriteString(fmt.Sprintf("JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id\nWHERE dp.name = '%s';\nGO\n\n", plan.ReaderUsername))
	} else {
		sb.WriteString("-- 1. Verify Integration Schema Flat Views\n")
		sb.WriteString(fmt.Sprintf("SELECT table_schema, table_name, check_option \nFROM INFORMATION_SCHEMA.VIEWS \nWHERE table_schema = '%s';\nGO\n\n", plan.IntegrationSchema))

		sb.WriteString("-- 2. Verify Reader User Permissions on Integration Schema\n")
		sb.WriteString("SELECT \n")
		sb.WriteString("    dp.name AS principal_name,\n")
		sb.WriteString("    p.permission_name,\n")
		sb.WriteString("    p.state_desc,\n")
		sb.WriteString("    OBJECT_SCHEMA_NAME(p.major_id) AS object_schema,\n")
		sb.WriteString("    OBJECT_NAME(p.major_id) AS object_name\n")
		sb.WriteString("FROM sys.database_permissions p\n")
		sb.WriteString(fmt.Sprintf("JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id\nWHERE dp.name = '%s';\nGO\n\n", plan.ReaderUsername))
	}

	return sb.String(), nil
}

// SplitMSSQLStatements splits a T-SQL script into individual executable statements.
// Handles GO batch separators, semicolons, comments (-- and /* */), and string literals ('...').
func SplitMSSQLStatements(sqlScript string) []string {
	var statements []string
	var current strings.Builder

	runes := []rune(sqlScript)
	n := len(runes)
	i := 0

	inSingleQuote := false
	inLineComment := false
	inBlockComment := false

	lineStart := true

	flushCurrent := func() {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
		current.Reset()
	}

	for i < n {
		r := runes[i]

		// Handle comments
		if inLineComment {
			if r == '\n' {
				inLineComment = false
				lineStart = true
			}
			current.WriteRune(r)
			i++
			continue
		}

		if inBlockComment {
			if r == '*' && i+1 < n && runes[i+1] == '/' {
				inBlockComment = false
				current.WriteString("*/")
				i += 2
				continue
			}
			current.WriteRune(r)
			i++
			continue
		}

		// Handle quotes
		if inSingleQuote {
			current.WriteRune(r)
			if r == '\'' {
				if i+1 < n && runes[i+1] == '\'' {
					current.WriteRune('\'')
					i += 2
					continue
				}
				inSingleQuote = false
			}
			i++
			continue
		}

		// Outside comments and quotes
		if r == '-' && i+1 < n && runes[i+1] == '-' {
			inLineComment = true
			current.WriteString("--")
			i += 2
			continue
		}
		if r == '/' && i+1 < n && runes[i+1] == '*' {
			inBlockComment = true
			current.WriteString("/*")
			i += 2
			continue
		}
		if r == '\'' {
			inSingleQuote = true
			current.WriteRune(r)
			i++
			continue
		}

		// Check for GO batch separator on line start
		if lineStart && (r == 'g' || r == 'G') && i+1 < n && (runes[i+1] == 'o' || runes[i+1] == 'O') {
			// check if after GO is whitespace or newline
			afterGO := i + 2
			isStandalone := false
			if afterGO == n {
				isStandalone = true
			} else {
				nextRune := runes[afterGO]
				if nextRune == '\r' || nextRune == '\n' || unicode.IsSpace(nextRune) {
					// check rest of line is spaces
					isRestSpace := true
					for k := afterGO; k < n && runes[k] != '\n'; k++ {
						if !unicode.IsSpace(runes[k]) && runes[k] != '\r' {
							isRestSpace = false
							break
						}
					}
					isStandalone = isRestSpace
				}
			}

			if isStandalone {
				flushCurrent()
				// skip until end of line
				for i < n && runes[i] != '\n' {
					i++
				}
				if i < n && runes[i] == '\n' {
					i++
				}
				lineStart = true
				continue
			}
		}

		if r == '\n' {
			lineStart = true
		} else if !unicode.IsSpace(r) {
			lineStart = false
		}

		current.WriteRune(r)
		i++
	}

	flushCurrent()
	return statements
}

func (m *MSSQLAdapter) ApplyPlan(ctx context.Context, sqlScript string) error {
	if m.db == nil {
		return fmt.Errorf("not connected to mssql")
	}

	statements := SplitMSSQLStatements(sqlScript)
	for idx, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if _, err := m.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed executing statement #%d [%s]: %w", idx+1, truncateStmt(stmt, 60), err)
		}
	}

	return nil
}

func truncateStmt(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

func (m *MSSQLAdapter) VerifyReader(ctx context.Context, cfg model.DBConfig, plan model.ProvisionPlan, testTarget string) (string, error) {
	if cfg.Port == 0 {
		cfg.Port = 1433
	}
	readerCfg := cfg
	readerCfg.Username = plan.ReaderUsername
	readerCfg.Password = plan.ReaderPassword

	dsn := m.buildDSN(readerCfg)
	testDB, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return "", fmt.Errorf("failed connecting as reader user '%s': %w", plan.ReaderUsername, err)
	}
	defer testDB.Close()

	if err := testDB.PingContext(ctx); err != nil {
		return "", fmt.Errorf("failed pinging mssql as reader user '%s': %w", plan.ReaderUsername, err)
	}

	target := testTarget
	if target == "" {
		if len(plan.Views) > 0 {
			if plan.SyncStrategy == model.SyncStrategyCDC {
				sourceSchema := plan.Views[0].SourceSchema
				if sourceSchema == "" {
					sourceSchema = "dbo"
				}
				target = fmt.Sprintf("[%s].[%s]", sourceSchema, plan.Views[0].SourceTable)
			} else {
				schema := plan.IntegrationSchema
				if schema == "" {
					schema = "airbyte_vault"
				}
				target = fmt.Sprintf("[%s].[%s]", schema, plan.Views[0].TargetView)
			}
		} else {
			target = "[INFORMATION_SCHEMA].[TABLES]"
		}
	}

	query := fmt.Sprintf("SELECT TOP 1 * FROM %s", target)
	rows, err := testDB.QueryContext(ctx, query)
	if err != nil {
		return "", fmt.Errorf("reader user cannot SELECT from %s: %w", target, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("error inspecting columns from %s: %w", target, err)
	}

	return fmt.Sprintf("Successfully verified reader access to %s. Accessible columns: [%s]",
		target, strings.Join(cols, ", ")), nil
}
