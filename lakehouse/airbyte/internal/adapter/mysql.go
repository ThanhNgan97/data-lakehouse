package adapter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/datalakehouse/airbyte-config-tool/internal/discovery"
	"github.com/datalakehouse/airbyte-config-tool/internal/model"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLAdapter struct {
	db     *sql.DB
	config model.DBConfig
}

func NewMySQLAdapter() *MySQLAdapter {
	return &MySQLAdapter{}
}

func (m *MySQLAdapter) buildDSN(cfg model.DBConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

func (m *MySQLAdapter) Connect(ctx context.Context, cfg model.DBConfig) error {
	m.config = cfg
	dsn := m.buildDSN(cfg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping mysql at %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	m.db = db
	return nil
}

func (m *MySQLAdapter) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MySQLAdapter) Ping(ctx context.Context) (*model.DBInfo, error) {
	if m.db == nil {
		return nil, fmt.Errorf("not connected to mysql")
	}

	var version string
	if err := m.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return nil, fmt.Errorf("error reading mysql version: %w", err)
	}

	var readOnly int
	_ = m.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
	isReplica := (readOnly == 1)

	info := &model.DBInfo{
		Engine:    "MySQL",
		Version:   version,
		IsReplica: isReplica,
	}

	if isReplica {
		info.Warnings = append(info.Warnings,
			"Detected Read-Only MySQL node. Note: DDL execution must be directed to Primary Master.")
	} else {
		info.Warnings = append(info.Warnings,
			"Detected Primary MySQL Master. Recommend routing Airbyte reads to replica if available.")
	}

	return info, nil
}

func (m *MySQLAdapter) DiscoverSchemas(ctx context.Context) ([]string, error) {
	query := `
		SELECT schema_name 
		FROM information_schema.schemata 
		WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
		ORDER BY schema_name;
	`
	rows, err := m.db.QueryContext(ctx, query)
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

func (m *MySQLAdapter) DiscoverTables(ctx context.Context, schema string) ([]model.TableMetadata, error) {
	if schema == "" {
		schema = m.config.Database
	}

	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name;
	`
	rows, err := m.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []model.TableMetadata
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, model.TableMetadata{
			Schema: schema,
			Name:   name,
		})
	}

	for i := range tables {
		tName := tables[i].Name
		cQuery := `
			SELECT column_name, data_type, is_nullable, column_key
			FROM information_schema.columns
			WHERE table_schema = ? AND table_name = ?
			ORDER BY ordinal_position;
		`
		cRows, err := m.db.QueryContext(ctx, cQuery, schema, tName)
		if err != nil {
			return nil, err
		}

		var columns []model.ColumnMetadata
		hasCursor := false
		suggestedCursor := ""

		for cRows.Next() {
			var colName, dataType, isNullStr, colKey string
			if err := cRows.Scan(&colName, &dataType, &isNullStr, &colKey); err != nil {
				cRows.Close()
				return nil, err
			}

			isPK := (colKey == "PRI")
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

func (m *MySQLAdapter) GenerateDDL(plan model.ProvisionPlan) (string, error) {
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

func (m *MySQLAdapter) generateCDCDDL(plan model.ProvisionPlan) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- Controlled CDC Provisioning Script generated for MySQL\n")
	sb.WriteString("-- Ingestion Mode  : Controlled CDC (Binlog ROW-based)\n")
	sb.WriteString(fmt.Sprintf("-- Restricted User : %s\n", plan.ReaderUsername))
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Create Restricted User
	escapedPass := strings.ReplaceAll(plan.ReaderPassword, "'", "''")
	sb.WriteString("-- 1. Create CDC Reader User\n")
	sb.WriteString(fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';\n", plan.ReaderUsername, escapedPass))
	sb.WriteString(fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s';\n\n", plan.ReaderUsername, escapedPass))

	// 2. Grant Binlog Replication and SELECT privileges
	sb.WriteString("-- 2. Grant Binlog Replication Privileges (Zero-Write)\n")
	sb.WriteString(fmt.Sprintf("GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '%s'@'%%';\n", plan.ReaderUsername))

	for _, v := range plan.Views {
		sourceDb := v.SourceSchema
		if sourceDb == "" {
			sourceDb = m.config.Database
		}
		sb.WriteString(fmt.Sprintf("GRANT SELECT ON `%s`.`%s` TO '%s'@'%%';\n", sourceDb, v.SourceTable, plan.ReaderUsername))
	}

	sb.WriteString(fmt.Sprintf("\nALTER USER '%s'@'%%' WITH MAX_USER_CONNECTIONS 4;\n", plan.ReaderUsername))
	sb.WriteString("FLUSH PRIVILEGES;\n")

	return sb.String(), nil
}

func (m *MySQLAdapter) generateStandardDDL(plan model.ProvisionPlan) (string, error) {
	if plan.IntegrationSchema == "" {
		plan.IntegrationSchema = "airbyte_vault"
	}

	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- Standalone Provisioning Script generated for MySQL\n")
	sb.WriteString(fmt.Sprintf("-- Integration Database : %s\n", plan.IntegrationSchema))
	sb.WriteString(fmt.Sprintf("-- Restricted User      : %s\n", plan.ReaderUsername))
	sb.WriteString("-- ====================================================================\n\n")

	// 1. Create Database
	sb.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;\n\n", plan.IntegrationSchema))

	// 2. Create Flat Views
	for _, v := range plan.Views {
		var colExpressions []string
		for _, col := range v.Columns {
			switch col.Masking {
			case model.MaskExclude:
				continue
			case model.MaskMD5:
				colExpressions = append(colExpressions, fmt.Sprintf("MD5(`%s`) AS `%s`", col.ColumnName, col.ColumnName))
			case model.MaskEmail:
				expr := fmt.Sprintf("IF(`%s` LIKE '%%@%%', CONCAT(SUBSTRING(`%s`, 1, 2), '***@', SUBSTRING_INDEX(`%s`, '@', -1)), '***@masked.com') AS `%s`",
					col.ColumnName, col.ColumnName, col.ColumnName, col.ColumnName)
				colExpressions = append(colExpressions, expr)
			case model.MaskPhone:
				expr := fmt.Sprintf("IF(CHAR_LENGTH(`%s`) >= 4, CONCAT('***-***-', RIGHT(`%s`, 4)), '***') AS `%s`",
					col.ColumnName, col.ColumnName, col.ColumnName)
				colExpressions = append(colExpressions, expr)
			case model.MaskCard:
				expr := fmt.Sprintf("CONCAT('****-****-****-', RIGHT(`%s`, 4)) AS `%s`", col.ColumnName, col.ColumnName)
				colExpressions = append(colExpressions, expr)
			case model.MaskNull:
				colExpressions = append(colExpressions, fmt.Sprintf("NULL AS `%s`", col.ColumnName))
			default:
				colExpressions = append(colExpressions, fmt.Sprintf("`%s`", col.ColumnName))
			}
		}

		if len(colExpressions) == 0 {
			colExpressions = append(colExpressions, "1 AS `placeholder`")
		}

		sourceDb := v.SourceSchema
		if sourceDb == "" {
			sourceDb = m.config.Database
		}
		targetView := v.TargetView
		if targetView == "" {
			targetView = "v_" + v.SourceTable
		}

		sb.WriteString(fmt.Sprintf("CREATE OR REPLACE SQL SECURITY DEFINER VIEW `%s`.`%s` AS\nSELECT\n  %s\nFROM `%s`.`%s`;\n\n",
			plan.IntegrationSchema, targetView,
			strings.Join(colExpressions, ",\n  "),
			sourceDb, v.SourceTable,
		))
	}

	// 3. Create Restricted User & Grant Permissions
	escapedPass := strings.ReplaceAll(plan.ReaderPassword, "'", "''")
	sb.WriteString(fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';\n", plan.ReaderUsername, escapedPass))
	sb.WriteString(fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s';\n", plan.ReaderUsername, escapedPass))
	sb.WriteString(fmt.Sprintf("GRANT SELECT ON `%s`.* TO '%s'@'%%';\n", plan.IntegrationSchema, plan.ReaderUsername))
	sb.WriteString(fmt.Sprintf("ALTER USER '%s'@'%%' WITH MAX_USER_CONNECTIONS 2;\n", plan.ReaderUsername))
	sb.WriteString("FLUSH PRIVILEGES;\n")

	return sb.String(), nil
}

// GenerateAuditSQL generates audit queries for MySQL DBAs
func (m *MySQLAdapter) GenerateAuditSQL(plan model.ProvisionPlan) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- ====================================================================\n")
	sb.WriteString("-- MYSQL DBA AUDIT & VERIFICATION SUITE\n")
	sb.WriteString(fmt.Sprintf("-- Mode: %s | Target User: %s\n", plan.SyncStrategy, plan.ReaderUsername))
	sb.WriteString("-- ====================================================================\n\n")

	sb.WriteString("-- 1. AUDIT USER PRIVILEGES (Check Zero-Write & Replication rights)\n")
	sb.WriteString(fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';\n\n", plan.ReaderUsername))

	if plan.SyncStrategy == model.SyncStrategyCDC {
		sb.WriteString("-- 2. AUDIT BINLOG STATUS & CONFIGURATION\n")
		sb.WriteString("SHOW VARIABLES LIKE 'log_bin';\n")
		sb.WriteString("SHOW VARIABLES LIKE 'binlog_format';\n")
		sb.WriteString("SHOW VARIABLES LIKE 'binlog_row_image';\n")
		sb.WriteString("SHOW VARIABLES LIKE 'expire_logs_days';\n")
		sb.WriteString("SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';\n\n")

		sb.WriteString("-- 3. AUDIT BINLOG COORDINATES & RETENTION FILES\n")
		sb.WriteString("SHOW MASTER STATUS;\n")
		sb.WriteString("SHOW BINARY LOGS;\n")
	} else {
		sb.WriteString("-- 2. AUDIT CREATED VIEWS IN INTEGRATION DATABASE\n")
		sb.WriteString(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE TABLE_TYPE LIKE 'VIEW';\n", plan.IntegrationSchema))
	}

	return sb.String(), nil
}

func (m *MySQLAdapter) ApplyPlan(ctx context.Context, sqlScript string) error {
	if m.db == nil {
		return fmt.Errorf("database connection is not open")
	}

	statements := SplitMySQLStatements(sqlScript)
	for _, stmt := range statements {
		stmtTrimmed := strings.TrimSpace(stmt)
		if stmtTrimmed == "" {
			continue
		}

		lines := strings.Split(stmtTrimmed, "\n")
		hasExecutableCode := false
		for _, line := range lines {
			l := strings.TrimSpace(line)
			if l != "" && !strings.HasPrefix(l, "--") && !strings.HasPrefix(l, "#") {
				hasExecutableCode = true
				break
			}
		}
		if !hasExecutableCode {
			continue
		}

		_, err := m.db.ExecContext(ctx, stmtTrimmed)
		if err != nil {
			return fmt.Errorf("error executing statement on mysql:\n%s\nError: %w", stmtTrimmed, err)
		}
	}
	return nil
}

// SplitMySQLStatements splits MySQL scripts into executable statements
func SplitMySQLStatements(script string) []string {
	var statements []string
	var cur strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false

	runes := []rune(script)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]

		// Line comments (-- or #)
		if !inSingleQuote && !inDoubleQuote && !inBacktick && !inBlockComment {
			if (r == '-' && i+1 < n && runes[i+1] == '-') || r == '#' {
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

		// Block comments (/* ... */)
		if !inSingleQuote && !inDoubleQuote && !inBacktick && !inLineComment {
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

		// String quotes
		if !inDoubleQuote && !inBacktick {
			if r == '\'' {
				if inSingleQuote && i+1 < n && runes[i+1] == '\'' {
					cur.WriteRune(r)
					cur.WriteRune(runes[i+1])
					i++
					continue
				}
				inSingleQuote = !inSingleQuote
			}
		}
		if !inSingleQuote && !inBacktick {
			if r == '"' {
				if inDoubleQuote && i+1 < n && runes[i+1] == '"' {
					cur.WriteRune(r)
					cur.WriteRune(runes[i+1])
					i++
					continue
				}
				inDoubleQuote = !inDoubleQuote
			}
		}
		if !inSingleQuote && !inDoubleQuote {
			if r == '`' {
				inBacktick = !inBacktick
			}
		}

		if r == ';' && !inSingleQuote && !inDoubleQuote && !inBacktick {
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

func (m *MySQLAdapter) VerifyReader(ctx context.Context, cfg model.DBConfig, plan model.ProvisionPlan, sampleTarget string) (string, error) {
	testCfg := cfg
	testCfg.Username = plan.ReaderUsername
	testCfg.Password = plan.ReaderPassword

	cleanTarget := sampleTarget
	if strings.Contains(sampleTarget, ".") {
		parts := strings.Split(sampleTarget, ".")
		testCfg.Database = parts[0]
		cleanTarget = parts[1]
	}

	dsn := m.buildDSN(testCfg)
	testDb, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", fmt.Errorf("failed to open connection with reader credentials: %w", err)
	}
	defer testDb.Close()

	if err := testDb.PingContext(ctx); err != nil {
		return "", fmt.Errorf("reader user failed to authenticate against database '%s': %w", testCfg.Database, err)
	}

	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", cleanTarget)
	if err := testDb.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return "", fmt.Errorf("reader user failed to read target '%s': %w", sampleTarget, err)
	}

	return fmt.Sprintf("✅ Verified read access on MySQL target '%s': returned %d rows.", sampleTarget, count), nil
}
