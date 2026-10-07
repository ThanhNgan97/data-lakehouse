package adapter_test

import (
	"strings"
	"testing"

	"github.com/datalakehouse/airbyte-config-tool/internal/adapter"
	"github.com/datalakehouse/airbyte-config-tool/internal/model"
)

func TestPostgresGenerateDDL_StandardAndCDC(t *testing.T) {
	pgAdapter := adapter.NewPostgresAdapter()

	views := []model.ViewRule{
		{
			SourceSchema: "public",
			SourceTable:  "customers",
			TargetView:   "v_customers",
			CursorColumn: "updated_at",
			Columns: []model.ColumnRule{
				{ColumnName: "id", Masking: model.MaskNone},
				{ColumnName: "email", Masking: model.MaskEmail},
				{ColumnName: "phone", Masking: model.MaskPhone},
			},
		},
		{
			SourceSchema: "public",
			SourceTable:  "orders",
			TargetView:   "v_orders",
			CursorColumn: "id",
			Columns: []model.ColumnRule{
				{ColumnName: "id", Masking: model.MaskNone},
				{ColumnName: "amount", Masking: model.MaskNone},
			},
		},
	}

	// 1. Test Standard Strategy (Flat Views)
	planStandard := model.ProvisionPlan{
		TenantID:            "test_tenant",
		SyncStrategy:        model.SyncStrategyStandard,
		IntegrationSchema:   "airbyte_vault",
		ReaderUsername:      "airbyte_reader",
		ReaderPassword:      "TestSecurePass123!",
		StatementTimeoutSec: 30,
		Views:               views,
	}

	ddlStandard, err := pgAdapter.GenerateDDL(planStandard)
	if err != nil {
		t.Fatalf("GenerateDDL standard failed: %v", err)
	}
	if !strings.Contains(ddlStandard, "CREATE SCHEMA IF NOT EXISTS airbyte_vault") {
		t.Errorf("Standard DDL should create integration schema")
	}
	if !strings.Contains(ddlStandard, "CREATE VIEW airbyte_vault.v_customers") {
		t.Errorf("Standard DDL should create view for customers")
	}
	if strings.Contains(ddlStandard, "CREATE PUBLICATION") {
		t.Errorf("Standard DDL should NOT create PUBLICATION")
	}

	auditStandard, err := pgAdapter.GenerateAuditSQL(planStandard)
	if err != nil {
		t.Fatalf("GenerateAuditSQL standard failed: %v", err)
	}
	if !strings.Contains(auditStandard, "information_schema.views") {
		t.Errorf("Standard audit should inspect views")
	}

	// 2. Test Controlled CDC Strategy (Logical Replication)
	planCDC := model.ProvisionPlan{
		TenantID:             "test_tenant",
		SyncStrategy:         model.SyncStrategyCDC,
		PublicationName:      "airbyte_pub_test_tenant",
		MaxSlotWALKeepSizeGB: 20,
		ReaderUsername:       "airbyte_reader",
		ReaderPassword:       "TestSecurePass123!",
		StatementTimeoutSec:  30,
		Views:                views,
	}

	ddlCDC, err := pgAdapter.GenerateDDL(planCDC)
	if err != nil {
		t.Fatalf("GenerateDDL CDC failed: %v", err)
	}
	if !strings.Contains(ddlCDC, "CREATE ROLE airbyte_reader WITH LOGIN REPLICATION") {
		t.Errorf("CDC DDL should grant REPLICATION attribute to user")
	}
	if !strings.Contains(ddlCDC, "CREATE PUBLICATION airbyte_pub_test_tenant FOR TABLE public.customers, public.orders") {
		t.Errorf("CDC DDL should create publication for selected tables")
	}
	if !strings.Contains(ddlCDC, "max_slot_wal_keep_size = '20GB'") {
		t.Errorf("CDC DDL should configure max_slot_wal_keep_size")
	}
	if !strings.Contains(ddlCDC, "pg_create_logical_replication_slot") {
		t.Errorf("CDC DDL should create logical replication slot")
	}
	if strings.Contains(ddlCDC, "CREATE VIEW") {
		t.Errorf("CDC DDL should NOT create Views")
	}

	auditCDC, err := pgAdapter.GenerateAuditSQL(planCDC)
	if err != nil {
		t.Fatalf("GenerateAuditSQL CDC failed: %v", err)
	}
	if !strings.Contains(auditCDC, "pg_publication") {
		t.Errorf("CDC audit should inspect pg_publication")
	}
	if !strings.Contains(auditCDC, "pg_replication_slots") {
		t.Errorf("CDC audit should inspect pg_replication_slots")
	}
	if !strings.Contains(auditCDC, "max_slot_wal_keep_size") {
		t.Errorf("CDC audit should inspect max_slot_wal_keep_size")
	}
}

func TestMySQLGenerateDDL_StandardAndCDC(t *testing.T) {
	myAdapter := adapter.NewMySQLAdapter()

	views := []model.ViewRule{
		{
			SourceSchema: "store_db",
			SourceTable:  "products",
			TargetView:   "v_products",
			CursorColumn: "updated_at",
			Columns: []model.ColumnRule{
				{ColumnName: "id", Masking: model.MaskNone},
				{ColumnName: "price", Masking: model.MaskNone},
			},
		},
	}

	// 1. Standard mode
	planStd := model.ProvisionPlan{
		TenantID:          "tenant_my",
		SyncStrategy:      model.SyncStrategyStandard,
		IntegrationSchema: "airbyte_vault",
		ReaderUsername:    "airbyte_reader",
		ReaderPassword:    "TestPass123!",
		Views:             views,
	}
	ddlStd, err := myAdapter.GenerateDDL(planStd)
	if err != nil {
		t.Fatalf("MySQL standard failed: %v", err)
	}
	if !strings.Contains(ddlStd, "CREATE DATABASE IF NOT EXISTS `airbyte_vault`") {
		t.Errorf("MySQL standard should create database")
	}

	// 2. CDC mode
	planCDC := model.ProvisionPlan{
		TenantID:       "tenant_my",
		SyncStrategy:   model.SyncStrategyCDC,
		ReaderUsername: "airbyte_reader",
		ReaderPassword: "TestPass123!",
		Views:          views,
	}
	ddlCDC, err := myAdapter.GenerateDDL(planCDC)
	if err != nil {
		t.Fatalf("MySQL CDC failed: %v", err)
	}
	if !strings.Contains(ddlCDC, "REPLICATION CLIENT, REPLICATION SLAVE") {
		t.Errorf("MySQL CDC should grant REPLICATION CLIENT, REPLICATION SLAVE")
	}

	auditCDC, err := myAdapter.GenerateAuditSQL(planCDC)
	if err != nil {
		t.Fatalf("MySQL audit failed: %v", err)
	}
	if !strings.Contains(auditCDC, "SHOW BINARY LOGS") {
		t.Errorf("MySQL CDC audit should inspect binary logs")
	}
}

func TestSplitStatements(t *testing.T) {
	pgScript := `
-- 1. Create Role
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airbyte_reader') THEN
    CREATE ROLE airbyte_reader WITH LOGIN REPLICATION PASSWORD 'secret';
  END IF;
END
$$;

-- 2. Grants
GRANT USAGE ON SCHEMA public TO airbyte_reader;
GRANT SELECT ON TABLE public.customers TO airbyte_reader;

-- 3. System settings
ALTER SYSTEM SET max_slot_wal_keep_size = '20GB';
SELECT pg_reload_conf();
`
	stmts := adapter.SplitPostgresStatements(pgScript)
	if len(stmts) != 5 {
		t.Fatalf("Expected 5 statements, got %d: %+v", len(stmts), stmts)
	}

	if !strings.Contains(stmts[0], "DO $$") || !strings.Contains(stmts[0], "CREATE ROLE") {
		t.Errorf("First statement should be DO block: %s", stmts[0])
	}
	if !strings.Contains(stmts[3], "ALTER SYSTEM SET") {
		t.Errorf("Fourth statement should be ALTER SYSTEM: %s", stmts[3])
	}
	if !strings.Contains(stmts[4], "SELECT pg_reload_conf()") {
		t.Errorf("Fifth statement should be SELECT pg_reload_conf: %s", stmts[4])
	}

	myScript := `
CREATE USER IF NOT EXISTS 'reader'@'%' IDENTIFIED BY 'pass;word';
GRANT SELECT ON db.* TO 'reader'@'%';
FLUSH PRIVILEGES;
`
	myStmts := adapter.SplitMySQLStatements(myScript)
	if len(myStmts) != 3 {
		t.Fatalf("Expected 3 MySQL statements, got %d: %+v", len(myStmts), myStmts)
	}

	mssqlScript := `
-- Comment line
CREATE LOGIN [reader] WITH PASSWORD = 'pass;word', CHECK_POLICY = OFF;
GO
CREATE USER [reader] FOR LOGIN [reader];
GO
GRANT SELECT ON SCHEMA::cdc TO [reader];
`
	msStmts := adapter.SplitMSSQLStatements(mssqlScript)
	if len(msStmts) != 3 {
		t.Fatalf("Expected 3 MSSQL statements, got %d: %+v", len(msStmts), msStmts)
	}
}

func TestMSSQLGenerateDDL_StandardAndCDC(t *testing.T) {
	msAdapter := adapter.NewMSSQLAdapter()

	views := []model.ViewRule{
		{
			SourceSchema: "dbo",
			SourceTable:  "customers",
			TargetView:   "v_customers",
			CursorColumn: "updated_at",
			Columns: []model.ColumnRule{
				{ColumnName: "id", Masking: model.MaskNone},
				{ColumnName: "email", Masking: model.MaskEmail},
				{ColumnName: "phone", Masking: model.MaskPhone},
			},
		},
		{
			SourceSchema: "dbo",
			SourceTable:  "orders",
			TargetView:   "v_orders",
			CursorColumn: "id",
			Columns: []model.ColumnRule{
				{ColumnName: "id", Masking: model.MaskNone},
				{ColumnName: "amount", Masking: model.MaskNone},
			},
		},
	}

	// 1. Test Standard Strategy
	planStandard := model.ProvisionPlan{
		TenantID:          "tenant_mssql",
		SyncStrategy:      model.SyncStrategyStandard,
		IntegrationSchema: "airbyte_vault",
		ReaderUsername:    "airbyte_reader",
		ReaderPassword:    "SecurePass123!",
		Views:             views,
	}

	ddlStandard, err := msAdapter.GenerateDDL(planStandard)
	if err != nil {
		t.Fatalf("MSSQL GenerateDDL standard failed: %v", err)
	}
	if !strings.Contains(ddlStandard, "CREATE SCHEMA [airbyte_vault]") {
		t.Errorf("MSSQL Standard DDL should create airbyte_vault schema")
	}
	if !strings.Contains(ddlStandard, "CREATE OR ALTER VIEW [airbyte_vault].[v_customers]") {
		t.Errorf("MSSQL Standard DDL should create view for customers")
	}
	if !strings.Contains(ddlStandard, "GRANT SELECT ON SCHEMA::[airbyte_vault]") {
		t.Errorf("MSSQL Standard DDL should grant select on schema")
	}

	// 2. Test Controlled CDC Strategy
	planCDC := model.ProvisionPlan{
		TenantID:       "tenant_mssql",
		SyncStrategy:   model.SyncStrategyCDC,
		ReaderUsername: "airbyte_reader",
		ReaderPassword: "SecurePass123!",
		Views:          views,
	}

	ddlCDC, err := msAdapter.GenerateDDL(planCDC)
	if err != nil {
		t.Fatalf("MSSQL GenerateDDL CDC failed: %v", err)
	}
	if !strings.Contains(ddlCDC, "sys.sp_cdc_enable_db") {
		t.Errorf("MSSQL CDC DDL should enable db cdc")
	}
	if !strings.Contains(ddlCDC, "sys.sp_cdc_enable_table") {
		t.Errorf("MSSQL CDC DDL should enable table cdc")
	}
	if !strings.Contains(ddlCDC, "GRANT SELECT ON SCHEMA::cdc TO [airbyte_reader]") {
		t.Errorf("MSSQL CDC DDL should grant select on cdc schema")
	}
	if !strings.Contains(ddlCDC, "DENY INSERT, UPDATE, DELETE, ALTER TO [airbyte_reader]") {
		t.Errorf("MSSQL CDC DDL should include zero-write DENY guardrails")
	}

	// 3. Test Audit SQL
	auditCDC, err := msAdapter.GenerateAuditSQL(planCDC)
	if err != nil {
		t.Fatalf("MSSQL GenerateAuditSQL failed: %v", err)
	}
	if !strings.Contains(auditCDC, "sys.dm_server_services") {
		t.Errorf("MSSQL audit should inspect SQL Server Agent services")
	}
	if !strings.Contains(auditCDC, "cdc.change_tables") {
		t.Errorf("MSSQL audit should inspect cdc.change_tables")
	}
}

