package model

import "time"

// DBConfig holds database connection parameters
type DBConfig struct {
	Engine   string `json:"engine"` // postgres, mysql
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"` // disable, require, verify-ca, verify-full
	Schema   string `json:"schema"`   // default: public (PG) or db name (MySQL)
}

// DBInfo holds runtime engine details
type DBInfo struct {
	Engine    string   `json:"engine"`
	Version   string   `json:"version"`
	IsReplica bool     `json:"is_replica"`
	Warnings  []string `json:"warnings,omitempty"`
}

// MaskingType defines masking algorithm
type MaskingType string

const (
	MaskNone     MaskingType = "NONE"
	MaskExclude  MaskingType = "EXCLUDE"
	MaskMD5      MaskingType = "MD5_HASH"
	MaskEmail    MaskingType = "MASK_EMAIL"
	MaskPhone    MaskingType = "MASK_PHONE"
	MaskCard     MaskingType = "MASK_CARD"
	MaskNull     MaskingType = "NULL_OUT"
)

// ColumnMetadata describes a table column
type ColumnMetadata struct {
	Name             string      `json:"name"`
	DataType         string      `json:"data_type"`
	IsNullable       bool        `json:"is_nullable"`
	IsPrimaryKey     bool        `json:"is_primary_key"`
	IsSensitive      bool        `json:"is_sensitive"`
	SuggestedMasking MaskingType `json:"suggested_masking"`
}

// TableMetadata describes a database table
type TableMetadata struct {
	Schema        string           `json:"schema"`
	Name          string           `json:"name"`
	EstimatedRows int64            `json:"estimated_rows"`
	Columns       []ColumnMetadata `json:"columns"`
	HasCursorCol  bool             `json:"has_cursor_col"`
	SuggestedCursor string         `json:"suggested_cursor"`
}

// ColumnRule represents column selection and masking
type ColumnRule struct {
	ColumnName string      `json:"column_name"`
	Masking    MaskingType `json:"masking"`
}

// ViewRule represents one view to be generated from a source table
type ViewRule struct {
	SourceSchema string       `json:"source_schema"`
	SourceTable  string       `json:"source_table"`
	TargetView   string       `json:"target_view"`
	Columns      []ColumnRule `json:"columns"`
	CursorColumn string       `json:"cursor_column"`
}

// SyncStrategy defines the data ingestion method
type SyncStrategy string

const (
	SyncStrategyStandard SyncStrategy = "STANDARD" // Flat Views + Cursor-based Incremental
	SyncStrategyCDC      SyncStrategy = "CDC"      // Controlled CDC (Log-based Streaming)
)

// ProvisionPlan holds the entire configuration to be generated/applied
type ProvisionPlan struct {
	TenantID             string       `json:"tenant_id"`
	SyncStrategy         SyncStrategy `json:"sync_strategy"`                   // STANDARD or CDC
	IntegrationSchema    string       `json:"integration_schema"`             // e.g., airbyte_vault (Standard)
	PublicationName      string       `json:"publication_name,omitempty"`      // e.g., airbyte_pub_tenant01 (CDC)
	ReplicationSlot      string       `json:"replication_slot,omitempty"`      // e.g., airbyte_slot_tenant01 (CDC)
	MaxSlotWALKeepSizeGB int          `json:"max_slot_wal_keep_size_gb,omitempty"` // default: 20GB (CDC)
	ReaderUsername       string       `json:"reader_username"`                // e.g., airbyte_reader
	ReaderPassword       string       `json:"reader_password"`
	StatementTimeoutSec  int          `json:"statement_timeout_sec"`          // default: 30s
	Views                []ViewRule   `json:"views"`
	MinIOBucket          string       `json:"minio_bucket,omitempty"`          // default: university-lakehouse
	MinIOPath            string       `json:"minio_path,omitempty"`            // default: bronze_archive/<tenant>
	RawSQLScript         string       `json:"raw_sql_script,omitempty"`
	AuditSQLScript       string       `json:"audit_sql_script,omitempty"`
}

// ProvisionResult holds execution outcome
type ProvisionResult struct {
	Success        bool     `json:"success"`
	ExecutedSteps  []string `json:"executed_steps"`
	ReaderUsername string   `json:"reader_username"`
	ReaderPassword string   `json:"reader_password"`
	TestOutput     string   `json:"test_output"`
	Warnings       []string `json:"warnings,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// AirbyteConnectionBundle is the exported handover file
type AirbyteConnectionBundle struct {
	Version            string                `json:"bundle_version"`
	GeneratedAt        time.Time             `json:"generated_at"`
	TenantID           string                `json:"tenant_id"`
	DatabaseEngine     string                `json:"database_engine"`
	SyncStrategy       SyncStrategy          `json:"sync_strategy"` // STANDARD or CDC
	PublicationName    string                `json:"publication_name,omitempty"`
	ReplicationSlot    string                `json:"replication_slot,omitempty"`
	Connection         AirbyteConnectionInfo `json:"connection"`
	ExposedViews       []ExposedViewInfo     `json:"exposed_views"`
	MinIODestination   MinIOGuidelines       `json:"minio_destination_guidelines"`
	FirewallGuidelines FirewallGuidelines    `json:"firewall_guidelines"`
}

type AirbyteConnectionInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"`
}

type ExposedViewInfo struct {
	SourceTable   string   `json:"source_table"`
	TargetView    string   `json:"target_view"`
	CursorColumn  string   `json:"cursor_column"`
	SyncMode      string   `json:"recommended_sync_mode"`
	ActiveColumns []string `json:"active_columns"`
}

type MinIOGuidelines struct {
	RecommendedBucket string `json:"recommended_bucket"`
	RecommendedPath   string `json:"recommended_path_format"`
	Format            string `json:"format"`
}

type FirewallGuidelines struct {
	WhitelistedAirbyteIPs []string `json:"whitelisted_airbyte_ips"`
	Notice                string   `json:"notice"`
}
