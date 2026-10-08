package exporter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/datalakehouse/airbyte-config-tool/internal/model"
)

// GenerateBundle creates a structured configuration bundle ready for Airbyte or platform handoff
func GenerateBundle(cfg model.DBConfig, plan model.ProvisionPlan, outputDir string) (*model.AirbyteConnectionBundle, string, error) {
	if outputDir == "" {
		outputDir = "."
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create output directory: %w", err)
	}

	tenantID := plan.TenantID
	if tenantID == "" {
		tenantID = "tenant_default"
	}

	var exposedViews []model.ExposedViewInfo
	for _, v := range plan.Views {
		var activeCols []string
		for _, c := range v.Columns {
			if c.Masking != model.MaskExclude {
				activeCols = append(activeCols, c.ColumnName)
			}
		}

		cursor := v.CursorColumn
		if cursor == "" {
			cursor = "updated_at"
		}

		targetView := v.TargetView
		if targetView == "" {
			targetView = "v_" + v.SourceTable
		}

		exposedViews = append(exposedViews, model.ExposedViewInfo{
			SourceTable:   v.SourceTable,
			TargetView:    targetView,
			CursorColumn:  cursor,
			SyncMode:      "Incremental | Append",
			ActiveColumns: activeCols,
		})
	}

	targetDatabase := cfg.Database
	targetSchema := plan.IntegrationSchema
	if plan.SyncStrategy == model.SyncStrategyCDC {
		targetSchema = cfg.Schema
		if targetSchema == "" {
			targetSchema = "public"
		}
	} else if cfg.Engine == "mysql" && plan.IntegrationSchema != "" {
		targetDatabase = plan.IntegrationSchema
	}

	strategy := plan.SyncStrategy
	if strategy == "" {
		strategy = model.SyncStrategyStandard
	}

	slotName := plan.ReplicationSlot
	if slotName == "" && strategy == model.SyncStrategyCDC {
		slotName = fmt.Sprintf("airbyte_slot_%s", tenantID)
	}
	slotName = strings.ToLower(strings.ReplaceAll(slotName, "-", "_"))

	bucketName := plan.MinIOBucket
	if bucketName == "" {
		bucketName = "university-lakehouse"
	}
	minioPath := plan.MinIOPath
	if minioPath == "" {
		minioPath = fmt.Sprintf("staging/%s/table=${STREAM_NAME}/", tenantID)
	}

	bundle := &model.AirbyteConnectionBundle{
		Version:         "1.1.0",
		GeneratedAt:     time.Now().UTC(),
		TenantID:        tenantID,
		DatabaseEngine:  cfg.Engine,
		SyncStrategy:    strategy,
		PublicationName: plan.PublicationName,
		ReplicationSlot: slotName,
		Connection: model.AirbyteConnectionInfo{
			Host:     cfg.Host,
			Port:     cfg.Port,
			Database: targetDatabase,
			Schema:   targetSchema,
			Username: plan.ReaderUsername,
			Password: plan.ReaderPassword,
			SSLMode:  cfg.SSLMode,
		},
		ExposedViews: exposedViews,
		MinIODestination: model.MinIOGuidelines{
			RecommendedBucket: bucketName,
			RecommendedPath:   minioPath,
			Format:            "Parquet (Snappy Compressed)",
		},
		FirewallGuidelines: model.FirewallGuidelines{
			WhitelistedAirbyteIPs: []string{
				"127.0.0.1/32 (Local Testing)",
				"Add your Airbyte static NAT IP here",
			},
			Notice: "Please ensure the client firewall or security group allows inbound TCP on database port from the above IPs.",
		},
	}

	bundlePath := filepath.Join(outputDir, fmt.Sprintf("airbyte_bundle_%s.json", tenantID))
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return nil, "", fmt.Errorf("failed to serialize bundle to json: %w", err)
	}

	if err := os.WriteFile(bundlePath, data, 0644); err != nil {
		return nil, "", fmt.Errorf("failed to write bundle file: %w", err)
	}

	// Also write raw SQL script for DBA manual archive
	sqlPath := filepath.Join(outputDir, fmt.Sprintf("db_hardening_script_%s.sql", tenantID))
	_ = os.WriteFile(sqlPath, []byte(plan.RawSQLScript), 0644)

	// Save audit verification script if available
	if plan.AuditSQLScript != "" {
		auditPath := filepath.Join(outputDir, fmt.Sprintf("dba_audit_queries_%s.sql", tenantID))
		_ = os.WriteFile(auditPath, []byte(plan.AuditSQLScript), 0644)
	}

	return bundle, bundlePath, nil
}
