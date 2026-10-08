package exporter

import (
	"testing"

	"github.com/datalakehouse/airbyte-config-tool/internal/model"
)

func TestGenerateBundleDefaultsToStagingPath(t *testing.T) {
	plan := model.ProvisionPlan{
		TenantID:       "tenant_01",
		RawSQLScript:   "-- setup",
		AuditSQLScript: "-- audit",
	}

	bundle, _, err := GenerateBundle(model.DBConfig{}, plan, t.TempDir())
	if err != nil {
		t.Fatalf("GenerateBundle() error = %v", err)
	}

	want := "staging/tenant_01/table=${STREAM_NAME}/"
	if got := bundle.MinIODestination.RecommendedPath; got != want {
		t.Fatalf("RecommendedPath = %q, want %q", got, want)
	}
}

func TestGenerateBundlePreservesExplicitMinIOPath(t *testing.T) {
	plan := model.ProvisionPlan{
		TenantID:       "tenant_01",
		MinIOPath:      "staging/custom-prefix",
		RawSQLScript:   "-- setup",
		AuditSQLScript: "-- audit",
	}

	bundle, _, err := GenerateBundle(model.DBConfig{}, plan, t.TempDir())
	if err != nil {
		t.Fatalf("GenerateBundle() error = %v", err)
	}

	want := "staging/custom-prefix"
	if got := bundle.MinIODestination.RecommendedPath; got != want {
		t.Fatalf("RecommendedPath = %q, want %q", got, want)
	}
}
