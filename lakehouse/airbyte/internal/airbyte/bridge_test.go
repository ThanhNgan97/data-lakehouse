package airbyte

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTriggerAirflowDAGCarriesContextAndJobID(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/dags/universal_lakehouse_pipeline/dagRuns" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != "airflow" || password != "secret" {
			t.Fatalf("unexpected basic auth: %q %q", username, password)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bridge := NewBridgeServer(BridgeConfig{
		AirflowURL:      server.URL,
		AirflowUsername: "airflow",
		AirflowPassword: "secret",
	})
	if err := bridge.triggerAirflowDAG(context.Background(), 65, "ctu_ioc_test"); err != nil {
		t.Fatalf("trigger DAG: %v", err)
	}

	if got["dag_run_id"] != "airbyte__65" {
		t.Fatalf("unexpected dag_run_id: %#v", got["dag_run_id"])
	}
	conf, ok := got["conf"].(map[string]any)
	if !ok {
		t.Fatalf("missing conf: %#v", got)
	}
	if conf["context_id"] != "ctu_ioc_test" || conf["airbyte_job_id"] != float64(65) {
		t.Fatalf("unexpected conf: %#v", conf)
	}
}
