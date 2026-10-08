package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/datalakehouse/airbyte-config-tool/internal/adapter"
	"github.com/datalakehouse/airbyte-config-tool/internal/airbyte"
	"github.com/datalakehouse/airbyte-config-tool/internal/exporter"
	"github.com/datalakehouse/airbyte-config-tool/internal/lakehouse"
	"github.com/datalakehouse/airbyte-config-tool/internal/model"
	"github.com/datalakehouse/airbyte-config-tool/web"
)

type Server struct {
	port     int
	lhClient *lakehouse.Client
	abClient *airbyte.AirbyteClient
}

func NewServer(port int) *Server {
	if port <= 0 {
		port = 8085
	}
	lhClient, err := lakehouse.NewClient(lakehouse.Config{
		Endpoint:   "127.0.0.1:9000",
		BucketName: "university-lakehouse",
	})
	if err != nil {
		fmt.Printf("⚠️ Warning: Could not initialize Lakehouse client: %v\n", err)
	}
	abClient := airbyte.NewAirbyteClient(airbyte.ResolveAirbyteURL(""), "cfcfc672-c3ad-4a59-bf48-abc9bc1efb26", "TyYQbcYAFigm1mrK7H8z50kHCcCVZA3g")

	return &Server{
		port:     port,
		lhClient: lhClient,
		abClient: abClient,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Serve static UI from embed with no-cache headers to prevent browser caching
	fileServer := http.FileServer(http.FS(web.StaticFS))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		fileServer.ServeHTTP(w, r)
	})

	// API Routes
	mux.HandleFunc("/api/connect", s.handleConnect)
	mux.HandleFunc("/api/plan", s.handlePlan)
	mux.HandleFunc("/api/apply", s.handleApply)
	mux.HandleFunc("/api/download-audit-sql", s.handleDownloadAuditSQL)
	mux.HandleFunc("/api/sync-to-airbyte", s.handleSyncToAirbyte)
	mux.HandleFunc("/api/lakehouse/streams", s.handleLakehouseStreams)
	mux.HandleFunc("/api/lakehouse/version-history", s.handleLakehouseVersionHistory)
	mux.HandleFunc("/api/check-tenant", s.handleCheckTenant)

	addr := fmt.Sprintf(":%d", s.port)
	url := fmt.Sprintf("http://localhost:%d", s.port)
	fmt.Printf("\n🚀 Launching Airbyte DB Provisioner Web UI at %s\n", url)

	// Auto-launch browser
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = openBrowser(url)
	}()

	return http.ListenAndServe(addr, mux)
}

func openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "rundll32"
		args = []string{"url.dll,FileProtocolHandler", url}
	case "darwin":
		cmd = "open"
		args = []string{url}
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
		args = []string{url}
	}
	return exec.Command(cmd, args...).Start()
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request")
		return
	}

	var cfg model.DBConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	adp, err := adapter.NewAdapter(cfg.Engine)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer adp.Close()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := adp.Connect(ctx, cfg); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Connection failed: %v", err))
		return
	}

	info, err := adp.Ping(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Ping failed: %v", err))
		return
	}

	tables, err := adp.DiscoverTables(ctx, cfg.Schema)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Table discovery failed: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"info":   info,
		"tables": tables,
	})
}

type PlanRequest struct {
	Config model.DBConfig      `json:"config"`
	Plan   model.ProvisionPlan `json:"plan"`
}

func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req PlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	adp, err := adapter.NewAdapter(req.Config.Engine)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Plan.ReaderPassword == "" {
		pwd, err := adapter.GenerateStrongPassword(24)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to generate password")
			return
		}
		req.Plan.ReaderPassword = pwd
	}

	sqlScript, err := adp.GenerateDDL(req.Plan)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to generate DDL: %v", err))
		return
	}

	auditScript, err := adp.GenerateAuditSQL(req.Plan)
	if err != nil {
		auditScript = fmt.Sprintf("-- Error generating audit script: %v", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"sql":       sqlScript,
		"audit_sql": auditScript,
		"password":  req.Plan.ReaderPassword,
	})
}

func (s *Server) handleApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req PlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	adp, err := adapter.NewAdapter(req.Config.Engine)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer adp.Close()

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := adp.Connect(ctx, req.Config); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Connection failed: %v", err))
		return
	}

	// 1. Execute DDL
	if err := adp.ApplyPlan(ctx, req.Plan.RawSQLScript); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("DDL execution failed: %v", err))
		return
	}

	// 2. Smoke Test with newly created reader user
	sampleTarget := ""
	if len(req.Plan.Views) > 0 {
		if req.Plan.SyncStrategy == model.SyncStrategyCDC {
			schema := req.Plan.Views[0].SourceSchema
			if schema == "" {
				schema = req.Config.Schema
				if schema == "" {
					schema = "public"
				}
			}
			sampleTarget = fmt.Sprintf("%s.%s", schema, req.Plan.Views[0].SourceTable)
		} else {
			targetView := req.Plan.Views[0].TargetView
			if targetView == "" {
				targetView = "v_" + req.Plan.Views[0].SourceTable
			}
			sampleTarget = fmt.Sprintf("%s.%s", req.Plan.IntegrationSchema, targetView)
		}
	}

	testOutput, err := adp.VerifyReader(ctx, req.Config, req.Plan, sampleTarget)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Access verification failed: %v", err))
		return
	}

	// Ensure AuditSQL is populated
	if req.Plan.AuditSQLScript == "" {
		req.Plan.AuditSQLScript, _ = adp.GenerateAuditSQL(req.Plan)
	}

	// 3. Generate Handover Bundle file on disk
	bundle, bundlePath, err := exporter.GenerateBundle(req.Config, req.Plan, "./exports")
	if err != nil {
		testOutput += fmt.Sprintf("\n⚠️ Warning: Bundle export encountered an error: %v", err)
	} else {
		testOutput += fmt.Sprintf("\n📁 Handover bundle exported to: %s", bundlePath)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":     true,
		"test_output": testOutput,
		"audit_sql":   req.Plan.AuditSQLScript,
		"bundle":      bundle,
	})
}

func (s *Server) handleDownloadAuditSQL(w http.ResponseWriter, r *http.Request) {
	sqlContent := r.URL.Query().Get("content")
	if sqlContent == "" {
		// Try to read from POST body
		body, _ := io.ReadAll(r.Body)
		sqlContent = string(body)
	}
	if sqlContent == "" {
		sqlContent = "-- No audit queries provided."
	}

	tenant := r.URL.Query().Get("tenant")
	if tenant == "" {
		tenant = "db"
	}
	fileName := fmt.Sprintf("audit_verification_%s.sql", tenant)

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))
	w.Header().Set("Content-Type", "application/sql; charset=utf-8")
	_, _ = w.Write([]byte(sqlContent))
}

type SyncToAirbyteRequest struct {
	CoordinatorURL string                       `json:"coordinator_url"`
	Bundle         model.AirbyteConnectionBundle `json:"bundle"`
}

func (s *Server) handleSyncToAirbyte(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req SyncToAirbyteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	if req.CoordinatorURL == "" {
		req.CoordinatorURL = "http://127.0.0.1:9090"
	}
	req.CoordinatorURL = strings.TrimRight(req.CoordinatorURL, "/")
	targetEndpoint := req.CoordinatorURL + "/api/v1/airbyte/onboard-source"

	bundleBytes, err := json.Marshal(req.Bundle)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to serialize bundle")
		return
	}

	// Airbyte connector container initialization & check can take 60-90s on Windows Docker
	client := &http.Client{Timeout: 180 * time.Second}
	resp, err := client.Post(targetEndpoint, "application/json", bytes.NewReader(bundleBytes))
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("Failed to contact Airbyte Coordinator at %s: %v", targetEndpoint, err))
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

func (s *Server) handleLakehouseStreams(w http.ResponseWriter, r *http.Request) {
	if s.lhClient == nil {
		writeError(w, http.StatusServiceUnavailable, "Lakehouse MinIO client not initialized")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	streams, err := s.lhClient.ListStreams(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list streams from MinIO: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, streams)
}

type VersionHistoryRequest struct {
	TenantID  string `json:"tenant_id"`
	TableName string `json:"table_name"`
	RecordID  string `json:"record_id"`
}

func (s *Server) handleLakehouseVersionHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if s.lhClient == nil {
		writeError(w, http.StatusServiceUnavailable, "Lakehouse MinIO client not initialized")
		return
	}

	var req VersionHistoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}
	if req.TenantID == "" || req.TableName == "" {
		writeError(w, http.StatusBadRequest, "tenant_id and table_name are required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	res, err := s.lhClient.QueryVersionHistory(ctx, req.TenantID, req.TableName, req.RecordID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to query version history: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type TenantCheckResponse struct {
	TenantID      string                      `json:"tenant_id"`
	IsDuplicate   bool                        `json:"is_duplicate"`
	SuggestedName string                      `json:"suggested_name"`
	MinIO         *lakehouse.MinIOTenantCheck `json:"minio"`
	Airbyte       *airbyte.AirbyteTenantCheck `json:"airbyte"`
	Message       string                      `json:"message"`
}

func (s *Server) handleCheckTenant(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	tenantID := r.URL.Query().Get("tenant")
	if r.Method == http.MethodPost {
		var req struct {
			TenantID string `json:"tenant_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil && req.TenantID != "" {
			tenantID = req.TenantID
		}
	}

	cleanID := strings.ToLower(strings.TrimSpace(tenantID))
	reg := regexp.MustCompile(`[^a-z0-9_]`)
	cleanID = reg.ReplaceAllString(cleanID, "_")

	if cleanID == "" {
		writeJSON(w, http.StatusOK, TenantCheckResponse{
			TenantID:    "",
			IsDuplicate: false,
			Message:     "Vui lòng nhập mã khách hàng.",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// 1. Check MinIO
	var minioRes *lakehouse.MinIOTenantCheck
	if s.lhClient != nil {
		minioRes, _ = s.lhClient.CheckTenantExists(ctx, cleanID)
	}
	if minioRes == nil {
		minioRes = &lakehouse.MinIOTenantCheck{Tables: []string{}}
	}

	// 2. Check Airbyte
	var airbyteRes *airbyte.AirbyteTenantCheck
	if s.abClient != nil {
		airbyteRes, _ = s.abClient.CheckTenant(ctx, cleanID)
	}
	if airbyteRes == nil {
		airbyteRes = &airbyte.AirbyteTenantCheck{
			Sources:         []string{},
			Destinations:    []string{},
			ConnectionNames: []string{},
		}
	}

	isDuplicate := minioRes.Exists || airbyteRes.Exists
	suggestedName := cleanID

	if isDuplicate {
		for i := 2; i <= 30; i++ {
			cand := fmt.Sprintf("%s_%d", cleanID, i)
			var mCheck *lakehouse.MinIOTenantCheck
			if s.lhClient != nil {
				mCheck, _ = s.lhClient.CheckTenantExists(ctx, cand)
			}
			var aCheck *airbyte.AirbyteTenantCheck
			if s.abClient != nil {
				aCheck, _ = s.abClient.CheckTenant(ctx, cand)
			}
			if (mCheck == nil || !mCheck.Exists) && (aCheck == nil || !aCheck.Exists) {
				suggestedName = cand
				break
			}
		}
	}

	var msgParts []string
	if minioRes.Exists {
		msgParts = append(msgParts, fmt.Sprintf("MinIO: Đã tồn tại dữ liệu (%d bảng: %s, %d files)", len(minioRes.Tables), strings.Join(minioRes.Tables, ", "), minioRes.FilesCount))
	}
	if airbyteRes.Exists {
		var items []string
		if len(airbyteRes.ConnectionNames) > 0 {
			items = append(items, fmt.Sprintf("Connection: %s", strings.Join(airbyteRes.ConnectionNames, ", ")))
		}
		if len(airbyteRes.Sources) > 0 {
			items = append(items, fmt.Sprintf("Source: %s", strings.Join(airbyteRes.Sources, ", ")))
		}
		if len(items) == 0 {
			items = append(items, "Destination/Sync tồn tại")
		}
		msgParts = append(msgParts, fmt.Sprintf("Airbyte: %s", strings.Join(items, " | ")))
	}

	var message string
	if isDuplicate {
		message = fmt.Sprintf("⚠️ Trùng tên: '%s' đã tồn tại trên hệ thống (%s).", cleanID, strings.Join(msgParts, " ; "))
	} else {
		message = fmt.Sprintf("✅ Tên '%s' hợp lệ & khả dụng (chưa tồn tại trên MinIO & Airbyte).", cleanID)
	}

	writeJSON(w, http.StatusOK, TenantCheckResponse{
		TenantID:      cleanID,
		IsDuplicate:   isDuplicate,
		SuggestedName: suggestedName,
		MinIO:         minioRes,
		Airbyte:       airbyteRes,
		Message:       message,
	})
}

