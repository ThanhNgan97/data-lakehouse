package airbyte

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/datalakehouse/airbyte-config-tool/internal/model"
)

type BridgeConfig struct {
	Port         int
	AirbyteURL   string
	ClientID     string
	ClientSecret string
}

type BridgeServer struct {
	cfg    BridgeConfig
	client *AirbyteClient
}

type OnboardResponse struct {
	Status              string            `json:"status"`
	Message             string            `json:"message"`
	TenantID            string            `json:"tenant_id"`
	AirbyteSourceID     string            `json:"airbyte_source_id"`
	AirbyteDestID       string            `json:"airbyte_dest_id"`
	AirbyteConnectionID string            `json:"airbyte_connection_id"`
	AirbyteJobID        int64             `json:"airbyte_job_id"`
	JobStatus           string            `json:"job_status"`
	DiscoveredResources []ResourceSummary `json:"discovered_resources"`
}

type ResourceSummary struct {
	ResourceName string `json:"resource_name"`
	TargetView   string `json:"target_view"`
	SyncMode     string `json:"sync_mode"`
	Status       string `json:"status"`
}

func NewBridgeServer(cfg BridgeConfig) *BridgeServer {
	if cfg.Port <= 0 {
		cfg.Port = 9090
	}
	cfg.AirbyteURL = ResolveAirbyteURL(cfg.AirbyteURL)
	if cfg.ClientID == "" {
		cfg.ClientID = "cfcfc672-c3ad-4a59-bf48-abc9bc1efb26"
	}
	if cfg.ClientSecret == "" {
		cfg.ClientSecret = "TyYQbcYAFigm1mrK7H8z50kHCcCVZA3g"
	}
	return &BridgeServer{
		cfg:    cfg,
		client: NewAirbyteClient(cfg.AirbyteURL, cfg.ClientID, cfg.ClientSecret),
	}
}

func (b *BridgeServer) StartServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/airbyte/onboard-source", b.handleOnboardSource)

	addr := fmt.Sprintf(":%d", b.cfg.Port)
	fmt.Printf("\n🚀 Airbyte Official API Bridge listening at %s\n", addr)
	fmt.Printf("🔌 Connected to Airbyte OSS Platform at %s\n", b.cfg.AirbyteURL)
	return http.ListenAndServe(addr, mux)
}

func (b *BridgeServer) handleOnboardSource(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var bundle model.AirbyteConnectionBundle
	if err := json.NewDecoder(r.Body).Decode(&bundle); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON bundle: %v", err), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 180*time.Second)
	defer cancel()

	resp, err := b.OnboardAndSync(ctx, bundle)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ERROR",
			"error":  err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (b *BridgeServer) OnboardAndSync(ctx context.Context, bundle model.AirbyteConnectionBundle) (*OnboardResponse, error) {
	// 1. Health check
	if ok, err := b.client.HealthCheck(ctx); !ok || err != nil {
		return nil, fmt.Errorf("cannot contact Airbyte Server at %s: %v", b.cfg.AirbyteURL, err)
	}

	// 2. Get Workspace
	workspaceID, err := b.client.GetDefaultWorkspace(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving Airbyte workspace: %w", err)
	}

	// Determine host for Docker container (map localhost/127.0.0.1 to host.docker.internal)
	targetHost := bundle.Connection.Host
	if targetHost == "127.0.0.1" || targetHost == "localhost" {
		targetHost = "host.docker.internal"
	}

	sourceType := "postgres"
	engLower := strings.ToLower(bundle.DatabaseEngine)
	if strings.Contains(engLower, "mysql") || strings.Contains(engLower, "mariadb") {
		sourceType = "mysql"
	} else if strings.Contains(engLower, "mssql") || strings.Contains(engLower, "sqlserver") {
		sourceType = "mssql"
	}

	// 3. Create Source via Public API
	sourceConfig := map[string]any{
		"sourceType": sourceType,
		"host":       targetHost,
		"port":       bundle.Connection.Port,
		"database":   bundle.Connection.Database,
		"username":   bundle.Connection.Username,
		"password":   bundle.Connection.Password,
		"tunnel_method": map[string]any{
			"tunnel_method": "NO_TUNNEL",
		},
	}

	if sourceType == "mysql" {
		sourceConfig["ssl_mode"] = map[string]any{
			"mode": "preferred",
		}
		if bundle.SyncStrategy == model.SyncStrategyCDC {
			sourceConfig["replication_method"] = map[string]any{
				"method": "CDC",
				"initial_waiting_seconds": 30,
			}
		} else {
			sourceConfig["replication_method"] = map[string]any{
				"method": "STANDARD",
			}
		}
	} else if sourceType == "mssql" {
		schema := bundle.Connection.Schema
		if schema == "" {
			schema = "dbo"
		}
		sourceConfig["schemas"] = []string{schema}
		if bundle.Connection.SSLMode == "require" || bundle.Connection.SSLMode == "verify-ca" || bundle.Connection.SSLMode == "verify-full" {
			sourceConfig["ssl_method"] = map[string]any{
				"ssl_method": "encrypted_trust_server_certificate",
			}
		} else {
			sourceConfig["ssl_method"] = map[string]any{
				"ssl_method": "unencrypted",
			}
		}
		if bundle.SyncStrategy == model.SyncStrategyCDC {
			sourceConfig["replication_method"] = map[string]any{
				"method": "CDC",
				"initial_waiting_seconds": 30,
			}
		} else {
			sourceConfig["replication_method"] = map[string]any{
				"method": "Standard",
			}
		}
	} else {
		sourceConfig["schemas"] = []string{bundle.Connection.Schema}
		sourceConfig["ssl_mode"] = map[string]any{
			"mode": "disable",
		}
		if bundle.SyncStrategy == model.SyncStrategyCDC {
			pubName := bundle.PublicationName
			if pubName == "" {
				pubName = fmt.Sprintf("airbyte_pub_%s", bundle.TenantID)
			}
			pubName = sanitizePostgresIdent(pubName)
			slotName := bundle.ReplicationSlot
			if slotName == "" {
				slotName = fmt.Sprintf("airbyte_slot_%s", bundle.TenantID)
			}
			slotName = sanitizePostgresSlotName(slotName)
			sourceConfig["replication_method"] = map[string]any{
				"method": "CDC",
				"plugin": "pgoutput",
				"publication": pubName,
				"replication_slot": slotName,
				"snapshot_mode": "initial",
				"initial_waiting_seconds": 30,
			}
		} else {
			sourceConfig["replication_method"] = map[string]any{
				"method": "Standard",
			}
		}
	}

	tenantID := sanitizePostgresIdent(bundle.TenantID)
	if tenantID == "" {
		tenantID = fmt.Sprintf("tenant_%s_%s", bundle.DatabaseEngine, bundle.Connection.Database)
	}

	sourceName := fmt.Sprintf("Source_%s_%s_%s", tenantID, bundle.DatabaseEngine, bundle.Connection.Schema)
	sourceID, err := b.client.FindSourceByName(ctx, workspaceID, sourceName)
	if err != nil || sourceID == "" {
		sourceID, err = b.client.CreateSource(ctx, workspaceID, sourceName, sourceConfig)
		if err != nil {
			return nil, fmt.Errorf("Airbyte source creation failed: %w", err)
		}
	} else {
		_ = b.client.UpdateSource(ctx, sourceID, sourceConfig)
	}

	// 4. Create or Find Destination (MinIO S3 Parquet) via Public API
	bucketName := bundle.MinIODestination.RecommendedBucket
	if bucketName == "" {
		bucketName = "university-lakehouse"
	}
	destPath := fmt.Sprintf("staging/%s", tenantID)
	if bundle.MinIODestination.RecommendedPath != "" {
		cleanP := strings.TrimRight(bundle.MinIODestination.RecommendedPath, "/")
		cleanP = strings.TrimSuffix(cleanP, "/table=${STREAM_NAME}")
		cleanP = strings.TrimSuffix(cleanP, "/${STREAM_NAME}")
		cleanP = strings.TrimSuffix(cleanP, "table=${STREAM_NAME}")
		cleanP = strings.TrimRight(cleanP, "/")
		cleanP = strings.ReplaceAll(cleanP, "/tenant_id=", "/")
		cleanP = strings.TrimPrefix(cleanP, "tenant_id=")
		if cleanP != "" {
			destPath = cleanP
		}
	}
	destName := fmt.Sprintf("MinIO_Lakehouse_%s", tenantID)

	destConfig := map[string]any{
		"destinationType":   "s3",
		"s3_endpoint":       "http://host.docker.internal:9000",
		"s3_bucket_name":    bucketName,
		"s3_bucket_region":  "",
		"s3_bucket_path":    destPath,
		"access_key_id":     "minioadmin",
		"secret_access_key": "minioadmin",
		"s3_path_format":    "${STREAM_NAME}/part_",
		"format": map[string]any{
			"format_type":       "Parquet",
			"compression_codec": "SNAPPY",
		},
	}

	destID, err := b.client.FindDestinationByName(ctx, workspaceID, destName)
	if err != nil || destID == "" {
		destID, err = b.client.CreateDestination(ctx, workspaceID, destName, destConfig)
		if err != nil {
			return nil, fmt.Errorf("Airbyte destination creation failed: %w", err)
		}
	} else {
		_ = b.client.UpdateDestination(ctx, destID, destConfig)
	}

	// 5. Build stream configs for Incremental Append
	var streamConfigs []map[string]any
	for _, v := range bundle.ExposedViews {
		if bundle.SyncStrategy == model.SyncStrategyCDC {
			streamConfigs = append(streamConfigs, map[string]any{
				"name":     v.SourceTable,
				"syncMode": "incremental_append",
			})
		} else {
			cursor := v.CursorColumn
			if cursor == "" {
				cursor = "updated_at"
			}
			streamConfigs = append(streamConfigs, map[string]any{
				"name":        v.TargetView,
				"syncMode":    "incremental_append",
				"cursorField": []string{cursor},
			})
		}
	}

	// 6. Create or Find Connection
	connName := fmt.Sprintf("Sync_%s_to_MinIO", tenantID)
	connID, err := b.client.FindConnectionBySourceAndDest(ctx, sourceID, destID)
	if err != nil || connID == "" {
		connID, err = b.client.CreateConnection(ctx, connName, sourceID, destID, streamConfigs)
		if err != nil {
			return nil, fmt.Errorf("Airbyte connection creation failed: %w", err)
		}
	}

	// 7. Trigger Sync
	jobID, err := b.client.TriggerSync(ctx, connID)
	if err != nil {
		return nil, fmt.Errorf("Airbyte sync trigger failed: %w", err)
	}

	var discoveredList []ResourceSummary
	for _, v := range bundle.ExposedViews {
		discoveredList = append(discoveredList, ResourceSummary{
			ResourceName: v.SourceTable,
			TargetView:   v.TargetView,
			SyncMode:     v.SyncMode,
			Status:       "SYNC_INITIATED",
		})
	}

	return &OnboardResponse{
		Status:              "SUCCESS",
		Message:             fmt.Sprintf("Airbyte OSS đã kết nối thành công tới database nguồn (%d Views), tự động cấu hình MinIO Parquet Destination và đã kích hoạt Sync Job #%d!", len(discoveredList), jobID),
		TenantID:            bundle.TenantID,
		AirbyteSourceID:     sourceID,
		AirbyteDestID:       destID,
		AirbyteConnectionID: connID,
		AirbyteJobID:        jobID,
		JobStatus:           "RUNNING",
		DiscoveredResources: discoveredList,
	}, nil
}

func sanitizePostgresSlotName(name string) string {
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

func sanitizePostgresIdent(name string) string {
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

