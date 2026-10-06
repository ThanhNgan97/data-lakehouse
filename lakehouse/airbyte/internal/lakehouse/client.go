package lakehouse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"
)

type Config struct {
	Endpoint        string // localhost:9000
	AccessKeyID     string // minioadmin
	SecretAccessKey string // minioadmin
	BucketName      string // university-lakehouse
	UseSSL          bool
}

type Client struct {
	cfg   Config
	minio *minio.Client
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "127.0.0.1:9000"
	}
	cfg.Endpoint = strings.TrimPrefix(cfg.Endpoint, "http://")
	cfg.Endpoint = strings.TrimPrefix(cfg.Endpoint, "https://")

	if cfg.AccessKeyID == "" {
		cfg.AccessKeyID = "minioadmin"
	}
	if cfg.SecretAccessKey == "" {
		cfg.SecretAccessKey = "minioadmin"
	}
	if cfg.BucketName == "" {
		cfg.BucketName = "university-lakehouse"
	}

	mc, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init minio client: %w", err)
	}

	return &Client{
		cfg:   cfg,
		minio: mc,
	}, nil
}

type StreamInfo struct {
	TenantID   string   `json:"tenant_id"`
	TableName  string   `json:"table_name"`
	Prefix     string   `json:"prefix"`
	FilesCount int      `json:"files_count"`
	TotalBytes int64    `json:"total_bytes"`
	Files      []string `json:"files"`
}

// ListStreams lists all table directories inside bronze_archive/ (or bronze/) for all tenants
func (c *Client) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	prefixes := []string{"bronze_archive/", "bronze/"}
	streamMap := make(map[string]*StreamInfo)

	for _, prefix := range prefixes {
		objects := c.minio.ListObjects(ctx, c.cfg.BucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})

		for obj := range objects {
			if obj.Err != nil {
				return nil, obj.Err
			}
			if !strings.HasSuffix(strings.ToLower(obj.Key), ".parquet") {
				continue
			}

			parts := strings.Split(obj.Key, "/")
			if len(parts) < 4 {
				continue
			}

			tenantPart := parts[1] // tenant_id=tenant_client_01
			tablePart := parts[2]  // orders
			tenantID := strings.TrimPrefix(tenantPart, "tenant_id=")

			keyID := tenantID + "::" + tablePart
			stream, ok := streamMap[keyID]
			if !ok {
				stream = &StreamInfo{
					TenantID:   tenantID,
					TableName:  tablePart,
					Prefix:     fmt.Sprintf("%s/%s/%s/", parts[0], parts[1], parts[2]),
					Files:      []string{},
				}
				streamMap[keyID] = stream
			}

			stream.Files = append(stream.Files, obj.Key)
			stream.FilesCount++
			stream.TotalBytes += obj.Size
		}
	}

	var result []StreamInfo
	for _, s := range streamMap {
		sort.Strings(s.Files)
		result = append(result, *s)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].TenantID == result[j].TenantID {
			return result[i].TableName < result[j].TableName
		}
		return result[i].TenantID < result[j].TenantID
	})

	return result, nil
}

// FieldDiff describes a change between two versions
type FieldDiff struct {
	FieldName string `json:"field_name"`
	OldValue  any    `json:"old_value"`
	NewValue  any    `json:"new_value"`
}

// RowVersion represents a single historical state of a row
type RowVersion struct {
	VersionNumber int            `json:"version_number"`
	Operation     string         `json:"operation"` // INSERT, UPDATE, DELETE, UNCHANGED
	ValidFrom     string         `json:"valid_from"`
	ValidTo       *string        `json:"valid_to,omitempty"`
	IsCurrent     bool           `json:"is_current"`
	SourceFile    string         `json:"source_file"`
	Changes       []FieldDiff    `json:"changes,omitempty"`
	Data          map[string]any `json:"data"`
}

// EntityTimeline groups all versions of a specific Primary Key record
type EntityTimeline struct {
	RecordID       string         `json:"record_id"`
	TotalVersions  int            `json:"total_versions"`
	CurrentStatus  string         `json:"current_status"` // ACTIVE, DELETED
	LatestUpdateAt string         `json:"latest_update_at"`
	InitialData    map[string]any `json:"initial_data"`
	LastKnownData  map[string]any `json:"last_known_data"`
	Versions       []RowVersion   `json:"versions"`
}

// TableHistoryResult is the complete query result for a table
type TableHistoryResult struct {
	TenantID       string           `json:"tenant_id"`
	TableName      string           `json:"table_name"`
	FilesRead      int              `json:"files_read"`
	TotalRecords   int              `json:"total_records"`
	UniqueEntities int              `json:"unique_entities"`
	Columns        []string         `json:"columns"`
	Timelines      []EntityTimeline `json:"timelines"`
}

// QueryVersionHistory reads parquet files for a stream and reconstructs version history
func (c *Client) QueryVersionHistory(ctx context.Context, tenantID, tableName, filterID string) (*TableHistoryResult, error) {
	prefixes := []string{
		fmt.Sprintf("bronze_archive/%s/%s/", tenantID, tableName),
		fmt.Sprintf("bronze_archive/tenant_id=%s/%s/", tenantID, tableName),
		fmt.Sprintf("bronze/%s/%s/", tenantID, tableName),
		fmt.Sprintf("bronze/tenant_id=%s/%s/", tenantID, tableName),
	}
	var parquetKeys []string
	for _, prefix := range prefixes {
		objects := c.minio.ListObjects(ctx, c.cfg.BucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})

		for obj := range objects {
			if obj.Err != nil {
				return nil, obj.Err
			}
			if strings.HasSuffix(strings.ToLower(obj.Key), ".parquet") {
				parquetKeys = append(parquetKeys, obj.Key)
			}
		}
		if len(parquetKeys) > 0 {
			break
		}
	}

	sort.Strings(parquetKeys)
	if len(parquetKeys) == 0 {
		return &TableHistoryResult{
			TenantID:  tenantID,
			TableName: tableName,
			Timelines: []EntityTimeline{},
		}, nil
	}

	type rawRecord struct {
		SourceFile string
		EmittedAt  time.Time
		Data       map[string]any
		IDStr      string
		IsDeleted  bool
	}

	var allRawRecords []rawRecord
	colSet := make(map[string]bool)

	for _, key := range parquetKeys {
		obj, err := c.minio.GetObject(ctx, c.cfg.BucketName, key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", key, err)
		}

		dataBytes, err := io.ReadAll(obj)
		_ = obj.Close()
		if err != nil {
			return nil, fmt.Errorf("failed reading bytes from %s: %w", key, err)
		}

		reader := bytes.NewReader(dataBytes)
		pf, err := parquet.OpenFile(reader, int64(len(dataBytes)))
		if err != nil {
			return nil, fmt.Errorf("failed parsing parquet file %s: %w", key, err)
		}

		leafCols := pf.Schema().Columns()

		for _, rg := range pf.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 64)
			for {
				n, err := rows.ReadRows(buf)
				for i := 0; i < n; i++ {
					row := buf[i]
					m := make(map[string]any)
					for _, val := range row {
						if val.IsNull() {
							continue
						}
						cIdx := val.Column()
						if cIdx < len(leafCols) {
							path := leafCols[cIdx]
							colName := path[len(path)-1]

							switch val.Kind() {
							case parquet.Boolean:
								m[colName] = val.Boolean()
							case parquet.Int32:
								m[colName] = val.Int32()
							case parquet.Int64:
								m[colName] = val.Int64()
							case parquet.Float:
								m[colName] = val.Float()
							case parquet.Double:
								m[colName] = val.Double()
							case parquet.ByteArray, parquet.FixedLenByteArray:
								m[colName] = string(val.ByteArray())
							default:
								m[colName] = val.String()
							}
						}
					}

					for k := range m {
						if !strings.HasPrefix(k, "_airbyte_") && k != "_ab_cdc_lsn" {
							colSet[k] = true
						}
					}

					// Primary Key detection
					idVal := ""
					for _, candidate := range []string{"id", "ID", "order_id", "user_id", "customer_id"} {
						if val, exists := m[candidate]; exists && val != nil {
							idVal = fmt.Sprintf("%v", val)
							break
						}
					}
					if idVal == "" {
						for k, v := range m {
							if !strings.HasPrefix(k, "_") {
								idVal = fmt.Sprintf("%v", v)
								break
							}
						}
					}

					// Timestamp detection
					var ts time.Time
					if s, ok := m["_ab_cdc_updated_at"].(string); ok && s != "" {
						if parsed, err := time.Parse(time.RFC3339Nano, s); err == nil {
							ts = parsed
						}
					}
					if ts.IsZero() {
						if micros, ok := m["updated_at"].(int64); ok && micros > 0 {
							ts = time.UnixMicro(micros)
						} else if micros, ok := m["created_at"].(int64); ok && micros > 0 {
							ts = time.UnixMicro(micros)
						} else if millis, ok := m["_airbyte_extracted_at"].(int64); ok && millis > 0 {
							ts = time.UnixMilli(millis)
						}
					}
					if ts.IsZero() {
						ts = time.Now()
					}

					// Check deletion flag
					isDeleted := false
					if delVal, exists := m["_ab_cdc_deleted_at"]; exists && delVal != nil {
						delStr := strings.TrimSpace(fmt.Sprintf("%v", delVal))
						if delStr != "" && delStr != "<nil>" && delStr != "null" {
							isDeleted = true
						}
					}
					if delVal, exists := m["is_deleted"]; exists && delVal != nil {
						if b, ok := delVal.(bool); ok && b {
							isDeleted = true
						} else {
							s := strings.ToLower(fmt.Sprintf("%v", delVal))
							if s == "true" || s == "1" || s == "t" {
								isDeleted = true
							}
						}
					}
					if delVal, exists := m["_op"]; exists && delVal != nil {
						if strings.ToUpper(fmt.Sprintf("%v", delVal)) == "DELETE" {
							isDeleted = true
						}
					}

					// Clean up internal Airbyte raw meta columns for display
					cleanData := make(map[string]any)
					for k, v := range m {
						if !strings.HasPrefix(k, "_airbyte_") && !strings.HasPrefix(k, "_ab_cdc_") {
							// Convert micros timestamps to formatted strings for user-friendly display
							if (strings.HasSuffix(k, "_at") || strings.HasSuffix(k, "_time")) && fmt.Sprintf("%T", v) == "int64" {
								tVal := time.UnixMicro(v.(int64))
								if tVal.Year() > 2000 && tVal.Year() < 2100 {
									cleanData[k] = tVal.Format("2006-01-02 15:04:05")
									continue
								}
							}
							cleanData[k] = v
						}
					}

					allRawRecords = append(allRawRecords, rawRecord{
						SourceFile: key,
						EmittedAt:  ts,
						Data:       cleanData,
						IDStr:      idVal,
						IsDeleted:  isDeleted,
					})
				}
				if err != nil {
					break
				}
			}
			rows.Close()
		}
	}

	// Filter by ID if specified
	var filtered []rawRecord
	for _, r := range allRawRecords {
		if filterID == "" || strings.EqualFold(r.IDStr, filterID) {
			filtered = append(filtered, r)
		}
	}

	// Group by Primary Key ID
	grouped := make(map[string][]rawRecord)
	for _, r := range filtered {
		grouped[r.IDStr] = append(grouped[r.IDStr], r)
	}

	var timelines []EntityTimeline
	for recordID, records := range grouped {
		sort.Slice(records, func(i, j int) bool {
			return records[i].EmittedAt.Before(records[j].EmittedAt)
		})

		var versions []RowVersion
		var prevData map[string]any

		for idx, rec := range records {
			verNum := idx + 1
			op := "UPDATE"
			if verNum == 1 {
				op = "INSERT"
			}
			if rec.IsDeleted {
				op = "DELETE"
			}

			var diffs []FieldDiff
			if prevData != nil {
				for k, newVal := range rec.Data {
					oldVal := prevData[k]
					if fmt.Sprintf("%v", oldVal) != fmt.Sprintf("%v", newVal) {
						diffs = append(diffs, FieldDiff{
							FieldName: k,
							OldValue:  oldVal,
							NewValue:  newVal,
						})
					}
				}
			}

			validFrom := rec.EmittedAt.Format("2006-01-02 15:04:05")
			isCurrent := (idx == len(records)-1)

			ver := RowVersion{
				VersionNumber: verNum,
				Operation:     op,
				ValidFrom:     validFrom,
				IsCurrent:     isCurrent,
				SourceFile:    rec.SourceFile,
				Changes:       diffs,
				Data:          rec.Data,
			}

			if idx > 0 {
				prevValidTo := validFrom
				versions[idx-1].ValidTo = &prevValidTo
				versions[idx-1].IsCurrent = false
			}

			versions = append(versions, ver)
			prevData = rec.Data
		}

		latestTime := ""
		currentStatus := "ACTIVE"
		if len(versions) > 0 {
			latestVer := versions[len(versions)-1]
			latestTime = latestVer.ValidFrom
			if latestVer.Operation == "DELETE" {
				currentStatus = "DELETED"
			}
		}

		var initialData map[string]any
		var lastKnownData map[string]any

		if len(versions) > 0 {
			initialData = versions[0].Data
			// Find the last version with non-empty attributes before deletion (or latest active)
			for i := len(versions) - 1; i >= 0; i-- {
				if len(versions[i].Data) > 0 && versions[i].Operation != "DELETE" {
					lastKnownData = versions[i].Data
					break
				}
			}
			if lastKnownData == nil {
				lastKnownData = versions[len(versions)-1].Data
			}
		}

		timelines = append(timelines, EntityTimeline{
			RecordID:       recordID,
			TotalVersions:  len(versions),
			CurrentStatus:  currentStatus,
			LatestUpdateAt: latestTime,
			InitialData:    initialData,
			LastKnownData:  lastKnownData,
			Versions:       versions,
		})
	}

	sort.Slice(timelines, func(i, j int) bool {
		return timelines[i].RecordID < timelines[j].RecordID
	})

	var cols []string
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)

	return &TableHistoryResult{
		TenantID:       tenantID,
		TableName:      tableName,
		FilesRead:      len(parquetKeys),
		TotalRecords:   len(allRawRecords),
		UniqueEntities: len(timelines),
		Columns:        cols,
		Timelines:      timelines,
	}, nil
}

// MinIOTenantCheck contains check details for MinIO storage
type MinIOTenantCheck struct {
	Exists     bool     `json:"exists"`
	Path       string   `json:"path"`
	Tables     []string `json:"tables"`
	FilesCount int      `json:"files_count"`
	TotalBytes int64    `json:"total_bytes"`
}

// CheckTenantExists checks if a tenant directory or parquet files exist in MinIO
func (c *Client) CheckTenantExists(ctx context.Context, tenantID string) (*MinIOTenantCheck, error) {
	cleanID := strings.TrimSpace(tenantID)
	if cleanID == "" {
		return &MinIOTenantCheck{Exists: false, Tables: []string{}}, nil
	}

	prefixes := []string{
		fmt.Sprintf("bronze_archive/%s/", cleanID),
		fmt.Sprintf("bronze_archive/tenant_id=%s/", cleanID),
		fmt.Sprintf("bronze/%s/", cleanID),
		fmt.Sprintf("bronze/tenant_id=%s/", cleanID),
	}

	tableMap := make(map[string]bool)
	var totalFiles int
	var totalBytes int64
	var foundPath string

	for _, p := range prefixes {
		objects := c.minio.ListObjects(ctx, c.cfg.BucketName, minio.ListObjectsOptions{
			Prefix:    p,
			Recursive: true,
		})

		for obj := range objects {
			if obj.Err != nil {
				return nil, obj.Err
			}
			if strings.HasSuffix(strings.ToLower(obj.Key), ".parquet") {
				if foundPath == "" {
					foundPath = p
				}
				totalFiles++
				totalBytes += obj.Size

				// Extract table name
				trimmed := strings.TrimPrefix(obj.Key, p)
				parts := strings.Split(trimmed, "/")
				if len(parts) > 0 && parts[0] != "" {
					tableMap[parts[0]] = true
				}
			}
		}
	}

	var tables []string
	for t := range tableMap {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	return &MinIOTenantCheck{
		Exists:     totalFiles > 0,
		Path:       foundPath,
		Tables:     tables,
		FilesCount: totalFiles,
		TotalBytes: totalBytes,
	}, nil
}
