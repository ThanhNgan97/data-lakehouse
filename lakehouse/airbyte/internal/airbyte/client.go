package airbyte

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// ResolveAirbyteURL resolves the Airbyte URL with support for AIRBYTE_URL, AIRBYTE_PORT,
// explicit preference, and automatic port fallback (8000 -> 8006 -> 8001).
func ResolveAirbyteURL(preferred string) string {
	if envURL := os.Getenv("AIRBYTE_URL"); envURL != "" {
		return strings.TrimRight(envURL, "/")
	}
	if envPort := os.Getenv("AIRBYTE_PORT"); envPort != "" {
		return fmt.Sprintf("http://localhost:%s", envPort)
	}
	if preferred != "" && preferred != "http://localhost:8000" {
		return strings.TrimRight(preferred, "/")
	}

	// Auto-probe candidate ports: 8000 (default abctl), 8006 (custom compose webapp), 8001 (direct server)
	candidates := []string{
		"http://localhost:8000",
		"http://localhost:8006",
		"http://localhost:8001",
	}

	for _, cand := range candidates {
		host := strings.TrimPrefix(cand, "http://")
		conn, err := net.DialTimeout("tcp", host, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return cand
		}
	}

	if preferred != "" {
		return strings.TrimRight(preferred, "/")
	}
	return "http://localhost:8000"
}

type AirbyteClient struct {
	baseURL      string
	clientID     string
	clientSecret string
	token        string
	tokenExpiry  time.Time
	tokenMu      sync.RWMutex
	httpClient   *http.Client
}

func NewAirbyteClient(baseURL, clientID, clientSecret string) *AirbyteClient {
	baseURL = ResolveAirbyteURL(baseURL)
	return &AirbyteClient{
		baseURL:      baseURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		httpClient: &http.Client{
			Timeout: 180 * time.Second,
		},
	}
}

func (c *AirbyteClient) ensureToken(ctx context.Context) (string, error) {
	if c.clientID == "" || c.clientSecret == "" {
		return "", nil
	}

	c.tokenMu.RLock()
	if c.token != "" && time.Now().Before(c.tokenExpiry.Add(-30*time.Second)) {
		t := c.token
		c.tokenMu.RUnlock()
		return t, nil
	}
	c.tokenMu.RUnlock()

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if c.token != "" && time.Now().Before(c.tokenExpiry.Add(-30*time.Second)) {
		return c.token, nil
	}

	tokenURL := fmt.Sprintf("%s/api/v1/applications/token", c.baseURL)
	reqBody := map[string]string{
		"client_id":     c.clientID,
		"client_secret": c.clientSecret,
	}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to request Airbyte token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("Airbyte token request failed [%d]: %s", resp.StatusCode, string(b))
	}

	var res struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", fmt.Errorf("failed to decode token response: %w", err)
	}

	c.token = res.AccessToken
	c.tokenExpiry = time.Now().Add(24 * time.Hour)
	return c.token, nil
}

func (c *AirbyteClient) request(ctx context.Context, method, path string, reqBody any, out any) error {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s", c.baseURL, strings.TrimLeft(path, "/"))
	var bodyReader io.Reader
	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Airbyte API request failed (%s): %w", path, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read Airbyte response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Airbyte API error [%d] at %s: %s", resp.StatusCode, path, string(respBytes))
	}

	if out != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, out); err != nil {
			return fmt.Errorf("failed to parse Airbyte response: %w", err)
		}
	}
	return nil
}

// HealthCheck checks if Airbyte Server is healthy
func (c *AirbyteClient) HealthCheck(ctx context.Context) (bool, error) {
	var res map[string]any
	// Try public API or root
	err := c.request(ctx, http.MethodGet, "api/public/v1/workspaces", nil, &res)
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetDefaultWorkspace retrieves the first active workspace ID
func (c *AirbyteClient) GetDefaultWorkspace(ctx context.Context) (string, error) {
	var res struct {
		Data []struct {
			WorkspaceID string `json:"workspaceId"`
			Name        string `json:"name"`
		} `json:"data"`
	}

	if err := c.request(ctx, http.MethodGet, "api/public/v1/workspaces", nil, &res); err != nil {
		return "", err
	}

	if len(res.Data) == 0 {
		return "", fmt.Errorf("no active workspaces found in Airbyte")
	}

	return res.Data[0].WorkspaceID, nil
}

// CreateSource creates a source (Postgres/MySQL) via Public API
func (c *AirbyteClient) CreateSource(ctx context.Context, workspaceID, name string, config map[string]any) (string, error) {
	req := map[string]any{
		"workspaceId":   workspaceID,
		"name":          name,
		"configuration": config,
	}

	var res struct {
		SourceID string `json:"sourceId"`
	}

	if err := c.request(ctx, http.MethodPost, "api/public/v1/sources", req, &res); err != nil {
		return "", err
	}

	return res.SourceID, nil
}

// CreateDestination creates an S3/MinIO destination via Public API
func (c *AirbyteClient) CreateDestination(ctx context.Context, workspaceID, name string, config map[string]any) (string, error) {
	req := map[string]any{
		"workspaceId":   workspaceID,
		"name":          name,
		"configuration": config,
	}

	var res struct {
		DestinationID string `json:"destinationId"`
	}

	if err := c.request(ctx, http.MethodPost, "api/public/v1/destinations", req, &res); err != nil {
		return "", err
	}

	return res.DestinationID, nil
}

// CreateConnection creates a connection between source and destination
func (c *AirbyteClient) CreateConnection(ctx context.Context, name, sourceID, destID string, streams []map[string]any) (string, error) {
	req := map[string]any{
		"name":          name,
		"sourceId":      sourceID,
		"destinationId": destID,
	}
	if len(streams) > 0 {
		req["configurations"] = map[string]any{
			"streams": streams,
		}
	}

	var res struct {
		ConnectionID string `json:"connectionId"`
	}

	if err := c.request(ctx, http.MethodPost, "api/public/v1/connections", req, &res); err != nil {
		return "", err
	}

	return res.ConnectionID, nil
}

// TriggerSync triggers an immediate sync job for a connection
func (c *AirbyteClient) TriggerSync(ctx context.Context, connectionID string) (int64, error) {
	req := map[string]any{
		"connectionId": connectionID,
		"jobType":      "sync",
	}

	var res struct {
		JobID int64 `json:"jobId"`
	}

	if err := c.request(ctx, http.MethodPost, "api/public/v1/jobs", req, &res); err != nil {
		return 0, err
	}

	return res.JobID, nil
}

// PollJobStatus checks the job status
func (c *AirbyteClient) PollJobStatus(ctx context.Context, jobID int64) (string, error) {
	var res struct {
		Status string `json:"status"`
	}

	path := fmt.Sprintf("api/public/v1/jobs/%d", jobID)
	if err := c.request(ctx, http.MethodGet, path, nil, &res); err != nil {
		return "", err
	}

	return res.Status, nil
}

// FindDestinationByName finds an existing destination with the specified name
func (c *AirbyteClient) FindDestinationByName(ctx context.Context, workspaceID, name string) (string, error) {
	path := fmt.Sprintf("api/public/v1/destinations?workspaceIds=%s", workspaceID)
	var res struct {
		Data []struct {
			DestinationID string `json:"destinationId"`
			Name          string `json:"name"`
		} `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, path, nil, &res); err != nil {
		return "", err
	}
	for _, d := range res.Data {
		if d.Name == name {
			return d.DestinationID, nil
		}
	}
	return "", nil
}

// FindSourceByName finds an existing source with the specified name
func (c *AirbyteClient) FindSourceByName(ctx context.Context, workspaceID, name string) (string, error) {
	path := fmt.Sprintf("api/public/v1/sources?workspaceIds=%s", workspaceID)
	var res struct {
		Data []struct {
			SourceID string `json:"sourceId"`
			Name     string `json:"name"`
		} `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, path, nil, &res); err != nil {
		return "", err
	}
	for _, s := range res.Data {
		if s.Name == name {
			return s.SourceID, nil
		}
	}
	return "", nil
}

// FindConnectionBySourceAndDest finds an existing connection between source and destination
func (c *AirbyteClient) FindConnectionBySourceAndDest(ctx context.Context, sourceID, destID string) (string, error) {
	path := "api/public/v1/connections"
	var res struct {
		Data []struct {
			ConnectionID  string `json:"connectionId"`
			SourceID      string `json:"sourceId"`
			DestinationID string `json:"destinationId"`
		} `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, path, nil, &res); err != nil {
		return "", err
	}
	for _, conn := range res.Data {
		if conn.SourceID == sourceID && conn.DestinationID == destID {
			return conn.ConnectionID, nil
		}
	}
	return "", nil
}

// UpdateDestination updates an existing destination
func (c *AirbyteClient) UpdateDestination(ctx context.Context, destID string, config map[string]any) error {
	req := map[string]any{
		"configuration": config,
	}
	path := fmt.Sprintf("api/public/v1/destinations/%s", destID)
	return c.request(ctx, http.MethodPatch, path, req, nil)
}

// UpdateSource updates an existing source
func (c *AirbyteClient) UpdateSource(ctx context.Context, sourceID string, config map[string]any) error {
	req := map[string]any{
		"configuration": config,
	}
	path := fmt.Sprintf("api/public/v1/sources/%s", sourceID)
	return c.request(ctx, http.MethodPatch, path, req, nil)
}

type AirbyteTenantCheck struct {
	Exists          bool     `json:"exists"`
	Sources         []string `json:"sources"`
	Destinations    []string `json:"destinations"`
	ConnectionNames []string `json:"connection_names"`
}

// CheckTenant searches if any sources, destinations, or connections contain the tenant identifier
func (c *AirbyteClient) CheckTenant(ctx context.Context, tenantID string) (*AirbyteTenantCheck, error) {
	res := &AirbyteTenantCheck{
		Sources:         []string{},
		Destinations:    []string{},
		ConnectionNames: []string{},
	}
	cleanID := strings.ToLower(strings.TrimSpace(tenantID))
	if cleanID == "" {
		return res, nil
	}

	// 1. Sources
	var sourcesRes struct {
		Data []struct {
			SourceID string `json:"sourceId"`
			Name     string `json:"name"`
		} `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, "api/public/v1/sources", nil, &sourcesRes); err == nil {
		for _, s := range sourcesRes.Data {
			if strings.Contains(strings.ToLower(s.Name), cleanID) {
				res.Exists = true
				res.Sources = append(res.Sources, s.Name)
			}
		}
	}

	// 2. Destinations
	var destsRes struct {
		Data []struct {
			DestinationID string `json:"destinationId"`
			Name          string `json:"name"`
		} `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, "api/public/v1/destinations", nil, &destsRes); err == nil {
		for _, d := range destsRes.Data {
			if strings.Contains(strings.ToLower(d.Name), cleanID) {
				res.Exists = true
				res.Destinations = append(res.Destinations, d.Name)
			}
		}
	}

	// 3. Connections
	var connsRes struct {
		Data []struct {
			ConnectionID string `json:"connectionId"`
			Name         string `json:"name"`
		} `json:"data"`
	}
	if err := c.request(ctx, http.MethodGet, "api/public/v1/connections", nil, &connsRes); err == nil {
		for _, conn := range connsRes.Data {
			if strings.Contains(strings.ToLower(conn.Name), cleanID) {
				res.Exists = true
				res.ConnectionNames = append(res.ConnectionNames, conn.Name)
			}
		}
	}

	return res, nil
}

