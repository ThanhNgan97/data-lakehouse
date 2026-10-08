package airbyte

import (
	"encoding/json"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

type BridgeStatus struct {
	Status              string            `json:"status"`
	Message             string            `json:"message,omitempty"`
	TenantID            string            `json:"tenant_id,omitempty"`
	DatabaseEngine      string            `json:"database_engine,omitempty"`
	AirbyteSourceID     string            `json:"airbyte_source_id,omitempty"`
	AirbyteDestID       string            `json:"airbyte_dest_id,omitempty"`
	AirbyteConnectionID string            `json:"airbyte_connection_id,omitempty"`
	AirbyteJobID        int64             `json:"airbyte_job_id,omitempty"`
	JobStatus           string            `json:"job_status,omitempty"`
	AirflowStatus       string            `json:"airflow_status,omitempty"`
	DiscoveredResources []ResourceSummary `json:"discovered_resources,omitempty"`
	UpdatedAt           string            `json:"updated_at"`
}

type bridgeStatusHub struct {
	mu          sync.RWMutex
	state       BridgeStatus
	subscribers map[chan []byte]struct{}
}

func newBridgeStatusHub() *bridgeStatusHub {
	return &bridgeStatusHub{
		state:       BridgeStatus{Status: "READY", JobStatus: "IDLE", UpdatedAt: time.Now().UTC().Format(time.RFC3339)},
		subscribers: make(map[chan []byte]struct{}),
	}
}

func (h *bridgeStatusHub) update(next BridgeStatus) {
	next.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	payload, _ := json.Marshal(next)
	h.mu.Lock()
	h.state = next
	for subscriber := range h.subscribers {
		select {
		case subscriber <- payload:
		default:
		}
	}
	h.mu.Unlock()
}

func (h *bridgeStatusHub) snapshot() BridgeStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

func (h *bridgeStatusHub) setJobStatus(jobID int64, status string, message string) {
	h.mu.RLock()
	next := h.state
	h.mu.RUnlock()
	if next.AirbyteJobID != jobID {
		return
	}
	next.JobStatus = status
	if message != "" {
		next.Message = message
	}
	switch status {
	case "SUCCEEDED":
		next.Status = "SUCCESS"
	case "FAILED", "CANCELLED", "INCOMPLETE":
		next.Status = "ERROR"
	default:
		next.Status = "SYNCING"
	}
	h.update(next)
}

func (h *bridgeStatusHub) setAirflowStatus(jobID int64, status string, message string) {
	h.mu.RLock()
	next := h.state
	h.mu.RUnlock()
	if next.AirbyteJobID != jobID {
		return
	}
	next.AirflowStatus = status
	if message != "" {
		next.Message = message
	}
	h.update(next)
}

func (b *BridgeServer) handleStatusEvents(conn *websocket.Conn) {
	updates := make(chan []byte, 8)
	b.statusHub.mu.Lock()
	b.statusHub.subscribers[updates] = struct{}{}
	initial, _ := json.Marshal(b.statusHub.state)
	b.statusHub.mu.Unlock()
	defer func() {
		b.statusHub.mu.Lock()
		delete(b.statusHub.subscribers, updates)
		b.statusHub.mu.Unlock()
		close(updates)
		_ = conn.Close()
	}()

	if _, err := conn.Write(initial); err != nil {
		return
	}
	for payload := range updates {
		if _, err := conn.Write(payload); err != nil {
			return
		}
	}
}
