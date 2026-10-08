package server

import (
	"encoding/json"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// PortalState is deliberately credential-free. It is safe to expose to the
// local user portal so users do not have to move the generated bundle by hand.
type PortalState struct {
	Status         string   `json:"status"`
	DatabaseEngine string   `json:"database_engine,omitempty"`
	DatabaseName   string   `json:"database_name,omitempty"`
	TenantID       string   `json:"tenant_id,omitempty"`
	TableCount     int      `json:"table_count"`
	Tables         []string `json:"tables,omitempty"`
	Message        string   `json:"message,omitempty"`
	UpdatedAt      string   `json:"updated_at"`
}

type portalStateHub struct {
	mu          sync.RWMutex
	state       PortalState
	subscribers map[chan []byte]struct{}
}

func newPortalStateHub() *portalStateHub {
	return &portalStateHub{
		state:       PortalState{Status: "WAITING", UpdatedAt: time.Now().UTC().Format(time.RFC3339)},
		subscribers: make(map[chan []byte]struct{}),
	}
}

func (h *portalStateHub) update(next PortalState) {
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

func (h *portalStateHub) snapshot() PortalState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

func (s *Server) handlePortalEvents(conn *websocket.Conn) {
	updates := make(chan []byte, 8)
	s.portalHub.mu.Lock()
	s.portalHub.subscribers[updates] = struct{}{}
	initial, _ := json.Marshal(s.portalHub.state)
	s.portalHub.mu.Unlock()
	defer func() {
		s.portalHub.mu.Lock()
		delete(s.portalHub.subscribers, updates)
		s.portalHub.mu.Unlock()
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
