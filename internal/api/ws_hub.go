package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	wsBroadcastBuffer = 512
	wsWriteDeadline   = 10 * time.Second
)

// wsClient is one browser (or tool) connection, optionally filtered to a single notification id.
type wsClient struct {
	conn    *websocket.Conn
	watchID *uuid.UUID
}

// WSHub fans out Redis status events to connected WebSocket clients.
type WSHub struct {
	clients    map[*wsClient]struct{}
	broadcast  chan []byte
	register   chan *wsClient
	unregister chan *wsClient
	mu         sync.Mutex
}

func NewWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*wsClient]struct{}),
		broadcast:  make(chan []byte, wsBroadcastBuffer),
		register:   make(chan *wsClient),
		unregister: make(chan *wsClient),
	}
}

// Run starts the core WebSocket state machine.
func (h *WSHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = struct{}{}
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				_ = client.conn.Close()
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			targetID, idOK := statusUpdateIDFromMessage(message)

			h.mu.Lock()
			snapshot := make([]*wsClient, 0, len(h.clients))
			for c := range h.clients {
				snapshot = append(snapshot, c)
			}
			h.mu.Unlock()

			for _, cli := range snapshot {
				if cli.watchID != nil {
					if !idOK || targetID != cli.watchID.String() {
						continue
					}
				}
				_ = cli.conn.SetWriteDeadline(time.Now().Add(wsWriteDeadline))
				if err := cli.conn.WriteMessage(websocket.TextMessage, message); err != nil {
					h.mu.Lock()
					delete(h.clients, cli)
					_ = cli.conn.Close()
					h.mu.Unlock()
				}
			}
		}
	}
}

func statusUpdateIDFromMessage(msg []byte) (id string, ok bool) {
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil || envelope.ID == "" {
		return "", false
	}
	return envelope.ID, true
}

// ListenRedis bridges Redis Pub/Sub into the hub broadcast channel.
func (h *WSHub) ListenRedis(ctx context.Context, subChannel <-chan *redis.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-subChannel:
			if !ok {
				return
			}
			if msg == nil {
				continue
			}
			payload := []byte(msg.Payload)
			select {
			case h.broadcast <- payload:
			default:
				slog.WarnContext(ctx, "websocket broadcast backlog full; dropping status update",
					"channel", msg.Channel,
					"payload_len", len(payload),
				)
			}
		}
	}
}

func websocketCheckOrigin(r *http.Request) bool {
	allowed := strings.TrimSpace(os.Getenv("WEBSOCKET_ALLOWED_ORIGINS"))
	if allowed == "" || allowed == "*" {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	for _, part := range strings.Split(allowed, ",") {
		if strings.TrimSpace(part) == origin {
			return true
		}
	}
	return false
}

// HandleWebSocket upgrades the HTTP request to a persistent WS connection.
// Optional query: watch=<uuid> — only messages for that notification id are delivered.
func (h *WSHub) HandleWebSocket(c *gin.Context) {
	var watchID *uuid.UUID
	if raw := c.Query("watch"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid watch parameter",
				"details": "watch must be a valid UUID when provided",
			})
			return
		}
		watchID = &id
	}

	upgrader := websocket.Upgrader{
		CheckOrigin:     websocketCheckOrigin,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.ErrorContext(c.Request.Context(), "failed to upgrade websocket", "error", err)
		return
	}

	client := &wsClient{conn: conn, watchID: watchID}
	h.register <- client

	go func() {
		defer func() { h.unregister <- client }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}
