package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // In production, restrict this to your dashboard's domain!
	},
}

type WSHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func NewWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

// Run starts the core WebSocket state machine.
func (h *WSHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.Close()
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				// Push the Redis event down the TCP socket to the browser
				if err := client.WriteMessage(websocket.TextMessage, message); err != nil {
					client.Close()
					delete(h.clients, client)
				}
			}
			h.mu.Unlock()
		}
	}
}

// ListenRedis bridges the gap between Redis Pub/Sub and the local WS clients.
func (h *WSHub) ListenRedis(ctx context.Context, subChannel <-chan *redis.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-subChannel:
			// When a worker publishes to Redis, forward it into the WS broadcast channel
			h.broadcast <- []byte(msg.Payload)
		}
	}
}

// HandleWebSocket upgrades the HTTP request to a persistent WS connection.
func (h *WSHub) HandleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Error("failed to upgrade websocket", "error", err)
		return
	}
	h.register <- conn

	// We must keep a read loop open to detect when the client disconnects,
	// otherwise we leak memory keeping dead connections in the map.
	go func() {
		defer func() { h.unregister <- conn }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}
