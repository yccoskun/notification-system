package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"notification-system/docs"
	"notification-system/internal/platform/telemetry"
)

// prometheusMiddleware records HTTP request duration and in-flight count for
// every domain route. Infrastructure endpoints (/health, /metrics) are skipped
// so they don't pollute latency percentiles.
func prometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqPath := c.Request.URL.Path
		if reqPath == "/health" || reqPath == "/metrics" || strings.HasPrefix(reqPath, "/docs") {
			c.Next()
			return
		}

		telemetry.HTTPRequestsInFlight.Inc()
		start := time.Now()

		c.Next()

		telemetry.HTTPRequestsInFlight.Dec()
		route := c.FullPath()
		if route == "" {
			route = reqPath
		}
		telemetry.HTTPRequestDuration.WithLabelValues(
			c.Request.Method,
			route,
			strconv.Itoa(c.Writer.Status()),
		).Observe(time.Since(start).Seconds())
	}
}

// NewRouter initializes the HTTP engine and maps all routes.
func NewRouter(serviceName string, notifHandler *NotificationHandler, wsHub *WSHub) *gin.Engine {
	// Disable default verbose logging for production performance
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	// Core Middlewares
	r.Use(gin.Recovery())
	r.Use(otelgin.Middleware(serviceName)) // must run first: populates OTel span so downstream middleware can read trace_id
	r.Use(correlationMiddleware())         // attaches request_id + per-request logger to context
	r.Use(prometheusMiddleware())

	// Infrastructure Endpoints (Unversioned)
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	docs.RegisterSwaggerUI(r)

	// Domain Endpoints
	v1 := r.Group("/api/v1")
	{
		v1.POST("/notifications", notifHandler.HandleCreate)
		v1.GET("/notifications", notifHandler.HandleList)
		v1.POST("/notifications/batch", notifHandler.HandleBatchSubmit)
		v1.GET("/notifications/batch/:batch_id", notifHandler.HandleGetBatchStatus)
		v1.GET("/notifications/:id", notifHandler.HandleGetStatus)
		v1.DELETE("/notifications/:id", notifHandler.HandleCancel)
		v1.GET("/ws", wsHub.HandleWebSocket)
	}

	return r
}
