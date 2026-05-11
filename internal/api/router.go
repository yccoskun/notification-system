package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NewRouter initializes the HTTP engine and maps all routes.
func NewRouter(serviceName string, notifHandler *NotificationHandler, wsHub *WSHub) *gin.Engine {
	// Disable default verbose logging for production performance
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	// Core Middlewares
	r.Use(gin.Recovery())
	r.Use(otelgin.Middleware(serviceName))

	// Infrastructure Endpoints (Unversioned)
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Domain Endpoints
	v1 := r.Group("/api/v1")
	{
		v1.POST("/notifications", notifHandler.HandleCreate)
		v1.POST("/notifications/batch", notifHandler.HandleBatchSubmit)
		v1.GET("/notifications/batch/:batch_id", notifHandler.HandleGetBatchStatus)
		v1.GET("/notifications/:id", notifHandler.HandleGetStatus)
		v1.DELETE("/notifications/:id", notifHandler.HandleCancel)
		v1.GET("/ws", wsHub.HandleWebSocket)
	}

	return r
}
