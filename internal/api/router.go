package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NewRouter initializes the HTTP engine and maps all routes.
func NewRouter(serviceName string, notifHandler *NotificationHandler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// 1. Core Middlewares
	r.Use(gin.Recovery())                  // Prevent panics from crashing the entire API pod
	r.Use(otelgin.Middleware(serviceName)) // OpenTelemetry trace injection/extraction

	// Kubernetes Liveness/Readiness Probe
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	// Prometheus Metrics Scraper
	// We wrap the standard promhttp.Handler() into Gin's handler signature
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Domain Endpoints
	v1 := r.Group("/api/v1")
	{
		// Map the handler to the POST route
		v1.POST("/notifications/batch", notifHandler.HandleBatchSubmit)
	}

	return r
}
