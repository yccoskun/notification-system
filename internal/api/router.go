package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NewRouter initializes the HTTP engine with observability middlewares.
func NewRouter(serviceName string) *gin.Engine {
	// In production, we don't want Gin's verbose debug logging console spam
	gin.SetMode(gin.ReleaseMode)

	// We use gin.New() instead of gin.Default() because we want to manually
	// control our middleware stack (omitting the standard, slower Gin logger).
	r := gin.New()

	// 1. Core Middlewares
	r.Use(gin.Recovery())                  // Prevent panics from crashing the entire API pod
	r.Use(otelgin.Middleware(serviceName)) // OpenTelemetry trace injection/extraction

	// 2. Infrastructure Endpoints (Un-versioned)

	// Kubernetes Liveness/Readiness Probe
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	// Prometheus Metrics Scraper
	// We wrap the standard promhttp.Handler() into Gin's handler signature
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	return r
}
