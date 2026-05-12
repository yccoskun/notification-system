package api

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"notification-system/internal/platform/telemetry"
)

// correlationMiddleware generates or propagates X-Request-ID for every HTTP
// request. It creates a request-scoped logger pre-enriched with request_id,
// stores it in the context via telemetry.WithLogger, and echoes the ID back in
// the response header so callers can reference it when reporting issues.
func correlationMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		c.Header("X-Request-ID", requestID)

		logger := telemetry.L(c.Request.Context()).With(
			"request_id", requestID,
			"http.method", c.Request.Method,
			"http.path", c.Request.URL.Path,
		)
		c.Request = c.Request.WithContext(
			telemetry.WithLogger(c.Request.Context(), logger),
		)

		c.Next()
	}
}
