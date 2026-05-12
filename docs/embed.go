package docs

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed openapi.yml
var openAPIYAML []byte

//go:embed swagger.html
var swaggerHTML []byte

// RegisterSwaggerUI serves embedded OpenAPI YAML and Swagger UI at /docs/ (Try it out uses the current host).
func RegisterSwaggerUI(r *gin.Engine) {
	r.GET("/docs/openapi.yml", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "application/yaml; charset=utf-8", openAPIYAML)
	})
	r.GET("/docs/", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "text/html; charset=utf-8", swaggerHTML)
	})
	r.GET("/docs", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/docs/")
	})
}
