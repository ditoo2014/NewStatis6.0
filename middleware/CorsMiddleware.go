// Package middleware 提供 Gin 服务使用的通用 HTTP 中间件。
package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Cors 设置跨域请求头，并直接结束浏览器发起的 OPTIONS 预检请求。
func Cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "x-projectid, Content-Type, AccessToken, X-CSRF-Token, Authorization, Token")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type, Content-Disposition")
		c.Header("Access-Control-Allow-Credentials", "true")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
