package middleware

import (
	"github.com/Darker-D/ddbase/ecode"
	"github.com/Darker-D/ddbase/log"
	"github.com/Darker-D/ddbase/net/http"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	// YGHEADER header key .
	YGHEADER = "Yg-Header-Hint-Content"
	// YGUSERAGENT user-agent key .
	YGUSERAGENT = "User-Agent"
)

// HgHeader .
func HgHeader() gin.HandlerFunc {
	return func(c *gin.Context) {
		headerText := c.Request.Header.Get(YGHEADER)
		header, err := http.YgHeaderUnmarshal(headerText)
		if err != nil {
			log.Logger().Error("middleware.HgHeader", zap.Error(err), zap.String("header", headerText))
			http.JSON(c, nil, ecode.YgHeaderErr)
			c.Abort()
			return
		}
		c.Set(YGHEADER, header)
		c.Next()
	}
}

// HgUserAgent .
func HgUserAgent() gin.HandlerFunc {
	return func(c *gin.Context) {
		userAgentText := c.Request.Header.Get(YGUSERAGENT)
		userAgent, err := http.YgUAUnmarshal(userAgentText)
		if err != nil {
			log.Logger().Error("middleware.HgUserAgent", zap.Error(err), zap.String("user-agent", userAgentText))
			http.JSON(c, nil, ecode.YgUserAgentErr)
			c.Abort()
			return
		}
		c.Set(YGHEADER, userAgent)
		c.Next()
	}
}
