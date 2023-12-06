package middleware

import (
	"bytes"
	"github.com/Darker-D/ddbase/net/http"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"io"
	"io/ioutil"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/Darker-D/ddbase/log"
	"github.com/Darker-D/ddbase/net/metadata"
	"github.com/Darker-D/ddbase/net/stat"
)

var (
	stats = stat.HTTPServer
)

type responseWriter struct {
	gin.ResponseWriter
	Body *bytes.Buffer
}

func (rw responseWriter) Write(b []byte) (int, error) {
	rw.Body.Write(b)
	return rw.ResponseWriter.Write(b)
}

// Log loger
func Log() gin.HandlerFunc {
	return func(c *gin.Context) {

		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		var traceID string
		ctx := http.ToContext(c)
		span := opentracing.SpanFromContext(ctx)
		if span != nil {
			switch span.Context().(type) {
			case jaeger.SpanContext:
				traceID = span.Context().(jaeger.SpanContext).TraceID().String()
			}
		}

		// 请求内容
		bodybuf := new(bytes.Buffer)
		_, _ = io.Copy(bodybuf, c.Request.Body)
		body := bodybuf.Bytes()
		c.Request.Body = ioutil.NopCloser(bytes.NewReader(body))

		// 输出内容
		rw := &responseWriter{
			Body:           bytes.NewBufferString(""),
			ResponseWriter: c.Writer,
		}
		c.Writer = rw

		fieldsBefore := []zapcore.Field{
			zap.String(metadata.Trace, traceID),
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.Any("body", string(body)),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.Any("header", c.Request.Header),
		}
		log.Logger().Named("api").Info("request_before", fieldsBefore...)
		_ = c.Request.ParseForm()
		_ = c.Request.ParseMultipartForm(2 << 32)
		c.Next()

		latency := time.Since(start)
		// 统计
		caller := c.Request.Header.Get("caller")
		if caller == "" {
			caller = "no_user"
		}
		stats.Incr(caller, path[1:], c.Errors.String())
		stats.Timing(caller, int64(latency/time.Millisecond), path[1:])

		fieldsEnd := []zapcore.Field{
			zap.String("path", path),
			zap.String(metadata.Trace, traceID),
			zap.Float64("latency", latency.Seconds()),
			zap.String("latency_human", latency.String()),
		}
		if log.Logger().LevelType("debug") == log.Logger().GetLevel() {
			fieldsEnd = append(fieldsEnd, zap.String("response", rw.Body.String()))
		}
		if len(c.Errors) > 0 {
			for _, err := range c.Errors {
				fieldsEnd = append(fieldsEnd, zap.Error(err))
				log.Logger().Named(path).Error("request_after",
					fieldsEnd...)
			}
			c.Abort()
			return
		}

		log.Logger().Named("api").Info("request_after", fieldsEnd...)
	}
}

// DebugPrintRouteFunc route debug log
func DebugPrintRouteFunc(httpMethod, absolutePath, handlerName string, nuHandlers int) {
	log.Logger().Named("gin.debug").Debug("route debug",
		zap.String("method", httpMethod),
		zap.String("path", absolutePath),
		zap.String("handler", handlerName),
		zap.Int("handler_number", nuHandlers),
	)
}
