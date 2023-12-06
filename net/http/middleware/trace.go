package middleware

import (
	"github.com/Darker-D/ddbase/net/http"
	"github.com/gin-gonic/gin"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// Trace is trace middleware
func Trace() gin.HandlerFunc {
	return func(c *gin.Context) {

		spanCtx, err := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(c.Request.Header))
		var span opentracing.Span
		if err != nil {
			span = opentracing.GlobalTracer().StartSpan(c.Request.URL.Path)
		} else {
			span = opentracing.GlobalTracer().StartSpan(c.Request.URL.Path, ext.RPCServerOption(spanCtx))
		}

		ext.Component.Set(span, "net/http")
		ext.SpanKind.Set(span, ext.SpanKindRPCServerEnum)

		ext.HTTPMethod.Set(span, c.Request.Method)
		ext.HTTPUrl.Set(span, c.Request.URL.String())

		c.Set(http.TraceKey, span)
		c.Next()
		span.Finish()
	}
}
