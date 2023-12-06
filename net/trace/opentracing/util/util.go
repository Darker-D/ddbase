package util

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
)

const NoTrace = "no_trace"

// TraceIDFromSpanContext .
func TraceIDFromSpanContext(spanCtx opentracing.SpanContext) string {
	var traceId string
	switch spanCtx.(type) {
	case jaeger.SpanContext:
		traceId = spanCtx.(jaeger.SpanContext).TraceID().String()
	}
	return traceId
}

// TraceIDFromContext get trace id from context.
func TraceIDFromContext(ctx context.Context) string {

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return NoTrace
	}
	var traceId string
	switch span.Context().(type) {
	case jaeger.SpanContext:
		traceId = span.Context().(jaeger.SpanContext).TraceID().String()
	}
	return traceId
}
