package httptrace

import (
	"io"
	"net/http"
	"net/http/httptrace"

	"github.com/opentracing/opentracing-go"

	"github.com/Darker-D/ddbase/net/trace/opentracing/ext"
	"github.com/Darker-D/ddbase/strings"
)

const (
	_defaultComponentName = "net/http"
)

type closeTracker struct {
	io.ReadCloser
	span opentracing.Span
}

func (c *closeTracker) Close() error {
	err := c.ReadCloser.Close()
	ext.LogEvent.Set(c.span, "ClosedBody")
	c.span.Finish()
	return err
}

// NewTraceTracesport NewTraceTracesport
func NewTraceTracesport(rt http.RoundTripper, peerService string, internalTags ...opentracing.Tag) *TraceTransport {
	return &TraceTransport{RoundTripper: rt, peerService: peerService, internalTags: internalTags}
}

// TraceTransport wraps a RoundTripper. If a request is being traced with
// Tracer, Transport will inject the current span into the headers,
// and set HTTP related tags on the span.
type TraceTransport struct {
	peerService  string
	internalTags []opentracing.Tag
	// The actual RoundTripper to use for the request. A nil
	// RoundTripper defaults to http.DefaultTransport.
	http.RoundTripper
}

// RoundTrip implements the RoundTripper interface
func (t *TraceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.RoundTripper
	if rt == nil {
		rt = http.DefaultTransport
	}
	operationName := "HTTP:" + req.Method
	// fork new trace
	span, _ := opentracing.StartSpanFromContext(req.Context(), operationName)

	ext.Component.Set(span, _defaultComponentName)
	ext.SpanKind.Set(span, ext.SpanKindRPCClientEnum)

	ext.HTTPUrl.Set(span, req.URL.Path)
	ext.HTTPMethod.Set(span, req.Method)
	if t.peerService != "" {
		ext.PeerService.Set(span, t.peerService)
	}

	// inject trace to http header
	_ = span.Tracer().Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
	// FIXME: uncomment after trace sdk is goroutinue safe
	ct := clientTracer{span: span}
	req = req.Clone(httptrace.WithClientTrace(req.Context(), ct.clientTrace()))
	resp, err := rt.RoundTrip(req)

	if err != nil {
		ext.Error.Set(span, true)
		ext.LogEvent.Set(span, "rt.RoundTrip")
		ext.LogMessage.Set(span, err.Error())
		span.Finish()
		return resp, err
	}

	// TODO: get ecode
	ext.HTTPStatusCode.Set(span, uint16(resp.StatusCode))

	if req.Method == "HEAD" {
		span.Finish()
	} else {
		resp.Body = &closeTracker{resp.Body, span}
	}
	return resp, err
}

type clientTracer struct {
	span opentracing.Span
}

func (h *clientTracer) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn:              h.getConn,
		GotConn:              h.gotConn,
		PutIdleConn:          h.putIdleConn,
		GotFirstResponseByte: h.gotFirstResponseByte,
		Got100Continue:       h.got100Continue,
		DNSStart:             h.dnsStart,
		DNSDone:              h.dnsDone,
		ConnectStart:         h.connectStart,
		ConnectDone:          h.connectDone,
		WroteHeaders:         h.wroteHeaders,
		Wait100Continue:      h.wait100Continue,
		WroteRequest:         h.wroteRequest,
	}
}

func (h *clientTracer) getConn(hostPort string) {
	ext.LogEvent.Set(h.span, strings.Combine("GetConn:", hostPort))
}

func (h *clientTracer) gotConn(info httptrace.GotConnInfo) {
	h.span.SetTag("net/http.reused", info.Reused)
	h.span.SetTag("net/http.was_idle", info.WasIdle)
	ext.LogEvent.Set(h.span, "GotConn")
}

func (h *clientTracer) putIdleConn(error) {
	ext.LogEvent.Set(h.span, "PutIdleConn")
}

func (h *clientTracer) gotFirstResponseByte() {
	ext.LogEvent.Set(h.span, "GotFirstResponseByte")
}

func (h *clientTracer) got100Continue() {
	ext.LogEvent.Set(h.span, "Got100Continue")
}

func (h *clientTracer) dnsStart(info httptrace.DNSStartInfo) {
	ext.LogEvent.Set(h.span, "DNSStart")
	ext.LogMessage.Set(h.span, info.Host)
}

func (h *clientTracer) dnsDone(info httptrace.DNSDoneInfo) {
	ext.LogEvent.Set(h.span, "DNSDone")
	ipAddr := "addr:"
	for _, addr := range info.Addrs {
		strings.Combine(ipAddr, addr.String(), " ")
	}
	ext.LogMessage.Set(h.span, ipAddr)
	if info.Err != nil {
		ext.LogErrorObject.Set(h.span, info.Err.Error())
		ext.Error.Set(h.span, true)
	}
}

func (h *clientTracer) connectStart(network, addr string) {
	ext.LogEvent.Set(h.span, "ConnectStart")
	ext.LogMessage.Set(h.span, strings.Combine("network:", network, " ", "addr:", addr))
}

func (h *clientTracer) connectDone(network, addr string, err error) {
	ext.LogEvent.Set(h.span, "ConnectDone")
	ext.LogMessage.Set(h.span, strings.Combine("network:", network, " ", "addr:", addr))
	if err != nil {
		ext.Error.Set(h.span, true)
		ext.LogErrorObject.Set(h.span, err.Error())
	}
}

func (h *clientTracer) wroteHeaders() {
	ext.LogEvent.Set(h.span, "WroteHeaders")
}

func (h *clientTracer) wait100Continue() {
	ext.LogEvent.Set(h.span, "Wait100Continue")
}

func (h *clientTracer) wroteRequest(info httptrace.WroteRequestInfo) {
	ext.LogEvent.Set(h.span, "WroteRequest")
	if info.Err != nil {
		ext.Error.Set(h.span, true)
		ext.LogErrorObject.Set(h.span, info.Err.Error())
	}
}
