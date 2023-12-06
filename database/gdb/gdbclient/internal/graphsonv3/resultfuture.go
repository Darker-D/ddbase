package graphsonv3

import (
	"github.com/Darker-D/ddbase/net/trace/opentracing/ext"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"sync/atomic"
	"time"
)

type ResponseFuture struct {
	originalRequest *Request
	response        *Response
	signalChan      chan struct{}
	isCompleted     uint32
	_callback       func() bool
	span            opentracing.Span
}

func NewResponseFuture(request *Request, cb func() bool) *ResponseFuture {
	return &ResponseFuture{
		originalRequest: request,
		signalChan:      make(chan struct{}, 1),
		isCompleted:     0,
		_callback:       cb}
}

func (r *ResponseFuture) Complete(response *Response) {
	if atomic.CompareAndSwapUint32(&r.isCompleted, 0, 1) {
		defer close(r.signalChan)

		if response != nil {
			r.response = response
		}
		_ = r._callback != nil && r._callback()
		r.signalChan <- struct{}{}
	}
}

func (r *ResponseFuture) Request() *Request {
	return r.originalRequest
}

func (r *ResponseFuture) IsCompleted() bool {
	return atomic.LoadUint32(&r.isCompleted) == 1
}

func (r *ResponseFuture) FixResponse(fn func(response *Response)) {
	if r.response == nil {
		r.response = &Response{RequestID: r.originalRequest.RequestID}
	}
	fn(r.response)
}

func (r *ResponseFuture) Get() *Response {
	if atomic.LoadUint32(&r.isCompleted) == 0 {
		<-r.signalChan
	}
	// NOTE 1
	if r.span == nil {
		return r.response
	}
	defer r.span.Finish()

	if r.response.Code != RESPONSE_STATUS_SUCCESS {
		ext.Error.Set(r.span, true)
		ext.LogEvent.Set(r.span, "rt.RoundTrip")
		ext.LogMessage.Set(r.span, fmt.Sprintf("%s:%d", r.response.RequestID, r.response.Code))
	}

	return r.response
}

func (r *ResponseFuture) SetSpan(span opentracing.Span) {
	r.span = span
}

func (r *ResponseFuture) GetOrTimeout(timeout time.Duration) (*Response, bool) {
	if atomic.LoadUint32(&r.isCompleted) == 1 {
		return r.response, false
	}

	select {
	case <-time.After(timeout):
		return nil, true
	case <-r.signalChan:
		return r.response, false
	}
}
