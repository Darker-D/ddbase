package http

import (
	"github.com/Darker-D/ddbase/ecode"
	"github.com/Darker-D/ddbase/net/trace/opentracing/util"
	"context"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/gin-gonic/gin/render"
	"github.com/opentracing/opentracing-go"

	"net/http"
	"strconv"
)

const TraceKey = "traceKey"

// ToContext is gin.Context convert to context.Context
func ToContext(c *gin.Context) context.Context {
	span, ok := c.Get("traceKey")
	if ok {
		return opentracing.ContextWithSpan(c, span.(opentracing.Span))
	}

	return context.WithValue(c, struct{}{}, opentracing.GlobalTracer().StartSpan("Default"))
}

// JSON serializes the given struct as JSON data into the response body.
// It also sets the Content-Type as "application/json".
func JSON(c *gin.Context, data interface{}, err error) {
	code := http.StatusOK

	bcode := ecode.Cause(err)

	writeTraceId(c.Writer, util.TraceIDFromContext(ToContext(c)))

	c.Render(code, render.JSON{Data: XJSON{
		BaseResponse: BaseResponse{
			Code:    bcode.Code(),
			Message: bcode.Message(),
		},
		Data: data,
	}})
}

// RenderJson serializes the given struct into the response body.
// It provide customize data
// It also sets the Content-Type as "application/json".
func RenderJson(c *gin.Context, data interface{}) {
	code := http.StatusOK

	writeTraceId(c.Writer, util.TraceIDFromContext(ToContext(c)))

	c.Render(code, render.JSON{Data: data})
}

func writeStatusCode(w http.ResponseWriter, ecode int) {
	header := w.Header()
	header.Set("jryg-status-code", strconv.FormatInt(int64(ecode), 10))
}

func writeTraceId(w http.ResponseWriter, traceId string) {
	header := w.Header()
	header.Set("jryg-trace-id", traceId)
}

type XJSON struct {
	BaseResponse
	Data interface{} `json:"data,omitempty"`
}

// BindWith bind req arg with parser.
func BindWith(c *gin.Context, obj interface{}, b binding.Binding) error {
	return mustBindWith(c, obj, b)
}

// Bind bind req arg with defult form binding.
func Bind(c *gin.Context, obj interface{}) error {
	return mustBindWith(c, obj, binding.Form)
}

// mustBindWith binds the passed struct pointer using the specified binding engine.
// It will abort the request with HTTP 400 if any error ocurrs.
// See the binding package.
func mustBindWith(c *gin.Context, obj interface{}, b binding.Binding) (err error) {
	if err = b.Bind(c.Request, obj); err != nil {
		c.Render(http.StatusOK, render.JSON{Data: XJSON{
			BaseResponse: BaseResponse{
				Code:    ecode.RequestErr.Code(),
				Message: err.Error(),
			},
			Data: nil,
		}})

		c.Abort()
	}
	return
}
