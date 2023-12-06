package gdbclient

import (
	"github.com/Darker-D/ddbase/database/gdb/gdbclient/graph"
	"github.com/Darker-D/ddbase/database/gdb/gdbclient/internal"
	"github.com/Darker-D/ddbase/database/gdb/gdbclient/internal/graphsonv3"
	"github.com/Darker-D/ddbase/database/gdb/gdbclient/internal/pool"
	"github.com/Darker-D/ddbase/net/netutil/breaker"
	"github.com/Darker-D/ddbase/net/stat"
	"github.com/Darker-D/ddbase/net/trace/opentracing/ext"
	"github.com/Darker-D/ddbase/strings"
	"context"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"strconv"
	xstrings "strings"
	"time"
	"unsafe"
)

func SetLogger(logger *zap.Logger) {
	internal.Logger = logger
}

//---------------------- Gdb baseClient ---------------------//

// transaction ops
const (
	_OPEN     = "g.tx().open()"
	_COMMIT   = "g.tx().commit()"
	_ROLLBACK = "g.tx().rollback()"
)

// prom
const _defaultComponent = "database/gdb"

var stats = stat.DB

// ClientShell client shell for submit serial API .
type ClientShell interface {
	SubmitScript(ctx context.Context, gremlin string) ([]Result, error)
	SubmitScriptBound(ctx context.Context, gremlin string, bindings map[string]interface{}) ([]Result, error)
	SubmitScriptOptions(ctx context.Context, gremlin string, options *graph.RequestOptions) ([]Result, error)

	SubmitScriptAsync(ctx context.Context, gremlin string) (ResultSetFuture, error)
	SubmitScriptBoundAsync(ctx context.Context, gremlin string, bindings map[string]interface{}) (ResultSetFuture, error)
	SubmitScriptOptionsAsync(ctx context.Context, gremlin string, options *graph.RequestOptions) (ResultSetFuture, error)
}

// SessionClient session client support batch submit
type SessionClient interface {
	BatchSubmit(context.Context, func(context.Context, ClientShell) error) error

	Close(context.Context)
}

// Client session-less client support submit in sync or async, all in auto-transaction
type Client interface {
	ClientShell

	Close(ctx context.Context)
}

// baseClient .
type baseClient struct {
	setting   *Settings
	sessionId string
	session   bool
	connPool  *pool.ConnPool
	breaker   breaker.Breaker
}

// NewClient .
func NewClient(settings *Settings) Client {
	settings.init()
	brkGroup := breaker.NewGroup(settings.Breaker)
	brk := brkGroup.Get(settings.Host)
	client := &baseClient{setting: settings, session: false, breaker: brk}

	client.connPool = pool.NewConnPool(settings.getOpts())
	// internal.Logger.Info("new client", zap.String("server", client.String()), zap.Bool("session", client.session), zap.Time("createTime", time.Now()))
	return client
}

func NewSessionClient(sessionId string, settings *Settings) SessionClient {
	settings.init()
	brkGroup := breaker.NewGroup(settings.Breaker)
	brk := brkGroup.Get(settings.Host)
	client := &baseClient{setting: settings, session: true, sessionId: sessionId, breaker: brk}

	client.connPool = pool.NewConnPool(settings.getSessionOpts())
	// internal.Logger.Info("new client", zap.String("server", client.String()), zap.Bool("session", client.session), zap.Time("createTime", time.Now()))
	return client
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Gdb<%s>", c.getEndpoint())
}

func (c *baseClient) Close(ctx context.Context) {
	if c.session {
		c.closeSession(ctx)
	}
	c.connPool.Close()
	// internal.Logger.Info("close client", zap.Bool("session", c.session), zap.Time("time", time.Now()))
}

func (c *baseClient) getEndpoint() string {
	return c.setting.Host + ":" + strconv.FormatInt(int64(c.setting.Port), 10)
}

func (c *baseClient) SubmitScript(ctx context.Context, gremlin string) ([]Result, error) {
	return c.SubmitScriptBound(ctx, gremlin, nil)
}

func (c *baseClient) SubmitScriptBound(ctx context.Context, gremlin string, bindings map[string]interface{}) ([]Result, error) {
	options := graph.NewRequestOptionsWithBindings(bindings)
	result, err := c.SubmitScriptOptions(ctx, gremlin, options)
	return result, err
}

func (c *baseClient) SubmitScriptOptions(ctx context.Context, gremlin string, options *graph.RequestOptions) ([]Result, error) {
	if future, err := c.SubmitScriptOptionsAsync(ctx, gremlin, options); err != nil {
		return nil, err
	} else {
		return future.GetResults()
	}
}

func (c *baseClient) SubmitScriptAsync(ctx context.Context, gremlin string) (ResultSetFuture, error) {
	return c.SubmitScriptBoundAsync(ctx, gremlin, nil)
}

func (c *baseClient) SubmitScriptBoundAsync(ctx context.Context, gremlin string, bindings map[string]interface{}) (ResultSetFuture, error) {
	options := graph.NewRequestOptionsWithBindings(bindings)
	return c.SubmitScriptOptionsAsync(ctx, gremlin, options)
}

func (c *baseClient) SubmitScriptOptionsAsync(ctx context.Context, gremlin string, options *graph.RequestOptions) (ResultSetFuture, error) {
	// set session args if session mode
	if c.session {
		if options == nil {
			options = graph.NewRequestOptionsWithBindings(nil)
		}
		options.AddArgs(graph.ARGS_SESSION, c.sessionId)
		options.AddArgs(graph.ARGS_MANAGE_TRANSACTION, c.setting.IsManageTransaction)
	}

	request, err := graphsonv3.MakeRequestWithOptions(ctx, gremlin, options)
	if err != nil {
		return nil, err
	}

	respFuture, err := c.requestAsync(ctx, request)

	if err != nil {
		return nil, err
	}
	return NewResultSetFuture(respFuture), nil
}

// session batch submit with 'SubmitScript' serial , must check return errors
func (c *baseClient) BatchSubmit(ctx context.Context, batchSubmit func(context.Context, ClientShell) error) error {
	if !c.session {
		return errors.New("batch submit is not allowed in non-session client")
	}

	if err := c.transaction(ctx, _OPEN); err != nil {
		return err
	}

	err := batchSubmit(ctx, c)
	if err == nil {
		err = c.transaction(ctx, _COMMIT)
	}

	// rollback submit errors, include batch submit and commit
	if err != nil {
		_err := c.transaction(ctx, _ROLLBACK)
		if _err != nil {
			internal.Logger.Error("unstable transaction status as rollback failed", zap.Error(err), zap.Time("time", time.Now()))
			return _err
		}
	}
	return err
}

func (c *baseClient) closeSession(ctx context.Context) {
	request := graphsonv3.MakeRequestCloseSession(c.sessionId)
	respFuture, err := c.requestAsync(ctx, request)
	if err != nil {
		internal.Logger.Warn("fail to close session", zap.Error(err), zap.Time("time", time.Now()))
		return
	}

	// NOTICE: wait to get response of session close request
	if resp, timeout := respFuture.GetOrTimeout(2 * time.Second); timeout {
		internal.Logger.Warn("response timeout for close session", zap.Time("time", time.Now()))
	} else {
		if resp.Code != graphsonv3.RESPONSE_STATUS_NO_CONTENT && resp.Code != graphsonv3.RESPONSE_STATUS_SUCCESS {
			internal.Logger.Warn("response error for close session", zap.Error(resp.Data.(error)), zap.Time("time", time.Now()))
		}
	}
}

func (c *baseClient) transaction(ctx context.Context, ops string) error {
	options := graph.NewRequestOptionsWithBindings(nil)
	options.AddArgs(graph.ARGS_SESSION, c.sessionId)
	options.AddArgs(graph.ARGS_MANAGE_TRANSACTION, c.setting.IsManageTransaction)

	request, err := graphsonv3.MakeRequestWithOptions(ctx, ops, options)
	if err != nil {
		return err
	}

	respFuture, err := c.requestAsync(ctx, request)
	if err != nil {
		return err
	}

	// just check response code instead of un-json Data, transaction return 'null'...
	resp := respFuture.Get()
	if err, ok := resp.Data.(error); ok {
		return err
	}
	return nil
}

func (c *baseClient) requestAsync(ctx context.Context, request *graphsonv3.Request) (*graphsonv3.ResponseFuture, error) {
	var err error
	//tracer := c.setTrace(ctx, request, &err)
	//defer tracer()
	now := time.Now()

	if err = c.breaker.Allow(); err != nil {
		stats.Incr("gdb:SubmitScriptOptionsAsync", "breaker")
		return nil, err
	}

	conn, err := c.connPool.Get(ctx)
	if err != nil {
		internal.Logger.Error("request connect failed",
			zap.Time("time", time.Now()),
			zap.Error(err))
		stats.Timing(strings.Combine(graph.ARGS_GREMLIN, ":", request.Args[graph.ARGS_GREMLIN].(string)), int64(time.Since(now)/time.Millisecond))
		return nil, err
	}

	//bindingsStr, _ := json.Marshal(request.Args[graph.ARGS_BINDINGS])
	// send request to connection, and return future
	//internal.Logger.Info("submit script",
	//	zap.Time("time", time.Now()),
	//	zap.Uintptr("conn", uintptr(unsafe.Pointer(conn))),
	//	zap.String("dsl", request.Args[graph.ARGS_GREMLIN].(string)),
	//	zap.String("bindings", string(bindingsStr)),
	//	zap.String("processor", request.Processor))

	f, err := conn.SubmitRequestAsync(ctx, request)
	c.onBreaker(err)
	if err != nil {
		// return connection to pool if request is not pending
		c.connPool.Put(conn)
		internal.Logger.Error("submit script failed",
			zap.Time("time", time.Now()),
			zap.Uintptr("conn", uintptr(unsafe.Pointer(conn))),
			zap.Error(err),
			zap.String("dsl", request.Args[graph.ARGS_GREMLIN].(string)))
		stats.Timing(strings.Combine(graph.ARGS_GREMLIN, ":", request.Args[graph.ARGS_GREMLIN].(string)), int64(time.Since(now)/time.Millisecond))
		return nil, err
	}

	span, _ := opentracing.StartSpanFromContext(ctx, strings.Combine("gdb:", c.convertOperate(request.Args[graph.ARGS_GREMLIN].(string))))
	if span != nil {
		ext.Component.Set(span, _defaultComponent)
		ext.SpanKind.Set(span, ext.SpanKindRPCClientEnum)

		ext.DBInstance.Set(span, c.setting.Host)
		ext.DBUser.Set(span, c.setting.Username)
		ext.DBType.Set(span, graph.ARGS_GREMLIN)
		ext.DBStatement.Set(span, request.Args[graph.ARGS_GREMLIN].(string))
		f.SetSpan(span)
	}

	stats.Timing(strings.Combine(graph.ARGS_GREMLIN, ":", request.Args[graph.ARGS_GREMLIN].(string)), int64(time.Since(now)/time.Millisecond))
	return f, err
}

func (c *baseClient) convertOperate(dsl string) string {
	if dsl == "" {
		return "none"
	}
	if xstrings.Contains(dsl, ".drop()") {
		return "delete"
	}
	if xstrings.Contains(dsl, ".addE") || xstrings.Contains(dsl, ".addV") || xstrings.Contains(dsl, ".property") {
		return "create/update"
	}
	return "query"
}

func (c *baseClient) onBreaker(err error) {
	if err != nil {
		c.breaker.MarkFailed()
	} else {
		c.breaker.MarkSuccess()
	}
}
