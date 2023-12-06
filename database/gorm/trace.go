package gorm

import (
	"github.com/Darker-D/ddbase/net/http"
	"github.com/Darker-D/ddbase/net/trace/opentracing/ext"
	"github.com/opentracing/opentracing-go"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/utils"
)

const (
	_defaultComponentName = "database/grom"
	_callBackBeforeName   = "core:before"
	_callBackAfterName    = "core:after"
	_startTime            = "_start_time"
	_spanKey              = "spanKey"
)

type TracePlugin struct{}

func (op *TracePlugin) Name() string {
	return "grom:trace"
}

func NewTracePlugin() *TracePlugin {
	return &TracePlugin{}
}

func (op *TracePlugin) Initialize(db *gorm.DB) (err error) {
	// 开始前
	_ = db.Callback().Create().Before("gorm:before_create").Register(_callBackBeforeName, before("gorm:create"))
	_ = db.Callback().Query().Before("gorm:query").Register(_callBackBeforeName, before("gorm:query"))
	_ = db.Callback().Delete().Before("gorm:before_delete").Register(_callBackBeforeName, before("gorm:delete"))
	_ = db.Callback().Update().Before("gorm:setup_reflect_value").Register(_callBackBeforeName, before("gorm:update"))
	_ = db.Callback().Row().Before("gorm:row").Register(_callBackBeforeName, before("gorm:row"))
	_ = db.Callback().Raw().Before("gorm:raw").Register(_callBackBeforeName, before("gorm:raw"))

	// 结束后
	_ = db.Callback().Create().After("gorm:after_create").Register(_callBackAfterName, after)
	_ = db.Callback().Query().After("gorm:after_query").Register(_callBackAfterName, after)
	_ = db.Callback().Delete().After("gorm:after_delete").Register(_callBackAfterName, after)
	_ = db.Callback().Update().After("gorm:after_update").Register(_callBackAfterName, after)
	_ = db.Callback().Row().After("gorm:row").Register(_callBackAfterName, after)
	_ = db.Callback().Raw().After("gorm:raw").Register(_callBackAfterName, after)
	return
}

var _ gorm.Plugin = &TracePlugin{}

func before(operationName string) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		db.InstanceSet(_startTime, time.Now())

		_ctx := db.Statement.Context
		tracer := _ctx.Value(http.TraceKey)
		if tracer == nil {
			return
		}

		span, _ := opentracing.StartSpanFromContext(_ctx, operationName)
		db.InstanceSet(_spanKey, span)
	}
}

func after(db *gorm.DB) {
	spanInter, isExist := db.InstanceGet(_spanKey)
	if !isExist {
		return
	}

	span := spanInter.(opentracing.Span)
	defer span.Finish()

	_ts, isExist := db.InstanceGet(_startTime)
	if !isExist {
		return
	}

	ts, ok := _ts.(time.Time)
	if !ok {
		return
	}

	ext.Component.Set(span, _defaultComponentName)
	ext.SpanKind.Set(span, ext.SpanKindRPCClientEnum)

	ext.DBType.Set(span, "sql")
	ext.DBStatement.Set(span, db.Statement.SQL.String())
	ext.DBSqlCostSeconds.Set(span, time.Since(ts).Seconds())
	ext.DBSqlRows.Set(span, db.Statement.RowsAffected)
	ext.DBSql.Set(span, db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...))
	ext.DBSqlStack.Set(span, utils.FileWithLineNum())

	return
}
