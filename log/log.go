package log

import (
	"context"
	"github.com/Darker-D/ddbase/net/trace/opentracing/util"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/Darker-D/ddbase/net/metadata"
)

// logger 标准日志
type zlog struct {
	*zap.Logger
	conf  *ZLogConfig
	level zap.AtomicLevel // level 日志级别操作

}

var once sync.Once
var zlogger = new(zlog)

// WithCTX 日志添加 traceId.
func (z zlog) WithCTX(ctx context.Context) zlog {
	z.Logger = z.With(zap.String(metadata.Trace, util.TraceIDFromContext(ctx)))
	return z
}

// ZLogConfig is log init conf
type ZLogConfig struct {
	Source     string // 日志来源
	Dir        string // 日志目录
	Filename   string // 日志名称
	Level      string // 日志级别
	Stdout     bool   // 是否标准输出
	MaxAge     int    // 日志时间限制
	MaxSize    int    // 日志大小限制
	MaxBackups int    // 备份数量
}

// Init init Logger
func Init(c *ZLogConfig) {
	once.Do(func() {
		zlogger.level = zap.NewAtomicLevel()
		zlogger.conf = c
		if c == nil {
			zlogger.conf = zlogger.defaultConfig()
		}
		zlogger.SetLevel(zlogger.conf.Level)

		encoderConfig := zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "category",
			CallerKey:      "line",
			MessageKey:     "msg",
			StacktraceKey:  "stack",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     timeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.FullCallerEncoder,
			EncodeName:     zapcore.FullNameEncoder,
		}

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.NewMultiWriteSyncer(zlogger.writers()...),
			zlogger.level,
		)
		zlogger.Logger = zap.New(
			core,
			zap.AddCaller(),
			zap.AddCallerSkip(0),
			zap.Development(),
		).With(zap.String("app_name", c.Source))
	})

}

// Logger new Logger
func Logger() *zlog {
	return zlogger
}

func (z *zlog) defaultConfig() *ZLogConfig {
	return &ZLogConfig{
		Source:     "default",
		Dir:        "./logs",
		Filename:   "default",
		Level:      "debug",
		Stdout:     true,
		MaxAge:     2,
		MaxSize:    1024,
		MaxBackups: 2,
	}
}

// GetLevelType 获取日志级别类型
func (z *zlog) LevelType(levelName string) zapcore.Level {
	var l zapcore.Level
	switch levelName {
	case "debug":
		l = zap.DebugLevel
	case "info":
		l = zap.InfoLevel
	case "warn":
		l = zap.WarnLevel
	case "error":
		l = zap.ErrorLevel
	default:
		l = zap.InfoLevel
	}
	return l
}

// SetLevel 设置日志级别
func (z *zlog) SetLevel(levelName string) {
	l := z.LevelType(levelName)
	if l == z.GetLevel() {
		return
	}
	z.level.SetLevel(l)
}

// GetLevel 获取当前日志级别
func (z *zlog) GetLevel() zapcore.Level {
	return z.level.Level()
}

// getLogfilePath 获取日志文件全路径
func (z *zlog) getLogfilePath() string {
	return path.Join(z.conf.Dir, fmt.Sprintf("%s.log", z.conf.Filename))
}

// timeEncoder 日志时间格式化
func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05"))
}

// writers 日志输出
func (z *zlog) writers() (ws []zapcore.WriteSyncer) {
	handle := lumberjack.Logger{
		Filename:   z.getLogfilePath(),
		MaxSize:    z.conf.MaxSize,
		MaxBackups: z.conf.MaxBackups,
		MaxAge:     z.conf.MaxAge,
		Compress:   true,
	}
	ws = []zapcore.WriteSyncer{
		zapcore.AddSync(&handle),
	}
	if z.conf.Stdout {
		ws = append(ws, zapcore.AddSync(os.Stdout))
	}
	return
}
