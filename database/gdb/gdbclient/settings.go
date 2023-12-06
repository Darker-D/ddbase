package gdbclient

import (
	"github.com/Darker-D/ddbase/database/gdb/gdbclient/internal/pool"
	"github.com/Darker-D/ddbase/net/netutil/breaker"
	"strconv"
	"time"
)

type Settings struct {
	// port of GDB to connect, default is 8182
	Port int
	// host that the driver will connect to, default is 'localhost'
	Host string
	// username and password for GDB auth
	Username, Password string
	// serializer for the driver of request to GDB and response from
	Serializer string
	// manageTransaction by user client or not in session
	IsManageTransaction bool

	// maximum number of socket connections, Default is 8
	PoolSize int
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration
	// Frequency of WebSocket ping checks, Default is 1 minute
	PingInterval time.Duration
	// max concurrent request pending on one connection, Default is 4
	MaxConcurrentRequest int
	// Amount of time client waits connection io write before returning an error.
	// Default is 5 second
	WriteTimeout time.Duration
	// Amount of time client waits connection io read before returning an error.
	// Default is the same with WriteTimeout
	ReadTimeout time.Duration
	// Interval of time to check connection health status in pool, new connection will be
	// created if someone broken in pool
	// Default is 1min, set minus value will disable it
	AliveCheckInterval time.Duration

	// deprecated
	MinIdleConns int
	// deprecated
	IdleTimeout time.Duration
	// deprecated
	IdleCheckFrequency time.Duration
	// deprecated
	MaxConnAge time.Duration

	// websocket ReadBufferSize、WriteBufferSize
	ReadBufferSize, WriteBufferSize int
	// websocket HandshakeTimeout
	HandshakeTimeout time.Duration

	Breaker *breaker.Config // breaker
}

func (s *Settings) init() {
	if s.Host == "" {
		s.Host = "localhost"
	}
	if s.Port == 0 {
		s.Port = 8182
	}
	if s.PoolSize == 0 {
		s.PoolSize = 8
	}
	if s.PingInterval == 0 {
		s.PingInterval = 1 * time.Minute
	}
	if s.MaxConcurrentRequest == 0 {
		s.MaxConcurrentRequest = 4
	}
	if s.WriteTimeout == 0 {
		s.WriteTimeout = 5 * time.Second
	}
	if s.ReadTimeout == 0 {
		s.ReadTimeout = s.WriteTimeout
	}
	if s.PoolTimeout == 0 {
		s.PoolTimeout = s.ReadTimeout + 1
	}
	if s.AliveCheckInterval == 0 {
		s.AliveCheckInterval = 1 * time.Minute
	}
	if s.ReadBufferSize == 0 {
		s.ReadBufferSize = 8 * 1024
	}
	if s.WriteBufferSize == 0 {
		s.WriteBufferSize = 8 * 1024
	}
	if s.HandshakeTimeout == 0 {
		s.HandshakeTimeout = 5 * time.Second
	}
}

func (s *Settings) getOpts() *pool.Options {
	return &pool.Options{
		GdbUrl:       "ws://" + s.Host + ":" + strconv.FormatInt(int64(s.Port), 10) + "/gremlin",
		Username:     s.Username,
		Password:     s.Password,
		PingInterval: s.PingInterval,
		WriteTimeout: s.WriteTimeout,
		ReadTimeout:  s.ReadTimeout,
		MaxConnAge:   s.MaxConnAge,

		PoolSize:                    s.PoolSize,
		PoolTimeout:                 s.PoolTimeout,
		MaxInProcessPerConn:         s.MaxConcurrentRequest,
		MaxSimultaneousUsagePerConn: s.MaxConcurrentRequest,
		AliveCheckInterval:          s.AliveCheckInterval,

		ReadBufferSize:   s.ReadBufferSize,
		WriteBufferSize:  s.WriteBufferSize,
		HandshakeTimeout: s.HandshakeTimeout,

		Dialer: pool.NewConnWebSocket,
	}
}

func (s *Settings) getSessionOpts() *pool.Options {
	return &pool.Options{
		GdbUrl:       "ws://" + s.Host + ":" + strconv.FormatInt(int64(s.Port), 10) + "/gremlin",
		Username:     s.Username,
		Password:     s.Password,
		PingInterval: s.PingInterval,
		WriteTimeout: s.WriteTimeout,
		ReadTimeout:  s.ReadTimeout,
		MaxConnAge:   s.MaxConnAge,

		PoolSize:                    1,
		PoolTimeout:                 s.PoolTimeout,
		MaxInProcessPerConn:         2,
		MaxSimultaneousUsagePerConn: 2,
		AliveCheckInterval:          s.AliveCheckInterval,

		ReadBufferSize:   s.ReadBufferSize,
		WriteBufferSize:  s.WriteBufferSize,
		HandshakeTimeout: s.HandshakeTimeout,

		Dialer: pool.NewConnWebSocket,
	}
}
