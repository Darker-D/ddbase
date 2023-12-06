package http

import (
	"github.com/pkg/errors"
	"net/http"
	"net/http/pprof"
	"net/url"
	"sync"
)

var (
	_perfOnce sync.Once
)

type PerfConfig struct {
	Open bool
	Addr string
}

// Perf .
func Perf(c *PerfConfig) {
	if c.Open {
		if c.Addr == "" {
			c.Addr = "tcp://0.0.0.0:6060"
		}
		_perfOnce.Do(func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

			d, err := url.Parse(c.Addr)
			if err != nil {
				panic(errors.Errorf("http perf dsn must be tcp://$host:port, %s:error(%v)", c.Addr, err))
			}
			if err := http.ListenAndServe(d.Host, mux); err != nil {
				panic(errors.Errorf("listen %s: error(%v)", d.Host, err))
			}
		})
	}
}
