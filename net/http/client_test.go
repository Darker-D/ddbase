package http

import (
	"context"
	"github.com/Darker-D/ddbase/net/netutil/breaker"
	"net/url"
	"testing"
	"time"
)

func TestRoundTripper(t *testing.T) {
	cli := NewClient(&ClientConfig{
		Domain:    "http://172.17.15.141:20220",
		Dial:      2 * time.Second,
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
		Breaker: &breaker.Config{
			Window:  10 * time.Second,
			Sleep:   time.Second,
			Bucket:  10,
			Ratio:   0.5,
			Request: 100,
		},
	})
	cli.Get(context.Background(), "", url.Values{}, nil)
	//req, _ := http.NewRequest("", "http://172.17.15.141:20220", nil)
	//cli.Raw(context.Background(), req)
}
