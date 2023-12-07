package http

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	xhttp "net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Darker-D/ddbase/encoding/json"
	"github.com/Darker-D/ddbase/net/http/httptrace"
	"github.com/Darker-D/ddbase/net/http/sign"
	"github.com/Darker-D/ddbase/net/netutil/breaker"
	"github.com/Darker-D/ddbase/net/stat"

	"github.com/gogo/protobuf/proto"
	pkgerr "github.com/pkg/errors"
)

const (
	_minRead = 16 * 1024 // 16kb
	_appId   = "appId"
	_ts      = "ts"
)

var (
	clientStats      = stat.HTTPClient
	_noKickUserAgent = "darker2dd@gmail.com "
)

func init() {
	n, err := os.Hostname()
	if err == nil {
		_noKickUserAgent = _noKickUserAgent + runtime.Version() + " " + n
	}
}

// ClientConfig is http client conf.
type ClientConfig struct {
	SignConfig *sign.Config
	PeerServer string
	Domain     string
	Dial       time.Duration
	Timeout    time.Duration
	KeepAlive  time.Duration
	Breaker    *breaker.Config
	URL        map[string]*ClientConfig
	Host       map[string]*ClientConfig
}

// Client is http client.
type Client struct {
	conf      *ClientConfig
	client    *xhttp.Client
	dialer    *net.Dialer
	transport xhttp.RoundTripper
	autograph *sign.Sign

	urlConf  map[string]*ClientConfig
	hostConf map[string]*ClientConfig
	mutex    sync.RWMutex
	breaker  *breaker.Group
}

// NewClient new a http client.
func NewClient(c *ClientConfig) *Client {
	client := new(Client)
	client.conf = c
	client.dialer = &net.Dialer{
		Timeout:   c.Dial,
		KeepAlive: c.KeepAlive,
	}

	// wraps RoundTripper for tracer
	client.transport = httptrace.NewTraceTracesport(xhttp.DefaultTransport, c.PeerServer)
	client.client = &xhttp.Client{
		Transport: client.transport,
	}
	client.urlConf = make(map[string]*ClientConfig)
	client.hostConf = make(map[string]*ClientConfig)
	client.breaker = breaker.NewGroup(c.Breaker)

	if c.Timeout <= 0 {
		panic("must config http timeout!!!")
	}
	for uri, cfg := range c.URL {
		client.urlConf[uri] = cfg
	}
	for host, cfg := range c.Host {
		client.hostConf[host] = cfg
	}

	if c.SignConfig != nil {
		client.autograph = sign.New(c.SignConfig)
	}

	return client
}

// SetTransport set client transport
func (client *Client) SetTransport(t xhttp.RoundTripper) {
	client.transport = t
	client.client.Transport = t
}

// SetConfig set client config.
func (client *Client) SetConfig(c *ClientConfig) {
	client.mutex.Lock()

	if c.Timeout > 0 {
		client.conf.Timeout = c.Timeout
	}
	if c.KeepAlive > 0 {
		client.dialer.KeepAlive = c.KeepAlive
		client.conf.KeepAlive = c.KeepAlive
	}
	if c.Dial > 0 {
		client.dialer.Timeout = c.Dial
		client.conf.Timeout = c.Dial
	}
	if c.Breaker != nil {
		client.conf.Breaker = c.Breaker
		client.breaker.Reload(c.Breaker)
	}
	for uri, cfg := range c.URL {
		client.urlConf[uri] = cfg
	}
	for host, cfg := range c.Host {
		client.hostConf[host] = cfg
	}
	client.mutex.Unlock()
}

// NewRequest new http request with method, uri, values and headers.
func (client *Client) NewRequest(method, uri string, params url.Values) (req *xhttp.Request, err error) {

	if method == xhttp.MethodGet {
		p := params.Encode()
		if len(p) > 0 {
			uri = fmt.Sprintf("%s?%s", uri, params.Encode())
		}
		req, err = xhttp.NewRequest(xhttp.MethodGet, uri, nil)
	} else {
		req, err = xhttp.NewRequest(xhttp.MethodPost, uri, strings.NewReader(params.Encode()))
	}
	if err != nil {
		err = pkgerr.Wrapf(err, "method:%s,uri:%s", method, uri)
		return
	}
	const (
		_contentType = "Content-Type"
		_urlencoded  = "application/x-www-form-urlencoded"
		_userAgent   = "User-Agent"
	)
	if method == xhttp.MethodPost {
		req.Header.Set(_contentType, _urlencoded)
	}

	req.Header.Set(_userAgent, _noKickUserAgent)
	return
}

// sign calc appkey and appsecret sign.
func (client *Client) sign(params interface{}) (query string, err error) {
	// FIXME 待实现
	//if params == nil {
	//	params = url.Values{}
	//}
	//params.Set(_appId, client.conf.AppID)
	//
	//if params.Get(_ts) == "" {
	//	params.Set(_ts, strconv.FormatInt(time.Now().Unix(), 10))
	//}
	//
	//switch params {
	//
	//}
	//
	//client.autograph.GenSign()

	return
}

// Get issues a GET to the specified URL.
func (client *Client) Get(c context.Context, uri string, params url.Values, res interface{}) (err error) {
	req, err := client.NewRequest(xhttp.MethodGet, uri, params)
	if err != nil {
		return
	}
	return client.Do(c, req, res)
}

// Post issues a Post to the specified URL.
func (client *Client) Post(c context.Context, uri string, params url.Values, res interface{}) (err error) {
	req, err := client.NewRequest(xhttp.MethodPost, uri, params)
	if err != nil {
		return
	}
	return client.Do(c, req, res)
}

// PostJson issues a Post json to the specified URL.
func (client *Client) PostJson(c context.Context, uri string, params interface{}, headers map[string]string, res interface{}) (err error) {
	dataBytes, err := json.Marshal(params)
	if err != nil {
		return
	}
	req, err := xhttp.NewRequest(xhttp.MethodPost, uri, bytes.NewBuffer(dataBytes))
	if err != nil {
		err = pkgerr.Wrapf(err, "method:%s,uri:%s", xhttp.MethodPost, uri)
		return
	}
	const (
		_contentType = "Content-Type"
		_urlencoded  = "application/json;charset=utf-8"
		_userAgent   = "User-Agent"
	)
	req.Header.Set(_contentType, _urlencoded)

	req.Header.Set(_userAgent, _noKickUserAgent)

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return client.Do(c, req, res)
}

// PostXml issues a Post xml to the specified URL.
func (client *Client) PostXml(c context.Context, uri string, params interface{}, headers map[string]string, res interface{}) (err error) {
	dataBytes, err := xml.Marshal(params)
	if err != nil {
		return
	}
	req, err := xhttp.NewRequest(xhttp.MethodPost, uri, bytes.NewBuffer(dataBytes))
	if err != nil {
		err = pkgerr.Wrapf(err, "method:%s,uri:%s", xhttp.MethodPost, uri)
		return
	}
	const (
		_contentType = "Content-Type"
		_urlencoded  = "text/plain;charset=utf-8"
		_userAgent   = "User-Agent"
	)
	req.Header.Set(_contentType, _urlencoded)

	req.Header.Set(_userAgent, _noKickUserAgent)

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return client.Do(c, req, res)
}

// RESTfulGet issues a RESTful GET to the specified URL.
func (client *Client) RESTfulGet(c context.Context, uri string, params url.Values, res interface{}, v ...interface{}) (err error) {
	req, err := client.NewRequest(xhttp.MethodGet, fmt.Sprintf(uri, v...), params)
	if err != nil {
		return
	}
	return client.Do(c, req, res, uri)
}

// RESTfulPost issues a RESTful Post to the specified URL.
func (client *Client) RESTfulPost(c context.Context, uri string, params url.Values, res interface{}, v ...interface{}) (err error) {
	req, err := client.NewRequest(xhttp.MethodPost, fmt.Sprintf(uri, v...), params)
	if err != nil {
		return
	}
	return client.Do(c, req, res, uri)
}

// Raw sends an HTTP request and returns bytes response
func (client *Client) Raw(c context.Context, req *xhttp.Request, v ...string) (bs []byte, err error) {
	var (
		ok      bool
		code    string
		cancel  func()
		resp    *xhttp.Response
		config  *ClientConfig
		timeout time.Duration
		uri     = fmt.Sprintf("%s://%s%s", req.URL.Scheme, req.Host, req.URL.Path)
	)

	// NOTE fix prom & config uri key.
	if len(v) == 1 {
		uri = v[0]
	}
	// breaker
	brk := client.breaker.Get(uri)
	if err = brk.Allow(); err != nil {
		code = "breaker"
		clientStats.Incr(uri, code)
		return
	}
	defer client.onBreaker(brk, &err)
	// stat
	now := time.Now()
	defer func() {
		clientStats.Timing(uri, int64(time.Since(now)/time.Millisecond))
		if code != "" {
			clientStats.Incr(uri, code)
		}
	}()
	// get config
	// 1.url config 2.host config 3.default
	client.mutex.RLock()
	if config, ok = client.urlConf[uri]; !ok {
		if config, ok = client.hostConf[req.Host]; !ok {
			config = client.conf
		}
	}
	client.mutex.RUnlock()
	// timeout
	deliver := true
	timeout = config.Timeout
	if deadline, ok := c.Deadline(); ok {
		if ctimeout := time.Until(deadline); ctimeout < timeout {
			// deliver small timeout
			timeout = ctimeout
			deliver = false
		}
	}
	if deliver {
		c, cancel = context.WithTimeout(c, timeout)
		defer cancel()
	}

	req = req.Clone(c)

	if resp, err = client.client.Do(req); err != nil {
		err = pkgerr.Wrapf(err, "host:%s, url:%s", req.URL.Host, realURL(req))
		code = "failed"
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= xhttp.StatusBadRequest {
		err = pkgerr.Errorf("incorrect http status:%d host:%s, url:%s", resp.StatusCode, req.URL.Host, realURL(req))
		code = strconv.Itoa(resp.StatusCode)
		return
	}
	if bs, err = readAll(resp.Body, _minRead); err != nil {
		err = pkgerr.Wrapf(err, "host:%s, url:%s", req.URL.Host, realURL(req))
		return
	}
	return
}

// Do sends an HTTP request and returns an HTTP json response.
func (client *Client) Do(c context.Context, req *xhttp.Request, res interface{}, v ...string) (err error) {
	var bs []byte
	if bs, err = client.Raw(c, req, v...); err != nil {
		return
	}
	if res != nil {
		if err = json.Unmarshal(bs, res); err != nil {
			err = pkgerr.Wrapf(err, "host:%s, url:%s", req.URL.Host, realURL(req))
		}
	}
	return
}

// JSON sends an HTTP request and returns an HTTP json response.
func (client *Client) JSON(c context.Context, req *xhttp.Request, res interface{}, v ...string) (err error) {
	var bs []byte
	if bs, err = client.Raw(c, req, v...); err != nil {
		return
	}
	if res != nil {
		if err = json.Unmarshal(bs, res); err != nil {
			err = pkgerr.Wrapf(err, "host:%s, url:%s", req.URL.Host, realURL(req))
		}
	}
	return
}

// PB sends an HTTP request and returns an HTTP proto response.
func (client *Client) PB(c context.Context, req *xhttp.Request, res proto.Message, v ...string) (err error) {
	var bs []byte
	if bs, err = client.Raw(c, req, v...); err != nil {
		return
	}
	if res != nil {
		if err = proto.Unmarshal(bs, res); err != nil {
			err = pkgerr.Wrapf(err, "host:%s, url:%s", req.URL.Host, realURL(req))
		}
	}
	return
}

func (client *Client) onBreaker(breaker breaker.Breaker, err *error) {
	if err != nil && *err != nil {
		breaker.MarkFailed()
	} else {
		breaker.MarkSuccess()
	}
}

// realUrl return url with http://host/params.
func realURL(req *xhttp.Request) string {
	if req.Method == xhttp.MethodGet {
		return req.URL.String()
	} else if req.Method == xhttp.MethodPost {
		ru := req.URL.Path
		if req.Body != nil {
			rd, ok := req.Body.(io.Reader)
			if ok {
				buf := bytes.NewBuffer([]byte{})
				buf.ReadFrom(rd)
				ru = ru + "?" + buf.String()
			}
		}
		return ru
	}
	return req.URL.Path
}

// readAll reads from r until an error or EOF and returns the data it read
// from the internal buffer allocated with a specified capacity.
func readAll(r io.Reader, capacity int64) (b []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, capacity))
	// If the buffer overflows, we will get bytes.ErrTooLarge.
	// Return that as an error. Any other panic remains.
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok && panicErr == bytes.ErrTooLarge {
			err = panicErr
		} else {
			panic(e)
		}
	}()
	_, err = buf.ReadFrom(r)
	return buf.Bytes(), err
}
