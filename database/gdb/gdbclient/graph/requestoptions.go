package graph

// GDB request options
type RequestOptions struct {
	requestId  string
	batchSize  int32 // not used
	timeout    int64
	aliases    map[string]string // not support
	parameters map[string]interface{}
}

func NewRequestOptionsWithBindings(bindings map[string]interface{}) *RequestOptions {
	opt := &RequestOptions{parameters: make(map[string]interface{})}
	if bindings != nil {
		opt.parameters[ARGS_BINDINGS] = bindings
	}
	return opt
}

func (opt *RequestOptions) GetOverrideRequestId() string {
	return opt.requestId
}

func (opt *RequestOptions) GetTimeout() int64 {
	return opt.timeout
}

func (opt *RequestOptions) GetArgs() map[string]interface{} {
	return opt.parameters
}

func (opt *RequestOptions) SetRequestId(requestId string) {
	opt.requestId = requestId
}

func (opt *RequestOptions) SetTimeout(timeout int64) {
	opt.timeout = timeout
}

func (opt *RequestOptions) AddArgs(key string, value interface{}) {
	opt.parameters[key] = value
}
