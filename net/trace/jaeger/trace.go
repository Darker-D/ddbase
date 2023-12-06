package jaeger

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
)

// Config
// TODO config jaeger

// Init returns an instance of Jaeger Tracer
func Init(serviceName string) (io.Closer, error) {
	sender, err := jaeger.NewUDPTransport("", 0)
	if err != nil {
		return nil, err
	}
	//to, _ := jaeger.NewProbabilisticSampler(0.015)
	tracer, Closer := jaeger.NewTracer(serviceName, jaeger.NewConstSampler(true), jaeger.NewRemoteReporter(sender))
	opentracing.SetGlobalTracer(tracer)
	return Closer, nil
}
