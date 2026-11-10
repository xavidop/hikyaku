package metrics

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// TracingOptions configure the OTLP/HTTP exporter.
type TracingOptions struct {
	Endpoint    string // host:port or a URL; empty turns tracing off
	Insecure    bool
	Headers     map[string]string
	ServiceName string
	SampleRatio float64 // 0 means 1
	Version     string
}

// SetupTracing installs a global tracer provider exporting over OTLP/HTTP
// and returns its shutdown. With no endpoint it does nothing.
func SetupTracing(ctx context.Context, o TracingOptions) (func(context.Context) error, error) {
	if o.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	opts := []otlptracehttp.Option{}
	if strings.HasPrefix(o.Endpoint, "http://") || strings.HasPrefix(o.Endpoint, "https://") {
		opts = append(opts, otlptracehttp.WithEndpointURL(strings.TrimSuffix(o.Endpoint, "/")+"/v1/traces"))
		if strings.HasPrefix(o.Endpoint, "http://") {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(o.Endpoint))
		if o.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
	}
	if len(o.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(o.Headers))
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otel: %w", err)
	}
	name := o.ServiceName
	if name == "" {
		name = "hikyaku"
	}
	ratio := o.SampleRatio
	if ratio <= 0 {
		ratio = 1
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(name), semconv.ServiceVersion(o.Version))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}
