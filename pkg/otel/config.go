package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Config holds OpenTelemetry configuration
type Config struct {
	ServiceName     string        `json:"serviceName" yaml:"serviceName"`
	ServiceVersion  string        `json:"serviceVersion" yaml:"serviceVersion"`
	Environment     string        `json:"environment" yaml:"environment"`
	ExporterType    string        `json:"exporterType" yaml:"exporterType"` // jaeger, otlp, stdout, none
	JaegerEndpoint  string        `json:"jaegerEndpoint" yaml:"jaegerEndpoint"`
	OTLPEndpoint    string        `json:"otlpEndpoint" yaml:"otlpEndpoint"`
	SamplingRatio   float64       `json:"samplingRatio" yaml:"samplingRatio"`
	BatchTimeout    time.Duration `json:"batchTimeout" yaml:"batchTimeout"`
	MaxExportBatch  int           `json:"maxExportBatch" yaml:"maxExportBatch"`
	MaxQueueSize    int           `json:"maxQueueSize" yaml:"maxQueueSize"`
	Enabled         bool          `json:"enabled" yaml:"enabled"`
}

// DefaultConfig returns a default OpenTelemetry configuration
func DefaultConfig() *Config {
	return &Config{
		ServiceName:     "unknown-service",
		ServiceVersion:  "1.0.0",
		Environment:     "development",
		ExporterType:    "stdout",
		JaegerEndpoint:  "http://localhost:14268/api/traces",
		OTLPEndpoint:    "http://localhost:4318/v1/traces",
		SamplingRatio:   1.0,
		BatchTimeout:    5 * time.Second,
		MaxExportBatch:  512,
		MaxQueueSize:    2048,
		Enabled:         true,
	}
}

// InitTracer initializes OpenTelemetry tracer with the given configuration
func InitTracer(cfg *Config) (*sdktrace.TracerProvider, error) {
	if !cfg.Enabled {
		// Return a no-op tracer provider
		return sdktrace.NewTracerProvider(), nil
	}

	// Create resource with service information
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String(cfg.ServiceVersion),
			semconv.DeploymentEnvironmentKey.String(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create exporter based on configuration
	exporter, err := createExporter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	// Create tracer provider with batch processor
	var batchOptions []sdktrace.BatchSpanProcessorOption
	if cfg.BatchTimeout > 0 {
		batchOptions = append(batchOptions, sdktrace.WithBatchTimeout(cfg.BatchTimeout))
	}
	if cfg.MaxExportBatch > 0 {
		batchOptions = append(batchOptions, sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatch))
	}
	if cfg.MaxQueueSize > 0 {
		batchOptions = append(batchOptions, sdktrace.WithMaxQueueSize(cfg.MaxQueueSize))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, batchOptions...),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SamplingRatio)),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	return tp, nil
}

// createExporter creates the appropriate exporter based on configuration
func createExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	switch cfg.ExporterType {
	case "jaeger":
		return jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(cfg.JaegerEndpoint)))
	case "otlp":
		return otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
			otlptracehttp.WithInsecure(),
		)
	case "stdout":
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "none":
		return &noopExporter{}, nil
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", cfg.ExporterType)
	}
}

// noopExporter is a no-op exporter for when tracing is disabled
type noopExporter struct{}

func (e *noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *noopExporter) Shutdown(ctx context.Context) error {
	return nil
}
