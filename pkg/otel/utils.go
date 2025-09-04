package otel

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ConfigFromEnv creates OpenTelemetry configuration from environment variables
func ConfigFromEnv() *Config {
	cfg := DefaultConfig()

	if serviceName := os.Getenv("OTEL_SERVICE_NAME"); serviceName != "" {
		cfg.ServiceName = serviceName
	}

	if serviceVersion := os.Getenv("OTEL_SERVICE_VERSION"); serviceVersion != "" {
		cfg.ServiceVersion = serviceVersion
	}

	if environment := os.Getenv("OTEL_ENVIRONMENT"); environment != "" {
		cfg.Environment = environment
	}

	if exporterType := os.Getenv("OTEL_EXPORTER_TYPE"); exporterType != "" {
		cfg.ExporterType = exporterType
	}

	if jaegerEndpoint := os.Getenv("OTEL_EXPORTER_JAEGER_ENDPOINT"); jaegerEndpoint != "" {
		cfg.JaegerEndpoint = jaegerEndpoint
	}

	if otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); otlpEndpoint != "" {
		cfg.OTLPEndpoint = otlpEndpoint
	}

	if samplingRatio := os.Getenv("OTEL_SAMPLING_RATIO"); samplingRatio != "" {
		if ratio, err := strconv.ParseFloat(samplingRatio, 64); err == nil {
			cfg.SamplingRatio = ratio
		}
	}

	if batchTimeout := os.Getenv("OTEL_BATCH_TIMEOUT"); batchTimeout != "" {
		if timeout, err := time.ParseDuration(batchTimeout); err == nil {
			cfg.BatchTimeout = timeout
		}
	}

	if maxExportBatch := os.Getenv("OTEL_MAX_EXPORT_BATCH"); maxExportBatch != "" {
		if batch, err := strconv.Atoi(maxExportBatch); err == nil {
			cfg.MaxExportBatch = batch
		}
	}

	if maxQueueSize := os.Getenv("OTEL_MAX_QUEUE_SIZE"); maxQueueSize != "" {
		if queue, err := strconv.Atoi(maxQueueSize); err == nil {
			cfg.MaxQueueSize = queue
		}
	}

	if enabled := os.Getenv("OTEL_ENABLED"); enabled != "" {
		cfg.Enabled = strings.ToLower(enabled) == "true"
	}

	return cfg
}

// InjectTraceContext injects trace context into a map (useful for message headers)
func InjectTraceContext(ctx context.Context) map[string]string {
	headers := make(map[string]string)
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	propagator.Inject(ctx, propagation.MapCarrier(headers))
	return headers
}

// ExtractTraceContext extracts trace context from a map (useful for message headers)
func ExtractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	return propagator.Extract(ctx, propagation.MapCarrier(headers))
}

// SpanFromContext returns the current span from context
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// ContextWithSpan returns a new context with the given span
func ContextWithSpan(ctx context.Context, span trace.Span) context.Context {
	return trace.ContextWithSpan(ctx, span)
}

// TraceIDFromContext returns the trace ID from the current span in context
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// SpanIDFromContext returns the span ID from the current span in context
func SpanIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

// IsTracingEnabled checks if tracing is enabled in the current context
func IsTracingEnabled(ctx context.Context) bool {
	span := trace.SpanFromContext(ctx)
	return span.IsRecording()
}

// AddCommonAttributes adds common attributes to a span
func AddCommonAttributes(span trace.Span, attrs map[string]interface{}) {
	if !span.IsRecording() {
		return
	}

	for key, value := range attrs {
		attr := convertToAttribute(key, value)
		span.SetAttributes(attr)
	}
}

// LogError logs an error with trace context
func LogError(ctx context.Context, err error, message string) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.RecordError(err)
		span.AddEvent(fmt.Sprintf("error: %s", message))
	}
}

// LogInfo logs an info message with trace context
func LogInfo(ctx context.Context, message string, attrs map[string]interface{}) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		eventAttrs := make([]trace.EventOption, 0)
		if attrs != nil {
			for key, value := range attrs {
				attr := convertToAttribute(key, value)
				eventAttrs = append(eventAttrs, trace.WithAttributes(attr))
			}
		}
		span.AddEvent(fmt.Sprintf("info: %s", message), eventAttrs...)
	}
}

// CreateChildSpan creates a child span from the current context
func CreateChildSpan(ctx context.Context, tracer *Tracer, spanName string, attrs map[string]interface{}) (context.Context, trace.Span) {
	return tracer.StartSpanWithAttributes(ctx, spanName, attrs)
}

// FinishSpan properly finishes a span with optional error
func FinishSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// WrapFunction wraps a function with tracing
func WrapFunction(ctx context.Context, tracer *Tracer, funcName string, fn func() error) error {
	return tracer.WithSpan(ctx, funcName, func(ctx context.Context, span trace.Span) error {
		return fn()
	})
}

// WrapAsyncFunction wraps an async function with tracing
func WrapAsyncFunction(ctx context.Context, tracer *Tracer, funcName string, fn func()) {
	tracer.WithSpanAsync(ctx, funcName, func(ctx context.Context, span trace.Span) {
		fn()
	})
}
