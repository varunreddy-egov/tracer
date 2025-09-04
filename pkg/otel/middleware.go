package otel

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware provides OpenTelemetry tracing for HTTP handlers
func HTTPMiddleware(serviceName string) func(http.Handler) http.Handler {
	tracer := otel.Tracer(serviceName)
	propagator := otel.GetTextMapPropagator()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract trace context from headers
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			
			// Start span
			spanName := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPMethodKey.String(r.Method),
					semconv.HTTPURLKey.String(r.URL.String()),
					semconv.HTTPSchemeKey.String(r.URL.Scheme),
					semconv.HTTPHostKey.String(r.Host),
					semconv.HTTPTargetKey.String(r.URL.Path),
					semconv.HTTPUserAgentKey.String(r.UserAgent()),
					semconv.ServiceNameKey.String(serviceName),
				),
			)
			defer span.End()

			// Wrap response writer to capture status code
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			
			// Update request context
			r = r.WithContext(ctx)
			
			// Record start time
			start := time.Now()
			
			// Call next handler
			next.ServeHTTP(wrapped, r)
			
			// Record span attributes
			span.SetAttributes(
				attribute.Int("http.status_code", wrapped.statusCode),
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.route", r.URL.Path),
				attribute.String("http.user_agent", r.UserAgent()),
				attribute.Int64("http.request_content_length", r.ContentLength),
				attribute.Int64("http.response_content_length", wrapped.bytesWritten),
				attribute.Float64("http.duration_ms", float64(time.Since(start).Nanoseconds())/1e6),
			)
			
			// Set span status based on HTTP status code
			if wrapped.statusCode >= 400 {
				span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", wrapped.statusCode))
			} else {
				span.SetStatus(codes.Ok, "")
			}
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture response details
type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// GinMiddleware provides OpenTelemetry tracing for Gin framework
// This is a placeholder - actual implementation would depend on importing Gin
func GinMiddleware(serviceName string) func(c interface{}) {
	return func(c interface{}) {
		// This is a generic interface to work with different frameworks
		// In actual usage, this would be typed as *gin.Context
		
		// For now, we'll provide a placeholder that can be adapted
		// The actual implementation would depend on the specific framework
		
		// Example implementation structure:
		/*
		tracer := otel.Tracer(serviceName)
		propagator := otel.GetTextMapPropagator()
		
		ctx := propagator.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		spanName := fmt.Sprintf("%s %s", c.Request.Method, c.FullPath())
		ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		
		// Record attributes and status
		*/
	}
}

// DatabaseMiddleware provides tracing for database operations
func DatabaseMiddleware(tracer *Tracer, operation, table string) func(ctx context.Context, fn func(context.Context) error) error {
	return func(ctx context.Context, fn func(context.Context) error) error {
		spanName := fmt.Sprintf("db.%s %s", operation, table)
		
		return tracer.WithSpan(ctx, spanName, func(ctx context.Context, span trace.Span) error {
			span.SetAttributes(
				attribute.String("db.operation", operation),
				attribute.String("db.table", table),
				attribute.String("db.system", "unknown"), // This should be set based on actual DB
			)
			
			start := time.Now()
			err := fn(ctx)
			duration := time.Since(start)
			
			span.SetAttributes(
				attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1e6),
			)
			
			return err
		}, trace.WithSpanKind(trace.SpanKindClient))
	}
}

// KafkaProducerMiddleware provides tracing for Kafka producer operations
func KafkaProducerMiddleware(tracer *Tracer, topic string) func(ctx context.Context, fn func(context.Context) error) error {
	return func(ctx context.Context, fn func(context.Context) error) error {
		spanName := fmt.Sprintf("kafka.produce %s", topic)
		
		return tracer.WithSpan(ctx, spanName, func(ctx context.Context, span trace.Span) error {
			span.SetAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", topic),
				attribute.String("messaging.operation", "publish"),
			)
			
			return fn(ctx)
		}, trace.WithSpanKind(trace.SpanKindProducer))
	}
}

// KafkaConsumerMiddleware provides tracing for Kafka consumer operations
func KafkaConsumerMiddleware(tracer *Tracer, topic string) func(ctx context.Context, fn func(context.Context) error) error {
	return func(ctx context.Context, fn func(context.Context) error) error {
		spanName := fmt.Sprintf("kafka.consume %s", topic)
		
		return tracer.WithSpan(ctx, spanName, func(ctx context.Context, span trace.Span) error {
			span.SetAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", topic),
				attribute.String("messaging.operation", "receive"),
			)
			
			return fn(ctx)
		}, trace.WithSpanKind(trace.SpanKindConsumer))
	}
}

// RedisMiddleware provides tracing for Redis operations
func RedisMiddleware(tracer *Tracer, operation string) func(ctx context.Context, fn func(context.Context) error) error {
	return func(ctx context.Context, fn func(context.Context) error) error {
		spanName := fmt.Sprintf("redis.%s", operation)
		
		return tracer.WithSpan(ctx, spanName, func(ctx context.Context, span trace.Span) error {
			span.SetAttributes(
				attribute.String("db.system", "redis"),
				attribute.String("db.operation", operation),
			)
			
			return fn(ctx)
		}, trace.WithSpanKind(trace.SpanKindClient))
	}
}
