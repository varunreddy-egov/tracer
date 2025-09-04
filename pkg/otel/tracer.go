package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracer wraps OpenTelemetry tracer with additional functionality
type Tracer struct {
	tracer trace.Tracer
	name   string
}

// NewTracer creates a new tracer instance
func NewTracer(name string) *Tracer {
	return &Tracer{
		tracer: otel.Tracer(name),
		name:   name,
	}
}

// StartSpan starts a new span with the given name and options
func (t *Tracer) StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, spanName, opts...)
}

// StartSpanWithAttributes starts a new span with attributes
func (t *Tracer) StartSpanWithAttributes(ctx context.Context, spanName string, attrs map[string]interface{}, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	// Convert attributes to OpenTelemetry format
	otelAttrs := make([]attribute.KeyValue, 0, len(attrs))
	for key, value := range attrs {
		otelAttrs = append(otelAttrs, convertToAttribute(key, value))
	}
	
	// Add attributes to span start options
	opts = append(opts, trace.WithAttributes(otelAttrs...))
	
	return t.tracer.Start(ctx, spanName, opts...)
}

// WithSpan executes a function within a span context
func (t *Tracer) WithSpan(ctx context.Context, spanName string, fn func(ctx context.Context, span trace.Span) error, opts ...trace.SpanStartOption) error {
	ctx, span := t.tracer.Start(ctx, spanName, opts...)
	defer span.End()
	
	if err := fn(ctx, span); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	
	span.SetStatus(codes.Ok, "")
	return nil
}

// WithSpanAsync executes a function within a span context asynchronously
func (t *Tracer) WithSpanAsync(ctx context.Context, spanName string, fn func(ctx context.Context, span trace.Span), opts ...trace.SpanStartOption) {
	go func() {
		ctx, span := t.tracer.Start(ctx, spanName, opts...)
		defer span.End()
		
		defer func() {
			if r := recover(); r != nil {
				span.RecordError(fmt.Errorf("panic: %v", r))
				span.SetStatus(codes.Error, fmt.Sprintf("panic: %v", r))
			}
		}()
		
		fn(ctx, span)
		span.SetStatus(codes.Ok, "")
	}()
}

// AddEvent adds an event to the current span
func (t *Tracer) AddEvent(ctx context.Context, name string, attrs map[string]interface{}) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		otelAttrs := make([]attribute.KeyValue, 0, len(attrs))
		for key, value := range attrs {
			otelAttrs = append(otelAttrs, convertToAttribute(key, value))
		}
		span.AddEvent(name, trace.WithAttributes(otelAttrs...))
	}
}

// SetAttributes sets attributes on the current span
func (t *Tracer) SetAttributes(ctx context.Context, attrs map[string]interface{}) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		otelAttrs := make([]attribute.KeyValue, 0, len(attrs))
		for key, value := range attrs {
			otelAttrs = append(otelAttrs, convertToAttribute(key, value))
		}
		span.SetAttributes(otelAttrs...)
	}
}

// RecordError records an error on the current span
func (t *Tracer) RecordError(ctx context.Context, err error, attrs map[string]interface{}) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		options := []trace.EventOption{}
		if attrs != nil {
			otelAttrs := make([]attribute.KeyValue, 0, len(attrs))
			for key, value := range attrs {
				otelAttrs = append(otelAttrs, convertToAttribute(key, value))
			}
			options = append(options, trace.WithAttributes(otelAttrs...))
		}
		span.RecordError(err, options...)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetStatus sets the status of the current span
func (t *Tracer) SetStatus(ctx context.Context, code codes.Code, description string) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(code, description)
	}
}

// GetTraceID returns the trace ID of the current span
func (t *Tracer) GetTraceID(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// GetSpanID returns the span ID of the current span
func (t *Tracer) GetSpanID(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

// IsRecording returns true if the current span is recording
func (t *Tracer) IsRecording(ctx context.Context) bool {
	span := trace.SpanFromContext(ctx)
	return span.IsRecording()
}

// WithSpanSimple executes a function within a span context with simplified error handling
// Returns the error from the function, automatically recording it in the span
func (t *Tracer) WithSpanSimple(ctx context.Context, spanName string, fn func(ctx context.Context) error) error {
	ctx, span := t.tracer.Start(ctx, spanName)
	defer span.End()
	
	if err := fn(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	
	span.SetStatus(codes.Ok, "")
	return nil
}

// AddAttribute adds a single attribute to the current span
func (t *Tracer) AddAttribute(ctx context.Context, key string, value interface{}) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(convertToAttribute(key, value))
	}
}

// RecordErrorSimple records an error on the current span with simplified API
func (t *Tracer) RecordErrorSimple(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetStatusOK sets the span status to OK
func (t *Tracer) SetStatusOK(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(codes.Ok, "")
	}
}

// SetStatusError sets the span status to Error with a message
func (t *Tracer) SetStatusError(ctx context.Context, message string) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(codes.Error, message)
	}
}

// convertToAttribute converts a generic value to OpenTelemetry attribute
func convertToAttribute(key string, value interface{}) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case bool:
		return attribute.Bool(key, v)
	case []string:
		return attribute.StringSlice(key, v)
	case []int:
		return attribute.IntSlice(key, v)
	case []int64:
		return attribute.Int64Slice(key, v)
	case []float64:
		return attribute.Float64Slice(key, v)
	case []bool:
		return attribute.BoolSlice(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}
