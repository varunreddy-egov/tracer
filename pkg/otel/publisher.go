package otel

import (
	"context"
	"encoding/json"
	"fmt"

	"digit-core/pkg/tracer/config"
	"digit-core/pkg/tracer/pubsub"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracedPublisher wraps a PubSub client with OpenTelemetry tracing
type TracedPublisher struct {
	client pubsub.PubSubClient
	tracer *Tracer
}

// NewTracedPublisher creates a new traced publisher
func NewTracedPublisher(cfg config.PubSubConfig, tracer *Tracer) (*TracedPublisher, error) {
	client, err := pubsub.NewPubSubClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	return &TracedPublisher{
		client: client,
		tracer: tracer,
	}, nil
}

// Connect connects to the PubSub system with tracing
func (tp *TracedPublisher) Connect(ctx context.Context) error {
	return tp.tracer.WithSpan(ctx, "pubsub.connect", func(ctx context.Context, span trace.Span) error {
		err := tp.client.Connect()
		if err != nil {
			span.SetAttributes(attribute.String("error", err.Error()))
		}
		return err
	})
}

// Disconnect disconnects from the PubSub system with tracing
func (tp *TracedPublisher) Disconnect(ctx context.Context) error {
	return tp.tracer.WithSpan(ctx, "pubsub.disconnect", func(ctx context.Context, span trace.Span) error {
		err := tp.client.Disconnect()
		if err != nil {
			span.SetAttributes(attribute.String("error", err.Error()))
		}
		return err
	})
}

// Publish publishes a message with tracing
func (tp *TracedPublisher) Publish(ctx context.Context, topic string, message interface{}) error {
	return tp.tracer.WithSpan(ctx, "pubsub.publish", func(ctx context.Context, span trace.Span) error {
		// Add trace context to message if it's a map
		if msgMap, ok := message.(map[string]interface{}); ok {
			traceHeaders := InjectTraceContext(ctx)
			msgMap["_trace"] = traceHeaders
			message = msgMap
		}

		// Set span attributes
		span.SetAttributes(
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.operation", "publish"),
		)

		// Add message size if possible
		if data, err := json.Marshal(message); err == nil {
			span.SetAttributes(attribute.Int("messaging.message_payload_size_bytes", len(data)))
		}

		err := tp.client.Publish(topic, message)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "Message published successfully")
		}
		return err
	}, trace.WithSpanKind(trace.SpanKindProducer))
}

// Subscribe subscribes to a topic with tracing
func (tp *TracedPublisher) Subscribe(ctx context.Context, topic string, callback func(message interface{})) error {
	return tp.tracer.WithSpan(ctx, "pubsub.subscribe", func(ctx context.Context, span trace.Span) error {
		span.SetAttributes(
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.operation", "subscribe"),
		)

		// Wrap callback with tracing
		tracedCallback := func(message interface{}) {
			// Create a new context for each message
			msgCtx := context.Background()
			
			// Extract trace context if present in message
			if msgMap, ok := message.(map[string]interface{}); ok {
				if traceData, exists := msgMap["_trace"]; exists {
					if traceHeaders, ok := traceData.(map[string]string); ok {
						msgCtx = ExtractTraceContext(msgCtx, traceHeaders)
						// Remove trace data from message before processing
						delete(msgMap, "_trace")
						message = msgMap
					}
				}
			}

			// Process message with tracing
			tp.tracer.WithSpan(msgCtx, "pubsub.message.process", func(ctx context.Context, span trace.Span) error {
				span.SetAttributes(
					attribute.String("messaging.destination", topic),
					attribute.String("messaging.operation", "receive"),
				)

				// Add message size if possible
				if data, err := json.Marshal(message); err == nil {
					span.SetAttributes(attribute.Int("messaging.message_payload_size_bytes", len(data)))
				}

				defer func() {
					if r := recover(); r != nil {
						err := fmt.Errorf("panic in message handler: %v", r)
						span.RecordError(err)
						span.SetStatus(codes.Error, err.Error())
					}
				}()

				callback(message)
				span.SetStatus(codes.Ok, "Message processed successfully")
				return nil
			}, trace.WithSpanKind(trace.SpanKindConsumer))
		}

		return tp.client.Subscribe(topic, tracedCallback)
	})
}

// Unsubscribe unsubscribes from a topic with tracing
func (tp *TracedPublisher) Unsubscribe(ctx context.Context, topic string, callback func(message interface{})) error {
	return tp.tracer.WithSpan(ctx, "pubsub.unsubscribe", func(ctx context.Context, span trace.Span) error {
		span.SetAttributes(
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.operation", "unsubscribe"),
		)

		err := tp.client.Unsubscribe(topic, callback)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "Unsubscribed successfully")
		}
		return err
	})
}
