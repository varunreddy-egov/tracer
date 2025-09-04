// tracer/kafka/producer.go
package kafka

import (
	"context"

	"github.com/varunreddy-egov/tracer/pkg/logger"
	"github.com/varunreddy-egov/tracer/pkg/validation"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) SendMessage(ctx context.Context, key, value string) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	})
	if err != nil {
		logger.Log.Error("Kafka Send Failed",
			"topic", p.writer.Topic,
			"key", key,
			"error", err)
		return err
	}

	// Use validation package to check if value is valid JSON
	if validation.IsValidJSON([]byte(value)) {
		// Pass the raw JSON string to the logger
		logger.Log.Info("Kafka Send Success",
			"topic", p.writer.Topic,
			"key", key,
			"value", value)
	} else {
		logger.Log.Info("Kafka Send Success",
			"topic", p.writer.Topic,
			"key", key,
			"value", value)
	}
	return nil
}
