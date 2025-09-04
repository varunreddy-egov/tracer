// tracer/kafka/consumer.go
package kafka

import (
	"context"

	"github.com/varunreddy-egov/tracer/pkg/logger"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

func (c *Consumer) Consume(ctx context.Context) {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			logger.Log.Error("Kafka Consume Error", "error", err)
			continue
		}
		logger.Log.Info("Kafka Received",
			"topic", msg.Topic,
			"key", string(msg.Key),
			"value", string(msg.Value))
		// Process message
	}
}
