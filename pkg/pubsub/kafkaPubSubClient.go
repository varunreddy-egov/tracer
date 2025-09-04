package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// NewKafkaPubSubClientFromEnv creates a KafkaPubSubClient using broker addresses from the environment variable.
func NewKafkaPubSubClientFromEnv() *KafkaPubSubClient {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	// Split on comma and trim spaces for each broker.
	brokers := []string{}
	for _, broker := range strings.Split(brokersEnv, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return NewKafkaPubSubClient(brokers)
}

// KafkaPubSubClient is an implementation of the PubSubClient interface for Kafka.
type KafkaPubSubClient struct {
	Brokers []string
	// writer for publishing messages
	writer *kafka.Writer
	// subscriptions keeps track of active subscription cancel functions keyed by topic.
	subscriptions map[string]context.CancelFunc
	mu            sync.Mutex
}

// NewKafkaPubSubClient creates a new KafkaPubSubClient given the Kafka broker addresses.
func NewKafkaPubSubClient(brokers []string) *KafkaPubSubClient {
	return &KafkaPubSubClient{
		Brokers:       brokers,
		subscriptions: make(map[string]context.CancelFunc),
	}
}

// Connect establishes connections needed for publishing.
// For a Kafka client, we can lazily initialize the writer.
func (k *KafkaPubSubClient) Connect() error {
	if len(k.Brokers) == 0 {
		return errors.New("no Kafka brokers provided")
	}

	// Initialize a writer with default options.
	k.writer = &kafka.Writer{
		Addr:     kafka.TCP(k.Brokers...),
		Balancer: &kafka.LeastBytes{},
	}

	// Optionally, you could check the connection by performing a metadata request.

	log.Println("connected to Kafka brokers:", k.Brokers)
	return nil
}

// Disconnect gracefully closes the connection to Kafka for publishing and cancels any active subscriptions.
func (k *KafkaPubSubClient) Disconnect() error {
	// Cancel all subscriptions.
	k.mu.Lock()
	for topic, cancelFunc := range k.subscriptions {
		cancelFunc()
		log.Printf("subscription for topic %q canceled\n", topic)
	}
	k.subscriptions = make(map[string]context.CancelFunc)
	k.mu.Unlock()

	// Close the writer if it exists.
	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			return err
		}
	}
	log.Println("disconnected from Kafka")
	return nil
}

// Publish sends a message to a given Kafka topic.
// In this example, message is first marshaled to JSON to accommodate various types.
func (k *KafkaPubSubClient) Publish(topic string, message interface{}) error {
	// Marshal the message as JSON.
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Create a writer that targets the specific topic.
	// Alternatively, you could set up topic routing on the writer.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  k.Brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   nil,
		Value: data,
	})
	if err != nil {
		return err
	}

	log.Printf("published message to topic %q\n", topic)
	return nil
}

// Subscribe starts a consumer that listens for messages on a given topic and calls the callback.
// For simplicity, we allow a single subscription per topic.
func (k *KafkaPubSubClient) Subscribe(topic string, callback func(message interface{})) error {
	k.mu.Lock()
	if _, exists := k.subscriptions[topic]; exists {
		k.mu.Unlock()
		return errors.New("subscription already exists for topic " + topic)
	}
	k.mu.Unlock()

	// Create a cancelable context so that we can stop the subscription.
	ctx, cancel := context.WithCancel(context.Background())

	// Save the cancel function so that Unsubscribe can cancel the goroutine.
	k.mu.Lock()
	k.subscriptions[topic] = cancel
	k.mu.Unlock()

	// Start a goroutine to continuously poll for messages.
	go func() {
		// Create a new Kafka reader for the topic.
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  k.Brokers,
			Topic:    topic,
			GroupID:  "pubsub-group-" + topic, // each topic gets its own group for this simplified example
			MinBytes: 10e3,                    // 10KB
			MaxBytes: 10e6,                    // 10MB
		})
		defer reader.Close()

		log.Printf("subscribed to topic %q\n", topic)
		for {
			select {
			case <-ctx.Done():
				log.Printf("stopping subscription for topic %q\n", topic)
				return
			default:
				// Use a timeout context for each read operation.
				readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
				msg, err := reader.ReadMessage(readCtx)
				readCancel() // ensure we cancel context after the read
				if err != nil {
					// Log error and continue. In production, you might differentiate between temporary and permanent errors.
					log.Printf("error reading message from topic %q: %v\n", topic, err)
					continue
				}

				// Attempt to unmarshal JSON back into a generic interface{}.
				var parsed interface{}
				if err := json.Unmarshal(msg.Value, &parsed); err != nil {
					// If unmarshaling fails, send raw string.
					parsed = string(msg.Value)
				}

				// Execute the callback in its own goroutine.
				go callback(parsed)
			}
		}
	}()

	return nil
}

// Unsubscribe cancels an active subscription for a given topic if the provided callback matches the registered one.
// In this implementation, since we allow a single subscription per topic, Unsubscribe simply cancels that subscription.
func (k *KafkaPubSubClient) Unsubscribe(topic string, callback func(message interface{})) error {
	k.mu.Lock()
	cancelFunc, exists := k.subscriptions[topic]
	if !exists {
		k.mu.Unlock()
		return errors.New("no active subscription for topic " + topic)
	}
	// Remove the subscription regardless of callback equality.
	delete(k.subscriptions, topic)
	k.mu.Unlock()

	// Cancel the subscription goroutine.
	cancelFunc()
	log.Printf("unsubscribed from topic %q\n", topic)
	return nil
}
