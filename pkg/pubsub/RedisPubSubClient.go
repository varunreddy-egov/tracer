package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
)

// subscription holds the details of an individual subscription.
type subscription struct {
	topic    string
	callback func(message interface{})
	pubsub   *redis.PubSub
	// cancel is used to signal the receiving goroutine to stop.
	cancel context.CancelFunc
}

// RedisPubSubClient implements the PubSubClient interface using Redis.
type RedisPubSubClient struct {
	redisClient *redis.Client
	ctx         context.Context

	// subs maintains a mapping of topic to a list of subscriptions.
	subs map[string][]*subscription
	mu   sync.Mutex
}

// NewRedisPubSubClient creates a new RedisPubSubClient using the provided address (e.g., "localhost:6379").
func NewRedisPubSubClient(addr string) *RedisPubSubClient {
	return &RedisPubSubClient{
		redisClient: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
		ctx:  context.Background(),
		subs: make(map[string][]*subscription),
	}
}

// Connect pings Redis to ensure a connection can be established.
func (r *RedisPubSubClient) Connect() error {
	if err := r.redisClient.Ping(r.ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}
	return nil
}

// Disconnect gracefully closes all active subscriptions and the Redis connection.
func (r *RedisPubSubClient) Disconnect() error {
	// Stop all subscription goroutines.
	r.mu.Lock()
	for topic, subs := range r.subs {
		for _, sub := range subs {
			// Cancel the subscription's goroutine.
			sub.cancel()
			// Close the underlying redis subscription.
			_ = sub.pubsub.Close()
		}
		delete(r.subs, topic)
	}
	r.mu.Unlock()

	// Close the Redis client.
	return r.redisClient.Close()
}

// Publish sends a message to the specified topic/channel.
func (r *RedisPubSubClient) Publish(topic string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return r.redisClient.Publish(r.ctx, topic, data).Err()
}

// Subscribe registers a callback for the given topic.
func (r *RedisPubSubClient) Subscribe(topic string, callback func(message interface{})) error {
	// Subscribe to the topic.
	pubsub := r.redisClient.Subscribe(r.ctx, topic)
	// It is good practice to wait for confirmation.
	_, err := pubsub.Receive(r.ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	// Create a cancellable context for the subscription goroutine.
	subCtx, cancel := context.WithCancel(r.ctx)

	sub := &subscription{
		topic:    topic,
		callback: callback,
		pubsub:   pubsub,
		cancel:   cancel,
	}

	// Start a goroutine to listen for messages.
	go func() {
		ch := pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					// Channel closed
					return
				}
				// Call the callback with the message payload.
				callback(msg.Payload)
			}
		}
	}()

	// Store the subscription.
	r.mu.Lock()
	r.subs[topic] = append(r.subs[topic], sub)
	r.mu.Unlock()

	return nil
}

// Unsubscribe removes a previously registered callback from a specific topic.
func (r *RedisPubSubClient) Unsubscribe(topic string, callback func(message interface{})) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subs, ok := r.subs[topic]
	if !ok {
		return errors.New("no subscription found for the given topic")
	}

	// Find subscriptions that match the callback.
	found := false
	for i := 0; i < len(subs); {
		// Function values can be compared directly if they are non-closures.
		if fmt.Sprintf("%p", subs[i].callback) == fmt.Sprintf("%p", callback) {
			// Stop the subscription goroutine.
			subs[i].cancel()
			// Close the redis subscription.
			_ = subs[i].pubsub.Close()

			// Remove the subscription from the slice.
			subs = append(subs[:i], subs[i+1:]...)
			found = true
		} else {
			i++
		}
	}

	if !found {
		return errors.New("specified callback not found for the topic")
	}

	// Update the subscriptions for the topic.
	if len(subs) == 0 {
		delete(r.subs, topic)
	} else {
		r.subs[topic] = subs
	}

	return nil
}
