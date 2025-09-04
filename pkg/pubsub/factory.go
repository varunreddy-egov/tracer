package pubsub

import (
	"digit-core/pkg/tracer/config"
	"fmt"
)

type PubSubClient interface {
	Connect() error
	Disconnect() error
	Publish(topic string, message interface{}) error
	Subscribe(topic string, callback func(message interface{})) error
	Unsubscribe(topic string, callback func(message interface{})) error
}

type FactoryFunc func(cfg config.PubSubConfig) PubSubClient

var factoryRegistry = map[string]FactoryFunc{}

// RegisterPubSubClient adds a new pubsub factory under a specified key.
func RegisterPubSubClient(key string, factory FactoryFunc) {
	factoryRegistry[key] = factory
}

// NewPubSubClient retrieves a PubSubClient based on the configuration.
func NewPubSubClient(cfg config.PubSubConfig) (PubSubClient, error) {
	if factory, ok := factoryRegistry[cfg.PubSubType]; ok {
		return factory(cfg), nil
	}
	return nil, fmt.Errorf("unsupported pubsub type: %s", cfg.PubSubType)
}
