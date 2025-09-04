// pubsub_registry.go
package pubsub

import (
	"log"

	"github.com/varunreddy-egov/tracer/pkg/config"
)

func init() {
	RegisterPubSubClient("kafka", func(cfg config.PubSubConfig) PubSubClient {
		client := NewKafkaPubSubClient(cfg.KafkaBrokers)
		if err := client.Connect(); err != nil {
			log.Printf("Error connecting Kafka client: %v", err)
		}
		return client
	})

	RegisterPubSubClient("redis", func(cfg config.PubSubConfig) PubSubClient {
		client := NewRedisPubSubClient(cfg.RedisAddr)
		if err := client.Connect(); err != nil {
			log.Printf("Error connecting Redis client: %v", err)
		}
		return client
	})
}
