package config

type PubSubConfig struct {
	PubSubType   string   // "redis" or "kafka"
	KafkaBrokers []string // for Kafka
	RedisAddr    string   // for Redis
}
