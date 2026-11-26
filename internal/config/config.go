// Package config junta todas las variables de entorno en un solo lugar.
// Los defaults apuntan a los nombres de servicio de docker-compose, así el
// mismo binario funciona adentro del container y en la laptop con un override.
package config

import "os"

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	KafkaBrokers  []string
	KafkaTopic    string
	ClickHouseDSN string
	WebhookSecret string
	Port          string
}

func Load() Config {
	return Config{
		PostgresDSN:   env("POSTGRES_DSN", "postgres://stripe:stripe@postgres:5432/stripe?sslmode=disable"),
		RedisAddr:     env("REDIS_ADDR", "redis:6379"),
		KafkaBrokers:  []string{env("KAFKA_BROKERS", "kafka:9092")},
		KafkaTopic:    env("KAFKA_TOPIC", "payments.events"),
		ClickHouseDSN: env("CLICKHOUSE_DSN", "clickhouse://stripe:stripe@clickhouse:9000/stripe"),
		WebhookSecret: env("STRIPE_WEBHOOK_SECRET", "whsec_test"),
		Port:          env("PORT", "8081"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
