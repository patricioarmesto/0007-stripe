package config

import "testing"

func TestLoadUsaDefaultsCuandoNoHayEnv(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")
	c := Load()
	if c.PostgresDSN != "postgres://stripe:stripe@postgres:5432/stripe?sslmode=disable" {
		t.Fatalf("DSN por default incorrecto: %s", c.PostgresDSN)
	}
	if c.WebhookSecret != "whsec_test" {
		t.Fatalf("secret por default incorrecto: %s", c.WebhookSecret)
	}
	if c.KafkaTopic != "payments.events" {
		t.Fatalf("topic por default incorrecto: %s", c.KafkaTopic)
	}
}

func TestLoadRespetaEnv(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "otro_secret")
	if Load().WebhookSecret != "otro_secret" {
		t.Fatal("debe respetar la variable de entorno")
	}
}
