// Binario consumidor: no sirve HTTP. Solo mueve eventos de Kafka a ClickHouse.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"github.com/patricioarmesto/0007-stripe/internal/analytics"
	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/consumer"
)

func main() {
	cfg := config.Load()
	store, err := analytics.Open(cfg.ClickHouseDSN)
	if err != nil {
		log.Fatal(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers, Topic: cfg.KafkaTopic,
		GroupID: "payments-report", MinBytes: 1, MaxBytes: 10e6,
	})
	defer reader.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("consumer leyendo %s", cfg.KafkaTopic)
	if err := consumer.New(reader, store).Run(ctx); err != nil {
		log.Fatal(err)
	}
}
