// Package events separa "avísale a reportes" de "confirmá el pago".
// Si Kafka está caído, el pago ya está en Postgres: se loguea y se sigue.
package events

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Publisher interface {
	Publish(ctx context.Context, key string, payload []byte) error
}

// Writer existe para poder testear sin un broker levantado.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	writer Writer
	topic  string
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		// Hash por payment_id: los eventos de un mismo pago caen en la misma
		// partición y llegan en orden al consumidor.
		writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}},
		topic:  topic,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, key string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload})
}
