package events

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fakeWriter struct {
	msgs []kafka.Message
	err  error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}
func (f *fakeWriter) Close() error { return nil }

func TestPublisherUsaPaymentIDComoKey(t *testing.T) {
	w := &fakeWriter{}
	p := &KafkaPublisher{writer: w, topic: "payments.events"}
	if err := p.Publish(context.Background(), "pay_1", []byte(`{"type":"payment.created"}`)); err != nil {
		t.Fatal(err)
	}
	if len(w.msgs) != 1 || string(w.msgs[0].Key) != "pay_1" {
		t.Fatalf("mensaje o key incorrecto: %+v", w.msgs)
	}
}
