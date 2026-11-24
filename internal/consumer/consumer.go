package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/patricioarmesto/0007-stripe/internal/analytics"
)

type rawEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	PaymentID string    `json:"payment_id"`
	Amount    int64     `json:"amount"`
	Email     string    `json:"email"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func Parse(payload []byte) (analytics.Row, error) {
	var raw rawEvent
	if err := json.Unmarshal(payload, &raw); err != nil {
		return analytics.Row{}, err
	}
	if raw.Timestamp.IsZero() {
		raw.Timestamp = time.Now().UTC()
	}
	return analytics.Row{
		Date: raw.Timestamp, Timestamp: raw.Timestamp, PaymentID: raw.PaymentID,
		Amount: raw.Amount, Status: raw.Status, Email: raw.Email,
	}, nil
}

// WithBackoff es el mismo patrón del retry del webhook: 200/400/800ms en la
// demo y máximo 3 intentos. Cada fallo se loguea para que se vea en docker logs.
func WithBackoff(ctx context.Context, fn func() error, logger *log.Logger) error {
	delays := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	for attempt := 1; attempt <= 3; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		logger.Printf("intento %d falló: %v", attempt, err)
		if attempt == 3 {
			return err
		}
		select {
		case <-time.After(delays[attempt-1]):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type Consumer struct {
	reader *kafka.Reader
	store  analytics.Store
	log    *log.Logger
	seen   map[string]bool
}

func New(reader *kafka.Reader, store analytics.Store) *Consumer {
	return &Consumer{reader: reader, store: store, log: log.Default(), seen: map[string]bool{}}
}

func (c *Consumer) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var rows []analytics.Row
	var msgs []kafka.Message

	// FetchMessage devuelve (mensaje, error): lo pasamos a un canal para que
	// el select pueda escucharlo junto al ticker y al ctx.
	messages := make(chan kafka.Message)
	go func() {
		defer close(messages)
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				return
			}
			select {
			case messages <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	flush := func() {
		if len(rows) == 0 {
			return
		}
		err := WithBackoff(ctx, func() error { return c.store.InsertBatch(ctx, rows) }, c.log)
		if err != nil {
			// No hacemos commit del offset: el evento se va a re-entregar.
			c.log.Printf("no se pudo insertar el batch: %v", err)
			return
		}
		if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
			c.log.Printf("no se pudo hacer commit de offsets: %v", err)
		}
		rows, msgs = nil, nil
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
		case <-ticker.C:
			flush()
		case msg, ok := <-messages:
			if !ok {
				flush()
				return nil
			}
			row, err := Parse(msg.Value)
			if err != nil {
				c.log.Printf("evento inválido: %v", err)
				continue
			}
			key := row.PaymentID + ":" + row.Status
			if c.seen[key] {
				continue
			}
			existe, err := c.store.Exists(ctx, row)
			if err != nil || existe {
				c.seen[key] = true
				continue
			}
			rows = append(rows, row)
			msgs = append(msgs, msg)
			c.seen[key] = true
			if len(rows) >= 50 {
				flush()
			}
		}
	}
}
