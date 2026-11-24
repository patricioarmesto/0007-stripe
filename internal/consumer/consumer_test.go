package consumer

import (
	"context"
	"errors"
	"log"
	"testing"

	"github.com/patricioarmesto/0007-stripe/internal/analytics"
)

func TestWithBackoffExitoEnSegundoIntento(t *testing.T) {
	llamadas := 0
	err := WithBackoff(context.Background(), func() error {
		llamadas++
		if llamadas == 1 {
			return errors.New("clickhouse caido")
		}
		return nil
	}, log.Default())
	if err != nil || llamadas != 2 {
		t.Fatalf("quería éxito en el intento 2, got err=%v llamadas=%d", err, llamadas)
	}
}

func TestWithBackoffReintentaTresVeces(t *testing.T) {
	llamadas := 0
	err := WithBackoff(context.Background(), func() error {
		llamadas++
		return errors.New("sigue caido")
	}, log.Default())
	if err == nil || llamadas != 3 {
		t.Fatalf("máximo 3 intentos: err=%v llamadas=%d", err, llamadas)
	}
}

func TestParseEvento(t *testing.T) {
	row, err := Parse([]byte(`{"event_id":"evt_1","type":"payment.succeeded","payment_id":"pay_1","amount":12500,"email":"ana@example.com","status":"succeeded","timestamp":"2027-02-16T10:00:00Z"}`))
	if err != nil || row.PaymentID != "pay_1" || row.Amount != 12500 || row.Status != "succeeded" {
		t.Fatalf("parse incorrecto: %+v %v", row, err)
	}
	var _ analytics.Row = row
}
