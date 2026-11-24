package webhook

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type countingPublisher struct{ calls int }

func (c *countingPublisher) Publish(context.Context, string, []byte) error {
	c.calls++
	return nil
}

func webhookDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL no configurado")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM payment_transitions; DELETE FROM webhook_events; DELETE FROM payments;`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedPago(t *testing.T, db *sql.DB, status state.Status) string {
	t.Helper()
	id := uuid.NewString()
	_, err := db.Exec(`INSERT INTO payments (id, idempotency_key, amount, currency, email, status)
VALUES ($1,$2,12500,'ARS','ana@example.com',$3)`, id, uuid.NewString(), string(status))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestServicioWebhookIdempotente(t *testing.T) {
	db := webhookDB(t)
	pub := &countingPublisher{}
	repo := NewSQLRepository(db)
	svc := NewService(repo, pub)
	ctx := context.Background()
	id := seedPago(t, db, state.StatusProcessing)
	ev := Event{EventID: "evt_456", Type: "payment.succeeded", PaymentID: id}

	first, err := svc.Handle(ctx, ev)
	if err != nil || !first {
		t.Fatalf("primer webhook debía procesar: %v %v", first, err)
	}
	second, err := svc.Handle(ctx, ev)
	if err != nil || second {
		t.Fatalf("segundo webhook debía ignorarse: %v %v", second, err)
	}
	if pub.calls != 1 {
		t.Fatalf("Kafka debe recibir un solo evento, recibió %d", pub.calls)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM payments WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(state.StatusSucceeded) {
		t.Fatalf("estado final: %s", status)
	}

	// Un evento viejo de failed no puede deshacer un cobro confirmado.
	viejo := Event{EventID: "evt_viejo", Type: "payment.failed", PaymentID: id}
	if changed, err := svc.Handle(ctx, viejo); err != nil || changed {
		t.Fatalf("evento viejo no debía cambiar nada: %v %v", changed, err)
	}
	if err := db.QueryRow(`SELECT status FROM payments WHERE id=$1`, id).Scan(&status); err != nil || status != string(state.StatusSucceeded) {
		t.Fatalf("succeeded regresó: %s %v", status, err)
	}
}
