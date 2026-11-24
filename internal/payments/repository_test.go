package payments

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL no configurado; correr con el postgres del compose")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM payment_transitions; DELETE FROM webhook_events; DELETE FROM payments;`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRepositorioClienteYTransiciones(t *testing.T) {
	db := testDB(t)
	repo := NewSQLRepository(db)
	ctx := context.Background()
	p := &Payment{
		ID: uuid.NewString(), IdempotencyKey: uuid.NewString(), Amount: 12500,
		Currency: "ARS", Email: "ana@example.com", Status: state.StatusCreated,
	}
	if err := repo.Insert(ctx, p); err != nil {
		t.Fatal(err)
	}
	found, err := repo.GetByIdempotencyKey(ctx, p.IdempotencyKey)
	if err != nil || found.ID != p.ID {
		t.Fatalf("no encontró por idempotency_key: %+v %v", found, err)
	}
	if err := repo.SetStatus(ctx, p.ID, state.StatusCreated, state.StatusProcessing); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetStatus(ctx, p.ID, state.StatusProcessing, state.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetByID(ctx, p.ID)
	if err != nil || got.Status != state.StatusSucceeded {
		t.Fatalf("estado final incorrecto: %+v %v", got, err)
	}
	ts, err := repo.Transitions(ctx, p.ID)
	if err != nil || len(ts) != 2 {
		t.Fatalf("historial incorrecto: %+v %v", ts, err)
	}
	if ts[0].FromStatus != string(state.StatusCreated) || ts[1].ToStatus != string(state.StatusSucceeded) {
		t.Fatalf("transiciones desordenadas: %+v", ts)
	}
	lista, err := repo.List(ctx)
	if err != nil || len(lista) != 1 || lista[0].Attempts != 0 {
		t.Fatalf("listado incorrecto: %+v %v", lista, err)
	}
}
