// Package webhook aplica los avisos del banco. El event_id UNIQUE es la razón
// por la que un reintento no duplica el cambio de estado: insertar el evento
// y actualizar el pago ocurren en la MISMA transacción.
package webhook

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type Event struct {
	EventID   string `json:"event_id"`
	Type      string `json:"type"`
	PaymentID string `json:"payment_id"`
}

type PaymentSnapshot struct {
	ID     string
	Amount int
	Email  string
	Status state.Status
}

type Repository interface {
	Exists(ctx context.Context, eventID string) (bool, error)
	Process(ctx context.Context, ev Event) (bool, error)
	GetPayment(ctx context.Context, id string) (*PaymentSnapshot, error)
	MarkAttempts(ctx context.Context, eventID string, attempts int) error
}

type SQLRepository struct{ db *sql.DB }

func NewSQLRepository(db *sql.DB) *SQLRepository { return &SQLRepository{db: db} }

func (r *SQLRepository) Exists(ctx context.Context, eventID string) (bool, error) {
	var ok bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM webhook_events WHERE event_id=$1)`, eventID).Scan(&ok)
	return ok, err
}

func targetStatus(t string) (state.Status, error) {
	switch t {
	case "payment.succeeded":
		return state.StatusSucceeded, nil
	case "payment.failed":
		return state.StatusFailed, nil
	default:
		return "", fmt.Errorf("tipo de evento desconocido: %s", t)
	}
}

func (r *SQLRepository) Process(ctx context.Context, ev Event) (bool, error) {
	to, err := targetStatus(ev.Type)
	if err != nil {
		return false, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
INSERT INTO webhook_events (event_id, type, payment_id) VALUES ($1,$2,$3)
ON CONFLICT (event_id) DO NOTHING`, ev.EventID, ev.Type, ev.PaymentID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		// Ya se procesó. Confirmamos la transacción vacía y salimos sin tocar
		// el pago ni publicar otro evento.
		return false, tx.Commit()
	}

	var from state.Status
	if err := tx.QueryRowContext(ctx, `SELECT status FROM payments WHERE id=$1 FOR UPDATE`, ev.PaymentID).Scan(&from); err != nil {
		return false, err
	}
	if !state.CanTransition(from, to) {
		// Evento viejo: se registra para no volver a recibirlo, pero no se
		// aplica. Así un succeeded nunca se convierte en failed.
		return false, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET status=$1 WHERE id=$2`, string(to), ev.PaymentID); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO payment_transitions (payment_id, from_status, to_status)
VALUES ($1,$2,$3)`, ev.PaymentID, string(from), string(to)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *SQLRepository) GetPayment(ctx context.Context, id string) (*PaymentSnapshot, error) {
	var p PaymentSnapshot
	err := r.db.QueryRowContext(ctx, `SELECT id, amount, email, status FROM payments WHERE id=$1`, id).
		Scan(&p.ID, &p.Amount, &p.Email, &p.Status)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *SQLRepository) MarkAttempts(ctx context.Context, eventID string, attempts int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE webhook_events SET attempts=$1 WHERE event_id=$2`, attempts, eventID)
	return err
}
