// Package payments guarda la verdad del dinero. El INSERT con idempotency_key
// UNIQUE es lo que salva el doble click: si dos requests llegan juntas, una
// falla la constraint y el servicio devuelve el pago que ya existe.
package payments

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type Repository interface {
	GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error)
	GetByID(ctx context.Context, id string) (*Payment, error)
	List(ctx context.Context) ([]Payment, error)
	Insert(ctx context.Context, p *Payment) error
	SetStatus(ctx context.Context, id string, from, to state.Status) error
	Transitions(ctx context.Context, id string) ([]Transition, error)
}

type SQLRepository struct{ db *sql.DB }

func NewSQLRepository(db *sql.DB) *SQLRepository { return &SQLRepository{db: db} }

const selectPayment = `
SELECT p.id, p.amount, p.currency, p.email, p.status, p.created_at,
       COALESCE(w.attempts, 0)
FROM payments p
LEFT JOIN webhook_events w ON w.payment_id = p.id`

func (r *SQLRepository) scan(rows *sql.Rows) (*Payment, error) {
	var p Payment
	if err := rows.Scan(&p.ID, &p.Amount, &p.Currency, &p.Email, &p.Status, &p.CreatedAt, &p.Attempts); err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *SQLRepository) GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error) {
	rows, err := r.db.QueryContext(ctx, selectPayment+` WHERE p.idempotency_key = $1`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	return r.scan(rows)
}

func (r *SQLRepository) GetByID(ctx context.Context, id string) (*Payment, error) {
	rows, err := r.db.QueryContext(ctx, selectPayment+` WHERE p.id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	return r.scan(rows)
}

func (r *SQLRepository) List(ctx context.Context) ([]Payment, error) {
	rows, err := r.db.QueryContext(ctx, selectPayment+` ORDER BY p.created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Payment{}
	for rows.Next() {
		p, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (r *SQLRepository) Insert(ctx context.Context, p *Payment) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO payments (id, idempotency_key, amount, currency, email, status)
VALUES ($1, $2, $3, $4, $5, $6)`, p.ID, p.IdempotencyKey, p.Amount, p.Currency, p.Email, string(p.Status))
	return err
}

func (r *SQLRepository) SetStatus(ctx context.Context, id string, from, to state.Status) error {
	if err := state.Transition(from, to); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// El WHERE status = from es el cerrojo optimista: si otro proceso ya
	// cambió el estado, RowsAffected es 0 y no pisamos su decisión.
	res, err := tx.ExecContext(ctx, `UPDATE payments SET status = $1 WHERE id = $2 AND status = $3`, string(to), id, string(from))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("el pago %s ya no está en %s", id, from)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO payment_transitions (payment_id, from_status, to_status)
VALUES ($1, $2, $3)`, id, string(from), string(to)); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *SQLRepository) Transitions(ctx context.Context, id string) ([]Transition, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT from_status, to_status, created_at FROM payment_transitions
WHERE payment_id = $1 ORDER BY created_at, id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Transition{}
	for rows.Next() {
		var t Transition
		var from sql.NullString
		if err := rows.Scan(&from, &t.ToStatus, &t.CreatedAt); err != nil {
			return nil, err
		}
		t.FromStatus = from.String
		out = append(out, t)
	}
	return out, rows.Err()
}
