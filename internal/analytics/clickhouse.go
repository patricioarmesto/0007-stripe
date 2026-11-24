// Package analytics es reporte, nunca decisión. Si acá falta un dato, el
// dashboard queda feo; si falta en Postgres, el pago no existió.
package analytics

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Row struct {
	Date      time.Time
	Timestamp time.Time
	PaymentID string
	Amount    int64
	Status    string
	Email     string
}

type Store interface {
	Exists(ctx context.Context, row Row) (bool, error)
	InsertBatch(ctx context.Context, rows []Row) error
}

type ClickHouseStore struct{ db *sql.DB }

func Open(dsn string) (*ClickHouseStore, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &ClickHouseStore{db: db}, nil
}

func (s *ClickHouseStore) Exists(ctx context.Context, row Row) (bool, error) {
	var n uint64
	err := s.db.QueryRowContext(ctx, `
SELECT count() FROM stripe.payments_report WHERE payment_id=$1 AND status=$2`, row.PaymentID, row.Status).Scan(&n)
	return n > 0, err
}

func (s *ClickHouseStore) InsertBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO stripe.payments_report (date, timestamp, payment_id, amount, status, email)
VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, r.Timestamp, r.Timestamp, r.PaymentID, r.Amount, r.Status, r.Email); err != nil {
			return err
		}
	}
	return tx.Commit()
}
