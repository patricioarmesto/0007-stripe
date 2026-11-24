package report

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const TotalByDaySQL = `SELECT toDate(timestamp) AS day, sum(amount) AS total FROM stripe.payments_report WHERE status = 'succeeded' GROUP BY day ORDER BY day`
const ByStatusSQL = `SELECT status, count() AS count FROM stripe.payments_report GROUP BY status ORDER BY status`

type DayTotal struct {
	Day   string `json:"day"`
	Total int64  `json:"total"`
}

type StatusCount struct {
	Status string `json:"status"`
	Count  uint64 `json:"count"`
}

type Store struct{ db *sql.DB }

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) TotalByDay(ctx context.Context) ([]DayTotal, error) {
	rows, err := s.db.QueryContext(ctx, TotalByDaySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DayTotal{}
	for rows.Next() {
		var day time.Time
		var total int64
		if err := rows.Scan(&day, &total); err != nil {
			return nil, err
		}
		out = append(out, DayTotal{Day: day.Format("2006-01-02"), Total: total})
	}
	return out, rows.Err()
}

func (s *Store) ByStatus(ctx context.Context) ([]StatusCount, error) {
	rows, err := s.db.QueryContext(ctx, ByStatusSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatusCount{}
	for rows.Next() {
		var sc StatusCount
		if err := rows.Scan(&sc.Status, &sc.Count); err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}
