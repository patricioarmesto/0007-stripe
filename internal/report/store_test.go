package report

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestStoreUsaQueriesEsperadas(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}

	mock.ExpectQuery(regexp.QuoteMeta(TotalByDaySQL)).
		WillReturnRows(sqlmock.NewRows([]string{"day", "total"}).
			AddRow(time.Date(2027, 2, 16, 0, 0, 0, 0, time.UTC), int64(25000)))
	dias, err := s.TotalByDay(context.Background())
	if err != nil || len(dias) != 1 || dias[0].Day != "2027-02-16" || dias[0].Total != 25000 {
		t.Fatalf("total por día: %+v %v", dias, err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(ByStatusSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).AddRow("succeeded", 3))
	estados, err := s.ByStatus(context.Background())
	if err != nil || len(estados) != 1 || estados[0].Count != 3 {
		t.Fatalf("por estado: %+v %v", estados, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
