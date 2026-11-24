package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/patricioarmesto/0007-stripe/internal/report"
)

type fakeQuerier struct{}

func (fakeQuerier) TotalByDay(context.Context) ([]report.DayTotal, error) {
	return []report.DayTotal{{Day: "2027-02-16", Total: 25000}}, nil
}
func (fakeQuerier) ByStatus(context.Context) ([]report.StatusCount, error) {
	return []report.StatusCount{{Status: "succeeded", Count: 2}}, nil
}

func TestHandlerStats(t *testing.T) {
	h := NewHandler(fakeQuerier{})
	for _, path := range []string{"/api/stats?total-by-day", "/api/stats?by-status"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK || w.Body.Len() == 0 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
}
