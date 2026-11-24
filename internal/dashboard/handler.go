package dashboard

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/patricioarmesto/0007-stripe/internal/report"
)

type Querier interface {
	TotalByDay(ctx context.Context) ([]report.DayTotal, error)
	ByStatus(ctx context.Context) ([]report.StatusCount, error)
}

type Handler struct{ store Querier }

func NewHandler(s Querier) *Handler { return &Handler{store: s} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path != "/api/stats" {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	switch {
	case q.Has("total-by-day"):
		data, err := h.store.TotalByDay(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(data)
	case q.Has("by-status"):
		data, err := h.store.ByStatus(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(data)
	default:
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "usá ?total-by-day o ?by-status"})
	}
}
