package webhook

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// WebhookService existe para testear el handler sin Postgres.
type WebhookService interface {
	Handle(ctx context.Context, ev Event) (bool, error)
	Exists(ctx context.Context, eventID string) (bool, error)
	MarkAttempts(ctx context.Context, eventID string, attempts int) error
}

type RetryRequest struct {
	PaymentID string `json:"payment_id"`
	EventID   string `json:"event_id"`
	Type      string `json:"type"`
}

type Handler struct {
	service WebhookService
	secret  string
	log     *log.Logger
}

func NewHandler(s WebhookService, secret string) *Handler {
	return &Handler{service: s, secret: secret, log: log.Default()}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodPost && r.URL.Path == "/webhooks":
		h.receive(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/webhooks/simulate":
		h.simulate(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/webhooks/retry":
		h.retry(w, r)
	default:
		http.NotFound(w, r)
	}
}

// process es el único lugar que verifica la firma. Tanto el webhook real
// como el simulado y el reintento pasan por acá: nadie la saltea.
func (h *Handler) process(ctx context.Context, body []byte, signature string) (int, any) {
	if !Verify(body, signature, h.secret) {
		return http.StatusUnauthorized, map[string]string{"error": "firma inválida"}
	}
	var ev Event
	if err := json.Unmarshal(body, &ev); err != nil {
		return http.StatusBadRequest, map[string]string{"error": "body inválido"}
	}
	if ev.EventID == "" || ev.PaymentID == "" ||
		(ev.Type != "payment.succeeded" && ev.Type != "payment.failed") {
		return http.StatusBadRequest, map[string]string{"error": "evento inválido"}
	}
	processed, err := h.service.Handle(ctx, ev)
	if err != nil {
		return http.StatusInternalServerError, map[string]string{"error": err.Error()}
	}
	return http.StatusOK, map[string]any{"processed": processed}
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request) {
	// Se lee el body crudo: si lo re-serializáramos, la firma no coincidiría.
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no se pudo leer el body"})
		return
	}
	code, resp := h.process(r.Context(), body, r.Header.Get("X-Signature"))
	writeJSON(w, code, resp)
}

func (h *Handler) simulate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PaymentID string `json:"payment_id"`
		Type      string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.PaymentID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payment_id obligatorio"})
		return
	}
	if req.Type == "" {
		req.Type = "payment.succeeded"
	}
	ev := Event{EventID: "evt_" + uuid.NewString(), Type: req.Type, PaymentID: req.PaymentID}
	body, _ := json.Marshal(ev)
	// Se firma acá adentro: el frontend nunca ve el secret.
	code, resp := h.process(r.Context(), body, Sign(body, h.secret))
	// Devolvemos el event_id para que la demo pueda probar el reintento con
	// EXACTAMENTE el mismo aviso. El secret nunca sale del servidor.
	m, _ := resp.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	m["event_id"] = ev.EventID
	writeJSON(w, code, m)
}

func (h *Handler) retry(w http.ResponseWriter, r *http.Request) {
	var req RetryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EventID == "" || req.PaymentID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "event_id y payment_id obligatorios"})
		return
	}
	if req.Type == "" {
		req.Type = "payment.succeeded"
	}
	exists, err := h.service.Exists(r.Context(), req.EventID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if exists {
		writeJSON(w, http.StatusOK, map[string]any{"processed": false, "reason": "already_processed"})
		return
	}

	ev := Event{EventID: req.EventID, Type: req.Type, PaymentID: req.PaymentID}
	body, _ := json.Marshal(ev)
	signature := Sign(body, h.secret) // misma firma en los 3 intentos
	delays := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}

	for attempt := 1; attempt <= 3; attempt++ {
		if attempt == 1 {
			h.log.Printf("intento 1 para %s falló", req.EventID)
			time.Sleep(delays[0])
			continue
		}
		code, resp := h.process(r.Context(), body, signature)
		if code != http.StatusOK {
			h.log.Printf("intento %d para %s falló: %v", attempt, req.EventID, resp)
			if attempt < 3 {
				time.Sleep(delays[attempt-1])
				continue
			}
			writeJSON(w, code, resp)
			return
		}
		if processed, ok := resp.(map[string]any)["processed"].(bool); ok && processed {
			_ = h.service.MarkAttempts(r.Context(), req.EventID, attempt)
			writeJSON(w, http.StatusOK, map[string]any{"processed": true, "attempt": attempt})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"processed": false, "reason": "already_processed"})
		return
	}
}
