package payments

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

func TestHandlerRequiereIdempotencyKey(t *testing.T) {
	h := NewHandler(NewService(newFakeRepo(), &fakeLocker{}, &fakePublisher{}))
	r := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount":100,"currency":"ARS","email":"a@b.com","payment_method":"card_test"}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("esperaba 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandlerCreaYDevuelveMismoID(t *testing.T) {
	s := NewService(newFakeRepo(), &fakeLocker{}, &fakePublisher{})
	h := NewHandler(s)
	key := uuid.NewString()
	body := `{"amount":100,"currency":"ARS","email":"a@b.com","payment_method":"card_test"}`
	for i, want := range []int{http.StatusCreated, http.StatusOK} {
		r := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("intento %d: esperaba %d, got %d: %s", i, want, w.Code, w.Body.String())
		}
		var got struct {
			Payment Payment `json:"payment"`
			Created bool    `json:"created"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Payment.Status != state.StatusProcessing {
			t.Fatalf("estado incorrecto: %s", got.Payment.Status)
		}
		if got.Created != (i == 0) {
			t.Fatalf("created incorrecto en el intento %d: %v", i, got.Created)
		}
	}
}

func TestHandlerHealth(t *testing.T) {
	h := NewHandler(NewService(newFakeRepo(), &fakeLocker{}, &fakePublisher{}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("health: %d", w.Code)
	}
}
