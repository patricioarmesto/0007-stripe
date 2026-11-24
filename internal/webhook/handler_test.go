package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeService struct {
	exists    bool
	processed bool
	calls     int
	markCalls int
}

func (f *fakeService) Handle(context.Context, Event) (bool, error) {
	f.calls++
	return f.processed, nil
}
func (f *fakeService) Exists(context.Context, string) (bool, error) { return f.exists, nil }
func (f *fakeService) MarkAttempts(context.Context, string, int) error {
	f.markCalls++
	return nil
}

func TestHandlerFirmaInvalida(t *testing.T) {
	svc := &fakeService{}
	h := NewHandler(svc, secret)
	body := `{"event_id":"evt_1","type":"payment.succeeded","payment_id":"pay_1"}`
	r := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	r.Header.Set("X-Signature", "sha256=malo")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized || svc.calls != 0 {
		t.Fatalf("firma inválida: code=%d calls=%d", w.Code, svc.calls)
	}
}

func TestHandlerRetrySegundoIntento(t *testing.T) {
	svc := &fakeService{processed: true}
	h := NewHandler(svc, secret)
	body := `{"payment_id":"pay_123","event_id":"evt_456","type":"payment.succeeded"}`
	r := httptest.NewRequest(http.MethodPost, "/webhooks/retry", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || svc.calls != 1 {
		t.Fatalf("retry: code=%d calls=%d body=%s", w.Code, svc.calls, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["processed"] != true || resp["attempt"] != float64(2) {
		t.Fatalf("respuesta incorrecta: %v", resp)
	}
}

func TestHandlerRetryDuplicado(t *testing.T) {
	svc := &fakeService{exists: true}
	h := NewHandler(svc, secret)
	body := `{"payment_id":"pay_123","event_id":"evt_456","type":"payment.succeeded"}`
	r := httptest.NewRequest(http.MethodPost, "/webhooks/retry", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if w.Code != http.StatusOK || resp["reason"] != "already_processed" || svc.calls != 0 {
		t.Fatalf("duplicado: code=%d resp=%v calls=%d", w.Code, resp, svc.calls)
	}
}

func TestHandlerSimulateFirmaInternamente(t *testing.T) {
	svc := &fakeService{processed: true}
	h := NewHandler(svc, secret)
	body := `{"payment_id":"pay_123","type":"payment.succeeded"}`
	r := httptest.NewRequest(http.MethodPost, "/webhooks/simulate", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || svc.calls != 1 {
		t.Fatalf("simulate: code=%d calls=%d body=%s", w.Code, svc.calls, w.Body.String())
	}
}
