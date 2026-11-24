package webhook

import "testing"

const secret = "whsec_test"

func TestVerifyFirma(t *testing.T) {
	body := []byte(`{"event_id":"evt_456","type":"payment.succeeded","payment_id":"pay_123"}`)
	firma := Sign(body, secret)
	if !Verify(body, firma, secret) {
		t.Fatal("la firma generada debe verificar")
	}
	tampered := []byte(`{"event_id":"evt_456","type":"payment.failed","payment_id":"pay_123"}`)
	if Verify(tampered, firma, secret) {
		t.Fatal("cualquier cambio en el body debe romper la firma")
	}
	if Verify(body, firma, "otro_secret") {
		t.Fatal("otro secret no debe verificar")
	}
	if Verify(body, "sha256=no-hex", secret) {
		t.Fatal("header mal formado debe ser rechazado")
	}
	if Verify(body, "md5=abc", secret) {
		t.Fatal("solo aceptamos sha256")
	}
}
