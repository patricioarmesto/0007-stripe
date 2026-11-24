package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// HMAC con un secret compartido es la prueba de que el aviso viene del banco.
// No alcanza con confiar en el body: cualquiera podría postear
// payment.succeeded y marcarse un pago como cobrado.
func Sign(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func Verify(body []byte, header, secret string) bool {
	if !strings.HasPrefix(header, "sha256=") {
		return false
	}
	got, err := hex.DecodeString(strings.TrimPrefix(header, "sha256="))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	// hmac.Equal compara en tiempo constante: no filtra cuántos bytes
	// acertamos del hash.
	return hmac.Equal(got, mac.Sum(nil))
}
