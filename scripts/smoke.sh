#!/usr/bin/env bash
set -euo pipefail
BASE=http://localhost:3000

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

echo "health payments: $(code $BASE/api/payments/health)"
echo "health dashboard: $(code "$BASE/api/stats?by-status")"

KEY=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)
BODY='{"amount":12500,"currency":"ARS","email":"ana@example.com","payment_method":"card_test"}'
PAYLOAD=$(curl -s -X POST $BASE/api/payments -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" -d "$BODY")
echo "primer pago: $PAYLOAD"
ID=$(echo "$PAYLOAD" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

PAYLOAD2=$(curl -s -X POST $BASE/api/payments -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" -d "$BODY")
echo "mismo key: $PAYLOAD2"
echo "$PAYLOAD2" | grep -q "$ID" && echo "OK: mismo id"

BAD=$(curl -s -o /dev/null -w '%{http_code}' -X POST $BASE/api/webhooks \
  -H 'Content-Type: application/json' -H 'X-Signature: sha256=malo' \
  -d '{"event_id":"evt_smoke_firma","type":"payment.succeeded","payment_id":"'"$ID"'"}')
echo "firma invalida: $BAD (esperado 401)"
[ "$BAD" = 401 ]

# El reintento usa un event_id NUEVO. Si reutilizás el del simulate, ese aviso
# ya está grabado y solo vas a ver already_processed.
EVT="evt_smoke_$KEY"
RETRY='{"payment_id":"'"$ID"'","event_id":"'"$EVT"'","type":"payment.succeeded"}'
R1=$(curl -s -X POST $BASE/api/webhooks/retry -H 'Content-Type: application/json' -d "$RETRY")
echo "reintento (intento 1 falla, intento 2 procesa): $R1"
echo "$R1" | grep -q '"attempt":2'
R2=$(curl -s -X POST $BASE/api/webhooks/retry -H 'Content-Type: application/json' -d "$RETRY")
echo "reintento posterior: $R2"
echo "$R2" | grep -q 'already_processed'

KEY2=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)
PAYLOAD_B=$(curl -s -X POST $BASE/api/payments -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY2" -d "$BODY")
ID2=$(echo "$PAYLOAD_B" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
SIM=$(curl -s -X POST $BASE/api/webhooks/simulate -H 'Content-Type: application/json' \
  -d '{"payment_id":"'"$ID2"'","type":"payment.succeeded"}')
echo "simulate: $SIM"
echo "$SIM" | grep -q 'event_id'

sleep 2
echo "detalle: $(curl -s $BASE/api/payments/$ID)"
echo "stats dia: $(curl -s "$BASE/api/stats?total-by-day")"
echo "stats estado: $(curl -s "$BASE/api/stats?by-status")"
