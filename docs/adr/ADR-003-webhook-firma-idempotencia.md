# ADR-003: Webhook con firma e idempotencia

Estado: aceptado.

## Contexto

Cualquiera podría postear `payment.succeeded`. Además, el emisor reintenta el
mismo aviso cuando no recibe 2xx: sin control, el segundo intento duplicaría el
cambio de estado o el evento de reporte.

## Decisión

`X-Signature` contiene `sha256=<HMAC del body crudo>` con un secret fijo en
`.env`. Se verifica antes de tocar la DB con comparación en tiempo constante.
`webhook_events.event_id` es `UNIQUE`: insertar el evento y actualizar el pago
ocurren en la misma transacción. `/webhooks/retry` simula el reintento del emisor
con el mismo `event_id` y la misma firma.

## Consecuencias

Firma inválida da 401. El tercer intento devuelve `already_processed` sin
publicar de nuevo. El pago nunca se marca cobrado por la respuesta del cliente.
