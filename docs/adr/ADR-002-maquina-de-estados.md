# ADR-002: Máquina de estados explícita

Estado: aceptado.

## Contexto

`created`, `processing`, `succeeded` y `failed` pueden mezclarse si cualquier
handler escribe `status` directamente. Un webhook viejo podría deshacer un cobro.

## Decisión

Un paquete `internal/state` puro define las únicas transiciones válidas:
`created → processing → succeeded | failed`. `succeeded` y `failed` son terminales.
El repositorio usa `UPDATE ... WHERE status = from` en una transacción y escribe
la fila de `payment_transitions`.

## Consecuencias

Los intentos inválidos se rechazan y quedan registrados en el historial.
`GET /payments/:id` puede mostrar por qué el pago terminó donde terminó.
