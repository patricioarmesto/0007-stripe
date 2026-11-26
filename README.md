# 0007-stripe — ¿Cómo funciona Stripe?

Curso de nivel avanzado · 16 feb 2027 · repo `patricioarmesto/0007-stripe`.

## Qué se construye

Un mini Stripe didáctico en localhost. No procesa dinero real: sirve para ver,
con código comentado en español rioplatense, cómo se encadenan idempotencia,
webhooks firmados, reintentos y reportes. La idempotencia de pago vive en Postgres,
el lock para requests paralelos vive en Redis, los avisos viajan por Kafka y
ClickHouse alimenta el dashboard.

### Para entender el webhook

El click en **Pagar** es una intención, no un cobro. El cobro lo confirma el
procesador con un webhook: es el recibo del banco, no el pedido del mozo.

`event_id` es la clave de idempotencia que genera el banco para ese aviso
(tipo `evt_...`). El `Idempotency-Key` de `/payments` lo genera el cliente. Los dos
terminan en columnas `UNIQUE` distintas: uno evita dos pagos por doble click, el
otro evita aplicar dos veces el mismo aviso. El `event_id` prueba identidad;
la firma HMAC prueba que viene del banco. Van juntos: primero firma, después
idempotencia.

## Cómo correrlo

```bash
docker compose up --build
```

Abrir http://localhost:3000. No hace falta configurar nada: `.env` trae el
secret de prueba `STRIPE_WEBHOOK_SECRET=whsec_test`. Para limpiar:
`docker compose down -v`.

Los puertos de infra en el host están corridos (`15432` para Postgres, `16379`
para Redis, `19092` para Kafka, `18123`/`19000` para ClickHouse) porque otros
proyectos del curso usan los puertos estándar. Adentro de la red de Compose
los servicios siguen escuchando en los puertos de siempre.

## Arquitectura en texto

- `payments` (Go, :8081 interno): crea pagos, aplica el lock de Redis y guarda
  la verdad en Postgres. Expone `/payments`, `/payments/:id`, `/health`.
- `webhook` (Go, :8082 interno): verifica HMAC, graba `webhook_events` y cambia
  el estado en una transacción. Expone `/webhooks`, `/webhooks/simulate`,
  `/webhooks/retry`, `/health`.
- `kafka-init` (job): crea `payments.events` al arrancar, para que el primer
  publish no llegue antes de que el broker tenga el topic.
- `consumer` (Go, sin HTTP): lee `payments.events` e inserta batches en
  `payments_report` con backoff 200/400/800ms y log por intento.
- `dashboard` (Go, :8083 interno): consulta ClickHouse para `/api/stats`.
- `frontend` (Vite + nginx, :3000): checkout y dashboard con Recharts.

Postgres es la fuente de verdad del dinero. Redis solo gana la carrera de
milisegundos entre dos requests iguales. Kafka desacopla el cobro del reporte.
ClickHouse nunca se consulta para decidir si un pago está cobrado.

## Alcance y decisiones de diseño

- `idempotency_key UNIQUE` en `payments`: aunque el proceso se caiga, no puede
  existir dos pagos para la misma clave. El lock de Redis evita la carrera,
  pero no es la verdad.
- La firma HMAC se verifica sobre el body crudo antes de tocar la DB.
- `event_id UNIQUE` en `webhook_events`: reenviar el mismo aviso es un no-op.
- Commit en Postgres primero, publicación en Kafka después con log de fallo.
  Si Kafka está caído, el pago queda correcto y el reporte se atrasa.
- Backoff corto en la demo (200/400/800ms) con la misma lógica de 1/2/4s.

## Qué queda fuera a propósito

PCI real, tarjetas reales, fraude con ML, multi-moneda, payouts, Transactional
Outbox, DLT, Exactly-Once, Schema Registry, manejo fino de rebalancing y
monitoreo de producción. Están **fuera de alcance a propósito**: el objetivo es
entender el flujo, no replicar un procesador de pagos real.

## Cómo probar

1. Abrí http://localhost:3000 y hacé click en **Doble click a propósito**.
   Los dos requests van con el mismo `Idempotency-Key` y vuelven con el mismo id.
2. Hacé click en **Simular webhook del banco**. El pago pasa de `processing` a
   `succeeded`. Antes de eso, el dashboard no lo muestra como cobrado.
3. En **Cómo romperlo**, probá firma inválida (401) y reintento con un
   `event_id` nuevo: el primer intento falla, el segundo procesa, el tercero
   devuelve `already_processed`. Si reutilizás el `event_id` del simulate, ese
   aviso ya está grabado y solo vas a ver `already_processed`.
4. Smoke test desde la raíz: `bash scripts/smoke.sh`.
5. Tests: `go test ./...` y `cd frontend && npm run build`.
