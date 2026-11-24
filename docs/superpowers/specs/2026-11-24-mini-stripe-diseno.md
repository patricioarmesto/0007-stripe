# Mini Stripe didáctico — diseño

Fecha: 2026-11-24
Curso: ¿Cómo funciona Stripe? — Nivel avanzado — 16 feb 2027
Repo: `patricioarmesto/0007-stripe`

## Intento y éxito

Construir un mini Stripe didáctico que arranque con `docker compose up` y abra en
`localhost:3000`. El objetivo no es procesar pagos reales sino hacer visibles, con
código comentado en español rioplatense, tres ideas: la idempotencia se apoya en un
`UNIQUE` de Postgres, el dinero lo confirma un webhook firmado (nunca el cliente) y
los reintentos solo son seguros si el `event_id` es estable y único.

Éxito: el doble click no genera dos pagos, el webhook con firma inválida devuelve
401, el tercer intento con el mismo `event_id` no vuelve a cambiar el estado ni a
publicar en Kafka, y el dashboard muestra los pagos cobrados leyendo ClickHouse.

### Para entender el webhook (va también en el README)

El click en **Pagar** es una intención, no un cobro. El cobro lo confirma el
procesador mediante un webhook: es el recibo del banco, no el pedido del mozo.
`event_id` es la clave de idempotencia que genera el banco para ese aviso (tipo
`evt_...`); el `Idempotency-Key` de `/payments` la genera el cliente. Las dos terminan
en columnas `UNIQUE` distintas: una evita dos pagos por doble click, la otra evita
aplicar dos veces el mismo aviso. El `event_id` prueba identidad del aviso; la firma
HMAC prueba que viene del banco. Van juntos: primero firma, después idempotencia.

## Stack y límites

Go, TypeScript + React + Vite + Tailwind + Recharts, Postgres, Redis, Kafka (KRaft,
sin Zookeeper), ClickHouse, Docker Compose. Sin Python, Ollama ni Qdrant.

Fuera de alcance a propósito: PCI real, tarjetas reales, fraude con ML,
multi-moneda, payouts, Transactional Outbox, DLT, Exactly-Once, Schema Registry,
manejo fino de rebalancing y monitoreo de producción.

## Arquitectura

Un módulo Go en la raíz del repo:

- `cmd/payments` — HTTP de pagos.
- `cmd/webhook` — HTTP de webhooks, simulación y reintento.
- `cmd/consumer` — consumidor de Kafka → ClickHouse.
- `cmd/dashboard` — HTTP de estadísticas.
- `internal/` — código compartido: `db`, `cache`, `events`, `analytics`, `state`,
  `webhook`, `config`.

Cada binario tiene su propio `Dockerfile` (o uno parametrizado por `SERVICE`) y su
servicio en Compose. Postgres, Redis, Kafka KRaft y ClickHouse son servicios con
healthchecks. El frontend compila con Vite a `dist/` y nginx lo sirve en `:3000`,
haciendo reverse proxy con rewrite a `payments:8081`, `webhook:8082` y
`dashboard:8083`. Nginx no es una pieza de negocio: es solo el servidor estático y
el punto de entrada único.

### Límite Postgres → Kafka

Se hace `commit` en Postgres primero y se publica el evento después, con log si la
publicación falla. Así el dinero nunca depende de Kafka. Un evento duplicado en
ClickHouse se evita con una verificación previa por `payment_id` + `status` y una
memoria de eventos ya insertados; no se promete Exactly-Once. Publicar antes del
commit y usar Outbox están descartados: el primero puede anunciar pagos inexistentes
y el segundo está fuera de alcance a propósito.

## Modelo de datos

### Postgres (`db/init.sql`)

```sql
CREATE TABLE payments (
  id uuid PRIMARY KEY,
  idempotency_key text UNIQUE NOT NULL,
  amount int NOT NULL,
  currency text NOT NULL,
  email text NOT NULL,
  status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE webhook_events (
  event_id text UNIQUE NOT NULL,
  type text NOT NULL,
  payment_id uuid NOT NULL REFERENCES payments(id),
  attempts int NOT NULL DEFAULT 1,
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE payment_transitions (
  id bigserial PRIMARY KEY,
  payment_id uuid NOT NULL REFERENCES payments(id),
  from_status text,
  to_status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

El `UNIQUE` de `idempotency_key` es la defensa definitiva: Redis solo evita la
carrera, Postgres decide la verdad aunque se caiga un proceso. El `UNIQUE` de
`event_id` hace que reenviar el mismo webhook sea inofensivo. La tabla de transiciones
existe para el historial de `GET /payments/:id`.

### Redis

Lock `idem:<idempotency-key>` con `SET NX PX 10000`. Si no se adquiere, se espera
brevemente y se relee Postgres. La liberación usa Lua con compare-and-delete para no
borrar el lock de otra request.

### ClickHouse (`db/clickhouse/init.sql`)

```sql
CREATE TABLE payments_report (
  date Date,
  timestamp DateTime,
  payment_id String,
  amount Int64,
  status String,
  email String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, status);
```

`timestamp` está porque la query pedida usa `toDate(timestamp)`; `date` se mantiene
para el particionado y el orden. ClickHouse es solo reporte: nunca se consulta para
decidir si un pago está cobrado.

## Máquina de estados

Transiciones válidas: `created → processing → succeeded | failed`. Un pago en
`succeeded` no vuelve a `processing` ni a `failed`. `POST /payments` inserta
`created`, publica `payment.created` y pasa a `processing` antes de responder,
registrando cada transición. Toda transición inválida se rechaza con error y se
registra el intento.

## Contratos HTTP

Nginx en `:3000` enruta `/api/payments/*` → `payments:8081`, `/api/webhooks/*` →
`webhook:8082` y `/api/stats` → `dashboard:8083`.

| Servicio | Método y ruta | Detalle |
|---|---|---|
| payments | `POST /payments` | Header obligatorio `Idempotency-Key` (uuid del cliente). Body `{ amount, currency: "ARS", email, payment_method: "card_test" }`. Si ya existe la clave, devuelve el mismo pago. Si no, inserta `created`, publica evento y pasa a `processing`. |
| payments | `GET /payments/:id` | Estado actual e historial de `payment_transitions`. |
| payments | `GET /payments` | Listado para la tabla del dashboard, con `attempts` de `webhook_events` (0 si no hay webhook). |
| payments | `GET /health` | 200. |
| webhook | `POST /webhooks` | Header `X-Signature: sha256=<hex>` con HMAC-SHA256 del body crudo y `STRIPE_WEBHOOK_SECRET=whsec_test` de `.env`. Firma inválida → 401. Body `{ event_id, type, payment_id }`. |
| webhook | `POST /webhooks/simulate` | Arma un evento para un `payment_id`, lo firma con el secret y lo entrega a `/webhooks`. Lo usa el botón del checkout; el frontend nunca ve el secret. |
| webhook | `POST /webhooks/retry` | Simula el reintento del emisor. Body `{ payment_id, event_id, type }`. Mismo `event_id` y misma firma en todos los intentos. |
| dashboard | `GET /api/stats?total-by-day` | `SELECT toDate(timestamp) AS day, sum(amount) FROM payments_report WHERE status = 'succeeded' GROUP BY day`. |
| dashboard | `GET /api/stats?by-status` | `SELECT status, count() FROM payments_report GROUP BY status`. |
| consumer | — | Sin HTTP. Consume `payments.events` (incluye `payment.created` y los eventos del webhook), inserta por batches en ClickHouse. Ante fallo de inserción reintenta con backoff 200/400/800ms (equivalente a 1/2/4s), máximo 3 intentos, con log de cada uno. |

### Flujo del webhook

1. Verificar HMAC en el body crudo con comparación en tiempo constante.
2. Insertar `webhook_events` con `event_id UNIQUE`; si ya existe, responder 200 sin
   tocar el pago ni publicar.
3. En una transacción, validar la transición de estado y actualizar `payments`,
   registrando el cambio en `payment_transitions`.
4. Publicar el evento en `payments.events` para reportes.

### `/webhooks/retry` — simulación exacta

1. **Intento 1:** log `intento 1 para <event_id> falló`, respuesta 500. No inserta en
   `webhook_events`, no cambia el pago: sigue `processing`.
2. **Backoff:** 200ms, 400ms y 800ms en demo (equivalen a 1s, 2s y 4s). Solo se usa
   el primero si el intento 2 ya sale bien.
3. **Intento 2:** verifica HMAC igual que `/webhooks`, inserta el `event_id`, en
   transacción actualiza el pago a `succeeded`, publica en Kafka y devuelve
   `{ "processed": true, "attempt": 2 }`.
4. **Intento 3 (llamada posterior con el mismo `event_id`):** detecta el `UNIQUE`,
   devuelve `{ "processed": false, "reason": "already_processed" }`, sin cambio de
   estado ni publicación.

Reglas: no crea pago nuevo, no genera `event_id` nuevo por intento, no saltea la
firma, no cambia un `succeeded` por un evento viejo. Demuestra que reintentar sin
idempotencia duplicaría el cambio de estado.

## Frontend

Checkout con monto, email, `Pagar` y `Doble click a propósito`. El doble click envía
dos requests en paralelo con el mismo `Idempotency-Key` generado una vez y muestra
que vuelve el mismo `id`. `Simular webhook del banco` dispara `/webhooks/simulate`.
Dashboard con `BarChart` de total cobrado por día, `PieChart` de pagos por estado y
tabla de pagos (`id`, `email`, `monto`, `estado`, reintentos). Sección «Cómo romperlo»
con los tres experimentos. Tailwind para el diseño limpio, sin librerías extra.

## Documentación y tono

`README.md` con: Qué se construye, Cómo correrlo, Arquitectura en texto, **Alcance y
decisiones de diseño**, Qué queda fuera a propósito, Cómo probar. Cuatro ADRs en
`docs/adr/`: idempotencia con `UNIQUE` en Postgres, máquina de estados, webhook con
firma e idempotencia, y qué queda fuera. Comentarios didácticos en español
rioplatense en todo el código, explicando el por qué. Tono profesional, no defensivo.

## Verificación

- `go test ./...`: máquina de estados, HMAC, manejo de eventos.
- `npm run build` en `frontend/`.
- Smoke test con `curl` sobre `docker compose up`: health, doble click, firma
  inválida (401), simulate, retry (500 → 200 → `already_processed`) y stats.
- Si Docker está disponible, correr compose completo antes de entregar.
