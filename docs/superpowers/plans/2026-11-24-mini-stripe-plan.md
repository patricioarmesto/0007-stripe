# Mini Stripe didáctico — Plan de implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar el repo completo del curso `0007-stripe`: un mini Stripe didáctico que arranca con `docker compose up` y abre en `localhost:3000`, con idempotencia en Postgres, lock en Redis, eventos en Kafka, reportes en ClickHouse y comentarios didácticos en español rioplatense.

**Architecture:** Un módulo Go con cuatro binarios independientes (`payments`, `webhook`, `consumer`, `dashboard`) que comparten `internal/`. Postgres es la fuente de verdad del dinero; Redis solo evita la carrera de requests paralelos; Kafka lleva eventos al consumidor; ClickHouse alimenta el dashboard. El frontend se compila con Vite y nginx lo sirve en `:3000` haciendo de único punto de entrada.

**Tech Stack:** Go 1.23, TypeScript, React 18, Vite 5, Tailwind 3, Recharts 2, Postgres 16, Redis 7, Kafka 3.7 KRaft, ClickHouse 24, Docker Compose, nginx (solo servidor estático y proxy).

**Spec:** `docs/superpowers/specs/2026-11-24-mini-stripe-diseno.md`

## Global Constraints

- Stack fijo: Go, TypeScript + React + Vite + Tailwind + Recharts, Postgres, Redis, Kafka (KRaft, sin Zookeeper), ClickHouse, Docker Compose.
- Sin Python, sin Ollama, sin Qdrant.
- Cuatro binarios Go separados: `cmd/payments`, `cmd/webhook`, `cmd/consumer`, `cmd/dashboard`.
- Frontend: build estático de Vite servido por nginx en `localhost:3000`.
- `STRIPE_WEBHOOK_SECRET=whsec_test` en `.env`.
- Lock Redis: `SET NX PX 10000`, liberación con Lua compare-and-delete.
- Backoff del reintento y del consumidor: 200ms, 400ms, 800ms en demo (equivalen a 1s, 2s, 4s), máximo 3 intentos, con log de cada intento.
- Moneda siempre `ARS`; `payment_method` siempre `card_test`.
- Transiciones válidas: `created → processing → succeeded | failed`. Un `succeeded` nunca vuelve a `processing` ni a `failed`.
- `/webhooks/retry` usa siempre el mismo `event_id` y la misma firma; el primer intento falla sin tocar Postgres.
- `X-Signature: sha256=<hex>` con HMAC-SHA256 del body crudo; firma inválida devuelve 401.
- Comentarios didácticos en español rioplatense en TODO el código, explicando el por qué, no solo el qué.
- Tono profesional, no defensivo. Frases obligatorias en la documentación: **Alcance y decisiones de diseño** y **fuera de alcance a propósito**.
- Quedan **fuera de alcance a propósito**: PCI real, tarjetas reales, fraude con ML, multi-moneda, payouts, Transactional Outbox, DLT, Exactly-Once, Schema Registry, manejo fino de rebalancing y monitoreo de producción.

## Review Focus

Estos son los casos que la spec implica pero que ningún test puntual cubre salvo que se agregue explícitamente. Cada línea tiene su test asignado en la tarea dueña del código.

1. **Dos `POST /payments` concurrentes con el mismo `Idempotency-Key`** → debe existir un solo pago y los dos responses traen el mismo `id`. Test: `TestServicioCreateEsIdempotente` (Task 6) + smoke con dos `curl` en paralelo (Task 15).
2. **Webhook con `X-Signature` inválido o body alterado** → 401, pago sigue `processing`, sin fila en `webhook_events`. Test: `TestVerifyFirma` (Task 8) + `TestHandlerFirmaInvalida` (Task 10) + smoke (Task 15).
3. **Reintento con el mismo `event_id` cuando el pago ya es `succeeded`** → `{ processed: false, reason: "already_processed" }`, sin regresión de estado ni segunda publicación. Test: `TestServicioWebhookIdempotente` (Task 9) + `TestHandlerRetryDuplicado` (Task 10) + smoke (Task 15).
4. **Fallo al publicar en Kafka durante `POST /payments`** → el pago queda en Postgres y se responde normal; el reporte puede atrasar, el dinero no. Test: `TestServicioCreatePublicacionFallidaNoRompePago` (Task 6).
5. **ClickHouse caído al insertar un batch** → backoff 200/400/800ms, máximo 3 intentos, log por intento, sin pérdida del evento. Test: `TestWithBackoffReintentaTresVeces` y `TestWithBackoffExitoEnSegundoIntento` (Task 11).

## Estructura de archivos

| Archivo | Responsabilidad |
|---|---|
| `go.mod`, `go.sum` | Módulo Go y dependencias. |
| `internal/config/config.go` | Configuración desde variables de entorno con defaults de localhost. |
| `internal/events/kafka.go` | `Publisher` y su implementación Kafka. |
| `internal/cache/lock.go` | Lock distribuido en Redis. |
| `internal/state/machine.go` | Máquina de estados pura, sin IO. |
| `internal/payments/{model,repository,service,handler}.go` | Dominio de pagos. |
| `internal/webhook/{signature,repository,service,handler}.go` | Firma, idempotencia y reintento de webhooks. |
| `internal/analytics/clickhouse.go` | Inserción en ClickHouse con dedupe. |
| `internal/consumer/consumer.go` | Consumo de Kafka, batches y backoff. |
| `internal/report/store.go`, `internal/dashboard/handler.go` | Queries y HTTP de estadísticas. |
| `cmd/{payments,webhook,consumer,dashboard}/main.go` | Cuatro binarios delgados. |
| `db/postgres/init.sql`, `db/clickhouse/init.sql` | Esquemas. |
| `Dockerfile`, `frontend/Dockerfile`, `deploy/nginx/*`, `docker-compose.yml`, `.env` | Infra. |
| `frontend/src/*` | Checkout, dashboard y sección "Cómo romperlo". |
| `README.md`, `docs/adr/ADR-00{1,2,3,4}.md`, `scripts/smoke.sh` | Documentación y verificación. |

Decisión de alcance del plan: la spec toca varios subsistemas, pero todos comparten un mismo módulo Go y un mismo `docker compose up`; separarlos en planes distintos dejaría cada pieza sin poder ejecutarse. Se mantiene un plan con tareas independientes y testeables.

---

### Task 1: Fundamentos Go — configuración y publicador de eventos

**Files:**
- Create: `go.mod`, `internal/config/config.go`, `internal/config/config_test.go`, `internal/events/kafka.go`, `internal/events/kafka_test.go`

**Interfaces:**
- Consumes: nada.
- Produces: `config.Load() Config` con campos `PostgresDSN`, `RedisAddr`, `KafkaBrokers []string`, `KafkaTopic`, `ClickHouseDSN`, `WebhookSecret`, `Port`; `events.Publisher` con `Publish(ctx context.Context, key string, payload []byte) error`; `events.NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher`.

- [ ] **Step 1: Write the failing test**

`internal/config/config_test.go`:

```go
package config

import "testing"

func TestLoadUsaDefaultsCuandoNoHayEnv(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")
	c := Load()
	if c.PostgresDSN != "postgres://stripe:stripe@postgres:5432/stripe?sslmode=disable" {
		t.Fatalf("DSN por default incorrecto: %s", c.PostgresDSN)
	}
	if c.WebhookSecret != "whsec_test" {
		t.Fatalf("secret por default incorrecto: %s", c.WebhookSecret)
	}
	if c.KafkaTopic != "payments.events" {
		t.Fatalf("topic por default incorrecto: %s", c.KafkaTopic)
	}
}

func TestLoadRespetaEnv(t *testing.T) {
	t.Setenv("STRIPE_WEBHOOK_SECRET", "otro_secret")
	if Load().WebhookSecret != "otro_secret" {
		t.Fatal("debe respetar la variable de entorno")
	}
}
```

`internal/events/kafka_test.go`:

```go
package events

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fakeWriter struct {
	msgs []kafka.Message
	err  error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}
func (f *fakeWriter) Close() error { return nil }

func TestPublisherUsaPaymentIDComoKey(t *testing.T) {
	w := &fakeWriter{}
	p := &KafkaPublisher{writer: w, topic: "payments.events"}
	if err := p.Publish(context.Background(), "pay_1", []byte(`{"type":"payment.created"}`)); err != nil {
		t.Fatal(err)
	}
	if len(w.msgs) != 1 || string(w.msgs[0].Key) != "pay_1" {
		t.Fatalf("mensaje o key incorrecto: %+v", w.msgs)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config ./internal/events`
Expected: FAIL con `go.mod file not found` (todavía no existe el módulo).

- [ ] **Step 3: Write minimal implementation**

`go.mod`:

```go
module github.com/patricioarmesto/0007-stripe

go 1.23

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/alicebob/miniredis/v2 v2.33.0
	github.com/ClickHouse/clickhouse-go/v2 v2.30.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	github.com/redis/go-redis/v9 v9.7.3
	github.com/segmentio/kafka-go v0.4.47
)
```

`internal/config/config.go`:

```go
// Package config junta todas las variables de entorno en un solo lugar.
// Los defaults apuntan a los nombres de servicio de docker-compose, así el
// mismo binario funciona adentro del container y en la laptop con un override.
package config

import "os"

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	KafkaBrokers  []string
	KafkaTopic    string
	ClickHouseDSN string
	WebhookSecret string
	Port          string
}

func Load() Config {
	return Config{
		PostgresDSN:   env("POSTGRES_DSN", "postgres://stripe:stripe@postgres:5432/stripe?sslmode=disable"),
		RedisAddr:     env("REDIS_ADDR", "redis:6379"),
		KafkaBrokers:  []string{env("KAFKA_BROKERS", "kafka:9092")},
		KafkaTopic:    env("KAFKA_TOPIC", "payments.events"),
		ClickHouseDSN: env("CLICKHOUSE_DSN", "clickhouse://default@clickhouse:9000/stripe"),
		WebhookSecret: env("STRIPE_WEBHOOK_SECRET", "whsec_test"),
		Port:          env("PORT", "8081"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

`internal/events/kafka.go`:

```go
// Package events separa "avísale a reportes" de "confirmá el pago".
// Si Kafka está caído, el pago ya está en Postgres: se loguea y se sigue.
package events

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Publisher interface {
	Publish(ctx context.Context, key string, payload []byte) error
}

// Writer existe para poder testear sin un broker levantado.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	writer Writer
	topic  string
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		// Hash por payment_id: los eventos de un mismo pago caen en la misma
		// partición y llegan en orden al consumidor.
		writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}},
		topic:  topic,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, key string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload})
}
```

- [ ] **Step 4: Run tests**

Run: `go mod tidy && go test ./internal/config ./internal/events`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum internal/config internal/events
git commit -m "feat: configuración y publicador de eventos"
```

---

### Task 2: Infra — Compose, esquemas SQL, Dockerfiles y nginx

**Files:**
- Create: `docker-compose.yml`, `.env`, `.gitignore`, `Dockerfile`, `frontend/Dockerfile`, `deploy/nginx/nginx.conf`, `db/postgres/init.sql`, `db/clickhouse/init.sql`

**Interfaces:**
- Consumes: `go.mod` (Task 1).
- Produces: servicios `postgres`, `redis`, `kafka`, `clickhouse`, `payments`, `webhook`, `consumer`, `dashboard`, `frontend`; puertos internos `8081`/`8082`/`8083`; frontend en `:3000`.

- [ ] **Step 1: Write the SQL schemas**

`db/postgres/init.sql`:

```sql
-- Postgres es la fuente de verdad del dinero. El UNIQUE de idempotency_key
-- es la defensa final: aunque se caiga el proceso, no hay dos pagos iguales.
CREATE TABLE IF NOT EXISTS payments (
    id uuid PRIMARY KEY,
    idempotency_key text UNIQUE NOT NULL,
    amount int NOT NULL,
    currency text NOT NULL,
    email text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Un aviso del banco se procesa una sola vez: el reintento con el mismo
-- event_id tiene que ser inofensivo. attempts es solo para la demo del retry.
CREATE TABLE IF NOT EXISTS webhook_events (
    event_id text UNIQUE NOT NULL,
    type text NOT NULL,
    payment_id uuid NOT NULL REFERENCES payments(id),
    attempts int NOT NULL DEFAULT 1,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- Sin este historial, GET /payments/:id no podría mostrar por qué un pago
-- pasó de processing a succeeded.
CREATE TABLE IF NOT EXISTS payment_transitions (
    id bigserial PRIMARY KEY,
    payment_id uuid NOT NULL REFERENCES payments(id),
    from_status text,
    to_status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

`db/clickhouse/init.sql`:

```sql
CREATE DATABASE IF NOT EXISTS stripe;

-- ClickHouse es SOLO reportes: nunca se consulta para decidir si cobramos.
CREATE TABLE IF NOT EXISTS stripe.payments_report (
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

- [ ] **Step 2: Write Dockerfiles y nginx**

`Dockerfile` (el mismo para los cuatro binarios):

```dockerfile
FROM golang:1.23-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/app ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /bin/app /app
ENTRYPOINT ["/app"]
```

`frontend/Dockerfile`:

```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY deploy/nginx/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

El contexto de build del frontend es la raíz del repo (ver compose). No hace falta un `deploy/nginx/Dockerfile`: la config se copia al image de nginx.

`deploy/nginx/nginx.conf`:

```nginx
server {
    listen 80;
    root /usr/share/nginx/html;
    index index.html;

    location / { try_files $uri /index.html; }

    # El frontend nunca habla con los puertos internos: nginx es la puerta.
    location = /api/payments { proxy_pass http://payments:8081/payments; }
    location /api/payments/ {
        rewrite ^/api/payments/(.*)$ /$1 break;
        proxy_pass http://payments:8081;
    }
    location = /api/webhooks { proxy_pass http://webhook:8082/webhooks; }
    location /api/webhooks/ {
        rewrite ^/api/webhooks/(.*)$ /$1 break;
        proxy_pass http://webhook:8082;
    }
    location /api/stats { proxy_pass http://dashboard:8083; }
}
```

- [ ] **Step 3: Write Compose y entorno**

`.env` (secret de prueba, nunca credencial real):

```dotenv
POSTGRES_USER=stripe
POSTGRES_PASSWORD=stripe
POSTGRES_DB=stripe
STRIPE_WEBHOOK_SECRET=whsec_test
KAFKA_TOPIC=payments.events
```

`.gitignore`:

```gitignore
node_modules/
dist/
*.log
.DS_Store
```

`docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports: ["5432:5432"]
    volumes:
      - ./db/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      retries: 10

  kafka:
    image: bitnami/kafka:3.7
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports: ["9092:9092"]
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 10s
      retries: 15

  clickhouse:
    image: clickhouse/clickhouse-server:24.3
    ports: ["8123:8123", "9000:9000"]
    volumes:
      - ./db/clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      - chdata:/var/lib/clickhouse
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s
      retries: 10

  payments:
    build: { context: ., dockerfile: Dockerfile, args: { SERVICE: payments } }
    env_file: .env
    environment:
      PORT: "8081"
      POSTGRES_DSN: postgres://stripe:stripe@postgres:5432/stripe?sslmode=disable
      REDIS_ADDR: redis:6379
      KAFKA_BROKERS: kafka:9092
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      kafka: { condition: service_healthy }

  webhook:
    build: { context: ., dockerfile: Dockerfile, args: { SERVICE: webhook } }
    env_file: .env
    environment:
      PORT: "8082"
      POSTGRES_DSN: postgres://stripe:stripe@postgres:5432/stripe?sslmode=disable
      KAFKA_BROKERS: kafka:9092
    depends_on:
      postgres: { condition: service_healthy }
      kafka: { condition: service_healthy }

  consumer:
    build: { context: ., dockerfile: Dockerfile, args: { SERVICE: consumer } }
    env_file: .env
    environment:
      KAFKA_BROKERS: kafka:9092
      CLICKHOUSE_DSN: clickhouse://default@clickhouse:9000/stripe
    depends_on:
      kafka: { condition: service_healthy }
      clickhouse: { condition: service_healthy }

  dashboard:
    build: { context: ., dockerfile: Dockerfile, args: { SERVICE: dashboard } }
    env_file: .env
    environment:
      PORT: "8083"
      CLICKHOUSE_DSN: clickhouse://default@clickhouse:9000/stripe
    depends_on:
      clickhouse: { condition: service_healthy }

  frontend:
    build: { context: ., dockerfile: frontend/Dockerfile }
    ports: ["3000:80"]
    depends_on: [payments, webhook, dashboard]

volumes:
  pgdata:
  chdata:
```

- [ ] **Step 4: Verify infra**

Run: `docker compose config -q && docker compose up -d postgres redis clickhouse`
Expected: comando exitoso. Después:

Run: `docker compose exec -T postgres psql -U stripe -d stripe -c '\dt'`
Expected: aparecen `payments`, `payment_transitions`, `webhook_events`.

Run: `docker compose exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM stripe"`
Expected: aparece `payments_report`.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml .env .gitignore Dockerfile frontend/Dockerfile deploy/nginx/nginx.conf db
git commit -m "feat: infra de compose con postgres, redis, kafka, clickhouse y nginx"
```

---

### Task 3: Máquina de estados

**Files:**
- Create: `internal/state/machine.go`, `internal/state/machine_test.go`

**Interfaces:**
- Consumes: nada.
- Produces: `type Status string`; constantes `StatusCreated`, `StatusProcessing`, `StatusSucceeded`, `StatusFailed`; `func CanTransition(from, to Status) bool`; `func Transition(from, to Status) error`.

- [ ] **Step 1: Write the failing test**

```go
package state

import "testing"

func TestCanTransition(t *testing.T) {
	casos := []struct {
		from, to Status
		want     bool
	}{
		{StatusCreated, StatusProcessing, true},
		{StatusProcessing, StatusSucceeded, true},
		{StatusProcessing, StatusFailed, true},
		{StatusCreated, StatusSucceeded, false},
		{StatusSucceeded, StatusProcessing, false},
		{StatusSucceeded, StatusFailed, false},
		{StatusFailed, StatusSucceeded, false},
		{StatusSucceeded, StatusSucceeded, false},
	}
	for _, c := range casos {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s,%s) = %v, quería %v", c.from, c.to, got, c.want)
		}
	}
}

func TestTransitionRechazaEstadoInvalido(t *testing.T) {
	if err := Transition(StatusSucceeded, StatusProcessing); err == nil {
		t.Fatal("un pago cobrado no puede volver a processing")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/state`
Expected: FAIL por símbolos indefinidos.

- [ ] **Step 3: Write minimal implementation**

```go
// Package state es el árbitro de los estados del pago. No toca la DB a propósito:
// así se puede testear sin levantar nada y tanto payments como webhook usan
// EXACTAMENTE la misma regla.
package state

import "fmt"

type Status string

const (
	StatusCreated    Status = "created"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

func CanTransition(from, to Status) bool {
	switch from {
	case StatusCreated:
		return to == StatusProcessing
	case StatusProcessing:
		return to == StatusSucceeded || to == StatusFailed
	default:
		// succeeded y failed son terminales. Un webhook viejo que llega tarde
		// no puede deshacer un cobro ya confirmado.
		return false
	}
}

func Transition(from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("transición inválida: %s -> %s", from, to)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/state`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/state
git commit -m "feat: máquina de estados del pago"
```

---

### Task 4: Repositorio Postgres de pagos

**Files:**
- Create: `internal/payments/model.go`, `internal/payments/repository.go`, `internal/payments/repository_test.go`

**Interfaces:**
- Consumes: `internal/state` (Task 3).
- Produces: `payments.Payment`, `payments.Transition`, `payments.Repository` con `GetByIdempotencyKey`, `GetByID`, `List`, `Insert`, `SetStatus`, `Transitions`; `payments.NewSQLRepository(db *sql.DB) *SQLRepository`.

- [ ] **Step 1: Write the failing test**

```go
package payments

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL no configurado; correr con el postgres del compose")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM payment_transitions; DELETE FROM webhook_events; DELETE FROM payments;`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRepositorioClienteYTransiciones(t *testing.T) {
	db := testDB(t)
	repo := NewSQLRepository(db)
	ctx := context.Background()
	p := &Payment{
		ID: uuid.NewString(), IdempotencyKey: uuid.NewString(), Amount: 12500,
		Currency: "ARS", Email: "ana@example.com", Status: state.StatusCreated,
	}
	if err := repo.Insert(ctx, p); err != nil {
		t.Fatal(err)
	}
	found, err := repo.GetByIdempotencyKey(ctx, p.IdempotencyKey)
	if err != nil || found.ID != p.ID {
		t.Fatalf("no encontró por idempotency_key: %+v %v", found, err)
	}
	if err := repo.SetStatus(ctx, p.ID, state.StatusCreated, state.StatusProcessing); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetStatus(ctx, p.ID, state.StatusProcessing, state.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetByID(ctx, p.ID)
	if err != nil || got.Status != state.StatusSucceeded {
		t.Fatalf("estado final incorrecto: %+v %v", got, err)
	}
	ts, err := repo.Transitions(ctx, p.ID)
	if err != nil || len(ts) != 2 {
		t.Fatalf("historial incorrecto: %+v %v", ts, err)
	}
	if ts[0].FromStatus != string(state.StatusCreated) || ts[1].ToStatus != string(state.StatusSucceeded) {
		t.Fatalf("transiciones desordenadas: %+v", ts)
	}
	lista, err := repo.List(ctx)
	if err != nil || len(lista) != 1 || lista[0].Attempts != 0 {
		t.Fatalf("listado incorrecto: %+v %v", lista, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/payments`
Expected: FAIL por símbolos indefinidos o `SKIP` si falta `DATABASE_URL`; en ese caso exportar la variable del paso siguiente.

- [ ] **Step 3: Write minimal implementation**

`internal/payments/model.go`:

```go
package payments

import (
	"time"

	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type Payment struct {
	ID             string       `json:"id"`
	IdempotencyKey string       `json:"-"`
	Amount         int          `json:"amount"`
	Currency       string       `json:"currency"`
	Email          string       `json:"email"`
	Status         state.Status `json:"status"`
	CreatedAt      time.Time    `json:"created_at"`
	Attempts       int          `json:"attempts"`
}

type Transition struct {
	FromStatus string    `json:"from_status"`
	ToStatus   string    `json:"to_status"`
	CreatedAt  time.Time `json:"created_at"`
}
```

`internal/payments/repository.go`:

```go
// Package payments guarda la verdad del dinero. El INSERT con idempotency_key
// UNIQUE es lo que salva el doble click: si dos requests llegan juntas, una
// falla la constraint y el servicio devuelve el pago que ya existe.
package payments

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type Repository interface {
	GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error)
	GetByID(ctx context.Context, id string) (*Payment, error)
	List(ctx context.Context) ([]Payment, error)
	Insert(ctx context.Context, p *Payment) error
	SetStatus(ctx context.Context, id string, from, to state.Status) error
	Transitions(ctx context.Context, id string) ([]Transition, error)
}

type SQLRepository struct{ db *sql.DB }

func NewSQLRepository(db *sql.DB) *SQLRepository { return &SQLRepository{db: db} }

const selectPayment = `
SELECT p.id, p.amount, p.currency, p.email, p.status, p.created_at,
       COALESCE(w.attempts, 0)
FROM payments p
LEFT JOIN webhook_events w ON w.payment_id = p.id`

func (r *SQLRepository) scan(rows *sql.Rows) (*Payment, error) {
	var p Payment
	if err := rows.Scan(&p.ID, &p.Amount, &p.Currency, &p.Email, &p.Status, &p.CreatedAt, &p.Attempts); err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *SQLRepository) GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error) {
	rows, err := r.db.QueryContext(ctx, selectPayment+` WHERE p.idempotency_key = $1`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	return r.scan(rows)
}

func (r *SQLRepository) GetByID(ctx context.Context, id string) (*Payment, error) {
	rows, err := r.db.QueryContext(ctx, selectPayment+` WHERE p.id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	return r.scan(rows)
}

func (r *SQLRepository) List(ctx context.Context) ([]Payment, error) {
	rows, err := r.db.QueryContext(ctx, selectPayment+` ORDER BY p.created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Payment{}
	for rows.Next() {
		p, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (r *SQLRepository) Insert(ctx context.Context, p *Payment) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO payments (id, idempotency_key, amount, currency, email, status)
VALUES ($1, $2, $3, $4, $5, $6)`, p.ID, p.IdempotencyKey, p.Amount, p.Currency, p.Email, string(p.Status))
	return err
}

func (r *SQLRepository) SetStatus(ctx context.Context, id string, from, to state.Status) error {
	if err := state.Transition(from, to); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// El WHERE status = from es el cerrojo optimista: si otro proceso ya
	// cambió el estado, RowsAffected es 0 y no pisamos su decisión.
	res, err := tx.ExecContext(ctx, `UPDATE payments SET status = $1 WHERE id = $2 AND status = $3`, string(to), id, string(from))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("el pago %s ya no está en %s", id, from)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO payment_transitions (payment_id, from_status, to_status)
VALUES ($1, $2, $3)`, id, string(from), string(to)); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *SQLRepository) Transitions(ctx context.Context, id string) ([]Transition, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT from_status, to_status, created_at FROM payment_transitions
WHERE payment_id = $1 ORDER BY created_at, id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Transition{}
	for rows.Next() {
		var t Transition
		var from sql.NullString
		if err := rows.Scan(&from, &t.ToStatus, &t.CreatedAt); err != nil {
			return nil, err
		}
		t.FromStatus = from.String
		out = append(out, t)
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL='postgres://stripe:stripe@localhost:5432/stripe?sslmode=disable' go test ./internal/payments -run TestRepositorio -v`
Expected: PASS (si el compose no está levantado, correr antes `docker compose up -d postgres`).

- [ ] **Step 5: Commit**

```bash
git add internal/payments/model.go internal/payments/repository.go internal/payments/repository_test.go
git commit -m "feat: repositorio de pagos con transiciones atómicas"
```

---

### Task 5: Lock distribuido en Redis

**Files:**
- Create: `internal/cache/lock.go`, `internal/cache/lock_test.go`

**Interfaces:**
- Consumes: nada.
- Produces: `cache.Locker` con `Acquire(ctx, key string) (token string, ok bool, err error)` y `Release(ctx, key, token string) error`; `cache.NewRedisLocker(client *redis.Client) *RedisLocker`.

- [ ] **Step 1: Write the failing test**

```go
package cache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLockNXYLiberacionConToken(t *testing.T) {
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	locker := NewRedisLocker(client)
	ctx := context.Background()

	token, ok, err := locker.Acquire(ctx, "idem:abc")
	if err != nil || !ok || token == "" {
		t.Fatalf("primero debía adquirir: %q %v %v", token, ok, err)
	}
	if _, ok, _ := locker.Acquire(ctx, "idem:abc"); ok {
		t.Fatal("el segundo NO debe adquirir el mismo lock")
	}
	if err := locker.Release(ctx, "idem:abc", "token-ajeno"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := locker.Acquire(ctx, "idem:abc"); ok {
		t.Fatal("no se puede liberar con el token de otro")
	}
	if err := locker.Release(ctx, "idem:abc", token); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := locker.Acquire(ctx, "idem:abc"); !ok {
		t.Fatal("después de liberar con el token correcto sí debe adquirir")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cache`
Expected: FAIL por símbolos indefinidos.

- [ ] **Step 3: Write minimal implementation**

```go
// Package cache existe por una razón puntual: dos requests con la misma clave
// pueden llegar al mismo tiempo y los dos encontrar "no existe" en Postgres.
// El lock de Redis gana esa carrera de milisegundos. No es la verdad: la verdad
// sigue siendo el UNIQUE de Postgres.
package cache

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Locker interface {
	Acquire(ctx context.Context, key string) (string, bool, error)
	Release(ctx context.Context, key, token string) error
}

type RedisLocker struct{ client *redis.Client }

func NewRedisLocker(client *redis.Client) *RedisLocker { return &RedisLocker{client: client} }

func (l *RedisLocker) Acquire(ctx context.Context, key string) (string, bool, error) {
	token := uuid.NewString()
	// NX = solo si no existe; PX 10000 = expira en 10s para que un proceso
	// caído no deje el pago trabado para siempre.
	ok, err := l.client.SetNX(ctx, key, token, 10*time.Second).Result()
	return token, ok, err
}

var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end`)

func (l *RedisLocker) Release(ctx context.Context, key, token string) error {
	return releaseScript.Run(ctx, l.client, []string{key}, token).Err()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/cache`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cache
git commit -m "feat: lock distribuido con SET NX y TTL"
```

---

### Task 6: Servicio de pagos — idempotencia de punta a punta

**Files:**
- Create: `internal/payments/service.go`, `internal/payments/service_test.go`

**Interfaces:**
- Consumes: `payments.Repository` (Task 4), `cache.Locker` (Task 5), `events.Publisher` (Task 1), `state` (Task 3).
- Produces: `payments.CreateInput`, `payments.Service`, `payments.NewService(repo Repository, locker cache.Locker, pub events.Publisher) *Service`, métodos `Create(ctx, in) (*Payment, bool, error)`, `Get(ctx,id) (*Payment, []Transition, error)`, `List(ctx) ([]Payment, error)`; centinelas `ErrInvalidInput`, `ErrBusy`.

- [ ] **Step 1: Write the failing test**

```go
package payments

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/patricioarmesto/0007-stripe/internal/cache"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type fakeRepo struct {
	byKey   map[string]*Payment
	byID    map[string]*Payment
	inserts int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{byKey: map[string]*Payment{}, byID: map[string]*Payment{}}
}

func (f *fakeRepo) GetByIdempotencyKey(_ context.Context, key string) (*Payment, error) {
	return f.byKey[key], nil
}
func (f *fakeRepo) GetByID(_ context.Context, id string) (*Payment, error) {
	return f.byID[id], nil
}
func (f *fakeRepo) List(_ context.Context) ([]Payment, error) { return nil, nil }
func (f *fakeRepo) Insert(_ context.Context, p *Payment) error {
	f.inserts++
	c := *p
	f.byKey[p.IdempotencyKey] = &c
	f.byID[p.ID] = &c
	return nil
}
func (f *fakeRepo) SetStatus(_ context.Context, id string, _, to state.Status) error {
	f.byID[id].Status = to
	return nil
}
func (f *fakeRepo) Transitions(_ context.Context, _ string) ([]Transition, error) {
	return []Transition{}, nil
}

type fakeLocker struct{ busy bool }

func (f *fakeLocker) Acquire(context.Context, string) (string, bool, error) {
	return "token", !f.busy, nil
}
func (f *fakeLocker) Release(context.Context, string, string) error { return nil }

type fakePublisher struct {
	calls int
	err   error
}

func (f *fakePublisher) Publish(context.Context, string, []byte) error {
	f.calls++
	return f.err
}

func input() CreateInput {
	return CreateInput{
		IdempotencyKey: uuid.NewString(), Amount: 12500, Currency: "ARS",
		Email: "ana@example.com", PaymentMethod: "card_test",
	}
}

func TestServicioCreateEsIdempotente(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	s := NewService(repo, &fakeLocker{}, pub)
	in := input()

	primero, created, err := s.Create(context.Background(), in)
	if err != nil || !created {
		t.Fatalf("primer create falló: %+v %v %v", primero, created, err)
	}
	segundo, created, err := s.Create(context.Background(), in)
	if err != nil || created {
		t.Fatalf("segundo create debía devolver existente: %+v %v %v", segundo, created, err)
	}
	if primero.ID != segundo.ID || repo.inserts != 1 || pub.calls != 1 {
		t.Fatalf("se duplicó el pago: ids %s/%s inserts=%d pubs=%d", primero.ID, segundo.ID, repo.inserts, pub.calls)
	}
	if segundo.Status != state.StatusProcessing {
		t.Fatalf("debió quedar processing: %s", segundo.Status)
	}
}

func TestServicioLockOcupadoDevuelvePagoExistente(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	s := NewService(repo, &fakeLocker{busy: true}, pub)
	in := input()
	existente := &Payment{ID: "pay_1", IdempotencyKey: in.IdempotencyKey, Status: state.StatusProcessing}
	_ = repo.Insert(context.Background(), existente)

	got, created, err := s.Create(context.Background(), in)
	if err != nil || created || got.ID != "pay_1" {
		t.Fatalf("lock ocupado debía devolver el existente: %+v %v %v", got, created, err)
	}
}

func TestServicioCreatePublicacionFallidaNoRompePago(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{err: errors.New("kafka caido")}
	s := NewService(repo, &fakeLocker{}, pub)
	p, created, err := s.Create(context.Background(), input())
	if err != nil || !created || p == nil {
		t.Fatalf("el pago no debe depender de Kafka: %+v %v %v", p, created, err)
	}
}

func TestServicioValidaEntrada(t *testing.T) {
	s := NewService(newFakeRepo(), &fakeLocker{}, &fakePublisher{})
	in := input()
	in.Currency = "USD"
	if _, _, err := s.Create(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("esperaba ErrInvalidInput, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/payments -run TestServicio`
Expected: FAIL por `NewService` y `CreateInput` indefinidos.

- [ ] **Step 3: Write minimal implementation**

```go
package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/patricioarmesto/0007-stripe/internal/cache"
	"github.com/patricioarmesto/0007-stripe/internal/events"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

var (
	ErrInvalidInput = errors.New("datos inválidos")
	ErrBusy         = errors.New("idempotency key en uso")
)

type CreateInput struct {
	IdempotencyKey string `json:"-"`
	Amount         int    `json:"amount"`
	Currency       string `json:"currency"`
	Email          string `json:"email"`
	PaymentMethod  string `json:"payment_method"`
}

type Service struct {
	repo   Repository
	locker cache.Locker
	pub    events.Publisher
	log    *log.Logger
}

func NewService(repo Repository, locker cache.Locker, pub events.Publisher) *Service {
	return &Service{repo: repo, locker: locker, pub: pub, log: log.Default()}
}

type createdEvent struct {
	EventID   string       `json:"event_id"`
	Type      string       `json:"type"`
	PaymentID string       `json:"payment_id"`
	Amount    int          `json:"amount"`
	Email     string       `json:"email"`
	Status    state.Status `json:"status"`
	Timestamp time.Time    `json:"timestamp"`
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*Payment, bool, error) {
	if _, err := uuid.Parse(in.IdempotencyKey); err != nil {
		return nil, false, fmt.Errorf("%w: Idempotency-Key debe ser uuid", ErrInvalidInput)
	}
	if in.Amount <= 0 || in.Currency != "ARS" || !strings.Contains(in.Email, "@") || in.PaymentMethod != "card_test" {
		return nil, false, fmt.Errorf("%w: revisá amount, currency, email y payment_method", ErrInvalidInput)
	}

	lockKey := "idem:" + in.IdempotencyKey
	token, ok, err := s.locker.Acquire(ctx, lockKey)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// El otro request está escribiendo. Esperamos un instante y releemos:
		// si ya guardó su pago, le devolvemos ese, no un error 500.
		for i := 0; i < 10; i++ {
			time.Sleep(20 * time.Millisecond)
			if p, err := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey); err == nil && p != nil {
				return p, false, nil
			}
		}
		return nil, false, ErrBusy
	}
	defer func() {
		if err := s.locker.Release(ctx, lockKey, token); err != nil {
			s.log.Printf("no se pudo liberar lock %s: %v", lockKey, err)
		}
	}()

	if p, err := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey); err == nil && p != nil {
		return p, false, nil
	}

	p := &Payment{
		ID: uuid.NewString(), IdempotencyKey: in.IdempotencyKey, Amount: in.Amount,
		Currency: in.Currency, Email: in.Email, Status: state.StatusCreated,
	}
	if err := s.repo.Insert(ctx, p); err != nil {
		return nil, false, err
	}
	// created -> processing pasa antes de responder: el webhook del banco
	// siempre encuentra un estado desde el cual avanzar a succeeded.
	if err := s.repo.SetStatus(ctx, p.ID, state.StatusCreated, state.StatusProcessing); err != nil {
		return nil, false, err
	}

	payload, _ := json.Marshal(createdEvent{
		EventID: "evt_" + uuid.NewString(), Type: "payment.created", PaymentID: p.ID,
		Amount: p.Amount, Email: p.Email, Status: p.Status, Timestamp: time.Now().UTC(),
	})
	if err := s.pub.Publish(ctx, p.ID, payload); err != nil {
		// Acá NO hacemos rollback: Postgres ya dijo que el pago existe.
		s.log.Printf("pago %s guardado pero no se pudo publicar: %v", p.ID, err)
	}
	return p, true, nil
}

func (s *Service) Get(ctx context.Context, id string) (*Payment, []Transition, error) {
	p, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	ts, err := s.repo.Transitions(ctx, id)
	return p, ts, err
}

func (s *Service) List(ctx context.Context) ([]Payment, error) { return s.repo.List(ctx) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/payments -run TestServicio`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/payments/service.go internal/payments/service_test.go
git commit -m "feat: servicios de pagos idempotente con lock y publicación"
```

---

### Task 7: HTTP del servicio de pagos y binario

**Files:**
- Create: `internal/payments/handler.go`, `internal/payments/handler_test.go`, `cmd/payments/main.go`

**Interfaces:**
- Consumes: `payments.Service` (Task 6), `config` (Task 1).
- Produces: `payments.NewHandler(s *Service) *Handler` implementando `http.Handler`; rutas `POST /payments`, `GET /payments`, `GET /payments/{id}`, `GET /health`.

- [ ] **Step 1: Write the failing test**

```go
package payments

import (
	"context"
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
		var got Payment
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status != state.StatusProcessing {
			t.Fatalf("estado incorrecto: %s", got.Status)
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/payments -run TestHandler`
Expected: FAIL por `NewHandler` indefinido.

- [ ] **Step 3: Write minimal implementation**

`internal/payments/handler.go`:

```go
package payments

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type Handler struct{ service *Service }

func NewHandler(s *Service) *Handler { return &Handler{service: s} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	case r.Method == http.MethodPost && r.URL.Path == "/payments":
		h.create(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/payments":
		h.list(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/payments/"):
		h.get(w, r)
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "falta Idempotency-Key"})
		return
	}
	var in CreateInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body inválido"})
		return
	}
	in.IdempotencyKey = key
	p, created, err := h.service.Create(r.Context(), in)
	if errors.Is(err, ErrInvalidInput) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if errors.Is(err, ErrBusy) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, map[string]any{"payment": p, "created": created})
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	pagos, err := h.service.List(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"payments": pagos})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/payments/")
	p, transitions, err := h.service.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "pago no encontrado"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"payment": p, "transitions": transitions})
}
```

`cmd/payments/main.go`:

```go
// Binario de pagos: solo HTTP. No conoce nada de ClickHouse ni de webhooks.
package main

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/cache"
	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/events"
	"github.com/patricioarmesto/0007-stripe/internal/payments"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()

	repo := payments.NewSQLRepository(db)
	svc := payments.NewService(repo, cache.NewRedisLocker(redisClient), events.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic))
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           payments.NewHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("payments escuchando en :%s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/payments`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/payments/handler.go internal/payments/handler_test.go cmd/payments/main.go
git commit -m "feat: API HTTP de pagos"
```

---

### Task 8: Firma HMAC del webhook

**Files:**
- Create: `internal/webhook/signature.go`, `internal/webhook/signature_test.go`

**Interfaces:**
- Consumes: nada.
- Produces: `webhook.Sign(body []byte, secret string) string`, `webhook.Verify(body []byte, header, secret string) bool`.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/webhook -run TestVerifyFirma`
Expected: FAIL por símbolos indefinidos.

- [ ] **Step 3: Write minimal implementation**

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/webhook -run TestVerifyFirma`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/webhook/signature.go internal/webhook/signature_test.go
git commit -m "feat: verificación HMAC del webhook"
```

---

### Task 9: Repositorio y servicio del webhook — idempotencia y transacción

**Files:**
- Create: `internal/webhook/repository.go`, `internal/webhook/service.go`, `internal/webhook/service_test.go`

**Interfaces:**
- Consumes: `state` (Task 3), `events.Publisher` (Task 1).
- Produces: `webhook.Event`, `webhook.PaymentSnapshot`, `webhook.Repository` (`Exists`, `Process`, `GetPayment`, `MarkAttempts`), `webhook.NewSQLRepository(db *sql.DB)`, `webhook.Service` con `NewService(repo Repository, pub events.Publisher)`, `Handle(ctx, Event) (bool, error)`, `Exists`, `MarkAttempts`.

- [ ] **Step 1: Write the failing test**

```go
package webhook

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type countingPublisher struct{ calls int }

func (c *countingPublisher) Publish(context.Context, string, []byte) error {
	c.calls++
	return nil
}

func webhookDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL no configurado")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM payment_transitions; DELETE FROM webhook_events; DELETE FROM payments;`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedPago(t *testing.T, db *sql.DB, status state.Status) string {
	t.Helper()
	id := uuid.NewString()
	_, err := db.Exec(`INSERT INTO payments (id, idempotency_key, amount, currency, email, status)
VALUES ($1,$2,12500,'ARS','ana@example.com',$3)`, id, uuid.NewString(), string(status))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestServicioWebhookIdempotente(t *testing.T) {
	db := webhookDB(t)
	pub := &countingPublisher{}
	repo := NewSQLRepository(db)
	svc := NewService(repo, pub)
	ctx := context.Background()
	id := seedPago(t, db, state.StatusProcessing)
	ev := Event{EventID: "evt_456", Type: "payment.succeeded", PaymentID: id}

	first, err := svc.Handle(ctx, ev)
	if err != nil || !first {
		t.Fatalf("primer webhook debía procesar: %v %v", first, err)
	}
	second, err := svc.Handle(ctx, ev)
	if err != nil || second {
		t.Fatalf("segundo webhook debía ignorarse: %v %v", second, err)
	}
	if pub.calls != 1 {
		t.Fatalf("Kafka debe recibir un solo evento, recibió %d", pub.calls)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM payments WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(state.StatusSucceeded) {
		t.Fatalf("estado final: %s", status)
	}

	// Un evento viejo de failed no puede deshacer un cobro confirmado.
	viejo := Event{EventID: "evt_viejo", Type: "payment.failed", PaymentID: id}
	if changed, err := svc.Handle(ctx, viejo); err != nil || changed {
		t.Fatalf("evento viejo no debía cambiar nada: %v %v", changed, err)
	}
	if err := db.QueryRow(`SELECT status FROM payments WHERE id=$1`, id).Scan(&status); err != nil || status != string(state.StatusSucceeded) {
		t.Fatalf("succeeded regresó: %s %v", status, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/webhook -run TestServicioWebhook`
Expected: FAIL por símbolos indefinidos.

- [ ] **Step 3: Write minimal implementation**

`internal/webhook/repository.go`:

```go
// Package webhook aplica los avisos del banco. El event_id UNIQUE es la razón
// por la que un reintento no duplica el cambio de estado: insertar el evento
// y actualizar el pago ocurren en la MISMA transacción.
package webhook

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/patricioarmesto/0007-stripe/internal/state"
)

type Event struct {
	EventID   string `json:"event_id"`
	Type      string `json:"type"`
	PaymentID string `json:"payment_id"`
}

type PaymentSnapshot struct {
	ID     string
	Amount int
	Email  string
	Status state.Status
}

type Repository interface {
	Exists(ctx context.Context, eventID string) (bool, error)
	Process(ctx context.Context, ev Event) (bool, error)
	GetPayment(ctx context.Context, id string) (*PaymentSnapshot, error)
	MarkAttempts(ctx context.Context, eventID string, attempts int) error
}

type SQLRepository struct{ db *sql.DB }

func NewSQLRepository(db *sql.DB) *SQLRepository { return &SQLRepository{db: db} }

func (r *SQLRepository) Exists(ctx context.Context, eventID string) (bool, error) {
	var ok bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM webhook_events WHERE event_id=$1)`, eventID).Scan(&ok)
	return ok, err
}

func targetStatus(t string) (state.Status, error) {
	switch t {
	case "payment.succeeded":
		return state.StatusSucceeded, nil
	case "payment.failed":
		return state.StatusFailed, nil
	default:
		return "", fmt.Errorf("tipo de evento desconocido: %s", t)
	}
}

func (r *SQLRepository) Process(ctx context.Context, ev Event) (bool, error) {
	to, err := targetStatus(ev.Type)
	if err != nil {
		return false, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
INSERT INTO webhook_events (event_id, type, payment_id) VALUES ($1,$2,$3)
ON CONFLICT (event_id) DO NOTHING`, ev.EventID, ev.Type, ev.PaymentID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		// Ya se procesó. Confirmamos la transacción vacía y salimos sin tocar
		// el pago ni publicar otro evento.
		return false, tx.Commit()
	}

	var from state.Status
	if err := tx.QueryRowContext(ctx, `SELECT status FROM payments WHERE id=$1 FOR UPDATE`, ev.PaymentID).Scan(&from); err != nil {
		return false, err
	}
	if !state.CanTransition(from, to) {
		// Evento viejo: se registra para no volver a recibirlo, pero no se
		// aplica. Así un succeeded nunca se convierte en failed.
		return false, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET status=$1 WHERE id=$2`, string(to), ev.PaymentID); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO payment_transitions (payment_id, from_status, to_status)
VALUES ($1,$2,$3)`, ev.PaymentID, string(from), string(to)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *SQLRepository) GetPayment(ctx context.Context, id string) (*PaymentSnapshot, error) {
	var p PaymentSnapshot
	err := r.db.QueryRowContext(ctx, `SELECT id, amount, email, status FROM payments WHERE id=$1`, id).
		Scan(&p.ID, &p.Amount, &p.Email, &p.Status)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *SQLRepository) MarkAttempts(ctx context.Context, eventID string, attempts int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE webhook_events SET attempts=$1 WHERE event_id=$2`, attempts, eventID)
	return err
}
```

`internal/webhook/service.go`:

```go
package webhook

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/patricioarmesto/0007-stripe/internal/events"
)

type Service struct {
	repo Repository
	pub  events.Publisher
	log  *log.Logger
}

func NewService(repo Repository, pub events.Publisher) *Service {
	return &Service{repo: repo, pub: pub, log: log.Default()}
}

type publishedEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	PaymentID string    `json:"payment_id"`
	Amount    int       `json:"amount"`
	Email     string    `json:"email"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func (s *Service) Handle(ctx context.Context, ev Event) (bool, error) {
	processed, err := s.repo.Process(ctx, ev)
	if err != nil || !processed {
		return processed, err
	}
	p, err := s.repo.GetPayment(ctx, ev.PaymentID)
	if err != nil {
		return true, err
	}
	payload, _ := json.Marshal(publishedEvent{
		EventID: ev.EventID, Type: ev.Type, PaymentID: ev.PaymentID,
		Amount: p.Amount, Email: p.Email, Status: string(p.Status), Timestamp: time.Now().UTC(),
	})
	// Postgres ya confirmó el cambio. Si Kafka falla, se loguea y listo:
	// el dashboard se atrasará, pero el cobro sigue siendo verdad.
	if err := s.pub.Publish(ctx, ev.PaymentID, payload); err != nil {
		s.log.Printf("evento %s no publicado: %v", ev.EventID, err)
	}
	return true, nil
}

func (s *Service) Exists(ctx context.Context, eventID string) (bool, error) {
	return s.repo.Exists(ctx, eventID)
}

func (s *Service) MarkAttempts(ctx context.Context, eventID string, attempts int) error {
	return s.repo.MarkAttempts(ctx, eventID, attempts)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL='postgres://stripe:stripe@localhost:5432/stripe?sslmode=disable' go test ./internal/webhook -run TestServicioWebhook -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/webhook/repository.go internal/webhook/service.go internal/webhook/service_test.go
git commit -m "feat: webhook transaccional e idempotente"
```

---

### Task 10: HTTP del webhook — verificación, simulate y retry

**Files:**
- Create: `internal/webhook/handler.go`, `internal/webhook/handler_test.go`, `cmd/webhook/main.go`

**Interfaces:**
- Consumes: `webhook.Verify`/`Sign` (Task 8), `webhook.Service` (Task 9).
- Produces: `webhook.NewHandler(s WebhookService, secret string) *Handler`; rutas `POST /webhooks`, `POST /webhooks/simulate`, `POST /webhooks/retry`, `GET /health`.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/webhook -run TestHandler`
Expected: FAIL por `NewHandler` indefinido.

- [ ] **Step 3: Write minimal implementation**

`internal/webhook/handler.go`:

```go
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
	writeJSON(w, code, resp)
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
```

`cmd/webhook/main.go`:

```go
// Binario de webhook: recibe los avisos del banco (simulados localmente).
package main

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/events"
	"github.com/patricioarmesto/0007-stripe/internal/webhook"
)

func main() {
	cfg := config.Load()
	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	repo := webhook.NewSQLRepository(db)
	svc := webhook.NewService(repo, events.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic))
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           webhook.NewHandler(svc, cfg.WebhookSecret),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("webhook escuchando en :%s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/webhook -run 'TestHandler|TestVerifyFirma'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/webhook/handler.go internal/webhook/handler_test.go cmd/webhook/main.go
git commit -m "feat: endpoints de webhook, simulate y retry con backoff"
```

---

### Task 11: Consumidor de Kafka y batch insert en ClickHouse

**Files:**
- Create: `internal/analytics/clickhouse.go`, `internal/consumer/consumer.go`, `internal/consumer/consumer_test.go`, `cmd/consumer/main.go`

**Interfaces:**
- Consumes: `events` (Task 1), `config` (Task 1).
- Produces: `analytics.Row`, `analytics.Store`, `analytics.Open(dsn string) (*ClickHouseStore, error)`; `consumer.WithBackoff(ctx, fn, log) error`, `consumer.Parse(payload []byte) (analytics.Row, error)`, `consumer.New(reader *kafka.Reader, store analytics.Store) *Consumer`, `(*Consumer).Run(ctx) error`.

- [ ] **Step 1: Write the failing test**

```go
package consumer

import (
	"context"
	"errors"
	"log"
	"testing"

	"github.com/patricioarmesto/0007-stripe/internal/analytics"
)

func TestWithBackoffExitoEnSegundoIntento(t *testing.T) {
	llamadas := 0
	err := WithBackoff(context.Background(), func() error {
		llamadas++
		if llamadas == 1 {
			return errors.New("clickhouse caido")
		}
		return nil
	}, log.Default())
	if err != nil || llamadas != 2 {
		t.Fatalf("quería éxito en el intento 2, got err=%v llamadas=%d", err, llamadas)
	}
}

func TestWithBackoffReintentaTresVeces(t *testing.T) {
	llamadas := 0
	err := WithBackoff(context.Background(), func() error {
		llamadas++
		return errors.New("sigue caido")
	}, log.Default())
	if err == nil || llamadas != 3 {
		t.Fatalf("máximo 3 intentos: err=%v llamadas=%d", err, llamadas)
	}
}

func TestParseEvento(t *testing.T) {
	row, err := Parse([]byte(`{"event_id":"evt_1","type":"payment.succeeded","payment_id":"pay_1","amount":12500,"email":"ana@example.com","status":"succeeded","timestamp":"2027-02-16T10:00:00Z"}`))
	if err != nil || row.PaymentID != "pay_1" || row.Amount != 12500 || row.Status != "succeeded" {
		t.Fatalf("parse incorrecto: %+v %v", row, err)
	}
	var _ analytics.Row = row
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/consumer`
Expected: FAIL por símbolos indefinidos.

- [ ] **Step 3: Write minimal implementation**

`internal/analytics/clickhouse.go`:

```go
// Package analytics es reporte, nunca decisión. Si acá falta un dato, el
// dashboard queda feo; si falta en Postgres, el pago no existió.
package analytics

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Row struct {
	Date      time.Time
	Timestamp time.Time
	PaymentID string
	Amount    int64
	Status    string
	Email     string
}

type Store interface {
	Exists(ctx context.Context, row Row) (bool, error)
	InsertBatch(ctx context.Context, rows []Row) error
}

type ClickHouseStore struct{ db *sql.DB }

func Open(dsn string) (*ClickHouseStore, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &ClickHouseStore{db: db}, nil
}

func (s *ClickHouseStore) Exists(ctx context.Context, row Row) (bool, error) {
	var n uint64
	err := s.db.QueryRowContext(ctx, `
SELECT count() FROM stripe.payments_report WHERE payment_id=$1 AND status=$2`, row.PaymentID, row.Status).Scan(&n)
	return n > 0, err
}

func (s *ClickHouseStore) InsertBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO stripe.payments_report (date, timestamp, payment_id, amount, status, email)
VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, r.Timestamp, r.Timestamp, r.PaymentID, r.Amount, r.Status, r.Email); err != nil {
			return err
		}
	}
	return tx.Commit()
}
```

`internal/consumer/consumer.go`:

```go
package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/patricioarmesto/0007-stripe/internal/analytics"
)

type rawEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	PaymentID string    `json:"payment_id"`
	Amount    int64     `json:"amount"`
	Email     string    `json:"email"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func Parse(payload []byte) (analytics.Row, error) {
	var raw rawEvent
	if err := json.Unmarshal(payload, &raw); err != nil {
		return analytics.Row{}, err
	}
	if raw.Timestamp.IsZero() {
		raw.Timestamp = time.Now().UTC()
	}
	return analytics.Row{
		Date: raw.Timestamp, Timestamp: raw.Timestamp, PaymentID: raw.PaymentID,
		Amount: raw.Amount, Status: raw.Status, Email: raw.Email,
	}, nil
}

// WithBackoff es el mismo patrón del retry del webhook: 200/400/800ms en la
// demo y máximo 3 intentos. Cada fallo se loguea para que se vea en docker logs.
func WithBackoff(ctx context.Context, fn func() error, logger *log.Logger) error {
	delays := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	for attempt := 1; attempt <= 3; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		logger.Printf("intento %d falló: %v", attempt, err)
		if attempt == 3 {
			return err
		}
		select {
		case <-time.After(delays[attempt-1]):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type Consumer struct {
	reader *kafka.Reader
	store  analytics.Store
	log    *log.Logger
	seen   map[string]bool
}

func New(reader *kafka.Reader, store analytics.Store) *Consumer {
	return &Consumer{reader: reader, store: store, log: log.Default(), seen: map[string]bool{}}
}

func (c *Consumer) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var rows []analytics.Row
	var msgs []kafka.Message

	flush := func() {
		if len(rows) == 0 {
			return
		}
		err := WithBackoff(ctx, func() error { return c.store.InsertBatch(ctx, rows) }, c.log)
		if err != nil {
			// No hacemos commit del offset: el evento se va a re-entregar.
			c.log.Printf("no se pudo insertar el batch: %v", err)
			return
		}
		if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
			c.log.Printf("no se pudo hacer commit de offsets: %v", err)
		}
		rows, msgs = nil, nil
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
		case <-ticker.C:
			flush()
		case msg := <-c.reader.FetchMessage(ctx):
			row, err := Parse(msg.Value)
			if err != nil {
				c.log.Printf("evento inválido: %v", err)
				continue
			}
			key := row.PaymentID + ":" + row.Status
			if c.seen[key] {
				continue
			}
			existe, err := c.store.Exists(ctx, row)
			if err != nil || existe {
				c.seen[key] = true
				continue
			}
			rows = append(rows, row)
			msgs = append(msgs, msg)
			c.seen[key] = true
			if len(rows) >= 50 {
				flush()
			}
		}
	}
}
```

`cmd/consumer/main.go`:

```go
// Binario consumidor: no sirve HTTP. Solo mueve eventos de Kafka a ClickHouse.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"github.com/patricioarmesto/0007-stripe/internal/analytics"
	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/consumer"
)

func main() {
	cfg := config.Load()
	store, err := analytics.Open(cfg.ClickHouseDSN)
	if err != nil {
		log.Fatal(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers, Topic: cfg.KafkaTopic,
		GroupID: "payments-report", MinBytes: 1, MaxBytes: 10e6,
	})
	defer reader.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("consumer leyendo %s", cfg.KafkaTopic)
	if err := consumer.New(reader, store).Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/consumer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/analytics internal/consumer cmd/consumer/main.go
git commit -m "feat: consumidor de kafka con batch y backoff a clickhouse"
```

---

### Task 12: API del dashboard sobre ClickHouse

**Files:**
- Create: `internal/report/store.go`, `internal/report/store_test.go`, `internal/dashboard/handler.go`, `internal/dashboard/handler_test.go`, `cmd/dashboard/main.go`

**Interfaces:**
- Consumes: `config.Load()` (Task 1).
- Produces: `report.DayTotal`, `report.StatusCount`, `report.Store`, `report.Open(dsn string)`, `report.TotalByDaySQL`, `report.ByStatusSQL`; `dashboard.NewHandler(s dashboard.Querier) *Handler` con `GET /api/stats?total-by-day`, `GET /api/stats?by-status`, `GET /health`.

- [ ] **Step 1: Write the failing tests**

`internal/report/store_test.go`:

```go
package report

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestStoreUsaQueriesEsperadas(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}

	mock.ExpectQuery(regexp.QuoteMeta(TotalByDaySQL)).
		WillReturnRows(sqlmock.NewRows([]string{"day", "total"}).
			AddRow(time.Date(2027, 2, 16, 0, 0, 0, 0, time.UTC), int64(25000)))
	dias, err := s.TotalByDay(context.Background())
	if err != nil || len(dias) != 1 || dias[0].Day != "2027-02-16" || dias[0].Total != 25000 {
		t.Fatalf("total por día: %+v %v", dias, err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(ByStatusSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).AddRow("succeeded", 3))
	estados, err := s.ByStatus(context.Background())
	if err != nil || len(estados) != 1 || estados[0].Count != 3 {
		t.Fatalf("por estado: %+v %v", estados, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
```

`internal/dashboard/handler_test.go`:

```go
package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/patricioarmesto/0007-stripe/internal/report"
)

type fakeQuerier struct{}

func (fakeQuerier) TotalByDay(context.Context) ([]report.DayTotal, error) {
	return []report.DayTotal{{Day: "2027-02-16", Total: 25000}}, nil
}
func (fakeQuerier) ByStatus(context.Context) ([]report.StatusCount, error) {
	return []report.StatusCount{{Status: "succeeded", Count: 2}}, nil
}

func TestHandlerStats(t *testing.T) {
	h := NewHandler(fakeQuerier{})
	for _, path := range []string{"/api/stats?total-by-day", "/api/stats?by-status"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK || w.Body.Len() == 0 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/report ./internal/dashboard`
Expected: FAIL por símbolos indefinidos.

- [ ] **Step 3: Write minimal implementation**

`internal/report/store.go`:

```go
package report

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const TotalByDaySQL = `SELECT toDate(timestamp) AS day, sum(amount) AS total FROM stripe.payments_report WHERE status = 'succeeded' GROUP BY day ORDER BY day`
const ByStatusSQL = `SELECT status, count() AS count FROM stripe.payments_report GROUP BY status ORDER BY status`

type DayTotal struct {
	Day   string `json:"day"`
	Total int64  `json:"total"`
}

type StatusCount struct {
	Status string `json:"status"`
	Count  uint64 `json:"count"`
}

type Store struct{ db *sql.DB }

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) TotalByDay(ctx context.Context) ([]DayTotal, error) {
	rows, err := s.db.QueryContext(ctx, TotalByDaySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DayTotal{}
	for rows.Next() {
		var day time.Time
		var total int64
		if err := rows.Scan(&day, &total); err != nil {
			return nil, err
		}
		out = append(out, DayTotal{Day: day.Format("2006-01-02"), Total: total})
	}
	return out, rows.Err()
}

func (s *Store) ByStatus(ctx context.Context) ([]StatusCount, error) {
	rows, err := s.db.QueryContext(ctx, ByStatusSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatusCount{}
	for rows.Next() {
		var sc StatusCount
		if err := rows.Scan(&sc.Status, &sc.Count); err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}
```

`internal/dashboard/handler.go`:

```go
package dashboard

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/patricioarmesto/0007-stripe/internal/report"
)

type Querier interface {
	TotalByDay(ctx context.Context) ([]report.DayTotal, error)
	ByStatus(ctx context.Context) ([]report.StatusCount, error)
}

type Handler struct{ store Querier }

func NewHandler(s Querier) *Handler { return &Handler{store: s} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path != "/api/stats" {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	switch {
	case q.Has("total-by-day"):
		data, err := h.store.TotalByDay(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(data)
	case q.Has("by-status"):
		data, err := h.store.ByStatus(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(data)
	default:
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "usá ?total-by-day o ?by-status"})
	}
}
```

`cmd/dashboard/main.go`:

```go
// Binario de dashboard: solo lee ClickHouse. Nunca decide si un pago está cobrado.
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/dashboard"
	"github.com/patricioarmesto/0007-stripe/internal/report"
)

func main() {
	cfg := config.Load()
	store, err := report.Open(cfg.ClickHouseDSN)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           dashboard.NewHandler(store),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("dashboard escuchando en :%s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/report ./internal/dashboard`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/report internal/dashboard cmd/dashboard/main.go
git commit -m "feat: API de estadísticas desde clickhouse"
```

---

### Task 13: Frontend — scaffold, API client y checkout

**Files:**
- Create: `frontend/package.json`, `frontend/package-lock.json` (generado por `npm install`), `frontend/vite.config.ts`, `frontend/tsconfig.json`, `frontend/tailwind.config.js`, `frontend/postcss.config.js`, `frontend/index.html`, `frontend/src/main.tsx`, `frontend/src/index.css`, `frontend/src/api.ts`, `frontend/src/components/Checkout.tsx`, `frontend/src/App.tsx`

**Interfaces:**
- Consumes: endpoints de Task 7 y Task 10 a través de nginx.
- Produces: `createPayment`, `simulateWebhook`, `retryWebhook`, `listPayments`, `fetchStats`; componente `Checkout`.

- [ ] **Step 1: Write the failing test (build)**

No hay test unitario de UI: el ciclo de este task es el build de TypeScript, que falla si faltan archivos o hay tipos rotos.

Run: `cd frontend && npm run build`
Expected: FAIL por `package.json` inexistente.

- [ ] **Step 2: Write scaffold y clientes**

`frontend/package.json`:

```json
{
  "name": "mini-stripe-frontend",
  "private": true,
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "18.3.1",
    "react-dom": "18.3.1",
    "recharts": "2.13.3"
  },
  "devDependencies": {
    "@types/react": "18.3.12",
    "@types/react-dom": "18.3.1",
    "@vitejs/plugin-react": "4.3.3",
    "autoprefixer": "10.4.20",
    "postcss": "8.4.47",
    "tailwindcss": "3.4.14",
    "typescript": "5.6.3",
    "vite": "5.4.10"
  }
}
```

`frontend/vite.config.ts`:

```ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/payments': 'http://localhost:8081',
      '/api/webhooks': 'http://localhost:8082',
      '/api/stats': 'http://localhost:8083',
    },
  },
})
```

`frontend/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "resolveJsonModule": true,
    "allowImportingTsExtensions": true
  },
  "include": ["src"]
}
```

`frontend/tailwind.config.js`:

```js
/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: { extend: {} },
  plugins: [],
}
```

`frontend/postcss.config.js`:

```js
export default {
  plugins: { tailwindcss: {}, autoprefixer: {} },
}
```

`frontend/index.html`:

```html
<!doctype html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>¿Cómo funciona Stripe?</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`frontend/src/index.css`:

```css
@tailwind base;
@tailwind components;
@tailwind utilities;
```

`frontend/src/main.tsx`:

```tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
```

`frontend/src/api.ts`:

```ts
export type Payment = {
  id: string
  amount: number
  currency: string
  email: string
  status: string
  created_at: string
  attempts: number
}

export type CreateResult = { payment: Payment; created: boolean }

export type DayTotal = { day: string; total: number }
export type StatusCount = { status: string; count: number }

const jsonHeaders = { 'Content-Type': 'application/json' }

export async function createPayment(
  input: { amount: number; email: string },
  idempotencyKey: string = crypto.randomUUID(),
): Promise<CreateResult> {
  const res = await fetch('/api/payments', {
    method: 'POST',
    headers: { ...jsonHeaders, 'Idempotency-Key': idempotencyKey },
    body: JSON.stringify({
      amount: input.amount,
      currency: 'ARS',
      email: input.email,
      payment_method: 'card_test',
    }),
  })
  if (!res.ok) throw new Error(`POST /payments: ${res.status}`)
  return res.json()
}

export async function simulateWebhook(paymentId: string) {
  const res = await fetch('/api/webhooks/simulate', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify({ payment_id: paymentId, type: 'payment.succeeded' }),
  })
  if (!res.ok) throw new Error(`simulate: ${res.status}`)
  return res.json() as Promise<{ processed: boolean; event_id?: string }>
}

export async function retryWebhook(input: {
  payment_id: string
  event_id: string
  type: string
}) {
  const res = await fetch('/api/webhooks/retry', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify(input),
  })
  return { status: res.status, body: await res.json() }
}

export async function listPayments(): Promise<Payment[]> {
  const res = await fetch('/api/payments')
  if (!res.ok) throw new Error(`list: ${res.status}`)
  const data = await res.json()
  return data.payments
}

export async function fetchStats(): Promise<{
  dias: DayTotal[]
  estados: StatusCount[]
}> {
  const [dias, estados] = await Promise.all([
    fetch('/api/stats?total-by-day').then((r) => r.json()),
    fetch('/api/stats?by-status').then((r) => r.json()),
  ])
  return { dias, estados }
}
```

`frontend/src/components/Checkout.tsx`:

```tsx
import { useState } from 'react'
import { createPayment, simulateWebhook, type Payment } from '../api'

export default function Checkout({ onPago }: { onPago: (p: Payment) => void }) {
  const [amount, setAmount] = useState(12500)
  const [email, setEmail] = useState('ana@example.com')
  const [mensaje, setMensaje] = useState('')
  const [ultimo, setUltimo] = useState<Payment | null>(null)

  const pagar = async (doble: boolean) => {
    setMensaje('')
    try {
      const key = crypto.randomUUID()
      const requests = doble
        ? [createPayment({ amount, email }, key), createPayment({ amount, email }, key)]
        : [createPayment({ amount, email }, key)]
      const results = await Promise.all(requests)
      const ids = results.map((r) => r.payment.id)
      const mismoId = ids.every((id) => id === ids[0])
      setUltimo(results[0].payment)
      onPago(results[0].payment)
      setMensaje(
        doble
          ? `Dos requests, mismo Idempotency-Key, mismo id: ${mismoId ? 'sí ✅' : 'NO ❌'} (${ids[0]})`
          : `Pago ${results[0].created ? 'creado' : 'reutilizado'}: ${ids[0]}`,
      )
    } catch (e) {
      setMensaje(`Error: ${(e as Error).message}`)
    }
  }

  const confirmar = async () => {
    if (!ultimo) return setMensaje('Primero hacé un pago')
    try {
      const r = await simulateWebhook(ultimo.id)
      setMensaje(`Webhook del banco: ${r.event_id ?? ''} → processed=${r.processed}`)
      onPago(ultimo)
    } catch (e) {
      setMensaje(`Error: ${(e as Error).message}`)
    }
  }

  return (
    <section className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-xl font-semibold text-slate-800">Checkout demo</h2>
      <div className="grid gap-4 sm:grid-cols-2">
        <label className="text-sm text-slate-600">
          Monto (centavos ARS)
          <input
            className="mt-1 w-full rounded border border-slate-300 px-3 py-2"
            type="number"
            value={amount}
            onChange={(e) => setAmount(Number(e.target.value))}
          />
        </label>
        <label className="text-sm text-slate-600">
          Email
          <input
            className="mt-1 w-full rounded border border-slate-300 px-3 py-2"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
        </label>
      </div>
      <div className="mt-5 flex flex-wrap gap-3">
        <button className="rounded bg-indigo-600 px-4 py-2 text-white hover:bg-indigo-500" onClick={() => pagar(false)}>
          Pagar
        </button>
        <button className="rounded bg-amber-500 px-4 py-2 text-white hover:bg-amber-400" onClick={() => pagar(true)}>
          Doble click a propósito
        </button>
        <button className="rounded bg-emerald-600 px-4 py-2 text-white hover:bg-emerald-500" onClick={confirmar}>
          Simular webhook del banco
        </button>
      </div>
      {mensaje && <p className="mt-4 rounded bg-slate-50 p-3 text-sm text-slate-700">{mensaje}</p>}
      <p className="mt-3 text-xs text-slate-500">
        El pago se crea en <code>processing</code>. Recién se marca <code>succeeded</code> cuando llega el webhook firmado.
      </p>
    </section>
  )
}
```

`frontend/src/App.tsx` (versión inicial; Task 14 la extiende):

```tsx
import { useState } from 'react'
import Checkout from './components/Checkout'
import type { Payment } from './api'

export default function App() {
  const [pagos, setPagos] = useState<Payment[]>([])
  return (
    <main className="min-h-screen bg-slate-50 p-8">
      <div className="mx-auto max-w-5xl space-y-6">
        <header>
          <h1 className="text-3xl font-bold text-slate-900">¿Cómo funciona Stripe?</h1>
          <p className="text-slate-600">Mini Stripe didáctico · idempotencia, webhooks y reintentos</p>
        </header>
        <Checkout onPago={(p) => setPagos((actuales) => [p, ...actuales.filter((x) => x.id !== p.id)])} />
        <pre className="rounded bg-slate-900 p-4 text-xs text-emerald-300">{JSON.stringify(pagos, null, 2)}</pre>
      </div>
    </main>
  )
}
```

- [ ] **Step 3: Install deps y verificar build**

Run: `cd frontend && npm install && npm run build`
Expected: PASS, genera `frontend/dist/`.

- [ ] **Step 4: Commit**

```bash
git add frontend && git commit -m "feat: frontend con checkout idempotente"
```

---

### Task 14: Frontend — dashboard con Recharts y sección "Cómo romperlo"

**Files:**
- Modify: `frontend/src/App.tsx`
- Create: `frontend/src/components/Dashboard.tsx`, `frontend/src/components/BreakIt.tsx`

**Interfaces:**
- Consumes: `fetchStats`, `listPayments`, `retryWebhook` de `api.ts` (Task 13).
- Produces: componentes `Dashboard` y `BreakIt`; `App` con pestañas Checkout/Dashboard.

- [ ] **Step 1: Write the failing test (build)**

Primero agregá los imports de `Dashboard` y `BreakIt` al `App.tsx` actual (paso 4) y corré el build antes de crear los componentes.

Run: `cd frontend && npm run build`
Expected: FAIL con `Cannot find module './components/Dashboard'`.

- [ ] **Step 2: Create Dashboard.tsx**

```tsx
import { useEffect, useState } from 'react'
import {
  Bar, BarChart, CartesianGrid, Pie, PieChart, ResponsiveContainer,
  Tooltip, XAxis, YAxis, Cell, Legend,
} from 'recharts'
import { fetchStats, listPayments, type DayTotal, type Payment, type StatusCount } from '../api'

const COLORES: Record<string, string> = {
  succeeded: '#10b981',
  processing: '#f59e0b',
  created: '#6366f1',
  failed: '#ef4444',
}

export default function Dashboard() {
  const [dias, setDias] = useState<DayTotal[]>([])
  const [estados, setEstados] = useState<StatusCount[]>([])
  const [pagos, setPagos] = useState<Payment[]>([])

  const cargar = () => {
    fetchStats().then(({ dias, estados }) => {
      setDias(dias)
      setEstados(estados)
    }).catch(() => {})
    listPayments().then(setPagos).catch(() => {})
  }

  useEffect(() => {
    cargar()
    const id = setInterval(cargar, 3000)
    return () => clearInterval(id)
  }, [])

  return (
    <section className="space-y-6">
      <h2 className="text-xl font-semibold text-slate-800">Dashboard</h2>
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <h3 className="mb-3 text-sm font-medium text-slate-600">Total cobrado por día (ARS)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={dias}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="day" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="total" fill="#4f46e5" />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <h3 className="mb-3 text-sm font-medium text-slate-600">Pagos por estado</h3>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={estados} dataKey="count" nameKey="status" outerRadius={70} label>
                {estados.map((e) => <Cell key={e.status} fill={COLORES[e.status] ?? '#94a3b8'} />)}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white">
        <table className="w-full text-left text-sm">
          <thead className="bg-slate-100 text-slate-600">
            <tr>
              <th className="p-3">ID</th><th className="p-3">Email</th>
              <th className="p-3">Monto</th><th className="p-3">Estado</th>
              <th className="p-3">Reintentos</th>
            </tr>
          </thead>
          <tbody>
            {pagos.map((p) => (
              <tr key={p.id} className="border-t border-slate-100">
                <td className="p-3 font-mono text-xs">{p.id.slice(0, 8)}</td>
                <td className="p-3">{p.email}</td>
                <td className="p-3">${(p.amount / 100).toFixed(2)}</td>
                <td className="p-3">
                  <span className="rounded bg-slate-100 px-2 py-1 text-xs">{p.status}</span>
                </td>
                <td className="p-3">{p.attempts}</td>
              </tr>
            ))}
            {pagos.length === 0 && <tr><td className="p-4 text-slate-500" colSpan={5}>Todavía no hay pagos.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  )
}
```

- [ ] **Step 3: Create BreakIt.tsx**

```tsx
import { useState } from 'react'
import { retryWebhook } from '../api'

export default function BreakIt() {
  const [paymentId, setPaymentId] = useState('pay_123')
  const [eventId, setEventId] = useState('evt_456')
  const [resultado, setResultado] = useState('')

  const probeFirma = async () => {
    const res = await fetch('/api/webhooks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Signature': 'sha256=malo' },
      body: JSON.stringify({
        event_id: 'evt_falso', type: 'payment.succeeded', payment_id: paymentId,
      }),
    })
    setResultado(`Firma inválida → HTTP ${res.status} (esperábamos 401)`)
  }

  const probeRetry = async () => {
    const r = await retryWebhook({
      payment_id: paymentId, event_id: eventId, type: 'payment.succeeded',
    })
    setResultado(`Retry → HTTP ${r.status} ${JSON.stringify(r.body)}`)
  }

  return (
    <section className="rounded-xl border border-dashed border-amber-400 bg-amber-50 p-6">
      <h2 className="text-xl font-semibold text-amber-900">Cómo romperlo</h2>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-amber-900">
        <li><strong>Doble click:</strong> dos requests con el mismo Idempotency-Key vuelven con el mismo id; Postgres tiene una sola fila.</li>
        <li><strong>Firma inválida:</strong> un POST a <code>/webhooks</code> con otra firma da 401 y el pago no cambia.</li>
        <li><strong>Webhook duplicado:</strong> el tercer intento con el mismo <code>event_id</code> devuelve <code>already_processed</code> y no publica de nuevo.</li>
      </ul>
      <div className="mt-4 flex flex-wrap items-end gap-3">
        <label className="text-xs text-amber-900">payment_id
          <input className="mt-1 block rounded border border-amber-300 px-2 py-1" value={paymentId} onChange={(e) => setPaymentId(e.target.value)} />
        </label>
        <label className="text-xs text-amber-900">event_id
          <input className="mt-1 block rounded border border-amber-300 px-2 py-1" value={eventId} onChange={(e) => setEventId(e.target.value)} />
        </label>
        <button className="rounded bg-rose-600 px-3 py-2 text-sm text-white" onClick={probeFirma}>Probar firma inválida</button>
        <button className="rounded bg-indigo-600 px-3 py-2 text-sm text-white" onClick={probeRetry}>Probar reintento</button>
      </div>
      {resultado && <p className="mt-3 text-sm font-medium text-amber-900">{resultado}</p>}
      <p className="mt-3 text-xs text-amber-800">
        Copiá el <code>payment_id</code> de la tabla y usá siempre el mismo <code>event_id</code>: si lo cambiás, no estás probando idempotencia.
      </p>
    </section>
  )
}
```

- [ ] **Step 4: Replace App.tsx**

```tsx
import { useState } from 'react'
import Checkout from './components/Checkout'
import Dashboard from './components/Dashboard'
import BreakIt from './components/BreakIt'
import type { Payment } from './api'

export default function App() {
  const [tab, setTab] = useState<'checkout' | 'dashboard'>('checkout')
  const [ultimo, setUltimo] = useState<Payment | null>(null)

  return (
    <main className="min-h-screen bg-slate-50 p-6 sm:p-10">
      <div className="mx-auto max-w-5xl space-y-6">
        <header className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">¿Cómo funciona Stripe?</h1>
            <p className="text-slate-600">Idempotencia, webhooks y reintentos · nivel avanzado</p>
          </div>
          <nav className="flex gap-2">
            <button
              className={`rounded px-4 py-2 text-sm ${tab === 'checkout' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700'}`}
              onClick={() => setTab('checkout')}
            >Checkout</button>
            <button
              className={`rounded px-4 py-2 text-sm ${tab === 'dashboard' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700'}`}
              onClick={() => setTab('dashboard')}
            >Dashboard</button>
          </nav>
        </header>

        {tab === 'checkout' ? (
          <Checkout onPago={setUltimo} />
        ) : (
          <Dashboard />
        )}
        <BreakIt />
        {ultimo && (
          <p className="text-xs text-slate-500">
            Último pago: <code>{ultimo.id}</code> · estado <code>{ultimo.status}</code>
          </p>
        )}
      </div>
    </main>
  )
}
```

- [ ] **Step 5: Verify build**

Run: `cd frontend && npm run build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src && git commit -m "feat: dashboard con recharts y sección cómo romperlo"
```

---

### Task 15: Documentación estándar, ADRs y verificación end-to-end

**Files:**
- Create: `README.md`, `docs/adr/ADR-001-idempotencia-postgres.md`, `docs/adr/ADR-002-maquina-de-estados.md`, `docs/adr/ADR-003-webhook-firma-idempotencia.md`, `docs/adr/ADR-004-fuera-de-alcance.md`, `scripts/smoke.sh`
- Modify: `internal/webhook/handler.go` (agregar `event_id` a la respuesta de `/webhooks/simulate`).

**Interfaces:**
- Consumes: todo lo anterior.
- Produces: documentación del curso y un smoke test ejecutable.

- [ ] **Step 1: Agregar event_id a la respuesta de simulate**

En `internal/webhook/handler.go`, reemplazar el final de `simulate`:

```go
	code, resp := h.process(r.Context(), body, Sign(body, h.secret))
	m, _ := resp.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	m["event_id"] = ev.EventID
	writeJSON(w, code, m)
```

- [ ] **Step 2: Write README.md**

```markdown
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

## Arquitectura en texto

- `payments` (Go, :8081 interno): crea pagos, aplica el lock de Redis y guarda
  la verdad en Postgres. Expone `/payments`, `/payments/:id`, `/health`.
- `webhook` (Go, :8082 interno): verifica HMAC, graba `webhook_events` y cambia
  el estado en una transacción. Expone `/webhooks`, `/webhooks/simulate`,
  `/webhooks/retry`, `/health`.
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
3. En **Cómo romperlo**, probá firma inválida (401) y reintento: el primer
   intento falla, el segundo procesa, el tercero devuelve `already_processed`.
4. Smoke test desde la raíz: `bash scripts/smoke.sh`.
5. Tests: `go test ./...` y `cd frontend && npm run build`.
```

- [ ] **Step 3: Write ADRs**

`docs/adr/ADR-001-idempotencia-postgres.md`:

```markdown
# ADR-001: Idempotencia con UNIQUE en Postgres

Estado: aceptado.

## Contexto

El mismo usuario puede hacer doble click, o el navegador puede reenviar el
request después de un timeout. Un lock en memoria no alcanza: si el proceso se
reinicia, se pierde; si hay varias réplicas, cada una tiene su memoria.

## Decisión

`payments.idempotency_key` es `UNIQUE`. El servicio busca por esa clave antes de
insertar. Redis aporta un lock `SET NX PX 10000` solo para ordenar los requests
que llegan en paralelo.

## Consecuencias

Un conflicto de `UNIQUE` se traduce en "devolvé el pago existente", no en error.
La idempotencia sobrevive a reinicios y a múltiples réplicas. Redis puede caerse:
el `UNIQUE` sigue protegiendo el dato.
```

`docs/adr/ADR-002-maquina-de-estados.md`:

```markdown
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
```

`docs/adr/ADR-003-webhook-firma-idempotencia.md`:

```markdown
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
```

`docs/adr/ADR-004-fuera-de-alcance.md`:

```markdown
# ADR-004: Qué queda fuera

Estado: aceptado.

## Contexto

Un procesador de pagos real tiene muchos subsistemas de cumplimiento, riesgo y
operaciones. Este repo es didáctico y corre en localhost.

## Decisión

Quedan **fuera de alcance a propósito**: PCI real, tarjetas reales, fraude con
ML, multi-moneda, payouts, Transactional Outbox, DLT, Exactly-Once, Schema
Registry, manejo fino de rebalancing y monitoreo de producción.

## Consecuencias

No se promete entrega exacta de eventos: si Kafka falla, se loguea y el dashboard
puede atrasarse. Postgres sigue siendo la única fuente de verdad del dinero.
Este límite está documentado para no confundir la demo con un sistema real.
```

- [ ] **Step 4: Write smoke test**

`scripts/smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
BASE=http://localhost:3000

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

echo "health payments: $(code $BASE/api/payments/health)"
echo "health dashboard: $(code $BASE/api/stats?by-status)"

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
  -d '{"event_id":"evt_smoke","type":"payment.succeeded","payment_id":"'"$ID"'"}')
echo "firma invalida: $BAD (esperado 401)"

SIM=$(curl -s -X POST $BASE/api/webhooks/simulate -H 'Content-Type: application/json' \
  -d '{"payment_id":"'"$ID"'","type":"payment.succeeded"}')
echo "simulate: $SIM"
EVT=$(echo "$SIM" | sed -n 's/.*"event_id":"\([^"]*\)".*/\1/p')

RETRY='{"payment_id":"'"$ID"'","event_id":"'"$EVT"'","type":"payment.succeeded"}'
echo "retry 1: $(curl -s -X POST $BASE/api/webhooks/retry -H 'Content-Type: application/json' -d "$RETRY")"
echo "retry 2: $(curl -s -X POST $BASE/api/webhooks/retry -H 'Content-Type: application/json' -d "$RETRY")"

sleep 2
echo "detalle: $(curl -s $BASE/api/payments/$ID)"
echo "stats dia: $(curl -s "$BASE/api/stats?total-by-day")"
echo "stats estado: $(curl -s "$BASE/api/stats?by-status")"
```

- [ ] **Step 5: Full verification**

Run: `go test ./... && cd frontend && npm run build`
Expected: PASS.

Run: `docker compose up --build -d && sleep 20 && bash scripts/smoke.sh`
Expected: health 200; mismo id en los dos pagos; firma inválida 401; primer retry con `processed: true, attempt: 2`; segundo retry con `already_processed`; detalle en `succeeded`; stats con el pago cobrado.

Run: `docker compose logs webhook | grep 'intento 1'`
Expected: aparece `intento 1 para evt_... falló`.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/adr scripts internal/webhook/handler.go
git commit -m "docs: README, ADRs y smoke test end-to-end"
```

---

## Self-review

Antes de ejecutar: verificar contra la spec que cada sección tiene tarea (idempotencia, máquina de estados, webhook, retry, consumer, dashboard, frontend, docs), que no queden `TBD`/`TODO`, y que los nombres de interfaces coincidan entre tareas. Los cinco casos de Review Focus tienen test asignado en las tareas 6, 8, 9, 10, 11 y 15.

