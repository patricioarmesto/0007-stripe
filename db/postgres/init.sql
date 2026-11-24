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
