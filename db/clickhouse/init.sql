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
