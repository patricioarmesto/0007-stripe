CREATE DATABASE IF NOT EXISTS stripe;

-- La imagen oficial deja al usuario `default` solo para localhost. Los
-- binarios se conectan por la red de compose, así que necesitan su propio user.
CREATE USER IF NOT EXISTS stripe IDENTIFIED WITH plaintext_password BY 'stripe';
GRANT ALL ON stripe.* TO stripe;

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
