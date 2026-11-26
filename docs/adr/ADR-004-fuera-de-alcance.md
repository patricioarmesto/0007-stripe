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
