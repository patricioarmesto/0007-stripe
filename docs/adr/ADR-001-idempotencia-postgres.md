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
