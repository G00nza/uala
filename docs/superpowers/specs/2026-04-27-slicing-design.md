# Slicing Design — Ualá Twitter-like Platform

## Contexto

Plataforma de microblogging similar a Twitter. Los requerimientos base son: publicar tweets (máx 280 chars), seguir usuarios y ver un timeline de tweets de los usuarios seguidos.

El sistema debe escalar a millones de usuarios y estar optimizado para lecturas.

---

## Arquitectura interna (Clean Architecture)

```
uala/
├── cmd/
│   └── api/          # main.go, wiring de dependencias
├── internal/
│   ├── domain/       # entidades e interfaces (puertos)
│   ├── usecase/      # lógica de negocio
│   ├── handler/      # HTTP handlers
│   ├── repository/   # implementaciones de persistencia (postgres, redis)
│   └── infra/        # clientes externos (rabbitmq, prometheus)
├── migrations/       # archivos SQL
├── docker-compose.yml
└── Makefile
```

**Flujo por capa:**

```
Handler → UseCase → Repository (PostgreSQL / Redis)
                  → Infra (RabbitMQ publisher)
```

El `domain` define interfaces (puertos). `repository` e `infra` son adaptadores. El `usecase` solo depende de interfaces, nunca de implementaciones concretas.

---

## Modelos de datos

### PostgreSQL

```sql
users   (id UUID PK, username TEXT UNIQUE, created_at TIMESTAMPTZ)
tweets  (id UUID PK, user_id UUID FK, content TEXT, created_at TIMESTAMPTZ)
follows (follower_id UUID FK, followee_id UUID FK, created_at TIMESTAMPTZ, PK (follower_id, followee_id))
```

### Redis (Iter 2 en adelante)

```
timeline:{user_id}  →  Sorted Set
                        score:  unix timestamp del tweet
                        member: JSON completo del tweet
```

`GET /timeline` ejecuta `ZREVRANGE timeline:{user_id} 0 N` y devuelve los N tweets más recientes en una sola operación.

El tweet completo se guarda como member para evitar lookups secundarios. Trade-off documentado en [`docs/tradeoffs/redis-timeline-storage.md`](../../tradeoffs/redis-timeline-storage.md): cuando la memoria de Redis sea un constraint, migrar a IDs + `MGET`.

---

## Iteraciones

| # | Nombre | Detalle |
|---|--------|---------|
| 0 | Infrastructure base | [`iter-0-infrastructure.md`](../../iterations/iter-0-infrastructure.md) |
| 1 | Core features | [`iter-1-core-features.md`](../../iterations/iter-1-core-features.md) |
| 2 | Read layer (Redis) | [`iter-2-redis.md`](../../iterations/iter-2-redis.md) |
| 3 | Async messaging (RabbitMQ) | [`iter-3-rabbitmq.md`](../../iterations/iter-3-rabbitmq.md) |
| 4 | Observabilidad | [`iter-4-observability.md`](../../iterations/iter-4-observability.md) |

---

## Backlog (nth)

- `DELETE /follow`: requiere limpiar el timeline de Redis del follower, añade complejidad al fanout. Pospuesto hasta tener las iteraciones de mensajería estables.
- Migrar Redis de tweet completo a IDs + `MGET` si la memoria se vuelve un constraint a escala.

---

## Variables de configuración

| Variable | Descripción |
|----------|-------------|
| `FOLLOW_BACKFILL_LIMIT` | Cantidad de tweets a backfillear al hacer follow |
| `TIMELINE_PAGE_SIZE` | Cantidad de tweets a devolver en GET /timeline |
| `REDIS_EVICTION_POLICY` | Política de eviction de Redis (LRU/TTL/FIFO) |
