# Iter 2 — Read layer (Redis)

## Objetivo

Mover las lecturas de timeline a Redis para cumplir con el requerimiento de optimización para lecturas.

## Tareas

- Agregar Redis a `docker-compose.yml`
- Definir política de eviction (`REDIS_EVICTION_POLICY`: LRU/TTL/FIFO)
- Al publicar un tweet: fanout del tweet completo al Sorted Set de cada seguidor
- `GET /timeline` lee de Redis con `ZREVRANGE`
- Fallback a PostgreSQL cuando no existe la key en Redis; el resultado se escribe en Redis (lazy population)

## Estructura en Redis

```
timeline:{user_id}  →  Sorted Set
                        score:  unix timestamp del tweet (ej: 1745755200)
                        member: JSON del tweet
```

El JSON almacenado como member es el mismo shape que devuelve `GET /timeline`:

```json
{
  "id": "661f9511-f3ac-52e5-b827-557766551111",
  "user_id": "772g0622-g4bd-63f6-c938-668877662222",
  "username": "gonzalo",
  "content": "Hola mundo",
  "created_at": "2026-04-27T12:00:00Z"
}
```

`ZREVRANGE timeline:{user_id} 0 N` devuelve los N tweets más recientes en una sola operación, listos para serializar directamente en el response.

## Trade-offs

Ver [`docs/tradeoffs/redis-timeline-storage.md`](../tradeoffs/redis-timeline-storage.md) para la comparación entre guardar el tweet completo vs IDs + `MGET`.
