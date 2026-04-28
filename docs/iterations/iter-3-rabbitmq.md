# Iter 3 — Async messaging (RabbitMQ)

## Objetivo

Desacoplar el fanout a Redis del request HTTP. El cliente no espera el fanout, que pasa a ser asíncrono con consistencia eventual.

## Tareas

- Agregar RabbitMQ a `docker-compose.yml`
- `POST /tweets` → guarda en PostgreSQL → publica evento a queue → responde 201
- Consumer: recibe evento de tweet → busca seguidores → hace fanout a Redis
- `POST /follow` → guarda en PostgreSQL → publica evento a queue → responde 201
- Consumer: recibe evento de follow → busca últimos `FOLLOW_BACKFILL_LIMIT` tweets del followee → los agrega al timeline del follower en Redis

## Flujo de tweet

```
POST /tweets
    → guarda tweet en PostgreSQL
    → publica evento a RabbitMQ
    → responde 201 al cliente

Consumer
    → recibe evento
    → busca seguidores del user_id en PostgreSQL
    → hace fanout: agrega tweet al Sorted Set de cada seguidor en Redis
```

**Payload del mensaje:**

```json
{
  "tweet_id":   "661f9511-f3ac-52e5-b827-557766551111",
  "user_id":    "772g0622-g4bd-63f6-c938-668877662222",
  "username":   "gonzalo",
  "content":    "Hola mundo",
  "created_at": "2026-04-27T12:00:00Z"
}
```

El consumer usa directamente este payload para construir el member del Sorted Set en Redis, sin necesidad de consultar PostgreSQL por el contenido del tweet.

## Flujo de follow

```
POST /follow
    → guarda follow en PostgreSQL
    → publica evento a RabbitMQ
    → responde 201 al cliente

Consumer
    → recibe evento
    → busca últimos FOLLOW_BACKFILL_LIMIT tweets del followee en PostgreSQL
    → los agrega al Sorted Set del follower en Redis
```

**Payload del mensaje:**

```json
{
  "follower_id": "550e8400-e29b-41d4-a716-446655440000",
  "followee_id": "772g0622-g4bd-63f6-c938-668877662222"
}
```

## Consideraciones

- El timeline tiene **consistencia eventual**: un seguidor puede ver el tweet con algunos ms/s de delay.
- `FOLLOW_BACKFILL_LIMIT` es una constante configurable vía variable de entorno.
