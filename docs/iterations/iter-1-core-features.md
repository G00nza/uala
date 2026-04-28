# Iter 1 — Core features

## Objetivo

Sistema funcional de punta a punta con los tres features requeridos por las consignas, persistiendo en PostgreSQL.

## Contratos HTTP

Ver [`docs/api/openapi.yaml`](../api/openapi.yaml)

## Tareas

- Migrations: tablas `users`, `tweets`, `follows`
- `POST /users` — crear usuario (sin auth ni sesiones)
- `POST /tweets` — publicar tweet, validar máx 280 chars, user ID por header `X-User-ID`
- `POST /follow` — seguir a un usuario
- `GET /timeline` — tweets de usuarios seguidos, ordenados por fecha desc, desde PostgreSQL
