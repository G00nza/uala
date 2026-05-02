# Ualá — Plataforma de Microblogging

Implementación de una plataforma similar a Twitter: publicación de tweets, follows y timeline personalizado. Kata técnica desarrollada en Go con arquitectura limpia.

---

## Requisitos previos

- [Go 1.25+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/) y Docker Compose
- `make`

---

## Setup rápido

```bash
# 1. Copiar variables de entorno
cp .env.example .env

# 2. Levantar la infraestructura y la aplicación
make up
```

Para reiniciar solo la aplicación sin tocar la infraestructura:

```bash
make run
```

La API queda disponible en `http://localhost:8080`.

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `DATABASE_URL` | `postgres://uala:uala@localhost:5432/uala` | Conexión a PostgreSQL |
| `REDIS_URL` | `redis://localhost:6379/0` | Conexión a Redis |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | Conexión a RabbitMQ |
| `PORT` | `8080` | Puerto HTTP de la aplicación |
| `TIMELINE_LIMIT` | `500` | Cantidad máxima de tweets almacenados por usuario en Redis |
| `FOLLOW_BACKFILL_LIMIT` | `20` | Cantidad máxima de tweets a backfillear al seguir un usuario nuevo |
| `ACTIVITY_TTL` | `24h` | Ventana de actividad de usuario; determina qué followers reciben fanout y el TTL de las keys de Redis |

---

## Servicios de infraestructura

| Servicio | Puerto(s) | Descripción |
|---|---|---|
| PostgreSQL | `5432` | Base de datos principal |
| Redis | `6379` | Cache del timeline |
| RabbitMQ | `5672` / `15672` | Message broker (fanout async). UI en `http://localhost:15672` (guest/guest) |
| Prometheus | `9090` | Recolección de métricas |
| Grafana | `3000` | Dashboards. UI en `http://localhost:3000` (admin/admin) |

---

## API

El contrato completo está en [`docs/api/openapi.yaml`](docs/api/openapi.yaml). Para explorarlo visualmente, importarlo en [Swagger Editor](https://editor.swagger.io) o usar la extensión REST Client del IDE.

### Autenticación

No hay sesiones. El usuario se identifica con el header `X-User-ID` (UUID) en todos los endpoints que lo requieren.

### Endpoints

#### Crear usuario
```bash
curl -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"username": "gonzalo"}'
# → 201 {"id": "<uuid>"}
```

#### Publicar tweet
```bash
curl -X POST http://localhost:8080/tweets \
  -H "Content-Type: application/json" \
  -H "X-User-ID: <uuid>" \
  -d '{"content": "Hola mundo"}'
# → 201 {"id": "<uuid>"}
```

#### Seguir a un usuario
```bash
curl -X POST http://localhost:8080/follow \
  -H "Content-Type: application/json" \
  -H "X-User-ID: <uuid-follower>" \
  -d '{"followee_id": "<uuid-followee>"}'
# → 201 {}
```

#### Ver timeline
```bash
curl http://localhost:8080/timeline \
  -H "X-User-ID: <uuid>"
# → 200 {"tweets": [...]}
```

---

## Tests

```bash
# Tests unitarios
make test

# Tests de integración (requiere infraestructura levantada)
INTEGRATION=1 go test ./...
```

