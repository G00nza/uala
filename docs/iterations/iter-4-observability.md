# Iter 4 — Observabilidad (Prometheus + Grafana)

## Objetivo

Instrumentar el sistema para tener visibilidad sobre rendimiento, salud de la infraestructura y comportamiento del cache.

## Tareas

- Agregar Prometheus + Grafana a `docker-compose.yml`
- Exponer endpoint `/metrics` en el servidor HTTP
- Instrumentar métricas HTTP, Redis, PostgreSQL y RabbitMQ
- Configurar dashboard en Grafana

## Métricas

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `http_request_duration_seconds` | Histogram | Latencia por endpoint y status code |
| `http_requests_total` | Counter | Requests por endpoint |
| `timeline_cache_hits_total` | Counter | Timelines servidos desde Redis |
| `timeline_cache_misses_total` | Counter | Caídas al fallback de PostgreSQL |
| `fanout_duration_seconds` | Histogram | Tiempo del consumer en hacer fanout |
| `rabbitmq_messages_processed_total` | Counter | Mensajes procesados por consumer |
| `rabbitmq_messages_failed_total` | Counter | Mensajes fallidos (para calcular success rate) |
| `rabbitmq_queue_depth` | Gauge | Mensajes acumulados en la queue (backpressure) |
| `db_query_duration_seconds` | Histogram | Latencia de queries por operación |
| `db_connections_active` | Gauge | Conexiones activas al pool de PostgreSQL |
| `db_errors_total` | Counter | Errores de base de datos por tipo |

## Dashboard Grafana

- Latencia p50/p95/p99 por endpoint
- Cache hit rate del timeline
- Queue depth de RabbitMQ
- RabbitMQ success rate (processed / processed + failed)
