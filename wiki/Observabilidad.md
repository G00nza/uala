# Observabilidad

## Stack

| Herramienta | Puerto | Descripción |
|---|---|---|
| Prometheus | `9090` | Scraping y almacenamiento de métricas |
| Grafana | `3000` | Dashboards. Credenciales: `admin` / `admin` |

Ambos levantan automáticamente con `make up`. Grafana tiene un dashboard pre-provisionado que no requiere configuración manual.

## Endpoint de métricas

```
GET http://localhost:8080/metrics
```

Expone todas las métricas en formato Prometheus. Prometheus hace scraping cada 15 segundos (configurado en `monitoring/prometheus/prometheus.yml`).

## Métricas

### HTTP

| Métrica | Tipo | Descripción |
|---|---|---|
| `http_request_duration_seconds` | Histogram | Latencia por endpoint y status code |
| `http_requests_total` | Counter | Requests totales por endpoint |

### Timeline / Redis

| Métrica | Tipo | Descripción |
|---|---|---|
| `timeline_cache_hits_total` | Counter | Timelines servidos directamente desde Redis |
| `timeline_cache_misses_total` | Counter | Caídas al fallback de PostgreSQL |

El ratio `hits / (hits + misses)` indica la efectividad del cache.

### RabbitMQ

| Métrica | Tipo | Descripción |
|---|---|---|
| `fanout_duration_seconds` | Histogram | Tiempo del consumer en procesar un evento de fanout |
| `rabbitmq_messages_processed_total` | Counter | Mensajes procesados exitosamente por consumer |
| `rabbitmq_messages_failed_total` | Counter | Mensajes que fallaron (para calcular success rate) |
| `rabbitmq_queue_depth` | Gauge | Mensajes acumulados en la queue (indicador de backpressure) |

### PostgreSQL

| Métrica | Tipo | Descripción |
|---|---|---|
| `db_query_duration_seconds` | Histogram | Latencia de queries por operación |
| `db_connections_active` | Gauge | Conexiones activas al pool de pgx |
| `db_errors_total` | Counter | Errores de base de datos por tipo |

## Alertas recomendadas

### 1. Tasa de errores HTTP elevada

Detecta cuando más del 1% de los requests están fallando con 5xx en los últimos 5 minutos. Indica un problema general del servicio.

```promql
sum(rate(http_requests_total{status=~"5.."}[5m]))
/
sum(rate(http_requests_total[5m])) > 0.01
```

---

### 2. Latencia p99 fuera de umbral

Detecta cuando el percentil 99 de latencia supera 500ms. Al ser un servicio optimizado para lectura, una degradación sostenida en `GET /timeline` es señal de que el cache está frío o hay presión en PostgreSQL.

```promql
histogram_quantile(0.99, sum by (le, endpoint) (
  rate(http_request_duration_seconds_bucket[5m])
)) > 0.5
```

---

### 3. Cache hit rate por debajo del umbral

Detecta cuando menos del 99% de los timelines se están sirviendo desde Redis. Un drop sostenido indica que el cache se está invalidando, Redis está siendo reiniciado, o el fanout está fallando silenciosamente.

```promql
rate(timeline_cache_hits_total[5m])
/
(rate(timeline_cache_hits_total[5m]) + rate(timeline_cache_misses_total[5m])) < 0.99
```

---

### 4. Queue depth de RabbitMQ en crecimiento

Detecta cuando hay más de 100 mensajes acumulados en alguna queue. Significa que el consumer no está procesando al ritmo de producción (backpressure): los timelines de los seguidores se están actualizando con retraso.

```promql
rabbitmq_queue_depth > 100
```

---

## Dashboard Grafana

El dashboard pre-provisionado incluye:

- **Latencia HTTP**: p50, p95, p99 por endpoint
- **Cache hit rate**: ratio de hits vs misses del timeline en Redis
- **RabbitMQ queue depth**: profundidad de la queue en tiempo real
- **RabbitMQ success rate**: `processed / (processed + failed)`
