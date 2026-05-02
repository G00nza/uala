# Arquitectura

## Diagrama

![Arquitectura](https://cdn.jsdelivr.net/gh/G00nza/uala@44ae955/docs/architecture.svg)

## Capas

El proyecto sigue Clean Architecture. La dependencia siempre fluye hacia adentro: `handler → usecase → domain`. Ninguna capa interna conoce las capas externas.

```
internal/
├── domain/          ← entidades y errores de negocio (sin dependencias externas)
├── usecase/         ← casos de uso concretos; orquestan domain + repositorios
├── handler/         ← HTTP: parseo de request, delegación al use case, serialización
├── repository/
│   ├── postgres/    ← implementación SQL de los repositorios
│   └── redis/       ← cache del timeline con fallback a PostgreSQL
├── messaging/
│   └── rabbitmq/    ← publisher y consumer de eventos de dominio
├── metrics/         ← definición y middleware de métricas Prometheus
└── infra/           ← carga de configuración desde variables de entorno
```

## Componentes de infraestructura

| Componente | Rol |
|---|---|
| **PostgreSQL** | Fuente de verdad. Almacena usuarios, tweets y follows. También mantiene `last_active` por usuario para el fanout selectivo. |
| **Redis** | Cache del timeline por usuario. Dos estructuras por usuario: un Sorted Set con IDs ordenados por timestamp y un Hash con el JSON de cada tweet. Las keys expiran naturalmente cuando el usuario pasa a inactivo. |
| **RabbitMQ** | Desacopla la escritura del tweet del fanout a seguidores. El producer publica y responde 201 inmediatamente; el consumer procesa el fanout de forma asíncrona. |
| **Prometheus + Grafana** | Observabilidad: latencia HTTP, cache hit rate, profundidad de queues. Ver [Observabilidad](Observabilidad). |

## Flujos principales

### Publicar tweet

1. `POST /tweets` → el use case persiste el tweet en PostgreSQL → publica `TweetCreatedEvent` en RabbitMQ → responde 201.
2. El consumer recibe el evento → consulta `GetActiveFollowers` (JOIN con `users.last_active`) → hace **fanout**: escribe el tweet en el timeline Redis de cada seguidor activo.
3. El TTL de cada key Redis se calcula como `ACTIVITY_TTL - time.Since(follower.LastActive)`, de modo que las keys de usuarios inactivos expiran antes.

### Seguir usuario

1. `POST /follow` → el use case persiste la relación en PostgreSQL → publica `FollowCreatedEvent` en RabbitMQ → responde 201.
2. El consumer recibe el evento → recupera los últimos `FOLLOW_BACKFILL_LIMIT` tweets del autor → los escribe en el timeline Redis del nuevo seguidor (**backfill**).

### Leer timeline

1. `GET /timeline` → el use case consulta Redis. **Cache hit**: devuelve en O(1). **Cache miss**: fallback a PostgreSQL, popula Redis para lecturas futuras.
2. Soporta paginación cursor-based con los parámetros `?after=<tweet_id>` y `?before=<tweet_id>`. La respuesta incluye `next_cursor` y `prev_cursor`.
3. Cada lectura publica un `UserActivityEvent` de forma asíncrona. El consumer actualiza `users.last_active` en PostgreSQL (guarda monótona: solo actualiza si el timestamp es más nuevo).

### Crear usuario

1. `POST /users` → el use case persiste el usuario en PostgreSQL.

## Fanout selectivo (celebrity problem)

El fanout no escribe en el timeline de **todos** los seguidores, sino solo en los que estuvieron activos en la última ventana de `ACTIVITY_TTL` (default 24h). Un seguidor inactivo no recibe el push; cuando vuelva a leer su timeline, el cache miss hace fallback a PostgreSQL.

Esto resuelve el problema de los usuarios con millones de seguidores: en lugar de 10M escrituras Redis por tweet, el fanout opera solo sobre los seguidores que realmente van a leer.

## Fanout retry y DLQ

Si una escritura a Redis falla durante el fanout, el consumer publica un `FanoutRetryEvent` en la queue `fanout.retry`. El mensaje espera 30 segundos en `fanout.wait` (dead-letter exchange) antes de volver a `fanout.retry`. Después de 10 reintentos sin éxito, el mensaje pasa a `fanout.dead` para inspección manual.

## Redis: estructura del timeline

Cada usuario tiene dos keys:

```
timeline:{user_id}       →  Sorted Set  (score: unix timestamp, member: tweet_id)
timeline:data:{user_id}  →  Hash        (field: tweet_id, value: JSON del tweet)
```

La lectura combina `ZREVRANGE` para obtener los IDs ordenados y `HMGET` para el contenido. Esta separación permite que el fanout use `HSetNX` (idempotente): si el tweet ya existe en el hash del usuario, no se reescribe el payload completo.
