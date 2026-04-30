# Arquitectura

## Diagrama

![Arquitectura](https://raw.githubusercontent.com/USER/REPO/master/docs/architecture.svg)

> Reemplazar `USER/REPO` por el path real del repositorio en GitHub.

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

## Flujos principales

### Publicar tweet

1. El handler recibe `POST /tweets` y delega al `TweetUseCase`.
2. El use case persiste el tweet en PostgreSQL.
3. Publica un evento `tweet.created` en RabbitMQ.
4. El consumer lo recibe de forma asíncrona y hace **fanout**: escribe el tweet en el timeline Redis de cada seguidor del autor.

### Seguir usuario

1. El handler recibe `POST /follow` y delega al `FollowUseCase`.
2. El use case persiste la relación en PostgreSQL.
3. Publica un evento `follow.created` en RabbitMQ.
4. El consumer hace **backfill**: copia los últimos `FOLLOW_BACKFILL_LIMIT` tweets del autor en el timeline Redis del nuevo seguidor.

### Leer timeline

1. El handler recibe `GET /timeline` y delega al `TimelineUseCase`.
2. El use case consulta Redis. Si hay datos (**cache hit**), devuelve directo en O(1).
3. Si Redis no tiene datos (**cache miss**), hace fallback a PostgreSQL y popula Redis para la próxima lectura.

### Crear usuario

1. El handler recibe `POST /users` y delega al `UserUseCase`.
2. El use case persiste el usuario en PostgreSQL.

## Decisión de diseño: fanout on write

El timeline se construye en el momento de escritura (fanout), no en el de lectura. Esto hace que `GET /timeline` sea siempre O(1) desde Redis, independientemente de cuántos usuarios siga el lector. El costo del fanout es asíncrono y se distribuye en RabbitMQ.

La contrapartida es que usuarios con muchos seguidores generan más trabajo al publicar. Para esta kata el trade-off favorece la lectura, que es el camino crítico a escala.
