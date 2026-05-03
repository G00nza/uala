# Decisiones de diseño

## ADR-001: Use cases concretos, sin interfaz propia

**Estado:** Aceptado

### Contexto

Los use cases representan lógica de negocio que no tiene alternativa: no hay dos implementaciones de "crear usuario". Exponerlos como interfaces los trataría como infraestructura intercambiable, lo cual contradice su rol en DDD.

### Decisión

Los use cases son structs concretos. Ningún package define una interfaz que los represente en código de producción. Los handlers dependen directamente de ellos:

```go
handler.NewUserHandler(usecase.NewCreateUserUseCase(userRepo))
```

Para testear handlers en aislamiento, cada handler define una **interfaz privada mínima** que vive únicamente en ese package y no es visible desde afuera:

```
internal/
  usecase/
    user.go     ← struct concreto, sin interfaz propia
  handler/
    user.go     ← userCreator (privada, solo para testing)
```

### Consecuencias

- Agregar un segundo adapter (gRPC, CLI) requiere depender del mismo struct concreto o duplicar la interfaz local. La duplicación es mínima y aceptable.
- Los tests del use case mockean repositorios (infraestructura), nunca el use case mismo.

---

## ADR-002: Fanout on write con fanout selectivo por actividad

**Estado:** Aceptado

### Contexto

El sistema debe estar optimizado para lecturas. El timeline requiere un JOIN entre follows, tweets y usuarios en PostgreSQL, con ordenamiento y paginación. A escala, hacerlo en cada lectura es caro.

El fanout ingenuo (escribir en el timeline de todos los seguidores al publicar un tweet) resuelve el read path pero introduce el **celebrity problem**: un usuario con 10M seguidores genera 10M escrituras Redis por cada tweet publicado.

### Alternativas evaluadas

| Opción | Descripción | Trade-off |
|---|---|---|
| **Fanout on write selectivo** (elegida) | Al publicar, fanout solo a seguidores activos en la última ventana de `ACTIVITY_TTL` | Lectura O(1); fanout proporcional a seguidores activos, no totales |
| Fanout on write completo | Fanout a todos los seguidores | Lectura O(1); costoso para celebrities |
| Fanout on read | Al leer, se construye el timeline en el momento | Lectura costosa; escritura simple |
| Hybrid push/pull | Fanout completo para usuarios normales; celebrities solo a activos, con merge en el read path | Mayor complejidad operativa en el read path |

### Decisión

**Fanout on write, filtrado por actividad.** Al publicar un tweet, se emite `TweetCreatedEvent` vía RabbitMQ. El consumer consulta `GetActiveFollowers` — un JOIN en PostgreSQL que retorna solo los seguidores con `last_active` dentro de la ventana de `ACTIVITY_TTL` (default 24h) — y escribe en sus timelines Redis.

Un seguidor inactivo no recibe el push. Cuando vuelva a consultar su timeline, el cache miss hace fallback a PostgreSQL (fan-out on read como fallback natural).

La actividad se registra en cada `GET /timeline`: el use case publica un `UserActivityEvent` de forma asíncrona; el consumer actualiza `users.last_active` en PostgreSQL con una guarda monótona (solo actualiza si el timestamp entrante es más nuevo).

### Dónde almacenar last_active: Redis vs PostgreSQL

| Opción | Ventaja | Problema |
|---|---|---|
| Redis (`active:{userID}` con TTL) | Escritura O(1) | Para filtrar en el fanout habría que cargar todos los follower IDs desde PostgreSQL y chequear Redis por cada uno — O(N) total |
| **PostgreSQL `last_active` column** (elegida) | Filtrado en el JOIN; índice en `last_active` hace el corte en la capa correcta | Escritura levemente más costosa |

### Consecuencias

- `GET /timeline` es O(1) desde Redis en casi todos los casos.
- El fanout es proporcional a los seguidores activos, no al total.
- Al hacer follow, se hace backfill de los últimos N tweets del autor (configurable via `FOLLOW_BACKFILL_LIMIT`).

---

## ADR-003: Redis timeline como Sorted Set + Hash

**Estado:** Aceptado

### Contexto

Durante la implementación inicial el timeline se almacenaba como JSON completo en el member del Sorted Set. Al introducir el fanout selectivo, cada tweet se escribe múltiples veces (una por seguidor activo). Con JSON como member, cada escritura duplica el payload completo en Redis.

### Decisión

Separar en dos estructuras:

```
timeline:{user_id}       →  Sorted Set  (score: unix timestamp, member: tweet_id)
timeline:data:{user_id}  →  Hash        (field: tweet_id, value: JSON del tweet)
```

La lectura combina `ZREVRANGE` + `HMGET`. La escritura usa `HSetNX` (idempotente): si el tweet ya existe en el hash, no se reescribe el payload.

### Comportamiento de TTL

- **En write (fanout):** `ExpireNX` — setea TTL solo en keys nuevas, no sobreescribe el TTL de keys existentes.
- **En read:** `Expire` — renueva el TTL al valor completo de `ACTIVITY_TTL`, extendiendo la ventana de actividad.
- Las keys expiran naturalmente cuando el usuario pasa a inactivo, sin eviction explícita.

### Consecuencias

- Un read requiere dos round-trips a Redis en lugar de uno. En la práctica, Redis pipeline los hace casi en paralelo.
- La separación habilita `HSetNX` idempotente en el fanout: el mismo tweet puede llegar dos veces sin duplicar datos.

---

## ADR-004: Estrategia de testing

**Estado:** Aceptado

### Decisión

Dos niveles de test, sin test E2E del flujo async completo:

**Unit tests** (siempre corren):
- Mockean repositorios e interfaces privadas de los handlers.
- Cubren: validación de inputs, mapeo de errores de dominio a HTTP status codes, serialización.

**Integration tests** (`INTEGRATION=1 go test ./...`):
- Levantan PostgreSQL y Redis reales (no mocks).
- Testean el flujo completo de punta a punta incluyendo escrituras en base de datos.
- Son el test primario para el happy path de cada handler.

### Por qué no hay test E2E del path async

Un test que recorra `POST /tweet → RabbitMQ → consumer → Redis → GET /timeline` con todos los servicios reales introduce timing no determinista (requiere sleep o polling antes del GET). Además, el contrato de serialización entre publisher y consumer está garantizado por el compilador: `TweetCreatedEvent`, `FollowCreatedEvent` y `FanoutRetryEvent` son structs tipados definidos en `domain/events.go` — ambos lados usan el mismo tipo, y cualquier cambio incompatible falla en compilación antes de correr ningún test.

### Motivación para integration tests con infra real

Los mocks de base de datos pasaron tests que fallaban en producción por divergencias en migraciones. Los tests de integración con infra real son la red de seguridad principal para el happy path.
