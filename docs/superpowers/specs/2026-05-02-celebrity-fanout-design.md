# Spec: Celebrity/Hot User Problem — Fanout filtrado por actividad

**Fecha:** 2026-05-02
**Gap:** 1 — fanout no escala a millones de seguidores

---

## Problema

`fanoutTweet` itera sobre todos los followers con 100 goroutines concurrentes. Para un usuario con 10M seguidores, esto implica 10M `ZADD` + 10M `HSetNX` (20M operaciones Redis) bloqueando el consumer durante minutos.

---

## Decisión adoptada: Opción D — Fanout filtrado por actividad + TTL universal

El fanout solo escribe a followers activos. Un usuario es activo si hizo `GET /timeline` en las últimas `ACTIVITY_TTL` horas. El concepto de "celebrity threshold" no existe — el mismo mecanismo aplica a todos los usuarios.

Ver `docs/tradeoffs/celebrity-fanout-strategy.md` para la evaluación de alternativas.

---

## Diseño

### 1. Nueva columna en Postgres

```sql
ALTER TABLE users ADD COLUMN last_active TIMESTAMPTZ;
CREATE INDEX idx_users_last_active ON users (last_active);
```

`last_active` es nullable — usuarios que nunca leyeron su timeline tienen `NULL` y no reciben fanout.

---

### 2. Nuevo evento de dominio

En `internal/domain/events.go`:

```go
type UserActivityEvent struct {
    UserID     uuid.UUID `json:"user_id"`
    LastActive time.Time `json:"last_active"`
}

type UserActivityPublisher interface {
    PublishUserActivity(ctx context.Context, evt UserActivityEvent) error
}
```

El `LastActive` se setea al momento del read — el consumer usa este valor directamente, no recalcula `now()` al procesar.

---

### 3. Timeline usecase publica actividad

`TimelineUseCase.GetTimeline` recibe un `UserActivityPublisher` y publica el evento tras devolver el timeline. El publish es best-effort — si falla, se loguea pero no se retorna error (no bloquea el read path).

El consumer absorbe duplicados — si un usuario lee 50 veces por hora, se procesan 50 eventos, todos idempotentes (`UPDATE ... SET last_active = $1`).

---

### 4. Nueva interfaz en FollowRepository

```go
type FollowerActivity struct {
    ID         uuid.UUID
    LastActive time.Time
}

// GetActiveFollowers retorna solo followers con last_active > activeSince.
GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]FollowerActivity, error)
```

Query subyacente:

```sql
SELECT f.follower_id, u.last_active
FROM follows f
JOIN users u ON u.id = f.follower_id
WHERE f.followee_id = $1
  AND u.last_active > $2
```

`GetFollowers` (sin filtro) se mantiene para otros usos (ej. backfill en follow).

---

### 5. Consumer procesa UserActivityEvent

Nuevo método en `Consumer`: `ConsumeUserActivity(ctx context.Context)`.

Consume la queue `queue.user.activity`. Por cada evento:

```sql
UPDATE users SET last_active = $event.LastActive
WHERE id = $event.UserID
  AND (last_active IS NULL OR last_active < $event.LastActive)
```

El UPDATE es condicional para tolerar mensajes desordenados — nunca pisa un `last_active` más nuevo con uno más viejo. Idempotente. No tiene retry elaborado — si falla, el mensaje se nackea y RabbitMQ lo reencola.

---

### 6. fanoutTweet usa GetActiveFollowers y calcula TTL restante

```go
followers, err := c.followRepo.GetActiveFollowers(ctx, evt.UserID, time.Now().Add(-activityTTL))

// por cada follower:
remaining := activityTTL - time.Since(follower.LastActive)
if remaining <= 0 {
    continue // usuario cruzó el límite entre el query y la escritura, skip
}
c.fanout.AppendTweet(ctx, follower.ID, item, remaining)
```

`AppendTweet` recibe el `remaining time.Duration` para el EXPIRE de Redis. El guard `remaining <= 0` evita escribir en Redis una key que se eliminaría inmediatamente o con TTL inválido — el usuario recibirá el tweet vía fallback a Postgres cuando vuelva a estar activo.

---

### 7. AppendTweet setea TTL proporcional al tiempo restante de actividad

```go
func (r *TimelineRepository) AppendTweet(ctx context.Context, userID uuid.UUID, item TweetItem, ttl time.Duration) error
```

Flujo:
1. `ZADD timeline:{userID}` — preserva TTL existente automáticamente
2. `HSetNX timeline:data:{userID}` — preserva TTL existente automáticamente
3. `EXPIRE timeline:{userID} int(ttl.Seconds()) NX` — solo setea TTL si la key es nueva (NX = solo si no tiene expiry)
4. `EXPIRE timeline:data:{userID} int(ttl.Seconds()) NX`

`ZADD` y `HSET` no modifican el TTL de keys existentes en Redis — el TTL solo se necesita setear en la primera escritura. El read path renueva el TTL completo.

---

### 8. Read path renueva TTL completo

En `TimelineRepository.readFromRedis`, al final:

```go
r.rdb.Expire(ctx, timelineKey(userID), activityTTL)
r.rdb.Expire(ctx, timelineDataKey(userID), activityTTL)
```

En `writeToRedis` (cache miss repopulation), al final del pipeline:

```go
r.rdb.Expire(ctx, key, activityTTL)
r.rdb.Expire(ctx, dataKey, activityTTL)
```

---

### 9. Publisher en main.go

El `Publisher` expone `PublishUserActivity`. Se inyecta en `TimelineUseCase`. Se agrega `consumer.ConsumeUserActivity(ctx)` como cuarta goroutine en el startup.

---

## Invariante garantizado

Si `timeline:{userID}` existe en Redis → el usuario estaba activo cuando se hizo el último fanout o read. El TTL de la key nunca supera `activityTTL` desde el último momento de actividad conocido.

---

## Configuración

```
ACTIVITY_TTL=24h   # ventana de actividad (default: 24h)
```

Misma constante usada en: filtro de `GetActiveFollowers`, TTL de Redis en writes, TTL de Redis en reads.

---

## Interfaces modificadas

| Interfaz / tipo | Cambio |
|---|---|
| `domain.UserActivityEvent` | nuevo |
| `domain.UserActivityPublisher` | nueva |
| `domain.FollowerActivity` | nuevo |
| `domain.FollowRepository` | agrega `GetActiveFollowers` |
| `domain.TimelineFanout` | `AppendTweet` recibe `ttl time.Duration` |
| `TimelineUseCase` | recibe `UserActivityPublisher` |
| `Consumer` | agrega `ConsumeUserActivity` |
| `Publisher` | agrega `PublishUserActivity` |
| `postgres.FollowRepository` | implementa `GetActiveFollowers` |
| `redis.TimelineRepository` | `AppendTweet` con TTL, `readFromRedis`/`writeToRedis` con Expire |

---

## Lo que NO cambia

- Read path de `GetTimeline` — el fallback a Postgres ya maneja el cache miss
- Lógica de DLQ y retry del fanout existente
- `GetFollowers` sin filtro — se mantiene para el backfill en follow
- Handler de timeline — sin cambios
