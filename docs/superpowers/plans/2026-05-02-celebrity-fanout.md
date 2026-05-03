# Celebrity Fanout — Fanout filtrado por actividad Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reemplazar el fanout a todos los followers por un fanout filtrado a followers activos, con TTL en las keys de Redis ligado a la ventana de actividad.

**Architecture:** Se agrega una columna `last_active` a `users` (Postgres) actualizada via un evento asíncrono `UserActivityEvent` publicado en cada `GET /timeline`. El fanout en el consumer usa `GetActiveFollowers` (JOIN en Postgres) para obtener solo followers activos con su `last_active`, calcula el TTL restante por follower, y lo pasa a `AppendTweet`. Las keys de Redis expiran naturalmente cuando el usuario pasa a inactivo; los reads renuevan el TTL al valor completo.

**Tech Stack:** Go, PostgreSQL (pgx/v5), Redis 7 (go-redis/v9, `ExpireNX`), RabbitMQ (amqp091-go), goose migrations.

---

## File Map

| Archivo | Acción | Responsabilidad |
|---|---|---|
| `migrations/00004_add_users_last_active.sql` | Crear | Agrega columna + índice |
| `internal/domain/events.go` | Modificar | `UserActivityEvent`, `UserActivityPublisher` |
| `internal/domain/follow.go` | Modificar | `FollowerActivity`, `GetActiveFollowers` en `FollowRepository`, TTL en `TimelineFanout.AppendTweet` |
| `internal/domain/user.go` | Modificar | `UserActivityRepository` interface |
| `internal/infra/config.go` | Modificar | `ActivityTTL time.Duration` |
| `internal/repository/postgres/follow.go` | Modificar | Implementa `GetActiveFollowers` |
| `internal/repository/postgres/user.go` | Modificar | Implementa `UpdateLastActive` |
| `internal/repository/redis/timeline.go` | Modificar | `AppendTweet` con TTL, `ExpireNX`, `Expire` en reads |
| `internal/messaging/rabbitmq/client.go` | Modificar | `QueueUserActivity`, `declareTopology` |
| `internal/messaging/rabbitmq/publisher.go` | Modificar | `PublishUserActivity` |
| `internal/messaging/rabbitmq/consumer.go` | Modificar | `ConsumeUserActivity`, `fanoutTweet` con actividad, `activityTTL` field |
| `internal/usecase/timeline.go` | Modificar | Inyecta `UserActivityPublisher`, publica evento |
| `cmd/api/main.go` | Modificar | Wiring de todo |
| `internal/repository/postgres/follow_test.go` | Modificar | Tests `GetActiveFollowers` |
| `internal/repository/postgres/user_test.go` | Modificar | Tests `UpdateLastActive` |
| `internal/repository/redis/timeline_test.go` | Modificar | Tests TTL |
| `internal/messaging/rabbitmq/consumer_test.go` | Modificar | Stubs actualizados, test `ConsumeUserActivity` |
| `internal/usecase/mocks_test.go` | Modificar | `mockUserActivityPublisher`, stubs actualizados |
| `internal/usecase/timeline_test.go` | Modificar | `NewTimelineUseCase` con 3er arg |
| `internal/handler/mocks_test.go` | Modificar | `noopUserActivityPublisher` |
| `internal/handler/setup_test.go` | Modificar | `NewTimelineRepository` y `NewTimelineUseCase` actualizados |

---

## Task 1: Migración — columna last_active

**Files:**
- Create: `migrations/00004_add_users_last_active.sql`

- [ ] **Step 1: Crear archivo de migración**

```sql
-- +goose Up
ALTER TABLE users ADD COLUMN last_active TIMESTAMPTZ;
CREATE INDEX idx_users_last_active ON users (last_active);

-- +goose Down
DROP INDEX IF EXISTS idx_users_last_active;
ALTER TABLE users DROP COLUMN IF EXISTS last_active;
```

- [ ] **Step 2: Verificar que el archivo se embebe correctamente**

El paquete `migrations` usa `//go:embed *.sql`. La migración se aplica automáticamente en TestMain de los tests de integración.

Run: `go build ./migrations/...`
Expected: sin errores

- [ ] **Step 3: Commit**

```bash
git add migrations/00004_add_users_last_active.sql
git commit -m "feat: add last_active column to users for activity-based fanout"
```

---

## Task 2: Domain — nuevos tipos e interfaces (additive)

**Files:**
- Modify: `internal/domain/events.go`
- Modify: `internal/domain/follow.go`
- Modify: `internal/domain/user.go`
- Modify: `internal/repository/postgres/follow.go` (stub)
- Modify: `internal/usecase/mocks_test.go`
- Modify: `internal/messaging/rabbitmq/consumer_test.go`

- [ ] **Step 1: Agregar UserActivityEvent y UserActivityPublisher a domain/events.go**

Al final del archivo, después de `FanoutRetryPublisher`:

```go
type UserActivityEvent struct {
	UserID     uuid.UUID `json:"user_id"`
	LastActive time.Time `json:"last_active"`
}

type UserActivityPublisher interface {
	PublishUserActivity(ctx context.Context, evt UserActivityEvent) error
}
```

- [ ] **Step 2: Agregar FollowerActivity + GetActiveFollowers a domain/follow.go**

Después del bloque `type Follow struct`:

```go
type FollowerActivity struct {
	ID         uuid.UUID
	LastActive time.Time
}
```

En `FollowRepository` interface, agregar método:

```go
type FollowRepository interface {
	Create(ctx context.Context, f *Follow) error
	Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error)
	GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error)
	GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]FollowerActivity, error)
}
```

- [ ] **Step 3: Agregar UserActivityRepository a domain/user.go**

Al final del archivo:

```go
type UserActivityRepository interface {
	UpdateLastActive(ctx context.Context, userID uuid.UUID, lastActive time.Time) error
}
```

- [ ] **Step 4: Agregar stub GetActiveFollowers en postgres/follow.go**

Al final del archivo (implementación real en Task 5):

```go
func (r *FollowRepository) GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]domain.FollowerActivity, error) {
	return []domain.FollowerActivity{}, nil
}
```

- [ ] **Step 5: Actualizar mockFollowRepo en usecase/mocks_test.go**

Agregar método al struct `mockFollowRepo`:

```go
func (m *mockFollowRepo) GetActiveFollowers(_ context.Context, _ uuid.UUID, _ time.Time) ([]domain.FollowerActivity, error) {
	return nil, nil
}
```

También agregar `"time"` al import si no está.

- [ ] **Step 6: Actualizar stubFollowRepo en consumer_test.go**

Agregar método al struct `stubFollowRepo`:

```go
func (s *stubFollowRepo) GetActiveFollowers(_ context.Context, _ uuid.UUID, _ time.Time) ([]domain.FollowerActivity, error) {
	result := make([]domain.FollowerActivity, len(s.followers))
	for i, id := range s.followers {
		result[i] = domain.FollowerActivity{ID: id, LastActive: time.Now()}
	}
	return result, nil
}
```

- [ ] **Step 7: Verificar que compila**

Run: `go build ./...`
Expected: sin errores de compilación

- [ ] **Step 8: Commit**

```bash
git add internal/domain/events.go internal/domain/follow.go internal/domain/user.go \
        internal/repository/postgres/follow.go \
        internal/usecase/mocks_test.go \
        internal/messaging/rabbitmq/consumer_test.go
git commit -m "feat: add UserActivityEvent, FollowerActivity and updated domain interfaces"
```

---

## Task 3: Domain — actualizar AppendTweet signature + fix todos los call sites

**Files:**
- Modify: `internal/domain/follow.go`
- Modify: `internal/repository/redis/timeline.go`
- Modify: `internal/messaging/rabbitmq/consumer.go`
- Modify: `internal/messaging/rabbitmq/consumer_test.go`

- [ ] **Step 1: Actualizar TimelineFanout en domain/follow.go**

Reemplazar la interface `TimelineFanout`:

```go
type TimelineFanout interface {
	AppendTweet(ctx context.Context, userID uuid.UUID, item TweetItem, ttl time.Duration) error
}
```

- [ ] **Step 2: Agregar import time en domain/follow.go si no está**

El archivo ya importa `"time"` para `Follow.CreatedAt`. Verificar que sigue ahí.

- [ ] **Step 3: Actualizar AppendTweet en redis/timeline.go (firma, lógica TTL en Task 7)**

Reemplazar el método `AppendTweet` completo:

```go
func (r *TimelineRepository) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem, ttl time.Duration) error {
	key := timelineKey(userID)
	dataKey := timelineDataKey(userID)

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	id := item.ID.String()

	if err := r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(item.CreatedAt.Unix()),
		Member: id,
	}).Err(); err != nil {
		return err
	}
	return r.rdb.HSetNX(ctx, dataKey, id, string(data)).Err()
}
```

(El TTL se implementa en Task 7; por ahora solo se acepta el parámetro.)

- [ ] **Step 4: Agregar activityTTL al Consumer y actualizar call sites en consumer.go**

Agregar campo al struct `Consumer`:

```go
type Consumer struct {
	conn             channeler
	followRepo       domain.FollowRepository
	fanout           domain.TimelineFanout
	userTweetsRepo   domain.UserTweetsRepository
	backfillLimit    int
	fanoutWorkers    int
	retryPublisher   domain.FanoutRetryPublisher
	deadLetterPub    domain.FanoutRetryPublisher
	activityTTL      time.Duration
}
```

Agregar constante y actualizar `NewConsumer`:

```go
const defaultActivityTTL = 24 * time.Hour

func NewConsumer(
	conn channeler,
	followRepo domain.FollowRepository,
	fanout domain.TimelineFanout,
	userTweetsRepo domain.UserTweetsRepository,
	backfillLimit int,
) *Consumer {
	return &Consumer{
		conn:           conn,
		followRepo:     followRepo,
		fanout:         fanout,
		userTweetsRepo: userTweetsRepo,
		backfillLimit:  backfillLimit,
		fanoutWorkers:  fanoutConcurrency,
		activityTTL:    defaultActivityTTL,
	}
}
```

Agregar método builder:

```go
func (c *Consumer) WithActivityTTL(d time.Duration) *Consumer {
	c.activityTTL = d
	return c
}
```

Actualizar call site en `handleFanoutRetry`:

```go
if err := c.fanout.AppendTweet(ctx, evt.FollowerID, evt.Tweet, c.activityTTL); err != nil {
```

Actualizar call site en `handleFollowCreated`:

```go
if err := c.fanout.AppendTweet(ctx, evt.FollowerID, tweet, c.activityTTL); err != nil {
```

Actualizar call site en `fanoutTweet` (el loop de goroutines):

```go
if err := c.fanout.AppendTweet(gctx, fid, item, c.activityTTL); err != nil {
```

- [ ] **Step 5: Actualizar stubs de TimelineFanout en consumer_test.go**

Reemplazar las tres implementaciones:

```go
func (f *concurrentFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	cur := atomic.AddInt64(&f.active, 1)
	f.mu.Lock()
	if cur > f.maxObserved {
		f.maxObserved = cur
	}
	f.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	atomic.AddInt64(&f.active, -1)
	return nil
}

func (e *errorFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	return errors.New("redis unavailable")
}

func (p *partialErrorFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	if int(p.calls.Add(1)) <= p.failCount {
		return errors.New("redis unavailable")
	}
	return nil
}
```

- [ ] **Step 6: Verificar que compila y los tests existentes pasan**

Run: `go build ./...`
Expected: sin errores

Run: `go test ./internal/messaging/rabbitmq/...`
Expected: todos los tests existentes pasan (el comportamiento no cambió, solo la firma)

- [ ] **Step 7: Commit**

```bash
git add internal/domain/follow.go internal/repository/redis/timeline.go \
        internal/messaging/rabbitmq/consumer.go \
        internal/messaging/rabbitmq/consumer_test.go
git commit -m "feat: update AppendTweet signature to accept TTL duration"
```

---

## Task 4: Config — agregar ActivityTTL

**Files:**
- Modify: `internal/infra/config.go`

- [ ] **Step 1: Agregar ActivityTTL al struct Config y a LoadConfig**

```go
import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL         string
	RedisURL            string
	AMQPURL             string
	Port                string
	FollowBackfillLimit int
	TimelineLimit       int
	ActivityTTL         time.Duration
}

func LoadConfig() Config {
	return Config{
		DatabaseURL:         getenv("DATABASE_URL", "postgres://uala:uala@localhost:5432/uala"),
		RedisURL:            getenv("REDIS_URL", "redis://localhost:6379/0"),
		AMQPURL:             getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		Port:                getenv("PORT", "8080"),
		FollowBackfillLimit: getenvInt("FOLLOW_BACKFILL_LIMIT", 20),
		TimelineLimit:       getenvInt("TIMELINE_LIMIT", 500),
		ActivityTTL:         getenvDuration("ACTIVITY_TTL", 24*time.Hour),
	}
}
```

Agregar helper al final del archivo:

```go
func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
```

- [ ] **Step 2: Verificar compilación**

Run: `go build ./internal/infra/...`
Expected: sin errores

- [ ] **Step 3: Commit**

```bash
git add internal/infra/config.go
git commit -m "feat: add ACTIVITY_TTL config value (default 24h)"
```

---

## Task 5: Postgres — GetActiveFollowers (TDD)

**Files:**
- Modify: `internal/repository/postgres/follow_test.go`
- Modify: `internal/repository/postgres/follow.go`

- [ ] **Step 1: Escribir el test que falla**

Agregar al final de `internal/repository/postgres/follow_test.go`:

```go
func TestFollowRepository_GetActiveFollowers_ReturnsOnlyActive(t *testing.T) {
	r := setup(t)
	ctx := context.Background()

	celebrity := seedUser(t, r.user, "celeb")
	activeFollower := seedUser(t, r.user, "active_f")
	inactiveFollower := seedUser(t, r.user, "inactive_f")
	nullFollower := seedUser(t, r.user, "null_f") // last_active IS NULL

	now := time.Now().UTC()
	if _, err := testDB.Exec(ctx, "UPDATE users SET last_active = $1 WHERE id = $2", now, activeFollower.ID); err != nil {
		t.Fatalf("set active: %v", err)
	}
	if _, err := testDB.Exec(ctx, "UPDATE users SET last_active = $1 WHERE id = $2", now.Add(-48*time.Hour), inactiveFollower.ID); err != nil {
		t.Fatalf("set inactive: %v", err)
	}

	for _, id := range []uuid.UUID{activeFollower.ID, inactiveFollower.ID, nullFollower.ID} {
		if err := r.follow.Create(ctx, &domain.Follow{FollowerID: id, FolloweeID: celebrity.ID, CreatedAt: now}); err != nil {
			t.Fatalf("create follow: %v", err)
		}
	}

	activeSince := now.Add(-24 * time.Hour)
	followers, err := r.follow.GetActiveFollowers(ctx, celebrity.ID, activeSince)
	if err != nil {
		t.Fatalf("GetActiveFollowers: %v", err)
	}
	if len(followers) != 1 {
		t.Fatalf("want 1 active follower, got %d", len(followers))
	}
	if followers[0].ID != activeFollower.ID {
		t.Errorf("want activeFollower ID %s, got %s", activeFollower.ID, followers[0].ID)
	}
	if followers[0].LastActive.IsZero() {
		t.Error("want non-zero LastActive in result")
	}
}

func TestFollowRepository_GetActiveFollowers_EmptyWhenNoFollowers(t *testing.T) {
	r := setup(t)
	celebrity := seedUser(t, r.user, "celeb_empty")

	followers, err := r.follow.GetActiveFollowers(context.Background(), celebrity.ID, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetActiveFollowers: %v", err)
	}
	if len(followers) != 0 {
		t.Fatalf("want 0, got %d", len(followers))
	}
}
```

- [ ] **Step 2: Correr el test para verificar que falla**

Run: `INTEGRATION=1 go test ./internal/repository/postgres/... -run TestFollowRepository_GetActiveFollowers -v`
Expected: FAIL — el stub retorna `[]domain.FollowerActivity{}` vacío, el test espera 1 resultado

- [ ] **Step 3: Implementar GetActiveFollowers en postgres/follow.go**

Reemplazar el stub del Task 2:

```go
func (r *FollowRepository) GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]domain.FollowerActivity, error) {
	rows, err := r.db.Query(ctx, `
		SELECT f.follower_id, u.last_active
		FROM follows f
		JOIN users u ON u.id = f.follower_id
		WHERE f.followee_id = $1
		  AND u.last_active > $2
	`, followeeID, activeSince)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []domain.FollowerActivity
	for rows.Next() {
		var fa domain.FollowerActivity
		if err := rows.Scan(&fa.ID, &fa.LastActive); err != nil {
			return nil, err
		}
		result = append(result, fa)
	}
	if result == nil {
		result = []domain.FollowerActivity{}
	}
	return result, rows.Err()
}
```

- [ ] **Step 4: Correr los tests para verificar que pasan**

Run: `INTEGRATION=1 go test ./internal/repository/postgres/... -run TestFollowRepository_GetActiveFollowers -v`
Expected: PASS

Run: `INTEGRATION=1 go test ./internal/repository/postgres/...`
Expected: todos los tests pasan

- [ ] **Step 5: Commit**

```bash
git add internal/repository/postgres/follow.go internal/repository/postgres/follow_test.go
git commit -m "feat: implement GetActiveFollowers with last_active JOIN filter"
```

---

## Task 6: Postgres — UpdateLastActive (TDD)

**Files:**
- Modify: `internal/repository/postgres/user_test.go`
- Modify: `internal/repository/postgres/user.go`

- [ ] **Step 1: Escribir el test que falla**

Agregar al final de `internal/repository/postgres/user_test.go`:

```go
func TestUserRepository_UpdateLastActive_SetsValue(t *testing.T) {
	r := setup(t)
	ctx := context.Background()
	u := seedUser(t, r.user, "activity_u1")

	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := r.user.UpdateLastActive(ctx, u.ID, now); err != nil {
		t.Fatalf("UpdateLastActive: %v", err)
	}

	var lastActive time.Time
	if err := testDB.QueryRow(ctx, "SELECT last_active FROM users WHERE id = $1", u.ID).Scan(&lastActive); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !lastActive.UTC().Truncate(time.Millisecond).Equal(now) {
		t.Errorf("want %v, got %v", now, lastActive.UTC().Truncate(time.Millisecond))
	}
}

func TestUserRepository_UpdateLastActive_IgnoresOlderTimestamp(t *testing.T) {
	r := setup(t)
	ctx := context.Background()
	u := seedUser(t, r.user, "activity_u2")

	newer := time.Now().UTC().Truncate(time.Millisecond)
	older := newer.Add(-1 * time.Hour)

	_ = r.user.UpdateLastActive(ctx, u.ID, newer)
	_ = r.user.UpdateLastActive(ctx, u.ID, older) // debe ignorarse

	var lastActive time.Time
	testDB.QueryRow(ctx, "SELECT last_active FROM users WHERE id = $1", u.ID).Scan(&lastActive)
	if !lastActive.UTC().Truncate(time.Millisecond).Equal(newer) {
		t.Errorf("newer timestamp must be preserved, got %v", lastActive)
	}
}

func TestUserRepository_UpdateLastActive_NullToValue(t *testing.T) {
	r := setup(t)
	ctx := context.Background()
	u := seedUser(t, r.user, "activity_u3")

	// last_active starts as NULL — update should work
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := r.user.UpdateLastActive(ctx, u.ID, now); err != nil {
		t.Fatalf("UpdateLastActive from NULL: %v", err)
	}

	var lastActive time.Time
	testDB.QueryRow(ctx, "SELECT last_active FROM users WHERE id = $1", u.ID).Scan(&lastActive)
	if lastActive.IsZero() {
		t.Error("want non-zero after update from NULL")
	}
}
```

- [ ] **Step 2: Correr el test para verificar que falla**

Run: `INTEGRATION=1 go test ./internal/repository/postgres/... -run TestUserRepository_UpdateLastActive -v`
Expected: FAIL — `UpdateLastActive` no existe en `UserRepository`

- [ ] **Step 3: Implementar UpdateLastActive en postgres/user.go**

Agregar al final del archivo:

```go
func (r *UserRepository) UpdateLastActive(ctx context.Context, userID uuid.UUID, lastActive time.Time) error {
	_, err := r.db.Exec(ctx, `
		UPDATE users SET last_active = $1
		WHERE id = $2
		  AND (last_active IS NULL OR last_active < $1)
	`, lastActive, userID)
	return err
}
```

- [ ] **Step 4: Correr los tests para verificar que pasan**

Run: `INTEGRATION=1 go test ./internal/repository/postgres/... -run TestUserRepository_UpdateLastActive -v`
Expected: PASS

Run: `INTEGRATION=1 go test ./internal/repository/postgres/...`
Expected: todos los tests pasan

- [ ] **Step 5: Commit**

```bash
git add internal/repository/postgres/user.go internal/repository/postgres/user_test.go
git commit -m "feat: implement UpdateLastActive with monotonic guard"
```

---

## Task 7: Redis — AppendTweet con TTL + EXPIRE en reads/writes (TDD)

**Files:**
- Modify: `internal/repository/redis/timeline.go`
- Modify: `internal/repository/redis/timeline_test.go`

- [ ] **Step 1: Actualizar NewTimelineRepository para aceptar activityTTL**

Reemplazar el struct y constructor en `redis/timeline.go`:

```go
type TimelineRepository struct {
	rdb         *redis.Client
	pgRepo      domain.TimelineRepository
	limit       int64
	activityTTL time.Duration
}

func NewTimelineRepository(rdb *redis.Client, pgRepo domain.TimelineRepository, limit int, activityTTL time.Duration) *TimelineRepository {
	return &TimelineRepository{rdb: rdb, pgRepo: pgRepo, limit: int64(limit), activityTTL: activityTTL}
}
```

- [ ] **Step 2: Actualizar llamadas existentes a NewTimelineRepository en tests**

En `internal/repository/redis/timeline_test.go`, reemplazar todas las ocurrencias de:
`redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)`
por:
`redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500, 24*time.Hour)`

En `internal/handler/setup_test.go`, reemplazar:
`redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500)`
por:
`redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500, 24*time.Hour)`

- [ ] **Step 3: Actualizar llamadas existentes a AppendTweet en timeline_test.go**

En `internal/repository/redis/timeline_test.go`, reemplazar todas las ocurrencias de:
`repo.AppendTweet(context.Background(), userID, item)`
por:
`repo.AppendTweet(context.Background(), userID, item, 24*time.Hour)`

Hacer lo mismo con cualquier `AppendTweet` en `handler/setup_test.go` si existe.

- [ ] **Step 4: Verificar que los tests existentes siguen pasando**

Run: `INTEGRATION=1 go test ./internal/repository/redis/... -v`
Expected: todos los tests existentes pasan

- [ ] **Step 5: Escribir tests de TTL que fallan**

Agregar al final de `internal/repository/redis/timeline_test.go`:

```go
func TestRedisTimeline_AppendTweet_SetsTTLOnNewKey(t *testing.T) {
	flushRedis(t)
	ctx := context.Background()
	userID := uuid.New()
	ttl := 2 * time.Hour
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500, 24*time.Hour)

	item := domain.TweetItem{
		ID:        uuid.New(),
		UserID:    uuid.New(),
		Username:  "bob",
		Content:   "ttl test",
		CreatedAt: time.Now().UTC(),
	}
	if err := repo.AppendTweet(ctx, userID, item, ttl); err != nil {
		t.Fatalf("AppendTweet: %v", err)
	}

	key := fmt.Sprintf("timeline:%s", userID)
	remaining := testRDB.TTL(ctx, key).Val()
	if remaining <= 0 || remaining > ttl {
		t.Errorf("want TTL in (0, %v], got %v", ttl, remaining)
	}
}

func TestRedisTimeline_AppendTweet_NXDoesNotOverwriteExistingTTL(t *testing.T) {
	flushRedis(t)
	ctx := context.Background()
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500, 24*time.Hour)

	item1 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "first", CreatedAt: time.Now().UTC()}
	item2 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "second", CreatedAt: time.Now().Add(time.Second).UTC()}

	_ = repo.AppendTweet(ctx, userID, item1, 2*time.Hour)
	// segundo append con TTL menor — NX no debe sobreescribir
	_ = repo.AppendTweet(ctx, userID, item2, 30*time.Minute)

	key := fmt.Sprintf("timeline:%s", userID)
	remaining := testRDB.TTL(ctx, key).Val()
	if remaining < time.Hour {
		t.Errorf("want TTL ~2h (NX should not overwrite), got %v", remaining)
	}
}

func TestRedisTimeline_ReadRenewsFullActivityTTL(t *testing.T) {
	flushRedis(t)
	ctx := context.Background()
	userID := uuid.New()
	activityTTL := 24 * time.Hour
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500, activityTTL)

	item := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "hi", CreatedAt: time.Now().UTC()}
	// Write with short TTL
	_ = repo.AppendTweet(ctx, userID, item, 30*time.Minute)

	// Read renews to full activityTTL
	_, _ = repo.GetTimeline(ctx, userID)

	key := fmt.Sprintf("timeline:%s", userID)
	remaining := testRDB.TTL(ctx, key).Val()
	if remaining < 23*time.Hour {
		t.Errorf("want TTL ~24h after read, got %v", remaining)
	}
}
```

Agregar `"fmt"` a los imports del test si no está.

- [ ] **Step 6: Correr los tests nuevos para verificar que fallan**

Run: `INTEGRATION=1 go test ./internal/repository/redis/... -run "TestRedisTimeline_AppendTweet_SetsTTL|TestRedisTimeline_AppendTweet_NX|TestRedisTimeline_ReadRenews" -v`
Expected: FAIL — TTL no se setea aún

- [ ] **Step 7: Implementar TTL en AppendTweet, readFromRedis y writeToRedis**

Reemplazar `AppendTweet` completo en `redis/timeline.go`:

```go
func (r *TimelineRepository) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem, ttl time.Duration) error {
	key := timelineKey(userID)
	dataKey := timelineDataKey(userID)

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	id := item.ID.String()

	if err := r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(item.CreatedAt.Unix()),
		Member: id,
	}).Err(); err != nil {
		return err
	}
	if err := r.rdb.HSetNX(ctx, dataKey, id, string(data)).Err(); err != nil {
		return err
	}
	// ExpireNX: solo setea TTL si la key no tiene expiry aún (primera escritura).
	// ZADD/HSetNX preservan el TTL existente — solo necesitamos setear en keys nuevas.
	r.rdb.ExpireNX(ctx, key, ttl)
	r.rdb.ExpireNX(ctx, dataKey, ttl)
	return nil
}
```

En `readFromRedis`, agregar al final antes del `return`:

```go
r.rdb.Expire(ctx, key, r.activityTTL)
r.rdb.Expire(ctx, dataKey, r.activityTTL)
```

En `writeToRedis`, agregar al final antes del `return nil`:

```go
r.rdb.Expire(ctx, key, r.activityTTL)
r.rdb.Expire(ctx, dataKey, r.activityTTL)
```

Agregar `"time"` al import de `redis/timeline.go` si no está.

- [ ] **Step 8: Correr todos los tests para verificar**

Run: `INTEGRATION=1 go test ./internal/repository/redis/... -v`
Expected: todos pasan

- [ ] **Step 9: Commit**

```bash
git add internal/repository/redis/timeline.go internal/repository/redis/timeline_test.go \
        internal/handler/setup_test.go
git commit -m "feat: implement TTL management in Redis timeline (ExpireNX on write, Expire on read)"
```

---

## Task 8: RabbitMQ — QueueUserActivity + Publisher

**Files:**
- Modify: `internal/messaging/rabbitmq/client.go`
- Modify: `internal/messaging/rabbitmq/publisher.go`
- Modify: `internal/messaging/rabbitmq/publisher_test.go`

- [ ] **Step 1: Agregar constante QueueUserActivity en client.go**

En el bloque `const`, agregar:

```go
QueueUserActivity = "user.activity"
```

- [ ] **Step 2: Declarar la queue en declareTopology**

En el loop de declaración de queues simples:

```go
for _, q := range []string{QueueTweetCreated, QueueFollowCreated, QueueUserActivity} {
    if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
        return err
    }
}
```

- [ ] **Step 3: Agregar PublishUserActivity en publisher.go**

```go
func (p *Publisher) PublishUserActivity(ctx context.Context, evt domain.UserActivityEvent) error {
	return p.publishToExchange("", QueueUserActivity, evt)
}
```

- [ ] **Step 4: Leer publisher_test.go para entender el patrón de tests existente**

Leer el archivo completo antes de escribir el test nuevo.

- [ ] **Step 5: Escribir test para PublishUserActivity en publisher_test.go**

Siguiendo el patrón existente (mock de `amqpChannel`). Buscar el tipo mock de channel que ya existe en el archivo. Si existe un `mockChannel` o similar, usarlo. Si no existe, agregar:

```go
type capturingChannel struct {
	exchange string
	key      string
	body     []byte
}

func (c *capturingChannel) Publish(exchange, key string, _, _ bool, msg amqp.Publishing) error {
	c.exchange = exchange
	c.key = key
	c.body = msg.Body
	return nil
}

func (c *capturingChannel) Close() error { return nil }
```

Test:

```go
func TestPublisher_PublishUserActivity(t *testing.T) {
	ch := &capturingChannel{}
	pub := &Publisher{
		openCh: func() (amqpChannel, error) { return ch, nil },
	}

	userID := uuid.New()
	evt := domain.UserActivityEvent{
		UserID:     userID,
		LastActive: time.Now().UTC(),
	}
	if err := pub.PublishUserActivity(context.Background(), evt); err != nil {
		t.Fatalf("PublishUserActivity: %v", err)
	}
	if ch.key != QueueUserActivity {
		t.Errorf("want queue %s, got %s", QueueUserActivity, ch.key)
	}
	if ch.exchange != "" {
		t.Errorf("want empty exchange, got %s", ch.exchange)
	}
}
```

Si en el archivo ya existe un tipo de mock de channel con otro nombre, usarlo en lugar de `capturingChannel`.

- [ ] **Step 6: Correr los tests**

Run: `go test ./internal/messaging/rabbitmq/... -run TestPublisher -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/messaging/rabbitmq/client.go \
        internal/messaging/rabbitmq/publisher.go \
        internal/messaging/rabbitmq/publisher_test.go
git commit -m "feat: add QueueUserActivity and PublishUserActivity to rabbitmq publisher"
```

---

## Task 9: Consumer — ConsumeUserActivity (TDD)

**Files:**
- Modify: `internal/messaging/rabbitmq/consumer.go`
- Modify: `internal/messaging/rabbitmq/consumer_test.go`

- [ ] **Step 1: Agregar userActivityRepo al Consumer y su builder**

En el struct `Consumer`:

```go
type Consumer struct {
	conn             channeler
	followRepo       domain.FollowRepository
	fanout           domain.TimelineFanout
	userTweetsRepo   domain.UserTweetsRepository
	backfillLimit    int
	fanoutWorkers    int
	retryPublisher   domain.FanoutRetryPublisher
	deadLetterPub    domain.FanoutRetryPublisher
	activityTTL      time.Duration
	userActivityRepo domain.UserActivityRepository
}
```

Agregar builder:

```go
func (c *Consumer) WithUserActivityRepo(r domain.UserActivityRepository) *Consumer {
	c.userActivityRepo = r
	return c
}
```

- [ ] **Step 2: Escribir el test que falla**

Agregar al final de `consumer_test.go`:

```go
type stubUserActivityRepo struct {
	mu    sync.Mutex
	calls []domain.UserActivityEvent
	err   error
}

func (s *stubUserActivityRepo) UpdateLastActive(_ context.Context, userID uuid.UUID, lastActive time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, domain.UserActivityEvent{UserID: userID, LastActive: lastActive})
	return s.err
}

func TestHandleUserActivity_UpdatesLastActive(t *testing.T) {
	repo := &stubUserActivityRepo{}
	c := &Consumer{userActivityRepo: repo}

	userID := uuid.New()
	lastActive := time.Now().UTC().Truncate(time.Millisecond)
	evt := domain.UserActivityEvent{UserID: userID, LastActive: lastActive}
	body, _ := json.Marshal(evt)

	c.handleUserActivity(context.Background(), amqp.Delivery{Body: body})

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.calls) != 1 {
		t.Fatalf("want 1 UpdateLastActive call, got %d", len(repo.calls))
	}
	if repo.calls[0].UserID != userID {
		t.Errorf("want userID %s, got %s", userID, repo.calls[0].UserID)
	}
	if !repo.calls[0].LastActive.Equal(lastActive) {
		t.Errorf("want LastActive %v, got %v", lastActive, repo.calls[0].LastActive)
	}
}

func TestHandleUserActivity_NacksOnInvalidJSON(t *testing.T) {
	c := &Consumer{userActivityRepo: &stubUserActivityRepo{}}

	nacked := false
	d := amqp.Delivery{
		Body: []byte("not-json"),
		Acknowledger: &mockAcknowledger{nackFn: func(_, _ bool) error {
			nacked = true
			return nil
		}},
	}
	c.handleUserActivity(context.Background(), d)

	if !nacked {
		t.Error("expected Nack on invalid JSON")
	}
}
```

Para `TestHandleUserActivity_NacksOnInvalidJSON` se necesita un mock de `amqp.Acknowledger`. Leer el archivo `consumer_test.go` existente para ver si ya existe uno. Si no existe, agregar:

```go
type mockAcknowledger struct {
	ackFn  func(multiple bool) error
	nackFn func(multiple, requeue bool) error
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	if m.ackFn != nil {
		return m.ackFn(multiple)
	}
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	if m.nackFn != nil {
		return m.nackFn(multiple, requeue)
	}
	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error { return nil }
```

- [ ] **Step 3: Correr los tests para verificar que fallan**

Run: `go test ./internal/messaging/rabbitmq/... -run TestHandleUserActivity -v`
Expected: FAIL — `handleUserActivity` no existe

- [ ] **Step 4: Implementar ConsumeUserActivity y handleUserActivity en consumer.go**

```go
func (c *Consumer) ConsumeUserActivity(ctx context.Context) {
	c.runLoop(ctx, QueueUserActivity, c.handleUserActivity)
}

func (c *Consumer) handleUserActivity(ctx context.Context, d amqp.Delivery) {
	var evt domain.UserActivityEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		log.Printf("rabbitmq: unmarshal user activity event: %v", err)
		_ = d.Nack(false, false)
		return
	}
	if c.userActivityRepo == nil {
		_ = d.Ack(false)
		return
	}
	if err := c.userActivityRepo.UpdateLastActive(ctx, evt.UserID, evt.LastActive); err != nil {
		log.Printf("rabbitmq: update last_active for %s: %v", evt.UserID, err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}
```

- [ ] **Step 5: Correr los tests para verificar que pasan**

Run: `go test ./internal/messaging/rabbitmq/... -run TestHandleUserActivity -v`
Expected: PASS

Run: `go test ./internal/messaging/rabbitmq/...`
Expected: todos los tests pasan

- [ ] **Step 6: Commit**

```bash
git add internal/messaging/rabbitmq/consumer.go internal/messaging/rabbitmq/consumer_test.go
git commit -m "feat: add ConsumeUserActivity to process UserActivityEvent and update last_active"
```

---

## Task 10: Consumer — fanoutTweet con GetActiveFollowers + TTL + guard (TDD)

**Files:**
- Modify: `internal/messaging/rabbitmq/consumer.go`
- Modify: `internal/messaging/rabbitmq/consumer_test.go`

- [ ] **Step 1: Escribir test que verifica skip de follower inactivo**

Agregar al final de `consumer_test.go`:

```go
func TestFanoutTweet_SkipsFollowerWithExpiredActivity(t *testing.T) {
	expiredFollower := domain.FollowerActivity{
		ID:         uuid.New(),
		LastActive: time.Now().Add(-25 * time.Hour), // expired con 24h TTL
	}
	activeFollower := domain.FollowerActivity{
		ID:         uuid.New(),
		LastActive: time.Now().Add(-1 * time.Hour), // activo
	}
	fanout := &concurrentFanout{}
	c := &Consumer{
		fanout:        fanout,
		fanoutWorkers: fanoutConcurrency,
		activityTTL:   24 * time.Hour,
	}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := c.fanoutTweet(context.Background(), evt, []domain.FollowerActivity{expiredFollower, activeFollower})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if fanout.maxObserved != 1 {
		t.Errorf("want exactly 1 AppendTweet call (only active follower), got max observed %d", fanout.maxObserved)
	}
}

func TestFanoutTweet_AllExpired_ReturnsNil(t *testing.T) {
	expiredFollower := domain.FollowerActivity{
		ID:         uuid.New(),
		LastActive: time.Now().Add(-48 * time.Hour), // expirado
	}
	fanout := &concurrentFanout{}
	c := &Consumer{
		fanout:        fanout,
		fanoutWorkers: fanoutConcurrency,
		activityTTL:   24 * time.Hour,
	}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := c.fanoutTweet(context.Background(), evt, []domain.FollowerActivity{expiredFollower})

	if err != nil {
		t.Errorf("all-expired must not return error, got: %v", err)
	}
	if fanout.maxObserved > 0 {
		t.Error("expected no AppendTweet calls for all-expired followers")
	}
}
```

- [ ] **Step 2: Actualizar todos los tests existentes que usan fanoutTweet con []uuid.UUID**

Reemplazar en `TestHandleTweetCreated_FanoutIsConcurrent`:

```go
// cambiar:
followers := make([]uuid.UUID, numFollowers)
for i := range followers {
    followers[i] = uuid.New()
}
// por:
followers := make([]domain.FollowerActivity, numFollowers)
for i := range followers {
    followers[i] = domain.FollowerActivity{ID: uuid.New(), LastActive: time.Now()}
}
```

El struct del Consumer (sin followRepo — fanoutTweet no lo usa):
```go
c := &Consumer{
    fanout:        fanout,
    fanoutWorkers: fanoutConcurrency,
    activityTTL:   24 * time.Hour,
}
```

La llamada permanece igual:
```go
c.fanoutTweet(context.Background(), evt, followers)
```

Reemplazar en `TestFanoutTweet_AllFail_ReturnsError`:

```go
followers := []domain.FollowerActivity{
    {ID: uuid.New(), LastActive: time.Now()},
    {ID: uuid.New(), LastActive: time.Now()},
    {ID: uuid.New(), LastActive: time.Now()},
}
c := &Consumer{fanout: &errorFanout{}, fanoutWorkers: fanoutConcurrency, activityTTL: 24 * time.Hour}
err := c.fanoutTweet(context.Background(), evt, followers)
```

Reemplazar en `TestFanoutTweet_PublishesRetryOnAppendFailure`:

```go
followerActivity := domain.FollowerActivity{ID: followerID, LastActive: time.Now()}
// ...
_ = c.fanoutTweet(context.Background(), evt, []domain.FollowerActivity{followerActivity})
```

Reemplazar en `TestFanoutTweet_CountsRetryPublishAsHandled`:

```go
followers := []domain.FollowerActivity{
    {ID: uuid.New(), LastActive: time.Now()},
    {ID: uuid.New(), LastActive: time.Now()},
}
// ...
err := c.fanoutTweet(context.Background(), evt, followers)
```

Reemplazar en `TestFanoutTweet_PartialFail_ReturnsNil`:

```go
followers := []domain.FollowerActivity{
    {ID: uuid.New(), LastActive: time.Now()},
    {ID: uuid.New(), LastActive: time.Now()},
    {ID: uuid.New(), LastActive: time.Now()},
}
c := &Consumer{fanout: &partialErrorFanout{failCount: 2}, fanoutWorkers: fanoutConcurrency, activityTTL: 24 * time.Hour}
err := c.fanoutTweet(context.Background(), evt, followers)
```

- [ ] **Step 3: Correr los tests para verificar que fallan**

Run: `go test ./internal/messaging/rabbitmq/... -v`
Expected: errores de compilación por firma de `fanoutTweet` — eso es esperado

- [ ] **Step 4: Actualizar fanoutTweet y handleTweetCreated en consumer.go**

Reemplazar `handleTweetCreated` para usar `GetActiveFollowers`:

```go
func (c *Consumer) handleTweetCreated(ctx context.Context, d amqp.Delivery) {
	var evt domain.TweetCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		log.Printf("rabbitmq: unmarshal tweet event: %v", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	start := time.Now()
	activeSince := time.Now().Add(-c.activityTTL)
	followers, err := c.followRepo.GetActiveFollowers(ctx, evt.UserID, activeSince)
	if err != nil {
		log.Printf("rabbitmq: get active followers for %s: %v", evt.UserID, err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	if err := c.fanoutTweet(ctx, evt, followers); err != nil {
		log.Printf("rabbitmq: all fanout writes failed for tweet %s: %v", evt.TweetID, err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	_ = d.Ack(false)
}
```

Reemplazar `fanoutTweet` completo:

```go
func (c *Consumer) fanoutTweet(ctx context.Context, evt domain.TweetCreatedEvent, followers []domain.FollowerActivity) error {
	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}

	var (
		sem      = make(chan struct{}, c.fanoutWorkers)
		g, gctx  = errgroup.WithContext(ctx)
		eligible atomic.Int64
		handled  atomic.Int64
	)

	for _, follower := range followers {
		remaining := c.activityTTL - time.Since(follower.LastActive)
		if remaining <= 0 {
			continue // usuario pasó a inactivo entre el query y la escritura
		}
		eligible.Add(1)
		fid := follower.ID
		rem := remaining
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if err := c.fanout.AppendTweet(gctx, fid, item, rem); err != nil {
				log.Printf("rabbitmq: fanout tweet to %s: %v", fid, err)
				if c.retryPublisher != nil {
					if pubErr := c.retryPublisher.PublishFanoutRetry(ctx, domain.FanoutRetryEvent{
						FollowerID: fid,
						Tweet:      item,
					}); pubErr != nil {
						log.Printf("rabbitmq: publish retry for %s: %v", fid, pubErr)
						return nil
					}
					handled.Add(1)
				}
				return nil
			}
			handled.Add(1)
			return nil
		})
	}

	_ = g.Wait()

	// Solo falla si había followers elegibles y ninguno se procesó ni encoló retry.
	// Si todos los followers estaban expirados (eligible=0), es una condición válida.
	if eligible.Load() > 0 && handled.Load() == 0 {
		return errors.New("all fanout writes failed and no retries enqueued")
	}
	return nil
}
```

- [ ] **Step 5: Correr los tests**

Run: `go test ./internal/messaging/rabbitmq/... -v`
Expected: todos los tests pasan

- [ ] **Step 6: Commit**

```bash
git add internal/messaging/rabbitmq/consumer.go internal/messaging/rabbitmq/consumer_test.go
git commit -m "feat: fanoutTweet uses GetActiveFollowers with per-follower TTL and inactivity guard"
```

---

## Task 11: Usecase — timeline publica UserActivityEvent (TDD)

**Files:**
- Modify: `internal/usecase/mocks_test.go`
- Modify: `internal/usecase/timeline_test.go`
- Modify: `internal/usecase/timeline.go`
- Modify: `internal/handler/mocks_test.go`

- [ ] **Step 1: Agregar mockUserActivityPublisher en usecase/mocks_test.go**

```go
type mockUserActivityPublisher struct {
	publishErr error
	calls      []domain.UserActivityEvent
}

func (m *mockUserActivityPublisher) PublishUserActivity(_ context.Context, evt domain.UserActivityEvent) error {
	m.calls = append(m.calls, evt)
	return m.publishErr
}
```

- [ ] **Step 2: Escribir test que verifica publicación del evento**

Agregar al final de `usecase/timeline_test.go`:

```go
func TestTimelineUseCase_GetTimeline_PublishesActivityEvent(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{}}
	activityPub := &mockUserActivityPublisher{}

	uc := usecase.NewTimelineUseCase(userRepo, timelineRepo, activityPub)
	_, _ = uc.GetTimeline(context.Background(), userID)

	if len(activityPub.calls) != 1 {
		t.Fatalf("want 1 activity event published, got %d", len(activityPub.calls))
	}
	if activityPub.calls[0].UserID != userID {
		t.Errorf("want userID %s, got %s", userID, activityPub.calls[0].UserID)
	}
	if activityPub.calls[0].LastActive.IsZero() {
		t.Error("want non-zero LastActive in published event")
	}
}

func TestTimelineUseCase_GetTimeline_ActivityPublishErrorDoesNotFail(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{}}
	activityPub := &mockUserActivityPublisher{publishErr: errors.New("broker down")}

	uc := usecase.NewTimelineUseCase(userRepo, timelineRepo, activityPub)
	items, err := uc.GetTimeline(context.Background(), userID)

	if err != nil {
		t.Fatalf("activity publish error must not propagate: %v", err)
	}
	if items == nil {
		t.Error("want items, got nil")
	}
}
```

Agregar `"errors"` al import de `timeline_test.go` si no está.

- [ ] **Step 3: Actualizar los tests existentes para pasar el 3er argumento**

En los tres tests existentes (`TestTimelineUseCase_GetTimeline_OK`, `TestTimelineUseCase_GetTimeline_UserNotFound`, `TestTimelineUseCase_GetTimeline_EmptyWhenNoFollows`), reemplazar:

```go
uc := usecase.NewTimelineUseCase(userRepo, timelineRepo)
```

por:

```go
uc := usecase.NewTimelineUseCase(userRepo, timelineRepo, &mockUserActivityPublisher{})
```

- [ ] **Step 4: Correr los tests para verificar que fallan**

Run: `go test ./internal/usecase/... -run TestTimelineUseCase -v`
Expected: error de compilación — `NewTimelineUseCase` todavía tiene 2 params

- [ ] **Step 5: Actualizar usecase/timeline.go**

```go
package usecase

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TimelineUseCase struct {
	userRepo          domain.UserRepository
	timelineRepo      domain.TimelineRepository
	activityPublisher domain.UserActivityPublisher
}

func NewTimelineUseCase(userRepo domain.UserRepository, timelineRepo domain.TimelineRepository, activityPublisher domain.UserActivityPublisher) *TimelineUseCase {
	return &TimelineUseCase{
		userRepo:          userRepo,
		timelineRepo:      timelineRepo,
		activityPublisher: activityPublisher,
	}
}

func (uc *TimelineUseCase) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
		return nil, err
	}
	items, err := uc.timelineRepo.GetTimeline(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := uc.activityPublisher.PublishUserActivity(ctx, domain.UserActivityEvent{
		UserID:     userID,
		LastActive: time.Now(),
	}); err != nil {
		log.Printf("timeline: publish user activity for %s: %v", userID, err)
	}
	return items, nil
}
```

- [ ] **Step 6: Agregar noopUserActivityPublisher en handler/mocks_test.go**

```go
type noopUserActivityPublisher struct{}

func (n *noopUserActivityPublisher) PublishUserActivity(_ context.Context, _ domain.UserActivityEvent) error {
	return nil
}
```

- [ ] **Step 7: Correr todos los tests**

Run: `go test ./internal/usecase/... -v`
Expected: todos pasan

Run: `go build ./...`
Expected: errores de compilación esperados en handler/setup_test.go y main.go (se resuelven en Task 12)

- [ ] **Step 8: Commit**

```bash
git add internal/usecase/timeline.go internal/usecase/timeline_test.go \
        internal/usecase/mocks_test.go internal/handler/mocks_test.go
git commit -m "feat: timeline usecase publishes UserActivityEvent on every GetTimeline call"
```

---

## Task 12: Wiring en main.go y handler tests

**Files:**
- Modify: `cmd/api/main.go`
- Modify: `internal/handler/setup_test.go`

- [ ] **Step 1: Actualizar handler/setup_test.go**

Reemplazar las líneas del `testServer`:

```go
redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500, 24*time.Hour)
```

```go
handler.NewTimelineHandler(usecase.NewTimelineUseCase(userRepo, redisTimeline, &noopUserActivityPublisher{})),
```

Agregar `"time"` al import si no está.

- [ ] **Step 2: Verificar que los handler tests compilan**

Run: `go build ./internal/handler/...`
Expected: sin errores

- [ ] **Step 3: Actualizar main.go**

Reemplazar la construcción de `redisTimeline`:

```go
redisTimeline := redisrepo.NewTimelineRepository(rdb, pgTimelineRepo, cfg.TimelineLimit, cfg.ActivityTTL)
```

Reemplazar la construcción del `consumer`:

```go
consumer := rabbitmq.NewConsumer(amqpConn, followRepo, redisTimeline, pgTimelineRepo, cfg.FollowBackfillLimit).
    WithRetryPublisher(publisher).
    WithDeadLetterPublisher(rabbitmq.NewDeadLetterPublisher(publisher)).
    WithUserActivityRepo(userRepo).
    WithActivityTTL(cfg.ActivityTTL)
```

Agregar la goroutine del nuevo consumer:

```go
go consumer.ConsumeTweets(ctx)
go consumer.ConsumeFollows(ctx)
go consumer.ConsumeFanoutRetry(ctx)
go consumer.ConsumeUserActivity(ctx)
```

Reemplazar la construcción de `timelineUC`:

```go
timelineUC := usecase.NewTimelineUseCase(userRepo, redisTimeline, publisher)
```

- [ ] **Step 4: Compilar el binario completo**

Run: `go build ./...`
Expected: sin errores

- [ ] **Step 5: Correr todos los tests**

Run: `go test ./...`
Expected: todos los tests no-integration pasan

Run: `INTEGRATION=1 go test ./...`
Expected: todos los tests de integración pasan

- [ ] **Step 6: Commit final**

```bash
git add cmd/api/main.go internal/handler/setup_test.go
git commit -m "feat: wire activity TTL, UserActivityRepo and ConsumeUserActivity in main"
```
