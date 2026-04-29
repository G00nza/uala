# Iter 3 — RabbitMQ Async Messaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple Redis fanout from the HTTP request path — `POST /tweets` and `POST /follow` publish events to RabbitMQ queues and return immediately; consumers process events asynchronously.

**Architecture:** `TweetUseCase.CreateTweet` publishes a `TweetItem` JSON to `tweet.created` queue then returns 201. `FollowUseCase.Follow` publishes `{follower_id, followee_id}` to `user.followed` queue then returns 201. `TweetConsumer` reads from `tweet.created`, fetches followers from Postgres, and fans out to Redis. `FollowConsumer` reads from `user.followed`, fetches the followee's recent tweets from Postgres, and writes them to the follower's Redis Sorted Set. Both consumers run as goroutines in `main`.

**Tech Stack:** Go 1.25, `github.com/rabbitmq/amqp091-go`, RabbitMQ 3 management alpine, existing pgx/v5 + go-redis/v9 + net/http

**Starting state (after iter-2 complete):**
- `domain/follow.go` has `GetFollowers` on `FollowRepository`, `TimelineFanout`, `TweetItem` with JSON tags
- `usecase/tweet.go`: `NewTweetUseCase(userRepo, tweetRepo, followRepo, fanout)` — 4 args
- `usecase/follow.go`: `NewFollowUseCase(userRepo, followRepo)` — 2 args
- `infra/config.go` has `DatabaseURL`, `RedisURL`, `Port`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-compose.yml` | Modify | Add RabbitMQ 3-management-alpine service |
| `.env` | Modify | Add `RABBITMQ_URL`, `FOLLOW_BACKFILL_LIMIT` |
| `.env.example` | Modify | Add `RABBITMQ_URL`, `FOLLOW_BACKFILL_LIMIT` |
| `internal/infra/config.go` | Modify | Add `RabbitMQURL`, `FollowBackfillLimit` |
| `internal/domain/tweet.go` | Modify | Add `TweetPublisher` interface; add `GetByUserID` to `TweetRepository` |
| `internal/domain/follow.go` | Modify | Add `FollowPublisher` interface |
| `internal/repository/postgres/tweet.go` | Modify | Implement `GetByUserID` (JOIN users to get username) |
| `internal/repository/postgres/tweet_test.go` | Modify | Add `GetByUserID` integration tests |
| `internal/usecase/mocks_test.go` | Modify | Add `GetByUserID` to `mockTweetRepo`; add `mockTweetPublisher`, `mockFollowPublisher` |
| `internal/usecase/tweet.go` | Modify | 3-arg constructor — drop `followRepo+fanout`, add `publisher` |
| `internal/usecase/tweet_test.go` | Modify | Update constructor; test publisher is called |
| `internal/usecase/follow.go` | Modify | 3-arg constructor — add `publisher`; publish after save |
| `internal/usecase/follow_test.go` | Modify | Add publisher assertion tests |
| `internal/broker/rabbitmq/client.go` | Create | `Connect`, `DeclareQueues`, queue constants, `followEvent` struct |
| `internal/broker/rabbitmq/tweet_publisher.go` | Create | `TweetPublisher` — publishes `TweetItem` JSON to `tweet.created` |
| `internal/broker/rabbitmq/follow_publisher.go` | Create | `FollowPublisher` — publishes `followEvent` JSON to `user.followed` |
| `internal/broker/rabbitmq/tweet_consumer.go` | Create | `TweetConsumer.Handle` + `Start` goroutine |
| `internal/broker/rabbitmq/follow_consumer.go` | Create | `FollowConsumer.Handle` + `Start` goroutine |
| `internal/broker/rabbitmq/mocks_test.go` | Create | Test mocks for `FollowRepository`, `TimelineFanout`, `TweetRepository` |
| `internal/broker/rabbitmq/setup_test.go` | Create | `TestMain` + `purgeQueues` for integration tests |
| `internal/broker/rabbitmq/publisher_test.go` | Create | Integration tests — publish then consume raw message |
| `internal/broker/rabbitmq/tweet_consumer_test.go` | Create | Unit tests for `TweetConsumer.Handle` |
| `internal/broker/rabbitmq/follow_consumer_test.go` | Create | Unit tests for `FollowConsumer.Handle` |
| `cmd/api/main.go` | Modify | Wire broker, start consumers as goroutines |

---

### Task 1: Add RabbitMQ to docker-compose + env

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env`
- Modify: `.env.example`

- [ ] **Step 1: Replace docker-compose.yml**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: uala_postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    container_name: uala_redis
    command: redis-server --maxmemory-policy allkeys-lru
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  rabbitmq:
    image: rabbitmq:3-management-alpine
    container_name: uala_rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

- [ ] **Step 2: Append to .env**

```
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
FOLLOW_BACKFILL_LIMIT=10
```

- [ ] **Step 3: Replace .env.example**

```
POSTGRES_USER=uala
POSTGRES_PASSWORD=uala
POSTGRES_DB=uala
DATABASE_URL=postgres://uala:uala@localhost:5432/uala
REDIS_URL=redis://localhost:6379/0
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
FOLLOW_BACKFILL_LIMIT=10
PORT=8080
```

- [ ] **Step 4: Start services**

```bash
make up
```

Expected: postgres, redis, rabbitmq containers start.

- [ ] **Step 5: Verify all healthy**

```bash
docker compose ps
```

Expected: `uala_rabbitmq` shows `healthy`. Management UI available at http://localhost:15672 (guest/guest).

---

### Task 2: Add amqp091-go dep + update config

**Files:**
- Modify: `go.mod`, `go.sum` (auto)
- Modify: `internal/infra/config.go`

- [ ] **Step 1: Add dep**

```bash
go get github.com/rabbitmq/amqp091-go@latest
```

- [ ] **Step 2: Verify**

```bash
grep "amqp091" go.mod
```

Expected: `github.com/rabbitmq/amqp091-go v1.x.x`

- [ ] **Step 3: Rewrite internal/infra/config.go**

```go
package infra

import (
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL         string
	RedisURL            string
	RabbitMQURL         string
	FollowBackfillLimit int
	Port                string
}

func LoadConfig() Config {
	return Config{
		DatabaseURL:         getenv("DATABASE_URL", "postgres://uala:uala@localhost:5432/uala"),
		RedisURL:            getenv("REDIS_URL", "redis://localhost:6379/0"),
		RabbitMQURL:         getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		FollowBackfillLimit: getenvInt("FOLLOW_BACKFILL_LIMIT", 10),
		Port:                getenv("PORT", "8080"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
```

- [ ] **Step 4: Build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml .env .env.example go.mod go.sum internal/infra/config.go
git commit -m "feat: add RabbitMQ to infra — docker-compose, env, config, amqp091 dep"
```

---

### Task 3: Domain extensions + update postgres + update mocks

**Files:**
- Modify: `internal/domain/tweet.go`
- Modify: `internal/domain/follow.go`
- Modify: `internal/repository/postgres/tweet.go`
- Modify: `internal/usecase/mocks_test.go`

- [ ] **Step 1: Rewrite internal/domain/tweet.go**

```go
package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Tweet struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Content   string
	CreatedAt time.Time
}

type TweetRepository interface {
	Create(ctx context.Context, t *Tweet) error
	GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]TweetItem, error)
}

type TweetPublisher interface {
	PublishTweetCreated(ctx context.Context, item TweetItem) error
}
```

- [ ] **Step 2: Rewrite internal/domain/follow.go**

```go
package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Follow struct {
	FollowerID uuid.UUID
	FolloweeID uuid.UUID
	CreatedAt  time.Time
}

type TweetItem struct {
	ID        uuid.UUID `json:"id"`
	UserID    uuid.UUID `json:"user_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type FollowRepository interface {
	Create(ctx context.Context, f *Follow) error
	Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error)
	GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error)
}

type TimelineRepository interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]TweetItem, error)
}

type TimelineFanout interface {
	AppendTweet(ctx context.Context, userID uuid.UUID, item TweetItem) error
}

type FollowPublisher interface {
	PublishFollowCreated(ctx context.Context, followerID, followeeID uuid.UUID) error
}
```

- [ ] **Step 3: Append GetByUserID to internal/repository/postgres/tweet.go**

Add this method after the existing `Create` method:

```go
func (r *TweetRepository) GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM tweets t
		JOIN users u ON u.id = t.user_id
		WHERE t.user_id = $1
		ORDER BY t.created_at DESC
		LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []domain.TweetItem
	for rows.Next() {
		var item domain.TweetItem
		if err := rows.Scan(&item.ID, &item.UserID, &item.Username, &item.Content, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if items == nil {
		items = []domain.TweetItem{}
	}
	return items, rows.Err()
}
```

- [ ] **Step 4: Rewrite internal/usecase/mocks_test.go**

```go
package usecase_test

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type mockUserRepo struct {
	createErr error
	getUser   *domain.User
	getErr    error
}

func (m *mockUserRepo) Create(ctx context.Context, u *domain.User) error { return m.createErr }
func (m *mockUserRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	return m.getUser, m.getErr
}

type mockTweetRepo struct {
	createErr   error
	byUserItems []domain.TweetItem
	byUserErr   error
}

func (m *mockTweetRepo) Create(ctx context.Context, t *domain.Tweet) error { return m.createErr }
func (m *mockTweetRepo) GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]domain.TweetItem, error) {
	return m.byUserItems, m.byUserErr
}

type mockFollowRepo struct {
	existsResult    bool
	existsErr       error
	createErr       error
	followers       []uuid.UUID
	getFollowersErr error
}

func (m *mockFollowRepo) Create(ctx context.Context, f *domain.Follow) error { return m.createErr }
func (m *mockFollowRepo) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	return m.existsResult, m.existsErr
}
func (m *mockFollowRepo) GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error) {
	return m.followers, m.getFollowersErr
}

type mockTimelineRepo struct {
	items []domain.TweetItem
	err   error
}

func (m *mockTimelineRepo) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}

type mockTimelineFanout struct {
	appendErr error
	calls     []fanoutCall
}

type fanoutCall struct {
	userID uuid.UUID
	item   domain.TweetItem
}

func (m *mockTimelineFanout) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem) error {
	m.calls = append(m.calls, fanoutCall{userID: userID, item: item})
	return m.appendErr
}

type mockTweetPublisher struct {
	publishErr error
	calls      []domain.TweetItem
}

func (m *mockTweetPublisher) PublishTweetCreated(ctx context.Context, item domain.TweetItem) error {
	m.calls = append(m.calls, item)
	return m.publishErr
}

type mockFollowPublisher struct {
	publishErr error
	calls      []followPublishCall
}

type followPublishCall struct {
	followerID uuid.UUID
	followeeID uuid.UUID
}

func (m *mockFollowPublisher) PublishFollowCreated(ctx context.Context, followerID, followeeID uuid.UUID) error {
	m.calls = append(m.calls, followPublishCall{followerID, followeeID})
	return m.publishErr
}
```

- [ ] **Step 5: Build and run existing tests**

```bash
go build ./...
go test ./internal/usecase/... -v
```

Expected: build succeeds; all existing usecase tests PASS (tweet still compiles with 4-arg signature; domain additions don't break anything).

- [ ] **Step 6: Commit**

```bash
git add internal/domain/tweet.go internal/domain/follow.go internal/repository/postgres/tweet.go internal/usecase/mocks_test.go
git commit -m "feat: extend domain — TweetPublisher, FollowPublisher, TweetRepository.GetByUserID"
```

---

### Task 4: Postgres GetByUserID integration tests

**Files:**
- Modify: `internal/repository/postgres/tweet_test.go`

- [ ] **Step 1: Append to internal/repository/postgres/tweet_test.go**

Add after the existing tests (add `"fmt"` to the import block):

```go
func seedTweet(t *testing.T, userID uuid.UUID, content string) *domain.Tweet {
	t.Helper()
	repo := postgres.NewTweetRepository(testDB)
	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    userID,
		Content:   content,
		CreatedAt: time.Now().UTC(),
	}
	if err := repo.Create(context.Background(), tweet); err != nil {
		t.Fatalf("seedTweet: %v", err)
	}
	return tweet
}

func TestTweetRepository_GetByUserID(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice_get")

	seedTweet(t, alice.ID, "tweet 1")
	seedTweet(t, alice.ID, "tweet 2")

	repo := postgres.NewTweetRepository(testDB)
	items, err := repo.GetByUserID(context.Background(), alice.ID, 10)
	if err != nil {
		t.Fatalf("GetByUserID: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Username != "alice_get" {
		t.Fatalf("want username 'alice_get', got %s", items[0].Username)
	}
}

func TestTweetRepository_GetByUserID_RespectsLimit(t *testing.T) {
	truncate(t)
	bob := seedUser(t, "bob_limit")

	for i := 0; i < 5; i++ {
		seedTweet(t, bob.ID, fmt.Sprintf("tweet %d", i))
	}

	repo := postgres.NewTweetRepository(testDB)
	items, err := repo.GetByUserID(context.Background(), bob.ID, 3)
	if err != nil {
		t.Fatalf("GetByUserID: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("want 3 items (limit), got %d", len(items))
	}
}

func TestTweetRepository_GetByUserID_Empty(t *testing.T) {
	truncate(t)
	carol := seedUser(t, "carol_empty")

	repo := postgres.NewTweetRepository(testDB)
	items, err := repo.GetByUserID(context.Background(), carol.ID, 10)
	if err != nil {
		t.Fatalf("GetByUserID: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}
```

- [ ] **Step 2: Run GetByUserID tests**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -run TestTweetRepository_GetByUserID -v
```

Expected:
```
--- PASS: TestTweetRepository_GetByUserID (0.00s)
--- PASS: TestTweetRepository_GetByUserID_RespectsLimit (0.00s)
--- PASS: TestTweetRepository_GetByUserID_Empty (0.00s)
PASS
```

- [ ] **Step 3: Run all postgres tests (no regressions)**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/repository/postgres/tweet_test.go internal/repository/postgres/tweet.go
git commit -m "feat: TweetRepository.GetByUserID with integration tests"
```

---

### Task 5: Update TweetUseCase — 3-arg constructor + publisher

**Files:**
- Modify: `internal/usecase/tweet.go`
- Modify: `internal/usecase/tweet_test.go`

- [ ] **Step 1: Rewrite internal/usecase/tweet_test.go**

```go
package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestTweetUseCase_CreateTweet_OK(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "alice"}}
	publisher := &mockTweetPublisher{}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher)

	tweet, err := uc.CreateTweet(context.Background(), userID, "hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tweet.Content != "hello world" {
		t.Fatalf("want 'hello world', got %s", tweet.Content)
	}
	if tweet.ID == (uuid.UUID{}) {
		t.Fatal("ID must be set")
	}
}

func TestTweetUseCase_CreateTweet_UserNotFound(t *testing.T) {
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, &mockTweetPublisher{})

	_, err := uc.CreateTweet(context.Background(), uuid.New(), "hello")
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTweetUseCase_CreateTweet_PublishesEvent(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "bob"}}
	publisher := &mockTweetPublisher{}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher)

	_, err := uc.CreateTweet(context.Background(), userID, "publish this")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("want 1 publish call, got %d", len(publisher.calls))
	}
	item := publisher.calls[0]
	if item.Content != "publish this" {
		t.Fatalf("want content 'publish this', got %s", item.Content)
	}
	if item.Username != "bob" {
		t.Fatalf("want username 'bob', got %s", item.Username)
	}
	if item.UserID != userID {
		t.Fatalf("want userID %s, got %s", userID, item.UserID)
	}
}

func TestTweetUseCase_CreateTweet_PublishError_Fails(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "carol"}}
	publisher := &mockTweetPublisher{publishErr: errors.New("rabbitmq down")}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher)

	_, err := uc.CreateTweet(context.Background(), userID, "should fail")
	if err == nil {
		t.Fatal("want error from publish failure, got nil")
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/usecase/... -run TestTweetUseCase 2>&1 | head -10
```

Expected: compile error — wrong number of arguments to `NewTweetUseCase`.

- [ ] **Step 3: Rewrite internal/usecase/tweet.go**

```go
package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TweetUseCase struct {
	userRepo  domain.UserRepository
	tweetRepo domain.TweetRepository
	publisher domain.TweetPublisher
}

func NewTweetUseCase(
	userRepo domain.UserRepository,
	tweetRepo domain.TweetRepository,
	publisher domain.TweetPublisher,
) *TweetUseCase {
	return &TweetUseCase{userRepo: userRepo, tweetRepo: tweetRepo, publisher: publisher}
}

func (uc *TweetUseCase) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	user, err := uc.userRepo.GetByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	t := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    userID,
		Content:   content,
		CreatedAt: time.Now().UTC(),
	}
	if err := uc.tweetRepo.Create(ctx, t); err != nil {
		return nil, err
	}
	item := domain.TweetItem{
		ID:        t.ID,
		UserID:    t.UserID,
		Username:  user.Username,
		Content:   t.Content,
		CreatedAt: t.CreatedAt,
	}
	if err := uc.publisher.PublishTweetCreated(ctx, item); err != nil {
		return nil, err
	}
	return t, nil
}
```

- [ ] **Step 4: Run tweet usecase tests**

```bash
go test ./internal/usecase/... -run TestTweetUseCase -v
```

Expected:
```
--- PASS: TestTweetUseCase_CreateTweet_OK (0.00s)
--- PASS: TestTweetUseCase_CreateTweet_UserNotFound (0.00s)
--- PASS: TestTweetUseCase_CreateTweet_PublishesEvent (0.00s)
--- PASS: TestTweetUseCase_CreateTweet_PublishError_Fails (0.00s)
PASS
```

- [ ] **Step 5: Run all usecase tests**

```bash
go test ./internal/usecase/... -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/usecase/tweet.go internal/usecase/tweet_test.go
git commit -m "feat: TweetUseCase — async publish via TweetPublisher (3-arg)"
```

---

### Task 6: Update FollowUseCase — add publisher

**Files:**
- Modify: `internal/usecase/follow.go`
- Modify: `internal/usecase/follow_test.go`

- [ ] **Step 1: Rewrite internal/usecase/follow_test.go**

```go
package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestFollowUseCase_Follow_OK(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: followee}}
	publisher := &mockFollowPublisher{}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{}, publisher)

	if err := uc.Follow(context.Background(), follower, followee); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("want 1 publish call, got %d", len(publisher.calls))
	}
	if publisher.calls[0].followerID != follower {
		t.Fatalf("want followerID %s, got %s", follower, publisher.calls[0].followerID)
	}
}

func TestFollowUseCase_Follow_SelfFollow(t *testing.T) {
	id := uuid.New()
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, &mockFollowRepo{}, &mockFollowPublisher{})

	err := uc.Follow(context.Background(), id, id)
	if err != domain.ErrSelfFollow {
		t.Fatalf("want ErrSelfFollow, got %v", err)
	}
}

func TestFollowUseCase_Follow_AlreadyFollowing(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	followRepo := &mockFollowRepo{existsResult: true}
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, followRepo, &mockFollowPublisher{})

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowUseCase_Follow_FolloweeNotFound(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{}, &mockFollowPublisher{})

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestFollowUseCase_Follow_PublishError_Fails(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: followee}}
	publisher := &mockFollowPublisher{publishErr: errors.New("rabbitmq down")}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{}, publisher)

	err := uc.Follow(context.Background(), follower, followee)
	if err == nil {
		t.Fatal("want error from publish failure, got nil")
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/usecase/... -run TestFollowUseCase 2>&1 | head -10
```

Expected: compile error — wrong number of arguments to `NewFollowUseCase`.

- [ ] **Step 3: Rewrite internal/usecase/follow.go**

```go
package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type FollowUseCase struct {
	userRepo   domain.UserRepository
	followRepo domain.FollowRepository
	publisher  domain.FollowPublisher
}

func NewFollowUseCase(
	userRepo domain.UserRepository,
	followRepo domain.FollowRepository,
	publisher domain.FollowPublisher,
) *FollowUseCase {
	return &FollowUseCase{userRepo: userRepo, followRepo: followRepo, publisher: publisher}
}

func (uc *FollowUseCase) Follow(ctx context.Context, followerID, followeeID uuid.UUID) error {
	if followerID == followeeID {
		return domain.ErrSelfFollow
	}
	exists, err := uc.followRepo.Exists(ctx, followerID, followeeID)
	if err != nil {
		return err
	}
	if exists {
		return domain.ErrAlreadyFollowing
	}
	if _, err := uc.userRepo.GetByID(ctx, followeeID); err != nil {
		return err
	}
	f := &domain.Follow{
		FollowerID: followerID,
		FolloweeID: followeeID,
		CreatedAt:  time.Now().UTC(),
	}
	if err := uc.followRepo.Create(ctx, f); err != nil {
		return err
	}
	return uc.publisher.PublishFollowCreated(ctx, followerID, followeeID)
}
```

- [ ] **Step 4: Run all usecase tests**

```bash
go test ./internal/usecase/... -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase/follow.go internal/usecase/follow_test.go
git commit -m "feat: FollowUseCase — async publish via FollowPublisher"
```

---

### Task 7: Broker client + publishers

**Files:**
- Create: `internal/broker/rabbitmq/client.go`
- Create: `internal/broker/rabbitmq/tweet_publisher.go`
- Create: `internal/broker/rabbitmq/follow_publisher.go`

- [ ] **Step 1: Create internal/broker/rabbitmq/client.go**

```go
package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
)

const (
	QueueTweetCreated = "tweet.created"
	QueueUserFollowed = "user.followed"
)

type followEvent struct {
	FollowerID uuid.UUID `json:"follower_id"`
	FolloweeID uuid.UUID `json:"followee_id"`
}

func Connect(url string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	return conn, nil
}

func DeclareQueues(ch *amqp.Channel) error {
	for _, name := range []string{QueueTweetCreated, QueueUserFollowed} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare queue %s: %w", name, err)
		}
	}
	return nil
}
```

- [ ] **Step 2: Create internal/broker/rabbitmq/tweet_publisher.go**

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type TweetPublisher struct {
	conn *amqp.Connection
}

func NewTweetPublisher(conn *amqp.Connection) *TweetPublisher {
	return &TweetPublisher{conn: conn}
}

func (p *TweetPublisher) PublishTweetCreated(ctx context.Context, item domain.TweetItem) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := DeclareQueues(ch); err != nil {
		return err
	}
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", QueueTweetCreated, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
}
```

- [ ] **Step 3: Create internal/broker/rabbitmq/follow_publisher.go**

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
)

type FollowPublisher struct {
	conn *amqp.Connection
}

func NewFollowPublisher(conn *amqp.Connection) *FollowPublisher {
	return &FollowPublisher{conn: conn}
}

func (p *FollowPublisher) PublishFollowCreated(ctx context.Context, followerID, followeeID uuid.UUID) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := DeclareQueues(ch); err != nil {
		return err
	}
	data, err := json.Marshal(followEvent{FollowerID: followerID, FolloweeID: followeeID})
	if err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", QueueUserFollowed, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
}
```

- [ ] **Step 4: Build broker package**

```bash
go build ./internal/broker/rabbitmq/...
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add internal/broker/rabbitmq/client.go internal/broker/rabbitmq/tweet_publisher.go internal/broker/rabbitmq/follow_publisher.go
git commit -m "feat: RabbitMQ client + TweetPublisher + FollowPublisher"
```

---

### Task 8: Broker integration tests — publishers

**Files:**
- Create: `internal/broker/rabbitmq/setup_test.go`
- Create: `internal/broker/rabbitmq/publisher_test.go`

- [ ] **Step 1: Create internal/broker/rabbitmq/setup_test.go**

```go
package rabbitmq_test

import (
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/broker/rabbitmq"
)

var testConn *amqp.Connection

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	var err error
	for i := 0; i < 5; i++ {
		testConn, err = rabbitmq.Connect(url)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		panic("rabbitmq connect: " + err.Error())
	}
	code := m.Run()
	testConn.Close()
	os.Exit(code)
}

func purgeQueues(t *testing.T, names ...string) {
	t.Helper()
	ch, _ := testConn.Channel()
	defer ch.Close()
	for _, name := range names {
		ch.QueuePurge(name, false)
	}
}
```

- [ ] **Step 2: Create internal/broker/rabbitmq/publisher_test.go**

```go
package rabbitmq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/broker/rabbitmq"
	"uala/internal/domain"
)

func consumeOne(t *testing.T, queue string) amqp.Delivery {
	t.Helper()
	ch, _ := testConn.Channel()
	ch.QueueDeclare(queue, true, false, false, false, nil)
	msgs, _ := ch.Consume(queue, "", true, false, false, false, nil)
	select {
	case msg := <-msgs:
		ch.Close()
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: no message received in " + queue)
		return amqp.Delivery{}
	}
}

func TestTweetPublisher_PublishTweetCreated(t *testing.T) {
	purgeQueues(t, rabbitmq.QueueTweetCreated)

	pub := rabbitmq.NewTweetPublisher(testConn)
	item := domain.TweetItem{
		ID:        uuid.New(),
		UserID:    uuid.New(),
		Username:  "alice",
		Content:   "publisher integration test",
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}

	if err := pub.PublishTweetCreated(context.Background(), item); err != nil {
		t.Fatalf("PublishTweetCreated: %v", err)
	}

	msg := consumeOne(t, rabbitmq.QueueTweetCreated)
	var got domain.TweetItem
	if err := json.Unmarshal(msg.Body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Content != "publisher integration test" {
		t.Fatalf("want 'publisher integration test', got %s", got.Content)
	}
	if got.Username != "alice" {
		t.Fatalf("want 'alice', got %s", got.Username)
	}
}

func TestFollowPublisher_PublishFollowCreated(t *testing.T) {
	purgeQueues(t, rabbitmq.QueueUserFollowed)

	pub := rabbitmq.NewFollowPublisher(testConn)
	followerID := uuid.New()
	followeeID := uuid.New()

	if err := pub.PublishFollowCreated(context.Background(), followerID, followeeID); err != nil {
		t.Fatalf("PublishFollowCreated: %v", err)
	}

	msg := consumeOne(t, rabbitmq.QueueUserFollowed)
	var got struct {
		FollowerID uuid.UUID `json:"follower_id"`
		FolloweeID uuid.UUID `json:"followee_id"`
	}
	if err := json.Unmarshal(msg.Body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.FollowerID != followerID {
		t.Fatalf("want followerID %s, got %s", followerID, got.FollowerID)
	}
	if got.FolloweeID != followeeID {
		t.Fatalf("want followeeID %s, got %s", followeeID, got.FolloweeID)
	}
}
```

- [ ] **Step 3: Run publisher integration tests**

```bash
INTEGRATION=1 go test ./internal/broker/rabbitmq/... -run "TestTweetPublisher|TestFollowPublisher" -v
```

Expected:
```
--- PASS: TestTweetPublisher_PublishTweetCreated (0.00s)
--- PASS: TestFollowPublisher_PublishFollowCreated (0.00s)
PASS
```

- [ ] **Step 4: Commit**

```bash
git add internal/broker/rabbitmq/setup_test.go internal/broker/rabbitmq/publisher_test.go
git commit -m "feat: RabbitMQ publisher integration tests"
```

---

### Task 9: TweetConsumer + unit tests

**Files:**
- Create: `internal/broker/rabbitmq/mocks_test.go`
- Create: `internal/broker/rabbitmq/tweet_consumer.go`
- Create: `internal/broker/rabbitmq/tweet_consumer_test.go`

- [ ] **Step 1: Create internal/broker/rabbitmq/mocks_test.go**

```go
package rabbitmq_test

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type mockFollowRepo struct {
	followers       []uuid.UUID
	getFollowersErr error
	createErr       error
	existsResult    bool
	existsErr       error
}

func (m *mockFollowRepo) GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error) {
	return m.followers, m.getFollowersErr
}
func (m *mockFollowRepo) Create(ctx context.Context, f *domain.Follow) error { return m.createErr }
func (m *mockFollowRepo) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	return m.existsResult, m.existsErr
}

type mockTimelineFanout struct {
	appendErr error
	calls     []brokerFanoutCall
}

type brokerFanoutCall struct {
	userID uuid.UUID
	item   domain.TweetItem
}

func (m *mockTimelineFanout) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem) error {
	m.calls = append(m.calls, brokerFanoutCall{userID, item})
	return m.appendErr
}

type mockBrokerTweetRepo struct {
	byUserItems []domain.TweetItem
	byUserErr   error
	createErr   error
}

func (m *mockBrokerTweetRepo) Create(ctx context.Context, t *domain.Tweet) error { return m.createErr }
func (m *mockBrokerTweetRepo) GetByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]domain.TweetItem, error) {
	return m.byUserItems, m.byUserErr
}
```

- [ ] **Step 2: Create internal/broker/rabbitmq/tweet_consumer_test.go**

```go
package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/broker/rabbitmq"
	"uala/internal/domain"
)

func TestTweetConsumer_Handle_FansoutToFollowers(t *testing.T) {
	follower1 := uuid.New()
	follower2 := uuid.New()
	authorID := uuid.New()

	followRepo := &mockFollowRepo{followers: []uuid.UUID{follower1, follower2}}
	fanout := &mockTimelineFanout{}
	c := rabbitmq.NewTweetConsumer(nil, followRepo, fanout)

	item := domain.TweetItem{
		ID: uuid.New(), UserID: authorID, Username: "bob",
		Content: "fanout test", CreatedAt: time.Now().UTC(),
	}
	if err := c.Handle(context.Background(), item); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(fanout.calls) != 2 {
		t.Fatalf("want 2 fanout calls, got %d", len(fanout.calls))
	}
	if fanout.calls[0].item.Content != "fanout test" {
		t.Fatalf("want 'fanout test', got %s", fanout.calls[0].item.Content)
	}
}

func TestTweetConsumer_Handle_NoFollowers_NoFanout(t *testing.T) {
	followRepo := &mockFollowRepo{followers: []uuid.UUID{}}
	fanout := &mockTimelineFanout{}
	c := rabbitmq.NewTweetConsumer(nil, followRepo, fanout)

	if err := c.Handle(context.Background(), domain.TweetItem{UserID: uuid.New()}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(fanout.calls) != 0 {
		t.Fatalf("want 0 fanout calls, got %d", len(fanout.calls))
	}
}

func TestTweetConsumer_Handle_GetFollowersError_ReturnsError(t *testing.T) {
	followRepo := &mockFollowRepo{getFollowersErr: domain.ErrNotFound}
	c := rabbitmq.NewTweetConsumer(nil, followRepo, &mockTimelineFanout{})

	if err := c.Handle(context.Background(), domain.TweetItem{UserID: uuid.New()}); err == nil {
		t.Fatal("want error, got nil")
	}
}

func TestTweetConsumer_Handle_FanoutError_IsIgnored(t *testing.T) {
	follower1 := uuid.New()
	followRepo := &mockFollowRepo{followers: []uuid.UUID{follower1}}
	fanout := &mockTimelineFanout{appendErr: domain.ErrNotFound}
	c := rabbitmq.NewTweetConsumer(nil, followRepo, fanout)

	if err := c.Handle(context.Background(), domain.TweetItem{UserID: uuid.New(), Content: "test"}); err != nil {
		t.Fatalf("fanout error must not fail handler, got: %v", err)
	}
}
```

- [ ] **Step 3: Run — expect compile error (NewTweetConsumer undefined)**

```bash
go test ./internal/broker/rabbitmq/... -run TestTweetConsumer 2>&1 | head -5
```

Expected: `undefined: rabbitmq.NewTweetConsumer`

- [ ] **Step 4: Create internal/broker/rabbitmq/tweet_consumer.go**

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type TweetConsumer struct {
	conn       *amqp.Connection
	followRepo domain.FollowRepository
	fanout     domain.TimelineFanout
}

func NewTweetConsumer(conn *amqp.Connection, followRepo domain.FollowRepository, fanout domain.TimelineFanout) *TweetConsumer {
	return &TweetConsumer{conn: conn, followRepo: followRepo, fanout: fanout}
}

func (c *TweetConsumer) Handle(ctx context.Context, item domain.TweetItem) error {
	followers, err := c.followRepo.GetFollowers(ctx, item.UserID)
	if err != nil {
		return fmt.Errorf("get followers: %w", err)
	}
	for _, followerID := range followers {
		if err := c.fanout.AppendTweet(ctx, followerID, item); err != nil {
			log.Printf("fanout tweet to %s: %v", followerID, err)
		}
	}
	return nil
}

func (c *TweetConsumer) Start(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	if err := DeclareQueues(ch); err != nil {
		return err
	}
	msgs, err := ch.Consume(QueueTweetCreated, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", QueueTweetCreated, err)
	}
	go func() {
		defer ch.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var item domain.TweetItem
				if err := json.Unmarshal(msg.Body, &item); err != nil {
					log.Printf("unmarshal tweet event: %v", err)
					msg.Nack(false, false)
					continue
				}
				if err := c.Handle(ctx, item); err != nil {
					log.Printf("handle tweet event: %v", err)
					msg.Nack(false, true)
				} else {
					msg.Ack(false)
				}
			}
		}
	}()
	return nil
}
```

- [ ] **Step 5: Run unit tests**

```bash
go test ./internal/broker/rabbitmq/... -run TestTweetConsumer -v
```

Expected:
```
--- PASS: TestTweetConsumer_Handle_FansoutToFollowers (0.00s)
--- PASS: TestTweetConsumer_Handle_NoFollowers_NoFanout (0.00s)
--- PASS: TestTweetConsumer_Handle_GetFollowersError_ReturnsError (0.00s)
--- PASS: TestTweetConsumer_Handle_FanoutError_IsIgnored (0.00s)
PASS
```

- [ ] **Step 6: Commit**

```bash
git add internal/broker/rabbitmq/mocks_test.go internal/broker/rabbitmq/tweet_consumer.go internal/broker/rabbitmq/tweet_consumer_test.go
git commit -m "feat: TweetConsumer — async fanout to Redis on tweet.created event"
```

---

### Task 10: FollowConsumer + unit tests

**Files:**
- Create: `internal/broker/rabbitmq/follow_consumer.go`
- Create: `internal/broker/rabbitmq/follow_consumer_test.go`

- [ ] **Step 1: Create internal/broker/rabbitmq/follow_consumer_test.go**

```go
package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/broker/rabbitmq"
	"uala/internal/domain"
)

func TestFollowConsumer_Handle_BackfillsTweets(t *testing.T) {
	followerID := uuid.New()
	followeeID := uuid.New()

	tweetItems := []domain.TweetItem{
		{ID: uuid.New(), UserID: followeeID, Username: "bob", Content: "tweet A", CreatedAt: time.Now().UTC()},
		{ID: uuid.New(), UserID: followeeID, Username: "bob", Content: "tweet B", CreatedAt: time.Now().UTC()},
	}
	tweetRepo := &mockBrokerTweetRepo{byUserItems: tweetItems}
	fanout := &mockTimelineFanout{}
	c := rabbitmq.NewFollowConsumer(nil, tweetRepo, fanout, 10)

	if err := c.Handle(context.Background(), followerID, followeeID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(fanout.calls) != 2 {
		t.Fatalf("want 2 fanout calls, got %d", len(fanout.calls))
	}
	for _, call := range fanout.calls {
		if call.userID != followerID {
			t.Fatalf("want fanout to follower %s, got %s", followerID, call.userID)
		}
	}
}

func TestFollowConsumer_Handle_NoTweets_NoFanout(t *testing.T) {
	tweetRepo := &mockBrokerTweetRepo{byUserItems: []domain.TweetItem{}}
	fanout := &mockTimelineFanout{}
	c := rabbitmq.NewFollowConsumer(nil, tweetRepo, fanout, 10)

	if err := c.Handle(context.Background(), uuid.New(), uuid.New()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(fanout.calls) != 0 {
		t.Fatalf("want 0 fanout calls, got %d", len(fanout.calls))
	}
}

func TestFollowConsumer_Handle_GetByUserIDError_ReturnsError(t *testing.T) {
	tweetRepo := &mockBrokerTweetRepo{byUserErr: domain.ErrNotFound}
	c := rabbitmq.NewFollowConsumer(nil, tweetRepo, &mockTimelineFanout{}, 10)

	if err := c.Handle(context.Background(), uuid.New(), uuid.New()); err == nil {
		t.Fatal("want error, got nil")
	}
}
```

- [ ] **Step 2: Run — expect compile error (NewFollowConsumer undefined)**

```bash
go test ./internal/broker/rabbitmq/... -run TestFollowConsumer 2>&1 | head -5
```

Expected: `undefined: rabbitmq.NewFollowConsumer`

- [ ] **Step 3: Create internal/broker/rabbitmq/follow_consumer.go**

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
	"uala/internal/domain"
)

type FollowConsumer struct {
	conn          *amqp.Connection
	tweetRepo     domain.TweetRepository
	fanout        domain.TimelineFanout
	backfillLimit int
}

func NewFollowConsumer(conn *amqp.Connection, tweetRepo domain.TweetRepository, fanout domain.TimelineFanout, backfillLimit int) *FollowConsumer {
	return &FollowConsumer{conn: conn, tweetRepo: tweetRepo, fanout: fanout, backfillLimit: backfillLimit}
}

func (c *FollowConsumer) Handle(ctx context.Context, followerID, followeeID uuid.UUID) error {
	items, err := c.tweetRepo.GetByUserID(ctx, followeeID, c.backfillLimit)
	if err != nil {
		return fmt.Errorf("get tweets for backfill: %w", err)
	}
	for _, item := range items {
		if err := c.fanout.AppendTweet(ctx, followerID, item); err != nil {
			log.Printf("backfill fanout to %s: %v", followerID, err)
		}
	}
	return nil
}

func (c *FollowConsumer) Start(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	if err := DeclareQueues(ch); err != nil {
		return err
	}
	msgs, err := ch.Consume(QueueUserFollowed, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", QueueUserFollowed, err)
	}
	go func() {
		defer ch.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var event followEvent
				if err := json.Unmarshal(msg.Body, &event); err != nil {
					log.Printf("unmarshal follow event: %v", err)
					msg.Nack(false, false)
					continue
				}
				if err := c.Handle(ctx, event.FollowerID, event.FolloweeID); err != nil {
					log.Printf("handle follow event: %v", err)
					msg.Nack(false, true)
				} else {
					msg.Ack(false)
				}
			}
		}
	}()
	return nil
}
```

- [ ] **Step 4: Run all broker tests**

```bash
go test ./internal/broker/rabbitmq/... -run TestFollowConsumer -v
```

Expected:
```
--- PASS: TestFollowConsumer_Handle_BackfillsTweets (0.00s)
--- PASS: TestFollowConsumer_Handle_NoTweets_NoFanout (0.00s)
--- PASS: TestFollowConsumer_Handle_GetByUserIDError_ReturnsError (0.00s)
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/broker/rabbitmq/follow_consumer.go internal/broker/rabbitmq/follow_consumer_test.go
git commit -m "feat: FollowConsumer — backfill Redis timeline on user.followed event"
```

---

### Task 11: Wire main.go

**Files:**
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Rewrite cmd/api/main.go**

```go
package main

import (
	"context"
	"log"
	"net/http"

	"uala/internal/broker/rabbitmq"
	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"
)

func main() {
	cfg := infra.LoadConfig()
	ctx := context.Background()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("postgres:", err)
	}
	defer db.Close()

	if err := postgres.Migrate(ctx, db); err != nil {
		log.Fatal("migrate:", err)
	}

	rdb, err := redisrepo.Connect(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatal("redis:", err)
	}
	defer rdb.Close()

	conn, err := rabbitmq.Connect(cfg.RabbitMQURL)
	if err != nil {
		log.Fatal("rabbitmq:", err)
	}
	defer conn.Close()

	userRepo := postgres.NewUserRepository(db)
	tweetRepo := postgres.NewTweetRepository(db)
	followRepo := postgres.NewFollowRepository(db)
	pgTimelineRepo := postgres.NewTimelineRepository(db)

	redisTimeline := redisrepo.NewTimelineRepository(rdb, pgTimelineRepo)

	tweetPub := rabbitmq.NewTweetPublisher(conn)
	followPub := rabbitmq.NewFollowPublisher(conn)

	tweetConsumer := rabbitmq.NewTweetConsumer(conn, followRepo, redisTimeline)
	followConsumer := rabbitmq.NewFollowConsumer(conn, tweetRepo, redisTimeline, cfg.FollowBackfillLimit)

	if err := tweetConsumer.Start(ctx); err != nil {
		log.Fatal("tweet consumer:", err)
	}
	if err := followConsumer.Start(ctx); err != nil {
		log.Fatal("follow consumer:", err)
	}

	userUC := usecase.NewUserUseCase(userRepo)
	tweetUC := usecase.NewTweetUseCase(userRepo, tweetRepo, tweetPub)
	followUC := usecase.NewFollowUseCase(userRepo, followRepo, followPub)
	timelineUC := usecase.NewTimelineUseCase(userRepo, redisTimeline)

	router := handler.NewRouter(
		handler.NewUserHandler(userUC),
		handler.NewTweetHandler(tweetUC),
		handler.NewFollowHandler(followUC),
		handler.NewTimelineHandler(timelineUC),
	)

	log.Printf("listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, router); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 2: Build binary**

```bash
go build ./cmd/api/...
```

Expected: no errors.

- [ ] **Step 3: Run all unit tests**

```bash
go test ./internal/usecase/... ./internal/handler/... -v 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages show `ok`, no `FAIL`.

- [ ] **Step 4: Commit**

```bash
git add cmd/api/main.go
git commit -m "feat: wire RabbitMQ broker — publishers + consumers in main"
```

---

### Task 12: E2E smoke test

- [ ] **Step 1: Start services and server**

```bash
make up
DATABASE_URL=postgres://uala:uala@localhost:5432/uala \
  REDIS_URL=redis://localhost:6379/0 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
  PORT=8080 \
  go run ./cmd/api/... &
```

Expected: `listening on :8080`

- [ ] **Step 2: Create two users**

```bash
ALICE=$(curl -s -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"username":"alice"}' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
BOB=$(curl -s -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"username":"bob"}' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
echo "Alice: $ALICE  Bob: $BOB"
```

Expected: two UUIDs printed.

- [ ] **Step 3: Alice follows Bob**

```bash
curl -s -X POST http://localhost:8080/follow \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $ALICE" \
  -d "{\"followee_id\":\"$BOB\"}"
```

Expected: `{}`

- [ ] **Step 4: Bob posts a tweet**

```bash
curl -s -X POST http://localhost:8080/tweets \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $BOB" \
  -d '{"content":"Hello via async fanout!"}'
```

Expected: `{"id":"..."}`

- [ ] **Step 5: Wait for consumer then verify Redis**

```bash
sleep 1
docker exec uala_redis redis-cli ZREVRANGE "timeline:$ALICE" 0 -1
```

Expected: JSON string containing `"Hello via async fanout!"`.

- [ ] **Step 6: Alice reads timeline (served from Redis)**

```bash
curl -s http://localhost:8080/timeline -H "X-User-ID: $ALICE"
```

Expected:
```json
{"tweets":[{"id":"...","user_id":"...","username":"bob","content":"Hello via async fanout!","created_at":"..."}]}
```

- [ ] **Step 7: Stop server + run full test suite**

```bash
kill %1
go test ./... -v 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages show `ok`.

- [ ] **Step 8: Final commit**

```bash
git add .
git commit -m "feat: iter-3 complete — async fanout + follow backfill via RabbitMQ"
```

---

## Self-Review

**Spec coverage:**
- [x] RabbitMQ added to `docker-compose.yml` → Task 1
- [x] `POST /tweets` → save to Postgres → publish event → respond 201 → Task 5
- [x] Tweet consumer: receive event → fetch followers → fanout to Redis → Task 9
- [x] `POST /follow` → save to Postgres → publish event → respond 201 → Task 6
- [x] Follow consumer: receive event → fetch recent tweets → backfill Redis → Task 10
- [x] `FOLLOW_BACKFILL_LIMIT` configurable via env → Task 2
- [x] Tweet payload: `{tweet_id, user_id, username, content, created_at}` → `TweetItem` JSON → Task 7
- [x] Follow payload: `{follower_id, followee_id}` → `followEvent` JSON → Tasks 7, 10

**Placeholder scan:** All steps contain complete Go code. No TBD references.

**Type consistency:**
- `domain.TweetPublisher.PublishTweetCreated(ctx, TweetItem)` → `rabbitmq.TweetPublisher` implements it — signatures match
- `domain.FollowPublisher.PublishFollowCreated(ctx, uuid, uuid)` → `rabbitmq.FollowPublisher` implements it — match
- `followEvent` defined once in `client.go`, used by both `follow_publisher.go` and `follow_consumer.go` — no duplicate
- `TweetConsumer.Handle(ctx, TweetItem)` and `FollowConsumer.Handle(ctx, uuid, uuid)` — both used in their `Start` goroutines — signatures consistent
