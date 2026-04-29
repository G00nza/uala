# Iter 1 — Core Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement POST /users, POST /tweets, POST /follow and GET /timeline backed by PostgreSQL, fully wired end-to-end following Clean Architecture.

**Architecture:** Domain defines entities and repository interfaces. Usecases contain business logic and depend only on domain interfaces. Handlers parse HTTP, validate request fields, delegate to usecases, and map domain errors to HTTP status codes. `internal/repository/postgres` implements domain interfaces against pgx/v5. App runs migrations on startup via inline SQL (CREATE TABLE IF NOT EXISTS). No external DI or router library — stdlib only.

**Tech Stack:** Go 1.24, pgx/v5 (PostgreSQL driver + pool), google/uuid, net/http stdlib (Go 1.22+ method+path routing)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `.env` | Create | Local env vars (docker compose + app) |
| `internal/domain/errors.go` | Create | Sentinel domain errors |
| `internal/domain/user.go` | Create | User entity + UserRepository interface |
| `internal/domain/tweet.go` | Create | Tweet entity + TweetRepository interface |
| `internal/domain/follow.go` | Create | Follow entity, TweetItem, FollowRepository, TimelineRepository interfaces |
| `internal/infra/config.go` | Create | Load config from env vars |
| `internal/repository/postgres/db.go` | Create | pgxpool connect + Migrate (inline CREATE TABLE IF NOT EXISTS) |
| `internal/repository/postgres/setup_test.go` | Create | TestMain + truncate helper for integration tests |
| `internal/repository/postgres/user.go` | Create | UserRepository pgx impl |
| `internal/repository/postgres/user_test.go` | Create | Integration tests (skip unless INTEGRATION=1) |
| `internal/repository/postgres/tweet.go` | Create | TweetRepository pgx impl |
| `internal/repository/postgres/tweet_test.go` | Create | Integration tests |
| `internal/repository/postgres/follow.go` | Create | FollowRepository pgx impl |
| `internal/repository/postgres/follow_test.go` | Create | Integration tests |
| `internal/repository/postgres/timeline.go` | Create | TimelineRepository pgx impl |
| `internal/repository/postgres/timeline_test.go` | Create | Integration tests |
| `internal/usecase/mocks_test.go` | Create | Shared hand-written mock repos for all usecase tests |
| `internal/usecase/user.go` | Create | CreateUser business logic |
| `internal/usecase/user_test.go` | Create | Unit tests with mock repo |
| `internal/usecase/tweet.go` | Create | CreateTweet business logic |
| `internal/usecase/tweet_test.go` | Create | Unit tests |
| `internal/usecase/follow.go` | Create | Follow business logic |
| `internal/usecase/follow_test.go` | Create | Unit tests |
| `internal/usecase/timeline.go` | Create | GetTimeline business logic |
| `internal/usecase/timeline_test.go` | Create | Unit tests |
| `internal/handler/helpers.go` | Create | writeJSON, parseJSON, parseUserID, domainErrToStatus |
| `internal/handler/mocks_test.go` | Create | Shared hand-written mock services for all handler tests |
| `internal/handler/user.go` | Create | POST /users handler |
| `internal/handler/user_test.go` | Create | httptest tests |
| `internal/handler/tweet.go` | Create | POST /tweets handler |
| `internal/handler/tweet_test.go` | Create | httptest tests |
| `internal/handler/follow.go` | Create | POST /follow handler |
| `internal/handler/follow_test.go` | Create | httptest tests |
| `internal/handler/timeline.go` | Create | GET /timeline handler |
| `internal/handler/timeline_test.go` | Create | httptest tests |
| `internal/handler/router.go` | Create | http.NewServeMux wiring |
| `cmd/api/main.go` | Modify | Full dependency wiring + http.ListenAndServe |

---

### Task 1: Add Go dependencies

**Files:**
- Modify: `go.mod`, `go.sum` (auto-generated)

- [ ] **Step 1: Add pgx/v5 and uuid**

```bash
go get github.com/jackc/pgx/v5@latest
go get github.com/google/uuid@latest
```

- [ ] **Step 2: Verify go.mod contains both**

```bash
grep -E "pgx|uuid" go.mod
```

Expected output (versions may differ):
```
github.com/google/uuid v1.6.0
github.com/jackc/pgx/v5 v5.7.2
```

- [ ] **Step 3: Verify project still compiles**

```bash
go build ./...
```

Expected: no errors.

---

### Task 2: Environment file + start PostgreSQL

**Files:**
- Create: `.env`

- [ ] **Step 1: Create .env**

```
POSTGRES_USER=uala
POSTGRES_PASSWORD=uala
POSTGRES_DB=uala
DATABASE_URL=postgres://uala:uala@localhost:5432/uala
PORT=8080
```

- [ ] **Step 2: Start PostgreSQL**

```bash
make up
```

Expected: `uala_postgres` container starts and becomes healthy (may take ~10s).

- [ ] **Step 3: Verify PostgreSQL is reachable**

```bash
docker compose ps
```

Expected: `uala_postgres` shows `healthy` status.

---

### Task 3: Domain layer — errors, entities, interfaces

**Files:**
- Create: `internal/domain/errors.go`
- Create: `internal/domain/user.go`
- Create: `internal/domain/tweet.go`
- Create: `internal/domain/follow.go`

- [ ] **Step 1: Create internal/domain/errors.go**

```go
package domain

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrUsernameConflict = errors.New("username already taken")
	ErrAlreadyFollowing = errors.New("already following")
	ErrSelfFollow       = errors.New("cannot follow yourself")
)
```

- [ ] **Step 2: Create internal/domain/user.go**

```go
package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID        uuid.UUID
	Username  string
	CreatedAt time.Time
}

type UserRepository interface {
	Create(ctx context.Context, u *User) error
	GetByID(ctx context.Context, id uuid.UUID) (*User, error)
}
```

- [ ] **Step 3: Create internal/domain/tweet.go**

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
}
```

- [ ] **Step 4: Create internal/domain/follow.go**

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
	ID        uuid.UUID
	UserID    uuid.UUID
	Username  string
	Content   string
	CreatedAt time.Time
}

type FollowRepository interface {
	Create(ctx context.Context, f *Follow) error
	Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error)
}

type TimelineRepository interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]TweetItem, error)
}
```

- [ ] **Step 5: Verify domain compiles**

```bash
go build ./internal/domain/...
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum .env internal/domain/
git commit -m "feat: iter-1 go deps + domain layer (entities + interfaces + errors)"
```

---

### Task 4: Config + PostgreSQL connection + schema migration

**Files:**
- Create: `internal/infra/config.go`
- Create: `internal/repository/postgres/db.go`

- [ ] **Step 1: Create internal/infra/config.go**

```go
package infra

import "os"

type Config struct {
	DatabaseURL string
	Port        string
}

func LoadConfig() Config {
	return Config{
		DatabaseURL: getenv("DATABASE_URL", "postgres://uala:uala@localhost:5432/uala"),
		Port:        getenv("PORT", "8080"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **Step 2: Create internal/repository/postgres/db.go**

```go
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY,
			username TEXT UNIQUE NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS tweets (
			id UUID PRIMARY KEY,
			user_id UUID NOT NULL REFERENCES users(id),
			content TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS follows (
			follower_id UUID NOT NULL REFERENCES users(id),
			followee_id UUID NOT NULL REFERENCES users(id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (follower_id, followee_id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 3: Verify internal packages compile**

```bash
go build ./internal/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/infra/ internal/repository/postgres/db.go
git commit -m "feat: postgres connect + inline schema migration"
```

---

### Task 5: User repository (integration tests + implementation)

**Files:**
- Create: `internal/repository/postgres/setup_test.go`
- Create: `internal/repository/postgres/user_test.go`
- Create: `internal/repository/postgres/user.go`

- [ ] **Step 1: Create setup_test.go — shared integration test infrastructure**

```go
package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/repository/postgres"
)

var testDB *pgxpool.Pool

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://uala:uala@localhost:5432/uala"
	}
	pool, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		panic("connect: " + err.Error())
	}
	testDB = pool
	if err := postgres.Migrate(context.Background(), pool); err != nil {
		panic("migrate: " + err.Error())
	}
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func truncate(t *testing.T) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"TRUNCATE TABLE follows, tweets, users RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
```

- [ ] **Step 2: Write failing user integration tests**

```go
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func TestUserRepository_Create(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	u := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), u); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestUserRepository_Create_DuplicateUsername(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	u1 := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	u2 := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), u1)

	err := repo.Create(context.Background(), u2)
	if err != domain.ErrUsernameConflict {
		t.Fatalf("want ErrUsernameConflict, got %v", err)
	}
}

func TestUserRepository_GetByID(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	u := &domain.User{ID: uuid.New(), Username: "carol", CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), u)

	got, err := repo.GetByID(context.Background(), u.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Username != "carol" {
		t.Fatalf("want carol, got %s", got.Username)
	}
}

func TestUserRepository_GetByID_NotFound(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	_, err := repo.GetByID(context.Background(), uuid.New())
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 3: Run tests without INTEGRATION — verify they skip**

```bash
go test ./internal/repository/postgres/... -v
```

Expected: exits 0 immediately with no test output (TestMain exits 0 when INTEGRATION unset).

- [ ] **Step 4: Create internal/repository/postgres/user.go**

```go
package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, u *domain.User) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO users (id, username, created_at) VALUES ($1, $2, $3)`,
		u.ID, u.Username, u.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrUsernameConflict
		}
		return err
	}
	return nil
}

func (r *UserRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	u := &domain.User{}
	err := r.db.QueryRow(ctx,
		`SELECT id, username, created_at FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return u, nil
}
```

- [ ] **Step 5: Run integration tests with real DB**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v -run TestUser
```

Expected:
```
--- PASS: TestUserRepository_Create (0.00s)
--- PASS: TestUserRepository_Create_DuplicateUsername (0.00s)
--- PASS: TestUserRepository_GetByID (0.00s)
--- PASS: TestUserRepository_GetByID_NotFound (0.00s)
PASS
```

- [ ] **Step 6: Commit**

```bash
git add internal/repository/postgres/setup_test.go internal/repository/postgres/user.go internal/repository/postgres/user_test.go
git commit -m "feat: user repository with integration tests"
```

---

### Task 6: User usecase (unit tests + implementation)

**Files:**
- Create: `internal/usecase/mocks_test.go`
- Create: `internal/usecase/user_test.go`
- Create: `internal/usecase/user.go`

- [ ] **Step 1: Create internal/usecase/mocks_test.go — shared mock repos**

```go
package usecase_test

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type mockUserRepo struct {
	createErr   error
	getUser     *domain.User
	getErr      error
}

func (m *mockUserRepo) Create(ctx context.Context, u *domain.User) error {
	return m.createErr
}

func (m *mockUserRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	return m.getUser, m.getErr
}

type mockTweetRepo struct {
	createErr error
}

func (m *mockTweetRepo) Create(ctx context.Context, t *domain.Tweet) error {
	return m.createErr
}

type mockFollowRepo struct {
	existsResult bool
	existsErr    error
	createErr    error
}

func (m *mockFollowRepo) Create(ctx context.Context, f *domain.Follow) error {
	return m.createErr
}

func (m *mockFollowRepo) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	return m.existsResult, m.existsErr
}

type mockTimelineRepo struct {
	items []domain.TweetItem
	err   error
}

func (m *mockTimelineRepo) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```

- [ ] **Step 2: Write failing user usecase tests**

```go
package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestUserUseCase_CreateUser_OK(t *testing.T) {
	uc := usecase.NewUserUseCase(&mockUserRepo{})
	user, err := uc.CreateUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user.Username != "alice" {
		t.Fatalf("want alice, got %s", user.Username)
	}
	if user.ID == (uuid.UUID{}) {
		t.Fatal("ID must be set")
	}
}

func TestUserUseCase_CreateUser_PropagatesRepoError(t *testing.T) {
	repo := &mockUserRepo{createErr: domain.ErrUsernameConflict}
	uc := usecase.NewUserUseCase(repo)
	_, err := uc.CreateUser(context.Background(), "alice")
	if err != domain.ErrUsernameConflict {
		t.Fatalf("want ErrUsernameConflict, got %v", err)
	}
}
```

- [ ] **Step 3: Run — expect compile error**

```bash
go test ./internal/usecase/... -run TestUserUseCase 2>&1 | head -5
```

Expected: `undefined: usecase.NewUserUseCase`

- [ ] **Step 4: Create internal/usecase/user.go**

```go
package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type UserUseCase struct {
	repo domain.UserRepository
}

func NewUserUseCase(repo domain.UserRepository) *UserUseCase {
	return &UserUseCase{repo: repo}
}

func (uc *UserUseCase) CreateUser(ctx context.Context, username string) (*domain.User, error) {
	u := &domain.User{
		ID:        uuid.New(),
		Username:  username,
		CreatedAt: time.Now().UTC(),
	}
	if err := uc.repo.Create(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/usecase/... -run TestUserUseCase -v
```

Expected:
```
--- PASS: TestUserUseCase_CreateUser_OK (0.00s)
--- PASS: TestUserUseCase_CreateUser_PropagatesRepoError (0.00s)
PASS
```

- [ ] **Step 6: Commit**

```bash
git add internal/usecase/mocks_test.go internal/usecase/user.go internal/usecase/user_test.go
git commit -m "feat: user usecase with unit tests"
```

---

### Task 7: Handler helpers + user handler (httptest tests + implementation)

**Files:**
- Create: `internal/handler/helpers.go`
- Create: `internal/handler/mocks_test.go`
- Create: `internal/handler/user_test.go`
- Create: `internal/handler/user.go`

- [ ] **Step 1: Create internal/handler/helpers.go**

```go
package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"uala/internal/domain"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func parseJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func parseUserID(r *http.Request) (uuid.UUID, error) {
	raw := r.Header.Get("X-User-ID")
	if raw == "" {
		return uuid.UUID{}, errors.New("X-User-ID header is required")
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, errors.New("X-User-ID must be a valid UUID")
	}
	return id, nil
}

func domainErrToStatus(err error) int {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, domain.ErrUsernameConflict):
		return http.StatusConflict
	case errors.Is(err, domain.ErrAlreadyFollowing):
		return http.StatusConflict
	case errors.Is(err, domain.ErrSelfFollow):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
```

- [ ] **Step 2: Create internal/handler/mocks_test.go — shared mock services**

```go
package handler_test

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type mockUserSvc struct {
	user *domain.User
	err  error
}

func (m *mockUserSvc) CreateUser(ctx context.Context, username string) (*domain.User, error) {
	return m.user, m.err
}

type mockTweetSvc struct {
	tweet *domain.Tweet
	err   error
}

func (m *mockTweetSvc) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	return m.tweet, m.err
}

type mockFollowSvc struct {
	err error
}

func (m *mockFollowSvc) Follow(ctx context.Context, followerID, followeeID uuid.UUID) error {
	return m.err
}

type mockTimelineSvc struct {
	items []domain.TweetItem
	err   error
}

func (m *mockTimelineSvc) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```

- [ ] **Step 3: Write failing user handler tests**

```go
package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestUserHandler_Create_OK(t *testing.T) {
	id := uuid.New()
	svc := &mockUserSvc{user: &domain.User{ID: id, Username: "alice", CreatedAt: time.Now()}}
	h := handler.NewUserHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/users",
		bytes.NewBufferString(`{"username":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rec.Code)
	}
	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["id"] != id.String() {
		t.Fatalf("want %s, got %s", id, resp["id"])
	}
}

func TestUserHandler_Create_EmptyUsername(t *testing.T) {
	h := handler.NewUserHandler(&mockUserSvc{})

	req := httptest.NewRequest(http.MethodPost, "/users",
		bytes.NewBufferString(`{"username":""}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestUserHandler_Create_Conflict(t *testing.T) {
	svc := &mockUserSvc{err: domain.ErrUsernameConflict}
	h := handler.NewUserHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/users",
		bytes.NewBufferString(`{"username":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d", rec.Code)
	}
}
```

- [ ] **Step 4: Run — expect compile error**

```bash
go test ./internal/handler/... -run TestUserHandler 2>&1 | head -5
```

Expected: `undefined: handler.NewUserHandler`

- [ ] **Step 5: Create internal/handler/user.go**

```go
package handler

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type userCreator interface {
	CreateUser(ctx context.Context, username string) (*domain.User, error)
}

type UserHandler struct {
	svc userCreator
}

func NewUserHandler(svc userCreator) *UserHandler {
	return &UserHandler{svc: svc}
}

func (h *UserHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
	}
	if err := parseJSON(r, &req); err != nil || req.Username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username is required"})
		return
	}
	user, err := h.svc.CreateUser(r.Context(), req.Username)
	if err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": user.ID.String()})
}

// suppress unused import warning — uuid used by other handlers in this package
var _ uuid.UUID
```

- [ ] **Step 6: Run tests**

```bash
go test ./internal/handler/... -run TestUserHandler -v
```

Expected:
```
--- PASS: TestUserHandler_Create_OK (0.00s)
--- PASS: TestUserHandler_Create_EmptyUsername (0.00s)
--- PASS: TestUserHandler_Create_Conflict (0.00s)
PASS
```

- [ ] **Step 7: Commit**

```bash
git add internal/handler/helpers.go internal/handler/mocks_test.go internal/handler/user.go internal/handler/user_test.go
git commit -m "feat: user handler with httptest tests"
```

---

### Task 8: Tweet repository (integration tests + implementation)

**Files:**
- Create: `internal/repository/postgres/tweet_test.go`
- Create: `internal/repository/postgres/tweet.go`

- [ ] **Step 1: Write failing tweet integration tests**

```go
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func TestTweetRepository_Create(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	tweetRepo := postgres.NewTweetRepository(testDB)

	user := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), user)

	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    user.ID,
		Content:   "hello world",
		CreatedAt: time.Now().UTC(),
	}
	if err := tweetRepo.Create(context.Background(), tweet); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestTweetRepository_Create_UserNotFound(t *testing.T) {
	truncate(t)
	tweetRepo := postgres.NewTweetRepository(testDB)

	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    uuid.New(), // non-existent user
		Content:   "orphan tweet",
		CreatedAt: time.Now().UTC(),
	}
	err := tweetRepo.Create(context.Background(), tweet)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 2: Run — verify tests skip (no INTEGRATION)**

```bash
go test ./internal/repository/postgres/... -run TestTweet
```

Expected: exits 0 with no output.

- [ ] **Step 3: Create internal/repository/postgres/tweet.go**

```go
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type TweetRepository struct {
	db *pgxpool.Pool
}

func NewTweetRepository(db *pgxpool.Pool) *TweetRepository {
	return &TweetRepository{db: db}
}

func (r *TweetRepository) Create(ctx context.Context, t *domain.Tweet) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO tweets (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)`,
		t.ID, t.UserID, t.Content, t.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return domain.ErrNotFound
		}
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run integration tests**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v -run TestTweet
```

Expected:
```
--- PASS: TestTweetRepository_Create (0.00s)
--- PASS: TestTweetRepository_Create_UserNotFound (0.00s)
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/repository/postgres/tweet.go internal/repository/postgres/tweet_test.go
git commit -m "feat: tweet repository with integration tests"
```

---

### Task 9: Tweet usecase (unit tests + implementation)

**Files:**
- Create: `internal/usecase/tweet_test.go`
- Create: `internal/usecase/tweet.go`

- [ ] **Step 1: Write failing tweet usecase tests**

```go
package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestTweetUseCase_CreateTweet_OK(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	tweetRepo := &mockTweetRepo{}
	uc := usecase.NewTweetUseCase(userRepo, tweetRepo)

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
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{})

	_, err := uc.CreateTweet(context.Background(), uuid.New(), "hello")
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/usecase/... -run TestTweetUseCase 2>&1 | head -5
```

Expected: `undefined: usecase.NewTweetUseCase`

- [ ] **Step 3: Create internal/usecase/tweet.go**

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
}

func NewTweetUseCase(userRepo domain.UserRepository, tweetRepo domain.TweetRepository) *TweetUseCase {
	return &TweetUseCase{userRepo: userRepo, tweetRepo: tweetRepo}
}

func (uc *TweetUseCase) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
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
	return t, nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/usecase/... -run TestTweetUseCase -v
```

Expected:
```
--- PASS: TestTweetUseCase_CreateTweet_OK (0.00s)
--- PASS: TestTweetUseCase_CreateTweet_UserNotFound (0.00s)
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/usecase/tweet.go internal/usecase/tweet_test.go
git commit -m "feat: tweet usecase with unit tests"
```

---

### Task 10: Tweet handler (httptest tests + implementation)

**Files:**
- Create: `internal/handler/tweet_test.go`
- Create: `internal/handler/tweet.go`

- [ ] **Step 1: Write failing tweet handler tests**

```go
package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestTweetHandler_Create_OK(t *testing.T) {
	userID := uuid.New()
	tweetID := uuid.New()
	svc := &mockTweetSvc{tweet: &domain.Tweet{ID: tweetID, UserID: userID, Content: "hello", CreatedAt: time.Now()}}
	h := handler.NewTweetHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", userID.String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rec.Code)
	}
	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["id"] != tweetID.String() {
		t.Fatalf("want %s, got %s", tweetID, resp["id"])
	}
}

func TestTweetHandler_Create_MissingUserID(t *testing.T) {
	h := handler.NewTweetHandler(&mockTweetSvc{})

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTweetHandler_Create_EmptyContent(t *testing.T) {
	h := handler.NewTweetHandler(&mockTweetSvc{})

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":""}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTweetHandler_Create_ContentTooLong(t *testing.T) {
	h := handler.NewTweetHandler(&mockTweetSvc{})

	body := `{"content":"` + strings.Repeat("a", 281) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/tweets", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTweetHandler_Create_UserNotFound(t *testing.T) {
	svc := &mockTweetSvc{err: domain.ErrNotFound}
	h := handler.NewTweetHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/handler/... -run TestTweetHandler 2>&1 | head -5
```

Expected: `undefined: handler.NewTweetHandler`

- [ ] **Step 3: Create internal/handler/tweet.go**

```go
package handler

import (
	"context"
	"net/http"
	"unicode/utf8"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type tweetCreator interface {
	CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error)
}

type TweetHandler struct {
	svc tweetCreator
}

func NewTweetHandler(svc tweetCreator) *TweetHandler {
	return &TweetHandler{svc: svc}
}

func (h *TweetHandler) Create(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var req struct {
		Content string `json:"content"`
	}
	if err := parseJSON(r, &req); err != nil || req.Content == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "content is required"})
		return
	}
	if utf8.RuneCountInString(req.Content) > 280 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "content exceeds 280 characters"})
		return
	}
	tweet, err := h.svc.CreateTweet(r.Context(), userID, req.Content)
	if err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": tweet.ID.String()})
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/handler/... -run TestTweetHandler -v
```

Expected: all 5 TestTweetHandler* tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/handler/tweet.go internal/handler/tweet_test.go
git commit -m "feat: tweet handler with httptest tests"
```

---

### Task 11: Follow repository (integration tests + implementation)

**Files:**
- Create: `internal/repository/postgres/follow_test.go`
- Create: `internal/repository/postgres/follow.go`

- [ ] **Step 1: Write failing follow integration tests**

```go
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func seedUser(t *testing.T, username string) *domain.User {
	t.Helper()
	repo := postgres.NewUserRepository(testDB)
	u := &domain.User{ID: uuid.New(), Username: username, CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), u); err != nil {
		t.Fatalf("seedUser: %v", err)
	}
	return u
}

func TestFollowRepository_Create(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")

	repo := postgres.NewFollowRepository(testDB)
	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), f); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestFollowRepository_Create_Duplicate(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")

	repo := postgres.NewFollowRepository(testDB)
	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), f)

	err := repo.Create(context.Background(), f)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowRepository_Exists(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")

	repo := postgres.NewFollowRepository(testDB)
	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), f)

	exists, err := repo.Exists(context.Background(), alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatal("want exists=true")
	}
}

func TestFollowRepository_Exists_NotFound(t *testing.T) {
	truncate(t)
	repo := postgres.NewFollowRepository(testDB)

	exists, err := repo.Exists(context.Background(), uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("want exists=false")
	}
}
```

- [ ] **Step 2: Create internal/repository/postgres/follow.go**

```go
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/google/uuid"
	"uala/internal/domain"
)

type FollowRepository struct {
	db *pgxpool.Pool
}

func NewFollowRepository(db *pgxpool.Pool) *FollowRepository {
	return &FollowRepository{db: db}
}

func (r *FollowRepository) Create(ctx context.Context, f *domain.Follow) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO follows (follower_id, followee_id, created_at) VALUES ($1, $2, $3)`,
		f.FollowerID, f.FolloweeID, f.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrAlreadyFollowing
		}
		return err
	}
	return nil
}

func (r *FollowRepository) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM follows WHERE follower_id=$1 AND followee_id=$2)`,
		followerID, followeeID,
	).Scan(&exists)
	return exists, err
}
```

- [ ] **Step 3: Run integration tests**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v -run TestFollow
```

Expected: all 4 TestFollow* tests PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/repository/postgres/follow.go internal/repository/postgres/follow_test.go
git commit -m "feat: follow repository with integration tests"
```

---

### Task 12: Follow usecase (unit tests + implementation)

**Files:**
- Create: `internal/usecase/follow_test.go`
- Create: `internal/usecase/follow.go`

- [ ] **Step 1: Write failing follow usecase tests**

```go
package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestFollowUseCase_Follow_OK(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: followee}}
	followRepo := &mockFollowRepo{}
	uc := usecase.NewFollowUseCase(userRepo, followRepo)

	if err := uc.Follow(context.Background(), follower, followee); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFollowUseCase_Follow_SelfFollow(t *testing.T) {
	id := uuid.New()
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, &mockFollowRepo{})

	err := uc.Follow(context.Background(), id, id)
	if err != domain.ErrSelfFollow {
		t.Fatalf("want ErrSelfFollow, got %v", err)
	}
}

func TestFollowUseCase_Follow_AlreadyFollowing(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	followRepo := &mockFollowRepo{existsResult: true}
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, followRepo)

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowUseCase_Follow_FolloweeNotFound(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{})

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/usecase/... -run TestFollowUseCase 2>&1 | head -5
```

Expected: `undefined: usecase.NewFollowUseCase`

- [ ] **Step 3: Create internal/usecase/follow.go**

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
}

func NewFollowUseCase(userRepo domain.UserRepository, followRepo domain.FollowRepository) *FollowUseCase {
	return &FollowUseCase{userRepo: userRepo, followRepo: followRepo}
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
	return uc.followRepo.Create(ctx, f)
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/usecase/... -run TestFollowUseCase -v
```

Expected: all 4 TestFollowUseCase* tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase/follow.go internal/usecase/follow_test.go
git commit -m "feat: follow usecase with unit tests"
```

---

### Task 13: Follow handler (httptest tests + implementation)

**Files:**
- Create: `internal/handler/follow_test.go`
- Create: `internal/handler/follow.go`

- [ ] **Step 1: Write failing follow handler tests**

```go
package handler_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestFollowHandler_Follow_OK(t *testing.T) {
	svc := &mockFollowSvc{}
	h := handler.NewFollowHandler(svc)

	follower := uuid.New()
	followee := uuid.New()
	body := fmt.Sprintf(`{"followee_id":"%s"}`, followee)
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", follower.String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_MissingUserID(t *testing.T) {
	h := handler.NewFollowHandler(&mockFollowSvc{})

	req := httptest.NewRequest(http.MethodPost, "/follow",
		bytes.NewBufferString(`{"followee_id":"` + uuid.New().String() + `"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_SelfFollow(t *testing.T) {
	svc := &mockFollowSvc{err: domain.ErrSelfFollow}
	h := handler.NewFollowHandler(svc)

	id := uuid.New()
	body := fmt.Sprintf(`{"followee_id":"%s"}`, id)
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", id.String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_AlreadyFollowing(t *testing.T) {
	svc := &mockFollowSvc{err: domain.ErrAlreadyFollowing}
	h := handler.NewFollowHandler(svc)

	body := fmt.Sprintf(`{"followee_id":"%s"}`, uuid.New())
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_FolloweeNotFound(t *testing.T) {
	svc := &mockFollowSvc{err: domain.ErrNotFound}
	h := handler.NewFollowHandler(svc)

	body := fmt.Sprintf(`{"followee_id":"%s"}`, uuid.New())
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/handler/... -run TestFollowHandler 2>&1 | head -5
```

Expected: `undefined: handler.NewFollowHandler`

- [ ] **Step 3: Create internal/handler/follow.go**

```go
package handler

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type userFollower interface {
	Follow(ctx context.Context, followerID, followeeID uuid.UUID) error
}

type FollowHandler struct {
	svc userFollower
}

func NewFollowHandler(svc userFollower) *FollowHandler {
	return &FollowHandler{svc: svc}
}

func (h *FollowHandler) Follow(w http.ResponseWriter, r *http.Request) {
	followerID, err := parseUserID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var req struct {
		FolloweeID string `json:"followee_id"`
	}
	if err := parseJSON(r, &req); err != nil || req.FolloweeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "followee_id is required"})
		return
	}
	followeeID, err := uuid.Parse(req.FolloweeID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "followee_id must be a valid UUID"})
		return
	}
	if err := h.svc.Follow(r.Context(), followerID, followeeID); err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, struct{}{})
}

// suppress unused import warning
var _ domain.Follow
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/handler/... -run TestFollowHandler -v
```

Expected: all 5 TestFollowHandler* tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/handler/follow.go internal/handler/follow_test.go
git commit -m "feat: follow handler with httptest tests"
```

---

### Task 14: Timeline repository (integration tests + implementation)

**Files:**
- Create: `internal/repository/postgres/timeline_test.go`
- Create: `internal/repository/postgres/timeline.go`

- [ ] **Step 1: Write failing timeline integration tests**

```go
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/repository/postgres"
)

func seedTweet(t *testing.T, userID uuid.UUID, content string) {
	t.Helper()
	repo := postgres.NewTweetRepository(testDB)
	tweet := &struct {
		ID        uuid.UUID
		UserID    uuid.UUID
		Content   string
		CreatedAt time.Time
	}{uuid.New(), userID, content, time.Now().UTC()}
	_ = repo.Create(context.Background(), &domainTweet(tweet))
}

func TestTimelineRepository_GetTimeline_Empty(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")

	repo := postgres.NewTimelineRepository(testDB)
	items, err := repo.GetTimeline(context.Background(), alice.ID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}

func TestTimelineRepository_GetTimeline_WithTweets(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	tweetRepo := postgres.NewTweetRepository(testDB)
	followRepo := postgres.NewFollowRepository(testDB)
	timelineRepo := postgres.NewTimelineRepository(testDB)

	alice := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), alice)
	_ = userRepo.Create(context.Background(), bob)

	_ = followRepo.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = tweetRepo.Create(context.Background(), &domain.Tweet{
		ID: uuid.New(), UserID: bob.ID, Content: "hello from bob", CreatedAt: time.Now().UTC(),
	})

	items, err := timelineRepo.GetTimeline(context.Background(), alice.ID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0].Content != "hello from bob" {
		t.Fatalf("want 'hello from bob', got %s", items[0].Content)
	}
	if items[0].Username != "bob" {
		t.Fatalf("want 'bob', got %s", items[0].Username)
	}
}
```

Note: the timeline test file imports `domain` directly. Add this import at the top:

```go
import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)
```

Remove the `seedTweet` helper defined above — it was a sketch. The test uses `tweetRepo` directly (as shown in `TestTimelineRepository_GetTimeline_WithTweets`). The final file is:

```go
package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func TestTimelineRepository_GetTimeline_Empty(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	alice := &domain.User{ID: uuid.New(), Username: "alice_tl", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), alice)

	repo := postgres.NewTimelineRepository(testDB)
	items, err := repo.GetTimeline(context.Background(), alice.ID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}

func TestTimelineRepository_GetTimeline_WithTweets(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	tweetRepo := postgres.NewTweetRepository(testDB)
	followRepo := postgres.NewFollowRepository(testDB)
	timelineRepo := postgres.NewTimelineRepository(testDB)

	alice := &domain.User{ID: uuid.New(), Username: "alice_tl2", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_tl2", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), alice)
	_ = userRepo.Create(context.Background(), bob)

	_ = followRepo.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = tweetRepo.Create(context.Background(), &domain.Tweet{
		ID: uuid.New(), UserID: bob.ID, Content: "hello from bob", CreatedAt: time.Now().UTC(),
	})

	items, err := timelineRepo.GetTimeline(context.Background(), alice.ID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0].Content != "hello from bob" {
		t.Fatalf("want 'hello from bob', got %s", items[0].Content)
	}
	if items[0].Username != "bob_tl2" {
		t.Fatalf("want 'bob_tl2', got %s", items[0].Username)
	}
}
```

- [ ] **Step 2: Create internal/repository/postgres/timeline.go**

```go
package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type TimelineRepository struct {
	db *pgxpool.Pool
}

func NewTimelineRepository(db *pgxpool.Pool) *TimelineRepository {
	return &TimelineRepository{db: db}
}

func (r *TimelineRepository) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM follows f
		JOIN tweets t ON t.user_id = f.followee_id
		JOIN users u ON u.id = t.user_id
		WHERE f.follower_id = $1
		ORDER BY t.created_at DESC
	`, userID)
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

- [ ] **Step 3: Run integration tests**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v -run TestTimeline
```

Expected:
```
--- PASS: TestTimelineRepository_GetTimeline_Empty (0.00s)
--- PASS: TestTimelineRepository_GetTimeline_WithTweets (0.00s)
PASS
```

- [ ] **Step 4: Commit**

```bash
git add internal/repository/postgres/timeline.go internal/repository/postgres/timeline_test.go
git commit -m "feat: timeline repository with integration tests"
```

---

### Task 15: Timeline usecase (unit tests + implementation)

**Files:**
- Create: `internal/usecase/timeline_test.go`
- Create: `internal/usecase/timeline.go`

- [ ] **Step 1: Write failing timeline usecase tests**

```go
package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestTimelineUseCase_GetTimeline_OK(t *testing.T) {
	userID := uuid.New()
	tweetID := uuid.New()
	authorID := uuid.New()

	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{
		{ID: tweetID, UserID: authorID, Username: "bob", Content: "hello", CreatedAt: time.Now()},
	}}
	uc := usecase.NewTimelineUseCase(userRepo, timelineRepo)

	items, err := uc.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
}

func TestTimelineUseCase_GetTimeline_UserNotFound(t *testing.T) {
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewTimelineUseCase(userRepo, &mockTimelineRepo{})

	_, err := uc.GetTimeline(context.Background(), uuid.New())
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTimelineUseCase_GetTimeline_EmptyWhenNoFollows(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{}}
	uc := usecase.NewTimelineUseCase(userRepo, timelineRepo)

	items, err := uc.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/usecase/... -run TestTimelineUseCase 2>&1 | head -5
```

Expected: `undefined: usecase.NewTimelineUseCase`

- [ ] **Step 3: Create internal/usecase/timeline.go**

```go
package usecase

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TimelineUseCase struct {
	userRepo     domain.UserRepository
	timelineRepo domain.TimelineRepository
}

func NewTimelineUseCase(userRepo domain.UserRepository, timelineRepo domain.TimelineRepository) *TimelineUseCase {
	return &TimelineUseCase{userRepo: userRepo, timelineRepo: timelineRepo}
}

func (uc *TimelineUseCase) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
		return nil, err
	}
	return uc.timelineRepo.GetTimeline(ctx, userID)
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/usecase/... -run TestTimelineUseCase -v
```

Expected: all 3 TestTimelineUseCase* tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase/timeline.go internal/usecase/timeline_test.go
git commit -m "feat: timeline usecase with unit tests"
```

---

### Task 16: Timeline handler (httptest tests + implementation)

**Files:**
- Create: `internal/handler/timeline_test.go`
- Create: `internal/handler/timeline.go`

- [ ] **Step 1: Write failing timeline handler tests**

```go
package handler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestTimelineHandler_GetTimeline_OK(t *testing.T) {
	userID := uuid.New()
	svc := &mockTimelineSvc{items: []domain.TweetItem{
		{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "hello", CreatedAt: time.Now()},
	}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", userID.String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp)
	tweets, ok := resp["tweets"].([]any)
	if !ok || len(tweets) != 1 {
		t.Fatalf("want 1 tweet in response, got %v", resp)
	}
}

func TestTimelineHandler_GetTimeline_Empty(t *testing.T) {
	svc := &mockTimelineSvc{items: []domain.TweetItem{}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp)
	tweets, ok := resp["tweets"].([]any)
	if !ok || len(tweets) != 0 {
		t.Fatalf("want empty tweets array, got %v", resp)
	}
}

func TestTimelineHandler_GetTimeline_MissingUserID(t *testing.T) {
	h := handler.NewTimelineHandler(&mockTimelineSvc{})

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTimelineHandler_GetTimeline_UserNotFound(t *testing.T) {
	svc := &mockTimelineSvc{err: domain.ErrNotFound}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
```

- [ ] **Step 2: Run — expect compile error**

```bash
go test ./internal/handler/... -run TestTimelineHandler 2>&1 | head -5
```

Expected: `undefined: handler.NewTimelineHandler`

- [ ] **Step 3: Create internal/handler/timeline.go**

```go
package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type timelineGetter interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error)
}

type TimelineHandler struct {
	svc timelineGetter
}

func NewTimelineHandler(svc timelineGetter) *TimelineHandler {
	return &TimelineHandler{svc: svc}
}

type tweetItemResponse struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

func (h *TimelineHandler) GetTimeline(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	items, err := h.svc.GetTimeline(r.Context(), userID)
	if err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	resp := make([]tweetItemResponse, len(items))
	for i, item := range items {
		resp[i] = tweetItemResponse{
			ID:        item.ID.String(),
			UserID:    item.UserID.String(),
			Username:  item.Username,
			Content:   item.Content,
			CreatedAt: item.CreatedAt,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tweets": resp})
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/handler/... -run TestTimelineHandler -v
```

Expected: all 4 TestTimelineHandler* tests PASS.

- [ ] **Step 5: Run all unit tests to confirm no regressions**

```bash
go test ./internal/usecase/... ./internal/handler/... -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/handler/timeline.go internal/handler/timeline_test.go
git commit -m "feat: timeline handler with httptest tests"
```

---

### Task 17: Router + main.go wiring

**Files:**
- Create: `internal/handler/router.go`
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Create internal/handler/router.go**

```go
package handler

import "net/http"

func NewRouter(
	user *UserHandler,
	tweet *TweetHandler,
	follow *FollowHandler,
	timeline *TimelineHandler,
) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", user.Create)
	mux.HandleFunc("POST /tweets", tweet.Create)
	mux.HandleFunc("POST /follow", follow.Follow)
	mux.HandleFunc("GET /timeline", timeline.GetTimeline)
	return mux
}
```

- [ ] **Step 2: Rewrite cmd/api/main.go**

```go
package main

import (
	"context"
	"log"
	"net/http"

	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/repository/postgres"
	"uala/internal/usecase"
)

func main() {
	cfg := infra.LoadConfig()
	ctx := context.Background()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer db.Close()

	if err := postgres.Migrate(ctx, db); err != nil {
		log.Fatal("migrate:", err)
	}

	userRepo := postgres.NewUserRepository(db)
	tweetRepo := postgres.NewTweetRepository(db)
	followRepo := postgres.NewFollowRepository(db)
	timelineRepo := postgres.NewTimelineRepository(db)

	userUC := usecase.NewUserUseCase(userRepo)
	tweetUC := usecase.NewTweetUseCase(userRepo, tweetRepo)
	followUC := usecase.NewFollowUseCase(userRepo, followRepo)
	timelineUC := usecase.NewTimelineUseCase(userRepo, timelineRepo)

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

- [ ] **Step 3: Build the binary**

```bash
go build ./cmd/api/...
```

Expected: no errors, binary created.

- [ ] **Step 4: Commit**

```bash
git add internal/handler/router.go cmd/api/main.go
git commit -m "feat: router + main.go wiring — server fully assembled"
```

---

### Task 18: End-to-end smoke test

- [ ] **Step 1: Start the server**

```bash
DATABASE_URL=postgres://uala:uala@localhost:5432/uala PORT=8080 go run ./cmd/api/... &
```

Expected: `listening on :8080`

- [ ] **Step 2: Create a user**

```bash
curl -s -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"username":"alice"}' | jq .
```

Expected:
```json
{"id": "<uuid>"}
```

Save the returned ID as `ALICE_ID`.

- [ ] **Step 3: Create a second user**

```bash
curl -s -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"username":"bob"}' | jq .
```

Save the returned ID as `BOB_ID`.

- [ ] **Step 4: Bob posts a tweet**

```bash
curl -s -X POST http://localhost:8080/tweets \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $BOB_ID" \
  -d '{"content":"Hello from bob!"}' | jq .
```

Expected: `{"id": "<uuid>"}`

- [ ] **Step 5: Alice follows Bob**

```bash
curl -s -X POST http://localhost:8080/follow \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $ALICE_ID" \
  -d "{\"followee_id\":\"$BOB_ID\"}" | jq .
```

Expected: `{}`

- [ ] **Step 6: Alice sees Bob's tweet in her timeline**

```bash
curl -s -X GET http://localhost:8080/timeline \
  -H "X-User-ID: $ALICE_ID" | jq .
```

Expected:
```json
{
  "tweets": [
    {
      "id": "...",
      "user_id": "...",
      "username": "bob",
      "content": "Hello from bob!",
      "created_at": "..."
    }
  ]
}
```

- [ ] **Step 7: Stop the server and run full test suite**

```bash
kill %1
go test ./... -v 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages show `ok`, no `FAIL`.

- [ ] **Step 8: Final commit**

```bash
git add .
git commit -m "feat: iter-1 complete — POST /users, POST /tweets, POST /follow, GET /timeline"
```

---

## Self-Review

**Spec coverage:**
- [x] `POST /users` — crear usuario → Task 6 (usecase) + Task 7 (handler)
- [x] `POST /tweets` — publicar tweet, validar máx 280 chars, user ID por header → Task 9 + Task 10
- [x] `POST /follow` — seguir a un usuario → Task 12 + Task 13
- [x] `GET /timeline` — tweets de usuarios seguidos, ordenados por fecha desc, desde PostgreSQL → Task 14 + Task 15 + Task 16
- [x] Migrations: tablas `users`, `tweets`, `follows` → Task 4 (`Migrate` inline SQL)
- [x] Clean Architecture: domain → usecase → handler + repository → Task 3-16
- [x] User ID por header `X-User-ID` → `parseUserID` helper in Task 7

**Placeholder scan:** All steps contain complete code with no TBD, TODO, or "similar to Task N" references.

**Type consistency:**
- `domain.User`, `domain.Tweet`, `domain.Follow`, `domain.TweetItem` — defined Task 3, used consistently throughout
- `domain.ErrNotFound`, `domain.ErrUsernameConflict`, `domain.ErrAlreadyFollowing`, `domain.ErrSelfFollow` — defined Task 3, mapped in `domainErrToStatus` (Task 7)
- `postgres.NewUserRepository`, `NewTweetRepository`, `NewFollowRepository`, `NewTimelineRepository` — defined Tasks 5/8/11/14, wired in Task 17
- `usecase.NewUserUseCase`, `NewTweetUseCase`, `NewFollowUseCase`, `NewTimelineUseCase` — defined Tasks 6/9/12/15, wired in Task 17
- `handler.NewUserHandler`, `NewTweetHandler`, `NewFollowHandler`, `NewTimelineHandler` — defined Tasks 7/10/13/16, wired in Task 17
