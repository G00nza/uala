# Versioned Migrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the inline `postgres.Migrate()` startup function with versioned goose SQL migrations run via a dedicated `cmd/migrate` CLI.

**Architecture:** SQL migration files live in `migrations/` and are embedded via `//go:embed`. A `migrations` Go package exports the `embed.FS`. A `cmd/migrate/main.go` CLI reads config via `infra.LoadConfig()`, opens a `*sql.DB` using the pgx stdlib bridge, and delegates to goose. The server no longer migrates on startup.

**Tech Stack:** `github.com/pressly/goose/v3`, `github.com/jackc/pgx/v5/stdlib` (already in module), Go embed.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `migrations/embed.go` | Exports `var FS embed.FS` with `//go:embed *.sql` |
| Create | `migrations/00001_create_users.sql` | users table up/down |
| Create | `migrations/00002_create_tweets.sql` | tweets table up/down |
| Create | `migrations/00003_create_follows.sql` | follows table up/down |
| Create | `cmd/migrate/main.go` | CLI entrypoint: up / down / status |
| Modify | `internal/repository/postgres/db.go` | Delete `Migrate()` function |
| Modify | `cmd/api/main.go` | Remove `postgres.Migrate()` call |
| Modify | `internal/repository/postgres/setup_test.go` | Replace `postgres.Migrate` with `goose.Up` |
| Modify | `Makefile` | Add `migrate` target |

---

## Task 1: Add goose dependency

**Files:**
- Modify: `go.mod` (via `go get`)

- [ ] **Step 1: Add goose v3**

```bash
cd /Users/gonza/Katas/uala && go get github.com/pressly/goose/v3
```

Expected: `go.mod` and `go.sum` updated with `github.com/pressly/goose/v3`.

- [ ] **Step 2: Tidy**

```bash
go mod tidy
```

Expected: no errors, `go.sum` clean.

---

## Task 2: Create migrations package with SQL files

**Files:**
- Create: `migrations/embed.go`
- Create: `migrations/00001_create_users.sql`
- Create: `migrations/00002_create_tweets.sql`
- Create: `migrations/00003_create_follows.sql`

- [ ] **Step 1: Create `migrations/embed.go`**

```go
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
```

- [ ] **Step 2: Create `migrations/00001_create_users.sql`**

```sql
-- +goose Up
CREATE TABLE users (
    id UUID PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE users;
```

- [ ] **Step 3: Create `migrations/00002_create_tweets.sql`**

```sql
-- +goose Up
CREATE TABLE tweets (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE tweets;
```

- [ ] **Step 4: Create `migrations/00003_create_follows.sql`**

```sql
-- +goose Up
CREATE TABLE follows (
    follower_id UUID NOT NULL REFERENCES users(id),
    followee_id UUID NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (follower_id, followee_id)
);

-- +goose Down
DROP TABLE follows;
```

- [ ] **Step 5: Verify compilation**

```bash
go build ./migrations/...
```

Expected: no errors.

---

## Task 3: Update setup_test.go to use goose (RED → GREEN)

**Files:**
- Modify: `internal/repository/postgres/setup_test.go`

- [ ] **Step 1: Write the failing version (RED)**

Replace the `postgres.Migrate` call in `TestMain` and update imports. Full file after change:

```go
package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"uala/internal/repository/postgres"
	"uala/migrations"
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

	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		panic("goose dialect: " + err.Error())
	}
	if err := goose.Up(sqlDB, "."); err != nil {
		panic("migrate: " + err.Error())
	}

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

type testRepos struct {
	user     *postgres.UserRepository
	tweet    *postgres.TweetRepository
	follow   *postgres.FollowRepository
	timeline *postgres.TimelineRepository
}

func setup(t *testing.T) testRepos {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"TRUNCATE TABLE follows, tweets, users RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return testRepos{
		user:     postgres.NewUserRepository(testDB),
		tweet:    postgres.NewTweetRepository(testDB),
		follow:   postgres.NewFollowRepository(testDB),
		timeline: postgres.NewTimelineRepository(testDB),
	}
}
```

- [ ] **Step 2: Verify RED — compile fails because postgres.Migrate still exists but is no longer imported**

```bash
go build ./internal/repository/postgres/...
```

Expected: compiles (the package itself is fine — `postgres.Migrate` still exists, we just stopped calling it in the test). The RED signal here is that `postgres.Migrate` is now dead code we're about to delete.

- [ ] **Step 3: Verify GREEN — integration tests pass**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v -count=1
```

Expected: all tests pass, goose creates `goose_db_version` table and runs the 3 migrations.

> Skip this step if no local Postgres is running — Task 4 (server build) and Task 5 (deleting Migrate) will serve as the compile-time verification.

---

## Task 4: Build `cmd/migrate/main.go`

**Files:**
- Create: `cmd/migrate/main.go`

- [ ] **Step 1: Create the CLI**

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"uala/internal/infra"
	"uala/migrations"
)

func main() {
	cfg := infra.LoadConfig()

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		log.Fatal("open db:", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal("ping:", err)
	}

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatal(err)
	}

	command := "up"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	if err := goose.RunContext(context.Background(), command, db, "."); err != nil {
		log.Fatalf("goose %s: %v", command, err)
	}
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./cmd/migrate/...
```

Expected: no errors, binary produced.

---

## Task 5: Delete `postgres.Migrate()` and clean up `main.go`

**Files:**
- Modify: `internal/repository/postgres/db.go`
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Delete `Migrate()` from `db.go`**

Remove lines 28–54 (the entire `Migrate` function). The file after deletion:

```go
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, dsn string, opts ...func(*pgxpool.Config)) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool parse config: %w", err)
	}
	for _, opt := range opts {
		opt(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.NewWithConfig: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return pool, nil
}
```

- [ ] **Step 2: Remove `postgres.Migrate()` call from `cmd/api/main.go`**

Remove lines 31–33:
```go
if err := postgres.Migrate(ctx, db); err != nil {
    log.Fatal("migrate:", err)
}
```

- [ ] **Step 3: Verify the full project compiles**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Run the unit test suite**

```bash
go test ./...
```

Expected: all tests pass (integration tests skip without `INTEGRATION=1`).

---

## Task 6: Add Makefile target and commit

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Update the Makefile**

```makefile
.PHONY: up down run test migrate

up:
	docker-compose up -d

down:
	docker-compose down

run:
	go run ./cmd/api/...

migrate:
	go run ./cmd/migrate

test:
	go test ./...
```

- [ ] **Step 2: Smoke-test the migrate target (optional, requires running Postgres)**

```bash
make migrate
```

Expected output (first run):
```
OK   00001_create_users.sql
OK   00002_create_tweets.sql
OK   00003_create_follows.sql
goose: successfully migrated database to version: 3
```

- [ ] **Step 3: Commit**

```bash
git add migrations/ cmd/migrate/ internal/repository/postgres/db.go cmd/api/main.go internal/repository/postgres/setup_test.go Makefile go.mod go.sum
git commit -m "feat: replace inline migrations with versioned goose CLI"
```
