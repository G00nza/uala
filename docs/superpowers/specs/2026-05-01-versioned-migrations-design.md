# Versioned Migrations with Goose

**Date:** 2026-05-01
**Gap:** 7 — Inline startup migrations replaced with versioned, rollback-capable schema management.

---

## Problem

`postgres.Migrate()` in `internal/repository/postgres/db.go` runs `CREATE TABLE IF NOT EXISTS` statements directly on every server startup. There is no schema version tracking, no rollback capability, and no separation between the migration step and the server start step. This is risky in production: schema changes land silently with no audit trail and cannot be undone without manual SQL.

---

## Solution

Replace the inline migration with [goose](https://github.com/pressly/goose), a well-known Go migration library. Migrations live as versioned SQL files in `migrations/`, embedded into the binary via `//go:embed`. A dedicated `cmd/migrate` CLI runs them explicitly — the server no longer migrates on startup.

---

## File Layout

```
migrations/
  00001_create_users.sql
  00002_create_tweets.sql
  00003_create_follows.sql

cmd/migrate/
  main.go
```

Each SQL file uses goose markers:

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

---

## CLI Design (`cmd/migrate/main.go`)

- Accepts a single optional argument: `up` (default), `down`, or `status`.
- Reads the DB connection string via `infra.LoadConfig()` — same env vars as the server.
- Opens a `*sql.DB` (goose uses `database/sql`; pgx is registered via `pgx/stdlib`).
- Calls `goose.SetDialect("postgres")` then the appropriate goose function.
- The `migrations/` directory is embedded with `//go:embed` so no files need to be present on disk at runtime.
- Exits non-zero on any error so CI can catch failed migrations.

**Usage:**
```
go run ./cmd/migrate           # apply all pending migrations
go run ./cmd/migrate status    # show applied / pending
go run ./cmd/migrate down      # roll back one version
make migrate                   # Makefile shortcut for the above
```

Goose creates and manages a `goose_db_version` table automatically.

---

## Changes to Existing Code

| Location | Change |
|---|---|
| `internal/repository/postgres/db.go` | Delete `Migrate()` function entirely |
| `cmd/api/main.go` | Remove `postgres.Migrate()` call |
| `internal/repository/postgres/setup_test.go` | Replace `postgres.Migrate()` with `goose.Up(db, migrationsFS)` using the embedded FS |
| `Makefile` | Add `migrate` target: `go run ./cmd/migrate` |
| `go.mod` | Add `github.com/pressly/goose/v3` |

---

## Error Handling

- CLI exits non-zero on connection failure or migration failure. No retries — the operator fixes the environment and re-runs.
- The server makes no attempt to migrate on startup. If the DB schema is not current, queries will fail with clear Postgres errors — the correct production behaviour.

---

## Testing

Integration tests in `internal/repository/postgres/setup_test.go` call `goose.Up(db, migrationsFS)` directly against the embedded FS. Tests remain self-contained — no SQL files need to be present on the filesystem at test time.
