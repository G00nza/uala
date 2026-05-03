# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start infra (Postgres, Redis, RabbitMQ, Prometheus, Grafana) + run app
make up

# Run app only (infra already running)
make run

# Run database migrations
make migrate

# Stop infra
make down

# Unit tests (no infra required)
go test ./...

# Integration tests (requires infra running)
INTEGRATION=1 go test ./...

# Single test package
go test ./internal/usecase/...

# Single test by name
go test ./internal/usecase/... -run TestTweetUseCase_CreateTweet_OK -v

# Single integration test
INTEGRATION=1 go test ./internal/repository/postgres/... -run TestFollowRepository_GetActiveFollowers -v
```

## Architecture

Clean architecture with four layers. Dependencies point inward — domain has no external imports.

```
domain  ←  usecase  ←  handler (HTTP)
  ↑              ↑
repository    messaging
(postgres,    (rabbitmq)
 redis)
```

**`internal/domain/`** — Pure Go structs and interfaces. All repository and publisher contracts live here. No framework dependencies. `errors.go` defines the sentinel errors that handlers map to HTTP status codes.

**`internal/usecase/`** — Concrete structs (no interfaces, per ADR-001). Each use case owns one operation exposed as `Execute`: `CreateUserUseCase`, `CreateTweetUseCase`, `FollowUserUseCase`, `GetTimelineUseCase`. Use cases validate business rules and orchestrate repositories + publishers.

**`internal/handler/`** — HTTP layer using stdlib `net/http`. Handlers depend directly on concrete use cases. For unit testing, each handler defines a private minimal interface used only in `*_test.go` files. Integration tests (`INTEGRATION=1`) spin up a real `httptest.Server` with Postgres + Redis.

**`internal/repository/postgres/`** and **`internal/repository/redis/`** — Infrastructure implementing domain interfaces. `redis.TimelineRepository` wraps the Postgres timeline repo: cache hit → Redis; miss → Postgres + lazy write to Redis.

**`internal/messaging/rabbitmq/`** — Publisher and Consumer for async fanout. `ResilientConn` handles reconnects automatically. The Consumer runs four goroutines: `ConsumeTweets`, `ConsumeFollows`, `ConsumeFanoutRetry`, `ConsumeUserActivity`.

## Key flows

**Tweet creation (async fanout):**
1. `POST /tweets` → saves to Postgres → publishes `TweetCreatedEvent` to `tweet.created` queue → 201
2. Consumer reads `tweet.created` → calls `GetActiveFollowers` (JOIN with `users.last_active`) → fans out to each active follower's Redis timeline with per-follower TTL

**Follow (backfill):**
1. `POST /follow` → saves to Postgres → publishes `FollowCreatedEvent` to `follow.created` → 201
2. Consumer reads `follow.created` → fetches last `FOLLOW_BACKFILL_LIMIT` tweets of followee → appends to follower's Redis timeline

**Activity tracking:**
- Every `GET /timeline` publishes a `UserActivityEvent` asynchronously via goroutine
- Consumer processes `user.activity` → `UPDATE users SET last_active` (monotonic guard: only updates if newer)
- `last_active` drives celebrity fanout: only followers active within `ACTIVITY_TTL` receive push

**Fanout retry/DLQ:** `fanout.retry` → 30s wait via `fanout.wait` (dead-letter exchange) → back to `fanout.retry` → max 10 retries → `fanout.dead`

## Redis timeline structure

Two keys per user:
- `timeline:{user_id}` — Sorted Set: score=unix timestamp, member=tweet_id
- `timeline:data:{user_id}` — Hash: field=tweet_id, value=JSON tweet

`AppendTweet` uses `ExpireNX` (set TTL only on new keys). Reads use `Expire` to renew TTL to full `ACTIVITY_TTL` window. Users that go inactive have their keys expire naturally.

## Testing strategy (ADR-001)

- **Integration tests** (`INTEGRATION=1`): hit real Postgres + Redis, run full handler→repo flow. These are primary for happy-path coverage.
- **Unit tests with mocks**: cover input validation, error-to-HTTP-status mapping, serialization. Mocks live in `mocks_test.go` files and are package-private.
- The serialization contract between publisher and consumer is guaranteed by shared domain types in `domain/events.go` — no async E2E test needed.

## Configuration

All via env vars (see `internal/infra/config.go`). Key ones beyond standard DB/Redis/AMQP URLs:

| Var | Default | Effect |
|-----|---------|--------|
| `ACTIVITY_TTL` | `24h` | Window for active-user fanout; also the Redis key TTL |
| `TIMELINE_LIMIT` | `500` | Max tweets stored per user in Redis |
| `FOLLOW_BACKFILL_LIMIT` | `20` | Tweets backfilled on new follow |

## Migrations

Managed with [goose](https://github.com/pressly/goose). SQL files live in `migrations/` and are embedded via `//go:embed *.sql`. `make migrate` runs `cmd/migrate`. Integration tests auto-migrate via `TestMain`.
