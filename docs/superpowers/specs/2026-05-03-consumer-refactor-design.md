# Consumer Refactor — Consumers as Infrastructure

**Date:** 2026-05-03

## Context

The current `Consumer` struct in `internal/messaging/rabbitmq/consumer.go` mixes infrastructure concerns (AMQP channel management, message acknowledgement) with business logic (fanout orchestration, backfill, retry policy). All four channels live in a single 327-line file. This design makes the consumers analogous to a monolithic handler — the same problem the HTTP layer already solved by splitting into per-resource files backed by use cases.

## Goal

Apply the same architectural rules as HTTP handlers to consumers:
- One file per channel
- Consumer is pure infrastructure: unmarshal → call UC → ack/nack
- Business logic lives in use cases with `Execute` method
- Private minimal interface per consumer, only used in `*_test.go`

## Architecture

### New Use Cases (`internal/usecase/`)

**`AppendTweetToTimelineUseCase`** (`append_tweet_to_timeline.go`)

The atomic operation: write one tweet to one user's timeline.

```
Execute(ctx context.Context, followerID uuid.UUID, tweet domain.TweetItem, ttl time.Duration) error
```

Deps: `domain.TimelineFanout`. TTL is caller-supplied because each call site needs a different value: `FanoutTweetUseCase` passes `activityTTL - time.Since(fa.LastActive)` (remaining window per follower), `BackfillTimelineUseCase` and `FanoutRetryConsumer` pass the full `activityTTL`.

---

**`FanoutTweetUseCase`** (`fanout_tweet.go`)

Processes a `TweetCreatedEvent` end-to-end:
1. Fetch active followers from `followRepo.GetActiveFollowers`
2. Prepend the author (so their own tweet appears in their timeline)
3. Fan out concurrently via goroutine pool (semaphore, `fanoutWorkers` concurrency)
4. Per-follower TTL = `activityTTL - time.Since(fa.LastActive)`; skip followers with remaining ≤ 0
5. On `AppendTweetToTimelineUseCase` failure: publish to `fanout.retry` via `retryPublisher`
6. Return error only when `eligible > 0 && handled == 0`

```
Execute(ctx context.Context, evt domain.TweetCreatedEvent) error
```

Deps: `domain.FollowRepository`, `*AppendTweetToTimelineUseCase`, `domain.FanoutRetryPublisher` (optional, `WithRetryPublisher`), `activityTTL`, `fanoutWorkers`.

---

**`BackfillTimelineUseCase`** (`backfill_timeline.go`)

Processes a `FollowCreatedEvent`:
1. Fetch last N tweets of the followee via `userTweetsRepo.GetLatestByUser`
2. For each tweet call `AppendTweetToTimelineUseCase.Execute(followerID, tweet)`

```
Execute(ctx context.Context, followerID, followeeID uuid.UUID) error
```

Deps: `domain.UserTweetsRepository`, `*AppendTweetToTimelineUseCase`, `backfillLimit int`.

---

### UC dependency graph

```
FanoutTweetUseCase
  └─ AppendTweetToTimelineUseCase

BackfillTimelineUseCase
  └─ AppendTweetToTimelineUseCase
```

---

### New Consumers (`internal/messaging/rabbitmq/`)

**Shared infrastructure**

`loop.go` — package-level `runLoop(ctx, conn, queue, handler)` function extracted from the current consumer. No business logic; handles channel open, consume, NotifyClose, reconnect backoff.

---

**`TweetCreatedConsumer`** (`tweet_consumer.go`)

Consumes `tweet.created`. Private interface for tests:
```go
type tweetFanout interface {
    Execute(ctx context.Context, evt domain.TweetCreatedEvent) error
}
```
Flow: unmarshal `TweetCreatedEvent` → call `svc.Execute` → ack/nack. Metrics stay in the consumer (infra concern).

---

**`FollowCreatedConsumer`** (`follow_consumer.go`)

Consumes `follow.created`. Private interface:
```go
type timelineBackfiller interface {
    Execute(ctx context.Context, followerID, followeeID uuid.UUID) error
}
```
Flow: unmarshal `FollowCreatedEvent` → call `svc.Execute` → ack/nack.

---

**`FanoutRetryConsumer`** (`fanout_retry_consumer.go`)

Consumes `fanout.retry`. Private interface:
```go
type tweetAppender interface {
    Execute(ctx context.Context, followerID uuid.UUID, tweet domain.TweetItem, ttl time.Duration) error
}
```
Dead-letter logic (xDeathCount check, DLQ publish) stays in the consumer — it's AMQP-specific infrastructure, not business logic.
Flow: unmarshal `FanoutRetryEvent` → check xDeathCount → if ≥ 10: publish DLQ + ack → else: call `svc.Execute` → ack/nack.

---

**`UserActivityConsumer`** (`user_activity_consumer.go`)

Consumes `user.activity`. No use case — logic is a single `UpdateLastActive` write, simple enough to remain inline.
Flow: unmarshal `UserActivityEvent` → call `repo.UpdateLastActive` → ack/nack.

---

### Consumer summary

| File                        | Channel | UC invoked | Own DB/Redis calls |
|-----------------------------|---|---|---|
| `tweet_created_consumer.go` | `tweet.created` | `FanoutTweetUseCase` | none |
| `follow_created_consumer.go`        | `follow.created` | `BackfillTimelineUseCase` | none |
| `fanout_retry_consumer.go`  | `fanout.retry` | `AppendTweetToTimelineUseCase` | none |
| `user_activity_consumer.go` | `user.activity` | inline | one write |

---

## Files deleted

- `internal/messaging/rabbitmq/consumer.go`
- `internal/messaging/rabbitmq/consumer_test.go`

## Testing strategy

Follows ADR-001:
- **Unit tests per consumer** (`*_test.go`): mock the private interface → test ack/nack for bad JSON, UC error, UC success. No infra required.
- **Unit tests per use case** (`*_test.go`): mock domain interfaces → test business rules (TTL expiry, partial fanout failure, all-fail error, retry publishing, backfill iteration).
- Existing integration tests in `handler/integration_test.go` cover the full flow end-to-end and remain unchanged.

## Wiring (`cmd/api/main.go`)

`NewConsumer(...)` call replaced with four individual consumer constructors. `AppendTweetToTimelineUseCase` is instantiated once and passed to both `FanoutTweetUseCase` and `BackfillTimelineUseCase`.
