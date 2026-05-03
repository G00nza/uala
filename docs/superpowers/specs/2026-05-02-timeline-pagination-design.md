# Timeline Pagination — Design Spec

**Date:** 2026-05-02  
**Status:** Approved

## Problem

`GET /timeline` returns all tweets without pagination. The goal is cursor-based pagination with 20 items per page. When the cursor falls outside the Redis cache (tweets older than the 500-item window), falling back directly to Postgres is acceptable.

## API

```
GET /timeline?user_id=<uuid>[&after=<tweet_id>][&before=<tweet_id>]
```

- No cursor → first page (20 most recent tweets)
- `after=<id>` → next page (tweets older than `id`)
- `before=<id>` → previous page (tweets newer than `id`)
- `after` and `before` are mutually exclusive → `400` if both are present

**Response:**
```json
{
  "tweets": [...],
  "next_cursor": "<tweet_id>",
  "prev_cursor": "<tweet_id>"
}
```

- `next_cursor`: ID of the **last** tweet in the page. `null` only if `tweets` is empty.
- `prev_cursor`: ID of the **first** tweet in the page. `null` only if `tweets` is empty.
- The client detects end-of-feed when it receives fewer than 20 tweets.

## Domain changes

Add `TimelineQuery` struct and update `TimelineRepository` interface:

```go
type TimelineQuery struct {
    UserID uuid.UUID
    After  *uuid.UUID // cursor: return tweets older than this ID
    Before *uuid.UUID // cursor: return tweets newer than this ID
    Limit  int
}

type TimelineRepository interface {
    GetTimeline(ctx context.Context, q TimelineQuery) ([]TweetItem, error)
}
```

## Redis logic (ZREVRANK approach)

All three cases operate on the `timeline:{user_id}` Sorted Set (score = unix timestamp, member = tweet_id).

**No cursor (first page):**
```
ZREVRANGE key 0 19
```

**`after=<id>` (next page, older tweets):**
```
rank = ZREVRANK(key, id)
if rank == nil → fallback to Postgres
ZREVRANGE(key, rank+1, rank+20)
```

**`before=<id>` (previous page, newer tweets):**
```
rank = ZREVRANK(key, id)
if rank == nil → fallback to Postgres
ZREVRANGE(key, max(0, rank-20), rank-1) → reverse result to maintain DESC order
```

**Cache miss (key doesn't exist):** load from Postgres, write to Redis, then paginate from Redis.

**Cursor not in Redis (ZREVRANK = nil):** delegate directly to `pgRepo.GetTimeline(ctx, q)`. Do NOT write to Redis — the cursor points to historical data outside the cache window.

## Postgres logic

**No cursor:**
```sql
SELECT t.id, t.user_id, u.username, t.content, t.created_at
FROM follows f
JOIN tweets t ON t.user_id = f.followee_id
JOIN users u ON u.id = t.user_id
WHERE f.follower_id = $1
ORDER BY t.created_at DESC
LIMIT 20
```

**`after=<id>` (next page):**
```sql
SELECT t.id, t.user_id, u.username, t.content, t.created_at
FROM follows f
JOIN tweets t ON t.user_id = f.followee_id
JOIN users u ON u.id = t.user_id
WHERE f.follower_id = $1
  AND t.created_at < (SELECT created_at FROM tweets WHERE id = $2)
ORDER BY t.created_at DESC
LIMIT 20
```

**`before=<id>` (previous page):**
```sql
SELECT t.id, t.user_id, u.username, t.content, t.created_at
FROM follows f
JOIN tweets t ON t.user_id = f.followee_id
JOIN users u ON u.id = t.user_id
WHERE f.follower_id = $1
  AND t.created_at > (SELECT created_at FROM tweets WHERE id = $2)
ORDER BY t.created_at ASC
LIMIT 20
-- reverse result in Go to maintain DESC order
```

## Layer-by-layer changes

| Layer | Change |
|-------|--------|
| `domain/follow.go` | Add `TimelineQuery`; update `TimelineRepository.GetTimeline` signature |
| `handler/timeline.go` | Parse `after`/`before` query params; validate mutual exclusion; build response with `next_cursor`/`prev_cursor` |
| `usecase/timeline.go` | Propagate `TimelineQuery` — no new business logic |
| `repository/redis/timeline.go` | Implement cursor logic with `ZREVRANK`/`ZREVRANGE`; fallback to pgRepo when cursor not in Redis |
| `repository/postgres/timeline.go` | Add cursor variants to `GetTimeline`; existing no-cursor path remains |

## Testing

- **Handler unit tests:** parse valid `after`, valid `before`, both together → 400, empty result → null cursors
- **UseCase unit tests:** minimal changes, update mock signatures
- **Redis integration tests:** no cursor, `after` (cursor in Redis), `before` (cursor in Redis), cursor not in Redis (Postgres fallback)
- **Postgres integration tests:** no cursor, `after`, `before`
