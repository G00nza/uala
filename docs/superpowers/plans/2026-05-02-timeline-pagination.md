# Timeline Cursor Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add cursor-based pagination (`?after=<id>` / `?before=<id>`, 20 items/page) to `GET /timeline`, falling back to Postgres when the cursor is outside the Redis window.

**Architecture:** `TimelineQuery` struct threads the cursor through domain → usecase → redis/postgres repos. Redis uses `ZREVRANK` to locate the cursor tweet in the sorted set and slices the result; if the tweet is absent, it delegates to Postgres. The handler parses `after`/`before` query params, validates they are mutually exclusive, and includes `next_cursor`/`prev_cursor` in the response.

**Tech Stack:** Go 1.25, `github.com/redis/go-redis/v9`, `github.com/jackc/pgx/v5`, `slices` (stdlib Go 1.21+)

---

## Files Modified

| File | Change |
|------|--------|
| `internal/domain/follow.go` | Add `TimelineQuery`; update `TimelineRepository` interface |
| `internal/repository/postgres/timeline.go` | Implement cursor SQL variants |
| `internal/repository/postgres/timeline_test.go` | Update calls + add cursor integration tests |
| `internal/repository/redis/timeline.go` | Implement ZREVRANK cursor logic |
| `internal/repository/redis/timeline_test.go` | Update mocks + add cursor integration tests |
| `internal/usecase/timeline.go` | Propagate cursor params; build `TimelineQuery` |
| `internal/usecase/mocks_test.go` | Update `mockTimelineRepo` signature |
| `internal/usecase/timeline_test.go` | Update existing calls to add `nil, nil` |
| `internal/handler/timeline.go` | Parse params; update private interface; build cursor response |
| `internal/handler/mocks_test.go` | Update `mockTimelineSvc` signature |
| `internal/handler/timeline_test.go` | Add cursor parsing and response tests |

---

## Task 1: Domain types + mechanical signature fixes (restores compilation)

**Files:**
- Modify: `internal/domain/follow.go`
- Modify: `internal/repository/postgres/timeline.go`
- Modify: `internal/repository/redis/timeline.go`
- Modify: `internal/usecase/timeline.go`
- Modify: `internal/usecase/mocks_test.go`
- Modify: `internal/usecase/timeline_test.go`
- Modify: `internal/handler/timeline.go`
- Modify: `internal/handler/mocks_test.go`
- Modify: `internal/repository/redis/timeline_test.go`
- Modify: `internal/repository/postgres/timeline_test.go`

- [ ] **Step 1: Add `TimelineQuery` and update `TimelineRepository` interface**

In `internal/domain/follow.go`, replace:
```go
type TimelineRepository interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]TweetItem, error)
}
```
with:
```go
type TimelineQuery struct {
	UserID uuid.UUID
	After  *uuid.UUID
	Before *uuid.UUID
	Limit  int
}

type TimelineRepository interface {
	GetTimeline(ctx context.Context, q TimelineQuery) ([]TweetItem, error)
}
```

- [ ] **Step 2: Update Postgres repo signature (minimal — no cursor logic yet)**

Replace the entire `GetTimeline` function in `internal/repository/postgres/timeline.go`:

```go
func (r *TimelineRepository) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM follows f
		JOIN tweets t ON t.user_id = f.followee_id
		JOIN users u ON u.id = t.user_id
		WHERE f.follower_id = $1
		ORDER BY t.created_at DESC
		LIMIT $2
	`, q.UserID, q.Limit)
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

- [ ] **Step 3: Update Redis repo signature**

In `internal/repository/redis/timeline.go`:

Replace the `GetTimeline` signature and update all internal calls:

```go
func (r *TimelineRepository) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	key := timelineKey(q.UserID)

	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return r.pgRepo.GetTimeline(ctx, q)
	}

	if exists > 0 {
		metrics.TimelineCacheHitsTotal.Inc()
		return r.readFromRedis(ctx, q)
	}

	metrics.TimelineCacheMissesTotal.Inc()
	items, err := r.pgRepo.GetTimeline(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		_ = r.writeToRedis(ctx, q.UserID, items)
	}
	return items, nil
}
```

Replace the `readFromRedis` signature:

```go
func (r *TimelineRepository) readFromRedis(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	key := timelineKey(q.UserID)
	dataKey := timelineDataKey(q.UserID)

	ids, err := r.rdb.ZRevRange(ctx, key, 0, r.limit-1).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []domain.TweetItem{}, nil
	}

	vals, err := r.rdb.HMGet(ctx, dataKey, ids...).Result()
	if err != nil {
		return nil, err
	}

	if r.activityTTL > 0 {
		_ = r.rdb.Expire(ctx, key, r.activityTTL)
		_ = r.rdb.Expire(ctx, dataKey, r.activityTTL)
	}

	items := make([]domain.TweetItem, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var item domain.TweetItem
		if err := json.Unmarshal([]byte(v.(string)), &item); err != nil {
			return nil, fmt.Errorf("unmarshal tweet item: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}
```

- [ ] **Step 4: Update UseCase signature**

Replace `GetTimeline` in `internal/usecase/timeline.go`:

```go
func (uc *TimelineUseCase) GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
		return nil, err
	}
	items, err := uc.timelineRepo.GetTimeline(ctx, domain.TimelineQuery{
		UserID: userID,
		After:  after,
		Before: before,
		Limit:  20,
	})
	if err != nil {
		return nil, err
	}
	if uc.activityPub != nil {
		evt := domain.UserActivityEvent{UserID: userID, LastActive: time.Now()}
		go func() {
			if pubErr := uc.activityPub.PublishUserActivity(context.Background(), evt); pubErr != nil {
				log.Printf("usecase: publish user activity for %s: %v", userID, pubErr)
			}
		}()
	}
	return items, nil
}
```

- [ ] **Step 5: Update `mockTimelineRepo` in `internal/usecase/mocks_test.go`**

Replace:
```go
func (m *mockTimelineRepo) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```
with:
```go
func (m *mockTimelineRepo) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```

- [ ] **Step 6: Update usecase test calls to add `nil, nil`**

In `internal/usecase/timeline_test.go`, replace every occurrence of:
```go
uc.GetTimeline(context.Background(), userID)
```
with:
```go
uc.GetTimeline(context.Background(), userID, nil, nil)
```

There are 5 occurrences (lines 21, 34, 47, 64, 89 approximately).

- [ ] **Step 7: Update Handler private interface and call**

In `internal/handler/timeline.go`, replace:
```go
type timelineGetter interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error)
}
```
with:
```go
type timelineGetter interface {
	GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error)
}
```

In the same file, replace:
```go
items, err := h.svc.GetTimeline(r.Context(), userID)
```
with:
```go
items, err := h.svc.GetTimeline(r.Context(), userID, nil, nil)
```

- [ ] **Step 8: Update `mockTimelineSvc` in `internal/handler/mocks_test.go`**

Replace:
```go
func (m *mockTimelineSvc) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```
with:
```go
func (m *mockTimelineSvc) GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```

- [ ] **Step 9: Update mocks in `internal/repository/redis/timeline_test.go`**

Replace:
```go
func (m *mockPgTimeline) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```
with:
```go
func (m *mockPgTimeline) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	return m.items, m.err
}
```

Replace:
```go
func (m *countingPgTimeline) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	*m.countPtr++
	return m.items, nil
}
```
with:
```go
func (m *countingPgTimeline) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	*m.countPtr++
	return m.items, nil
}
```

In each test body that calls `repo.GetTimeline(context.Background(), userID)`, replace with `repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})`.
There are 4 occurrences: `TestRedisTimeline_AppendAndGet`, `TestRedisTimeline_MultipleItems_OrderedByScoreDesc`, `TestRedisTimeline_EmptyTimeline_NoFallbackLoop`, `TestRedisTimeline_GetTimeline_RefreshesTTL`.

`TestRedisTimeline_FallbackToPostgresOnMiss` and `TestRedisTimeline_FallbackPopulatesRedis` call `repo.GetTimeline(context.Background(), userID)` — replace those too.

- [ ] **Step 10: Update existing calls in `internal/repository/postgres/timeline_test.go`**

Replace both:
```go
r.timeline.GetTimeline(context.Background(), alice.ID)
```
with:
```go
r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{UserID: alice.ID, Limit: 20})
```

Add import for `domain` package if not already present:
```go
import "uala/internal/domain"
```

- [ ] **Step 11: Verify compilation and unit tests pass**

```bash
go build ./...
go test ./...
```

Expected: all packages compile, unit tests pass (integration tests skip without `INTEGRATION=1`).

- [ ] **Step 12: Commit**

```bash
git add internal/domain/follow.go \
        internal/repository/postgres/timeline.go \
        internal/repository/redis/timeline.go \
        internal/usecase/timeline.go \
        internal/usecase/mocks_test.go \
        internal/usecase/timeline_test.go \
        internal/handler/timeline.go \
        internal/handler/mocks_test.go \
        internal/repository/redis/timeline_test.go \
        internal/repository/postgres/timeline_test.go
git commit -m "refactor: introduce TimelineQuery and update all GetTimeline signatures"
```

---

## Task 2: Postgres cursor implementation + integration tests

**Files:**
- Modify: `internal/repository/postgres/timeline.go`
- Modify: `internal/repository/postgres/timeline_test.go`

- [ ] **Step 1: Write failing integration tests**

Add to `internal/repository/postgres/timeline_test.go`:

```go
import (
    "fmt"
    // existing imports...
)

func TestTimelineRepository_GetTimeline_FirstPage_Limit(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_pg_lim", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_pg_lim", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 5; i++ {
		_ = r.tweet.Create(context.Background(), &domain.Tweet{
			ID: uuid.New(), UserID: bob.ID,
			Content:   fmt.Sprintf("tweet%d", i),
			CreatedAt: base.Add(time.Duration(i) * time.Second),
		})
	}

	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{UserID: alice.ID, Limit: 3})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("want 3 items, got %d", len(items))
	}
}

func TestTimelineRepository_GetTimeline_AfterCursor(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_pg_after", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_pg_after", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	base := time.Now().UTC().Truncate(time.Second)
	t1ID := uuid.New()
	t2ID := uuid.New()
	t3ID := uuid.New()
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t1ID, UserID: bob.ID, Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t2ID, UserID: bob.ID, Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t3ID, UserID: bob.ID, Content: "tweet3", CreatedAt: base})

	// after=t3ID → tweets older than tweet3 → [tweet2, tweet1] DESC
	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{
		UserID: alice.ID, After: &t3ID, Limit: 20,
	})
	if err != nil {
		t.Fatalf("GetTimeline after: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet2" {
		t.Fatalf("want tweet2 first, got %s", items[0].Content)
	}
	if items[1].Content != "tweet1" {
		t.Fatalf("want tweet1 second, got %s", items[1].Content)
	}
}

func TestTimelineRepository_GetTimeline_BeforeCursor(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_pg_before", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_pg_before", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	base := time.Now().UTC().Truncate(time.Second)
	t1ID := uuid.New()
	t2ID := uuid.New()
	t3ID := uuid.New()
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t1ID, UserID: bob.ID, Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t2ID, UserID: bob.ID, Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t3ID, UserID: bob.ID, Content: "tweet3", CreatedAt: base})

	// before=t1ID → tweets newer than tweet1 → [tweet3, tweet2] DESC
	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{
		UserID: alice.ID, Before: &t1ID, Limit: 20,
	})
	if err != nil {
		t.Fatalf("GetTimeline before: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet3" {
		t.Fatalf("want tweet3 first (newest), got %s", items[0].Content)
	}
	if items[1].Content != "tweet2" {
		t.Fatalf("want tweet2 second, got %s", items[1].Content)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -run "TestTimelineRepository_GetTimeline_(FirstPage_Limit|AfterCursor|BeforeCursor)" -v
```

Expected: FAIL — the limit test fails (current impl ignores q.Limit with no cursor), cursor tests fail (no cursor SQL).

- [ ] **Step 3: Implement full Postgres cursor logic**

Replace `GetTimeline` in `internal/repository/postgres/timeline.go` with:

```go
func (r *TimelineRepository) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	switch {
	case q.After != nil:
		return r.getTimelineAfter(ctx, q)
	case q.Before != nil:
		return r.getTimelineBefore(ctx, q)
	default:
		return r.getTimelineFirst(ctx, q)
	}
}

func (r *TimelineRepository) getTimelineFirst(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM follows f
		JOIN tweets t ON t.user_id = f.followee_id
		JOIN users u ON u.id = t.user_id
		WHERE f.follower_id = $1
		ORDER BY t.created_at DESC
		LIMIT $2
	`, q.UserID, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimeline(rows)
}

func (r *TimelineRepository) getTimelineAfter(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM follows f
		JOIN tweets t ON t.user_id = f.followee_id
		JOIN users u ON u.id = t.user_id
		WHERE f.follower_id = $1
		  AND t.created_at < (SELECT created_at FROM tweets WHERE id = $2)
		ORDER BY t.created_at DESC
		LIMIT $3
	`, q.UserID, q.After, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimeline(rows)
}

func (r *TimelineRepository) getTimelineBefore(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM follows f
		JOIN tweets t ON t.user_id = f.followee_id
		JOIN users u ON u.id = t.user_id
		WHERE f.follower_id = $1
		  AND t.created_at > (SELECT created_at FROM tweets WHERE id = $2)
		ORDER BY t.created_at ASC
		LIMIT $3
	`, q.UserID, q.Before, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanTimeline(rows)
	if err != nil {
		return nil, err
	}
	slices.Reverse(items)
	return items, nil
}

func scanTimeline(rows pgx.Rows) ([]domain.TweetItem, error) {
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

Add imports `"slices"` and `"github.com/jackc/pgx/v5"` to the file's import block.

- [ ] **Step 4: Run all Postgres integration tests**

```bash
INTEGRATION=1 go test ./internal/repository/postgres/... -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/repository/postgres/timeline.go internal/repository/postgres/timeline_test.go
git commit -m "feat(postgres): cursor-based pagination in GetTimeline (after/before/limit)"
```

---

## Task 3: Redis cursor implementation + integration tests

**Files:**
- Modify: `internal/repository/redis/timeline.go`
- Modify: `internal/repository/redis/timeline_test.go`

- [ ] **Step 1: Write failing integration tests**

Add to `internal/repository/redis/timeline_test.go`:

```go
func TestRedisTimeline_AfterCursor_InRedis(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	base := time.Now().UTC()
	t1 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute).Truncate(time.Second)}
	t2 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute).Truncate(time.Second)}
	t3 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet3", CreatedAt: base.Truncate(time.Second)}

	_ = repo.AppendTweet(context.Background(), userID, t1, 0)
	_ = repo.AppendTweet(context.Background(), userID, t2, 0)
	_ = repo.AppendTweet(context.Background(), userID, t3, 0)

	// after=t3 → older than t3 → [t2, t1]
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, After: &t3.ID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline after: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet2" {
		t.Fatalf("want tweet2 first, got %s", items[0].Content)
	}
	if items[1].Content != "tweet1" {
		t.Fatalf("want tweet1 second, got %s", items[1].Content)
	}
}

func TestRedisTimeline_BeforeCursor_InRedis(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	base := time.Now().UTC()
	t1 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute).Truncate(time.Second)}
	t2 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute).Truncate(time.Second)}
	t3 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet3", CreatedAt: base.Truncate(time.Second)}

	_ = repo.AppendTweet(context.Background(), userID, t1, 0)
	_ = repo.AppendTweet(context.Background(), userID, t2, 0)
	_ = repo.AppendTweet(context.Background(), userID, t3, 0)

	// before=t1 → newer than t1 → [t3, t2] DESC
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Before: &t1.ID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline before: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet3" {
		t.Fatalf("want tweet3 first (newest), got %s", items[0].Content)
	}
	if items[1].Content != "tweet2" {
		t.Fatalf("want tweet2 second, got %s", items[1].Content)
	}
}

func TestRedisTimeline_CursorNotInRedis_FallsBackToPostgres(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	ghostID := uuid.New()

	pgItems := []domain.TweetItem{
		{ID: uuid.New(), UserID: uuid.New(), Username: "pg", Content: "from_pg", CreatedAt: time.Now().UTC().Truncate(time.Second)},
	}
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{items: pgItems}, 500)

	// Seed one tweet so the Redis key exists
	seed := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "in_redis", CreatedAt: time.Now().UTC().Truncate(time.Second)}
	_ = repo.AppendTweet(context.Background(), userID, seed, 0)

	// Use a cursor NOT in Redis → fallback to Postgres
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, After: &ghostID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item from postgres, got %d", len(items))
	}
	if items[0].Content != "from_pg" {
		t.Fatalf("want 'from_pg', got %s", items[0].Content)
	}
}

func TestRedisTimeline_FirstPage_ReturnsLimit(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	base := time.Now().UTC()
	for i := 0; i < 5; i++ {
		item := domain.TweetItem{
			ID:        uuid.New(),
			UserID:    uuid.New(),
			Username:  "bob",
			Content:   fmt.Sprintf("tweet%d", i),
			CreatedAt: base.Add(time.Duration(i) * time.Second).Truncate(time.Second),
		}
		_ = repo.AppendTweet(context.Background(), userID, item, 0)
	}

	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 3})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("want 3 items, got %d", len(items))
	}
}
```

Also add `"fmt"` to the imports in `timeline_test.go` if not present.

- [ ] **Step 2: Run tests to confirm they fail**

```bash
INTEGRATION=1 go test ./internal/repository/redis/... -run "TestRedisTimeline_(AfterCursor|BeforeCursor|CursorNotInRedis|FirstPage_ReturnsLimit)" -v
```

Expected: FAIL.

- [ ] **Step 3: Implement full Redis cursor logic**

Replace the `GetTimeline` and `readFromRedis` functions in `internal/repository/redis/timeline.go`:

```go
func (r *TimelineRepository) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	key := timelineKey(q.UserID)

	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return r.pgRepo.GetTimeline(ctx, q)
	}

	if exists == 0 {
		metrics.TimelineCacheMissesTotal.Inc()
		if q.After != nil || q.Before != nil {
			// Key expired but cursor is present — go straight to Postgres
			return r.pgRepo.GetTimeline(ctx, q)
		}
		// First page cache miss: warm Redis with up to r.limit tweets, then paginate
		allItems, err := r.pgRepo.GetTimeline(ctx, domain.TimelineQuery{UserID: q.UserID, Limit: int(r.limit)})
		if err != nil {
			return nil, err
		}
		if len(allItems) == 0 {
			return allItems, nil
		}
		_ = r.writeToRedis(ctx, q.UserID, allItems)
	} else {
		metrics.TimelineCacheHitsTotal.Inc()
	}

	return r.readFromRedis(ctx, q)
}

func (r *TimelineRepository) readFromRedis(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	key := timelineKey(q.UserID)
	dataKey := timelineDataKey(q.UserID)

	var ids []string

	switch {
	case q.After != nil:
		rank, err := r.rdb.ZRevRank(ctx, key, q.After.String()).Result()
		if err == redis.Nil {
			return r.pgRepo.GetTimeline(ctx, q)
		}
		if err != nil {
			return nil, err
		}
		ids, err = r.rdb.ZRevRange(ctx, key, rank+1, rank+int64(q.Limit)).Result()
		if err != nil {
			return nil, err
		}
	case q.Before != nil:
		rank, err := r.rdb.ZRevRank(ctx, key, q.Before.String()).Result()
		if err == redis.Nil {
			return r.pgRepo.GetTimeline(ctx, q)
		}
		if err != nil {
			return nil, err
		}
		start := rank - int64(q.Limit)
		if start < 0 {
			start = 0
		}
		ids, err = r.rdb.ZRevRange(ctx, key, start, rank-1).Result()
		if err != nil {
			return nil, err
		}
	default:
		var err error
		ids, err = r.rdb.ZRevRange(ctx, key, 0, int64(q.Limit)-1).Result()
		if err != nil {
			return nil, err
		}
	}

	if len(ids) == 0 {
		return []domain.TweetItem{}, nil
	}

	vals, err := r.rdb.HMGet(ctx, dataKey, ids...).Result()
	if err != nil {
		return nil, err
	}

	if r.activityTTL > 0 {
		_ = r.rdb.Expire(ctx, key, r.activityTTL)
		_ = r.rdb.Expire(ctx, dataKey, r.activityTTL)
	}

	items := make([]domain.TweetItem, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var item domain.TweetItem
		if err := json.Unmarshal([]byte(v.(string)), &item); err != nil {
			return nil, fmt.Errorf("unmarshal tweet item: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}
```

- [ ] **Step 4: Run all Redis integration tests**

```bash
INTEGRATION=1 go test ./internal/repository/redis/... -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/repository/redis/timeline.go internal/repository/redis/timeline_test.go
git commit -m "feat(redis): cursor-based pagination via ZREVRANK in GetTimeline"
```

---

## Task 4: Handler cursor parsing + response + unit tests

**Files:**
- Modify: `internal/handler/timeline.go`
- Modify: `internal/handler/mocks_test.go`
- Modify: `internal/handler/timeline_test.go`

- [ ] **Step 1: Write failing unit tests**

Add to `internal/handler/timeline_test.go`:

```go
func TestTimelineHandler_GetTimeline_BothCursors_Returns400(t *testing.T) {
	h := handler.NewTimelineHandler(&mockTimelineSvc{})

	url := "/timeline?after=" + uuid.New().String() + "&before=" + uuid.New().String()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTimelineHandler_GetTimeline_InvalidAfterUUID_Returns400(t *testing.T) {
	h := handler.NewTimelineHandler(&mockTimelineSvc{})

	req := httptest.NewRequest(http.MethodGet, "/timeline?after=not-a-uuid", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTimelineHandler_GetTimeline_CursorsInResponse(t *testing.T) {
	firstID := uuid.New()
	lastID := uuid.New()
	svc := &mockTimelineSvc{items: []domain.TweetItem{
		{ID: firstID, UserID: uuid.New(), Username: "a", Content: "first", CreatedAt: time.Now().Add(-1 * time.Second)},
		{ID: lastID, UserID: uuid.New(), Username: "b", Content: "last", CreatedAt: time.Now().Add(-2 * time.Second)},
	}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["prev_cursor"] != firstID.String() {
		t.Fatalf("want prev_cursor=%s, got %v", firstID, resp["prev_cursor"])
	}
	if resp["next_cursor"] != lastID.String() {
		t.Fatalf("want next_cursor=%s, got %v", lastID, resp["next_cursor"])
	}
}

func TestTimelineHandler_GetTimeline_EmptyResult_NullCursors(t *testing.T) {
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
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["next_cursor"] != nil {
		t.Fatalf("want next_cursor=null, got %v", resp["next_cursor"])
	}
	if resp["prev_cursor"] != nil {
		t.Fatalf("want prev_cursor=null, got %v", resp["prev_cursor"])
	}
}

func TestTimelineHandler_GetTimeline_AfterCursorPassedToService(t *testing.T) {
	afterID := uuid.New()
	svc := &mockTimelineSvc{items: []domain.TweetItem{}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline?after="+afterID.String(), nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if svc.capturedAfter == nil || *svc.capturedAfter != afterID {
		t.Fatalf("want after=%s passed to svc, got %v", afterID, svc.capturedAfter)
	}
}
```

- [ ] **Step 2: Update `mockTimelineSvc` to capture cursor params**

In `internal/handler/mocks_test.go`, replace the `mockTimelineSvc` struct and method:

```go
type mockTimelineSvc struct {
	items          []domain.TweetItem
	err            error
	capturedAfter  *uuid.UUID
	capturedBefore *uuid.UUID
}

func (m *mockTimelineSvc) GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error) {
	m.capturedAfter = after
	m.capturedBefore = before
	return m.items, m.err
}
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
go test ./internal/handler/... -run "TestTimelineHandler_GetTimeline_(BothCursors|InvalidAfter|CursorsInResponse|EmptyResult_NullCursors|AfterPassedToService)" -v
```

Expected: FAIL (handler doesn't parse params or return cursors yet).

- [ ] **Step 4: Implement full handler**

Replace the entire content of `internal/handler/timeline.go`:

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
	GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error)
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

	afterStr := r.URL.Query().Get("after")
	beforeStr := r.URL.Query().Get("before")

	if afterStr != "" && beforeStr != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "after and before are mutually exclusive"})
		return
	}

	var after, before *uuid.UUID
	if afterStr != "" {
		id, err := uuid.Parse(afterStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "after must be a valid UUID"})
			return
		}
		after = &id
	}
	if beforeStr != "" {
		id, err := uuid.Parse(beforeStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "before must be a valid UUID"})
			return
		}
		before = &id
	}

	items, err := h.svc.GetTimeline(r.Context(), userID, after, before)
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

	var nextCursor, prevCursor *string
	if len(items) > 0 {
		first := items[0].ID.String()
		last := items[len(items)-1].ID.String()
		prevCursor = &first
		nextCursor = &last
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tweets":      resp,
		"next_cursor": nextCursor,
		"prev_cursor": prevCursor,
	})
}
```

- [ ] **Step 5: Run all handler unit tests**

```bash
go test ./internal/handler/... -v
```

Expected: all tests PASS.

- [ ] **Step 6: Run full test suite**

```bash
go test ./...
```

Expected: all unit tests PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/handler/timeline.go internal/handler/mocks_test.go internal/handler/timeline_test.go
git commit -m "feat(handler): cursor pagination params and next/prev cursors in response"
```
