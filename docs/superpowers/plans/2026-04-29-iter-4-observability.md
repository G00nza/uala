# Iter 4 — Observability (Prometheus + Grafana) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Instrument the system with Prometheus metrics (HTTP, Redis cache, RabbitMQ, Postgres) and visualize them in a Grafana dashboard.

**Architecture:** A package-level `internal/metrics` package holds all Prometheus metric variables registered via `init()`. An HTTP middleware wraps the router to record duration and request counts. Redis `TimelineRepository.GetTimeline` increments cache-hit/miss counters. RabbitMQ consumers time their `Handle` method and increment processed/failed counters. `postgres.Connect` accepts variadic config options so `main` can inject a pgx `QueryTracer` for query duration. A goroutine in `main` samples the Postgres pool's active connections every 5s and RabbitMQ queue depth every 15s. The `/metrics` endpoint is added to the router using `promhttp.Handler()`.

**Tech Stack:** Go 1.25, `github.com/prometheus/client_golang`, Prometheus 2.x, Grafana 10.x (with provisioning), existing pgx/v5 + go-redis/v9 + amqp091

**Starting state (after iter-3 complete):**
- `postgres.Connect(ctx, dsn)` — 2 args (will add variadic opts)
- `redis.TimelineRepository.GetTimeline` — no metrics calls yet
- `broker/rabbitmq` consumers — `Handle` methods have no timing
- `handler/router.go` — no middleware wrapping

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-compose.yml` | Modify | Add Prometheus + Grafana services |
| `monitoring/prometheus/prometheus.yml` | Create | Prometheus scrape config |
| `monitoring/grafana/provisioning/datasources/prometheus.yml` | Create | Grafana datasource provisioning |
| `monitoring/grafana/provisioning/dashboards/dashboard.yml` | Create | Grafana dashboard provider config |
| `monitoring/grafana/dashboards/uala.json` | Create | Grafana dashboard JSON |
| `internal/metrics/metrics.go` | Create | All Prometheus metric vars + `init()` registration |
| `internal/metrics/middleware.go` | Create | HTTP middleware capturing duration + status |
| `internal/metrics/middleware_test.go` | Create | Unit tests for middleware pass-through and status capture |
| `internal/metrics/pgx_tracer.go` | Create | `pgx.QueryTracer` for `db_query_duration_seconds` + `db_errors_total` |
| `internal/repository/postgres/db.go` | Modify | Accept `...func(*pgxpool.Config)` opts; use `ParseConfig` + `NewWithConfig` |
| `internal/repository/redis/timeline.go` | Modify | Call `metrics.TimelineCacheHitsTotal.Inc()` / `CacheMissesTotal.Inc()` |
| `internal/broker/rabbitmq/tweet_consumer.go` | Modify | Time `Handle`; inc `RabbitMQMessagesProcessed`/`Failed` |
| `internal/broker/rabbitmq/follow_consumer.go` | Modify | Time `Handle`; inc `RabbitMQMessagesProcessed`/`Failed` |
| `internal/handler/router.go` | Modify | Add `/metrics` endpoint; wrap with `metrics.Middleware` |
| `cmd/api/main.go` | Modify | Inject pgx tracer; start pool + queue-depth samplers |

---

### Task 1: Add Prometheus + Grafana to docker-compose + monitoring files

**Files:**
- Modify: `docker-compose.yml`
- Create: `monitoring/prometheus/prometheus.yml`
- Create: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Create: `monitoring/grafana/provisioning/dashboards/dashboard.yml`

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

  prometheus:
    image: prom/prometheus:latest
    container_name: uala_prometheus
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:latest
    container_name: uala_grafana
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
    ports:
      - "3000:3000"
    depends_on:
      - prometheus

volumes:
  postgres_data:
```

- [ ] **Step 2: Create monitoring/prometheus/prometheus.yml**

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: uala
    static_configs:
      - targets: ["host.docker.internal:8080"]
```

- [ ] **Step 3: Create monitoring/grafana/provisioning/datasources/prometheus.yml**

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 4: Create monitoring/grafana/provisioning/dashboards/dashboard.yml**

```yaml
apiVersion: 1

providers:
  - name: uala
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 5: Start services**

```bash
make up
```

Expected: `uala_prometheus` and `uala_grafana` containers start. Prometheus UI at http://localhost:9090, Grafana at http://localhost:3000 (admin/admin).

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml monitoring/
git commit -m "feat: add Prometheus + Grafana to docker-compose with monitoring config"
```

---

### Task 2: Add prometheus/client_golang dep

**Files:**
- Modify: `go.mod`, `go.sum` (auto)

- [ ] **Step 1: Add dep**

```bash
go get github.com/prometheus/client_golang@latest
```

- [ ] **Step 2: Verify**

```bash
grep "client_golang" go.mod
```

Expected: `github.com/prometheus/client_golang v1.x.x`

- [ ] **Step 3: Build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "feat: add prometheus/client_golang dep"
```

---

### Task 3: Modify postgres.Connect for variadic config options

**Files:**
- Modify: `internal/repository/postgres/db.go`

- [ ] **Step 1: Rewrite internal/repository/postgres/db.go**

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

- [ ] **Step 2: Build and run integration tests (existing callers use 0 opts — still compile)**

```bash
go build ./...
INTEGRATION=1 go test ./internal/repository/postgres/... -v 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: build succeeds; all postgres tests PASS (variadic with no args is backward-compatible).

- [ ] **Step 3: Commit**

```bash
git add internal/repository/postgres/db.go
git commit -m "refactor: postgres.Connect accepts variadic config opts for pgx tracer injection"
```

---

### Task 4: Create internal/metrics/metrics.go

**Files:**
- Create: `internal/metrics/metrics.go`

- [ ] **Step 1: Create internal/metrics/metrics.go**

```go
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration by path, method and status code",
		Buckets: prometheus.DefBuckets,
	}, []string{"path", "method", "status"})

	HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests by path, method and status code",
	}, []string{"path", "method", "status"})

	TimelineCacheHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "timeline_cache_hits_total",
		Help: "Timelines served from Redis cache",
	})

	TimelineCacheMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "timeline_cache_misses_total",
		Help: "Timeline cache misses that fell back to Postgres",
	})

	FanoutDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fanout_duration_seconds",
		Help:    "Time spent by tweet consumer fanning out to Redis",
		Buckets: prometheus.DefBuckets,
	})

	RabbitMQMessagesProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rabbitmq_messages_processed_total",
		Help: "Messages successfully processed by a consumer",
	}, []string{"queue"})

	RabbitMQMessagesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rabbitmq_messages_failed_total",
		Help: "Messages that failed during consumer processing",
	}, []string{"queue"})

	RabbitMQQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rabbitmq_queue_depth",
		Help: "Number of messages pending in a RabbitMQ queue",
	}, []string{"queue"})

	DBQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "db_query_duration_seconds",
		Help:    "Postgres query latency by SQL operation (SELECT/INSERT/etc.)",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	DBConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_connections_active",
		Help: "Number of active connections in the Postgres pool",
	})

	DBErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "db_errors_total",
		Help: "Postgres errors by type",
	}, []string{"type"})
)

func init() {
	prometheus.MustRegister(
		HTTPRequestDuration,
		HTTPRequestsTotal,
		TimelineCacheHitsTotal,
		TimelineCacheMissesTotal,
		FanoutDuration,
		RabbitMQMessagesProcessed,
		RabbitMQMessagesFailed,
		RabbitMQQueueDepth,
		DBQueryDuration,
		DBConnectionsActive,
		DBErrorsTotal,
	)
}
```

- [ ] **Step 2: Build**

```bash
go build ./internal/metrics/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/metrics/metrics.go
git commit -m "feat: metrics package — all Prometheus metric definitions"
```

---

### Task 5: HTTP middleware + tests

**Files:**
- Create: `internal/metrics/middleware.go`
- Create: `internal/metrics/middleware_test.go`

- [ ] **Step 1: Create internal/metrics/middleware.go**

```go
package metrics

import (
	"net/http"
	"strconv"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Middleware records http_request_duration_seconds and http_requests_total per request.
// If WriteHeader is never called (response body written directly), status defaults to 200.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(sw.status)
		HTTPRequestDuration.WithLabelValues(r.URL.Path, r.Method, status).Observe(duration)
		HTTPRequestsTotal.WithLabelValues(r.URL.Path, r.Method, status).Inc()
	})
}
```

- [ ] **Step 2: Create internal/metrics/middleware_test.go**

```go
package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"uala/internal/metrics"
)

func TestMiddleware_PassesThroughResponse(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"id":"abc"}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/tweets", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rr.Code)
	}
	if rr.Body.String() != `{"id":"abc"}` {
		t.Fatalf("want body '{\"id\":\"abc\"}', got %s", rr.Body.String())
	}
}

func TestMiddleware_DefaultsTo200WhenWriteHeaderNotCalled(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
}

func TestMiddleware_CapturesNonOKStatus(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rr.Code)
	}
}
```

- [ ] **Step 3: Run middleware tests**

```bash
go test ./internal/metrics/... -v
```

Expected:
```
--- PASS: TestMiddleware_PassesThroughResponse (0.00s)
--- PASS: TestMiddleware_DefaultsTo200WhenWriteHeaderNotCalled (0.00s)
--- PASS: TestMiddleware_CapturesNonOKStatus (0.00s)
PASS
```

- [ ] **Step 4: Commit**

```bash
git add internal/metrics/middleware.go internal/metrics/middleware_test.go
git commit -m "feat: HTTP metrics middleware — duration + request count per endpoint"
```

---

### Task 6: pgx QueryTracer for db_query_duration_seconds + db_errors_total

**Files:**
- Create: `internal/metrics/pgx_tracer.go`

- [ ] **Step 1: Create internal/metrics/pgx_tracer.go**

```go
package metrics

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type traceStartKey struct{}

// PgxTracer implements pgx.QueryTracer to record db_query_duration_seconds and db_errors_total.
type PgxTracer struct{}

func (t *PgxTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, traceStartKey{}, time.Now())
}

func (t *PgxTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(traceStartKey{}).(time.Time)
	if !ok {
		return
	}
	op := sqlOperation(data.SQL)
	DBQueryDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if data.Err != nil {
		DBErrorsTotal.WithLabelValues("query").Inc()
	}
}

func sqlOperation(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "unknown"
	}
	return strings.ToUpper(fields[0])
}
```

- [ ] **Step 2: Build**

```bash
go build ./internal/metrics/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/metrics/pgx_tracer.go
git commit -m "feat: pgx QueryTracer — db_query_duration_seconds + db_errors_total"
```

---

### Task 7: Instrument Redis — cache hits/misses

**Files:**
- Modify: `internal/repository/redis/timeline.go`

- [ ] **Step 1: Read current internal/repository/redis/timeline.go**

The `GetTimeline` method checks `EXISTS` on the Redis key. If the key exists (`exists > 0`), it reads from Redis (cache hit). If not, it falls back to Postgres (cache miss).

- [ ] **Step 2: Modify GetTimeline in internal/repository/redis/timeline.go**

Add `"uala/internal/metrics"` to the import block. Then replace the `GetTimeline` method with:

```go
func (r *TimelineRepository) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	key := timelineKey(userID)

	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return r.pgRepo.GetTimeline(ctx, userID)
	}

	if exists > 0 {
		metrics.TimelineCacheHitsTotal.Inc()
		return r.readFromRedis(ctx, key)
	}

	metrics.TimelineCacheMissesTotal.Inc()
	items, err := r.pgRepo.GetTimeline(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		_ = r.writeToRedis(ctx, key, items)
	}
	return items, nil
}
```

- [ ] **Step 3: Build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Run redis integration tests (no regressions)**

```bash
INTEGRATION=1 go test ./internal/repository/redis/... -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/repository/redis/timeline.go
git commit -m "feat: instrument Redis GetTimeline — timeline_cache_hits/misses_total"
```

---

### Task 8: Instrument RabbitMQ consumers

**Files:**
- Modify: `internal/broker/rabbitmq/tweet_consumer.go`
- Modify: `internal/broker/rabbitmq/follow_consumer.go`

- [ ] **Step 1: Modify TweetConsumer.Handle in internal/broker/rabbitmq/tweet_consumer.go**

Add `"time"` and `"uala/internal/metrics"` to imports. Replace `Handle` and the message-processing block in `Start`:

```go
func (c *TweetConsumer) Handle(ctx context.Context, item domain.TweetItem) error {
	start := time.Now()
	followers, err := c.followRepo.GetFollowers(ctx, item.UserID)
	if err != nil {
		return fmt.Errorf("get followers: %w", err)
	}
	for _, followerID := range followers {
		if err := c.fanout.AppendTweet(ctx, followerID, item); err != nil {
			log.Printf("fanout tweet to %s: %v", followerID, err)
		}
	}
	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
	return nil
}
```

In `Start`, replace the `Ack`/`Nack` block with:

```go
if err := c.Handle(ctx, item); err != nil {
	log.Printf("handle tweet event: %v", err)
	metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
	msg.Nack(false, true)
} else {
	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	msg.Ack(false)
}
```

Full rewrite of `internal/broker/rabbitmq/tweet_consumer.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
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
	start := time.Now()
	followers, err := c.followRepo.GetFollowers(ctx, item.UserID)
	if err != nil {
		return fmt.Errorf("get followers: %w", err)
	}
	for _, followerID := range followers {
		if err := c.fanout.AppendTweet(ctx, followerID, item); err != nil {
			log.Printf("fanout tweet to %s: %v", followerID, err)
		}
	}
	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
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
					metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
					msg.Nack(false, true)
				} else {
					metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
					msg.Ack(false)
				}
			}
		}
	}()
	return nil
}
```

- [ ] **Step 2: Full rewrite of internal/broker/rabbitmq/follow_consumer.go**

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
	"uala/internal/metrics"
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
					metrics.RabbitMQMessagesFailed.WithLabelValues(QueueUserFollowed).Inc()
					msg.Nack(false, true)
				} else {
					metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueUserFollowed).Inc()
					msg.Ack(false)
				}
			}
		}
	}()
	return nil
}
```

- [ ] **Step 3: Build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Run broker unit tests**

```bash
go test ./internal/broker/rabbitmq/... -run "TestTweetConsumer|TestFollowConsumer" -v
```

Expected: all PASS (Handle behaviour unchanged — metrics calls don't affect return values).

- [ ] **Step 5: Commit**

```bash
git add internal/broker/rabbitmq/tweet_consumer.go internal/broker/rabbitmq/follow_consumer.go
git commit -m "feat: instrument RabbitMQ consumers — fanout_duration, messages_processed/failed"
```

---

### Task 9: Wire metrics in router + main.go

**Files:**
- Modify: `internal/handler/router.go`
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Read current internal/handler/router.go**

Identify where `http.ServeMux` or similar is returned.

- [ ] **Step 2: Modify internal/handler/router.go**

Add `"github.com/prometheus/client_golang/prometheus/promhttp"` and `"uala/internal/metrics"` to imports. Add the `/metrics` route and wrap the handler with `metrics.Middleware`.

Replace the existing `NewRouter` function with:

```go
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
	mux.Handle("GET /metrics", promhttp.Handler())

	return metrics.Middleware(mux)
}
```

Note: read the current router.go first to see the exact route patterns used (they may be `"POST /users"` or `r.HandleFunc("/users", ...)` — match the existing style, only adding `/metrics` and the middleware wrap).

- [ ] **Step 3: Modify cmd/api/main.go**

Add imports: `"time"`, `"uala/internal/metrics"`. Add the pgx tracer opt to `postgres.Connect`, and start the two sampler goroutines after `db` and `conn` are created.

Replace `cmd/api/main.go` with:

```go
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"uala/internal/broker/rabbitmq"
	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/metrics"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := infra.LoadConfig()
	ctx := context.Background()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL, func(c *pgxpool.Config) {
		c.ConnConfig.Tracer = &metrics.PgxTracer{}
	})
	if err != nil {
		log.Fatal("postgres:", err)
	}
	defer db.Close()

	if err := postgres.Migrate(ctx, db); err != nil {
		log.Fatal("migrate:", err)
	}

	// Sample Postgres active connections every 5s
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			metrics.DBConnectionsActive.Set(float64(db.Stat().AcquiredConns()))
		}
	}()

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

	// Sample RabbitMQ queue depth every 15s
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		ch, err := conn.Channel()
		if err != nil {
			log.Printf("queue depth sampler: open channel: %v", err)
			return
		}
		defer ch.Close()
		for range ticker.C {
			for _, qName := range []string{rabbitmq.QueueTweetCreated, rabbitmq.QueueUserFollowed} {
				q, err := ch.QueueDeclarePassive(qName, true, false, false, false, nil)
				if err == nil {
					metrics.RabbitMQQueueDepth.WithLabelValues(qName).Set(float64(q.Messages))
				}
			}
		}
	}()

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

- [ ] **Step 4: Build binary**

```bash
go build ./cmd/api/...
```

Expected: no errors.

- [ ] **Step 5: Run all unit tests**

```bash
go test ./internal/... -v 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages show `ok`, no `FAIL`.

- [ ] **Step 6: Commit**

```bash
git add internal/handler/router.go cmd/api/main.go
git commit -m "feat: wire metrics — /metrics endpoint, HTTP middleware, pgx tracer, pool sampler"
```

---

### Task 10: Grafana dashboard + E2E verification

**Files:**
- Create: `monitoring/grafana/dashboards/uala.json`

- [ ] **Step 1: Create monitoring/grafana/dashboards/uala.json**

```json
{
  "__inputs": [],
  "__requires": [],
  "annotations": { "list": [] },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": {
        "defaults": { "unit": "s" },
        "overrides": []
      },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "id": 1,
      "options": { "legend": { "calcs": ["p50", "p95", "p99"], "displayMode": "table", "placement": "bottom" } },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, path))",
          "legendFormat": "p50 {{path}}"
        },
        {
          "expr": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, path))",
          "legendFormat": "p95 {{path}}"
        },
        {
          "expr": "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, path))",
          "legendFormat": "p99 {{path}}"
        }
      ],
      "title": "HTTP Latency p50/p95/p99 by Endpoint",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "percentunit", "min": 0, "max": 1 }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "id": 2,
      "targets": [
        {
          "expr": "rate(timeline_cache_hits_total[5m]) / (rate(timeline_cache_hits_total[5m]) + rate(timeline_cache_misses_total[5m]))",
          "legendFormat": "Cache Hit Rate"
        }
      ],
      "title": "Timeline Cache Hit Rate",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "id": 3,
      "targets": [
        {
          "expr": "rabbitmq_queue_depth",
          "legendFormat": "{{queue}}"
        }
      ],
      "title": "RabbitMQ Queue Depth",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "percentunit", "min": 0, "max": 1 }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "id": 4,
      "targets": [
        {
          "expr": "rate(rabbitmq_messages_processed_total[5m]) / (rate(rabbitmq_messages_processed_total[5m]) + rate(rabbitmq_messages_failed_total[5m]))",
          "legendFormat": "Success Rate {{queue}}"
        }
      ],
      "title": "RabbitMQ Consumer Success Rate",
      "type": "timeseries"
    }
  ],
  "refresh": "30s",
  "schemaVersion": 38,
  "tags": ["uala"],
  "templating": { "list": [] },
  "time": { "from": "now-1h", "to": "now" },
  "timepicker": {},
  "timezone": "browser",
  "title": "Uala — System Metrics",
  "uid": "uala-metrics",
  "version": 1
}
```

- [ ] **Step 2: Start everything and generate some traffic**

```bash
make up
DATABASE_URL=postgres://uala:uala@localhost:5432/uala \
  REDIS_URL=redis://localhost:6379/0 \
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
  PORT=8080 \
  go run ./cmd/api/... &
sleep 2

# Create users + follow + tweets to generate metrics
ALICE=$(curl -s -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{"username":"alice"}' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
BOB=$(curl -s -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{"username":"bob"}' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
curl -s -X POST http://localhost:8080/follow -H "Content-Type: application/json" -H "X-User-ID: $ALICE" -d "{\"followee_id\":\"$BOB\"}"
curl -s -X POST http://localhost:8080/tweets -H "Content-Type: application/json" -H "X-User-ID: $BOB" -d '{"content":"metrics test tweet"}'
sleep 1
curl -s http://localhost:8080/timeline -H "X-User-ID: $ALICE"
curl -s http://localhost:8080/timeline -H "X-User-ID: $ALICE"
```

- [ ] **Step 3: Verify /metrics endpoint**

```bash
curl -s http://localhost:8080/metrics | grep -E "^(http_requests_total|timeline_cache|fanout_duration|rabbitmq_messages)"
```

Expected output includes lines like:
```
http_requests_total{method="POST",path="/follow",status="200"} 1
http_requests_total{method="POST",path="/tweets",status="201"} 1
timeline_cache_hits_total 1
timeline_cache_misses_total 1
rabbitmq_messages_processed_total{queue="tweet.created"} 1
```

- [ ] **Step 4: Verify Prometheus is scraping**

Open http://localhost:9090/targets — `uala` job should show `UP`.

Run in Prometheus UI: `http_requests_total` — should return time-series data.

- [ ] **Step 5: Verify Grafana dashboard**

Open http://localhost:3000 (admin/admin) → Dashboards → "Uala — System Metrics". All four panels should show data.

- [ ] **Step 6: Stop server, run full test suite**

```bash
kill %1
go test ./... -v 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages show `ok`, no `FAIL`.

- [ ] **Step 7: Commit**

```bash
git add monitoring/grafana/dashboards/uala.json
git commit -m "feat: iter-4 complete — Prometheus metrics + Grafana dashboard"
```

---

## Self-Review

**Spec coverage:**
- [x] Prometheus + Grafana in `docker-compose.yml` → Task 1
- [x] `/metrics` endpoint → Task 9 (router.go)
- [x] `http_request_duration_seconds` + `http_requests_total` → Tasks 4, 5
- [x] `timeline_cache_hits_total` + `timeline_cache_misses_total` → Task 7
- [x] `fanout_duration_seconds` → Task 8 (tweet_consumer.go)
- [x] `rabbitmq_messages_processed_total` + `rabbitmq_messages_failed_total` → Task 8
- [x] `rabbitmq_queue_depth` → Task 9 (main.go sampler goroutine)
- [x] `db_query_duration_seconds` → Tasks 6, 9 (pgx tracer + postgres.Connect opt)
- [x] `db_connections_active` → Task 9 (main.go pool sampler goroutine)
- [x] `db_errors_total` → Task 6 (pgx tracer)
- [x] Grafana dashboard: latency p50/p95/p99, cache hit rate, queue depth, success rate → Task 10

**Placeholder scan:** All steps contain complete Go code. No TBD references.

**Type consistency:**
- `metrics.PgxTracer` implements `pgx.QueryTracer` (`TraceQueryStart` + `TraceQueryEnd`) — both methods defined in Task 6
- `metrics.Middleware(next http.Handler) http.Handler` — used in router.go Task 9, matches signature
- `postgres.Connect(ctx, dsn, ...func(*pgxpool.Config))` variadic — used in main.go as `func(c *pgxpool.Config) { c.ConnConfig.Tracer = &metrics.PgxTracer{} }` — type matches
- All `metrics.*` vars used in redis/timeline.go and consumer files match the names defined in `metrics.go`
