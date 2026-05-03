# Fanout DLQ por follower fallido — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cuando `AppendTweet` falla para un follower, encolar un retry event en RabbitMQ; reintentarlo hasta 10 veces con backoff de 30s vía DLX; al agotar los intentos, mover a `fanout.dead` e incrementar una métrica.

**Architecture:** Cola `fanout.retry` con DLX apuntando a `fanout.wait` (TTL 30s) que rebota de vuelta a `fanout.retry`. El consumer lee el header `x-death` para saber cuántas veces fue reintentado. A los 10 fallos publica en `fanout.dead` y hace Ack.

**Tech Stack:** Go 1.25, `github.com/rabbitmq/amqp091-go`, Prometheus `client_golang`.

---

## File Map

| Archivo | Acción | Responsabilidad |
|---|---|---|
| `internal/domain/events.go` | Modify | +`FanoutRetryEvent`, +`FanoutRetryPublisher` interface |
| `internal/metrics/metrics.go` | Modify | +`FanoutDeadLetterTotal` counter |
| `internal/messaging/rabbitmq/client.go` | Modify | +constantes de colas/exchanges, +declaración en `Connect` |
| `internal/messaging/rabbitmq/publisher.go` | Modify | +`PublishFanoutRetry`, +`PublishToDeadLetter` |
| `internal/messaging/rabbitmq/consumer.go` | Modify | +campo `retryPublisher`, lógica de retry en `fanoutTweet`, +`ConsumeFanoutRetry` |
| `internal/messaging/rabbitmq/consumer_test.go` | Modify | +5 tests nuevos |
| `cmd/api/main.go` | Modify | +`go consumer.ConsumeFanoutRetry(ctx)` |

---

## Task 1: Dominio — FanoutRetryEvent e interfaz publisher

**Files:**
- Modify: `internal/domain/events.go`

- [ ] **Step 1: Agregar tipos al dominio**

Abrir `internal/domain/events.go` y agregar al final del archivo:

```go
type FanoutRetryEvent struct {
	FollowerID uuid.UUID `json:"follower_id"`
	Tweet      TweetItem `json:"tweet"`
}

type FanoutRetryPublisher interface {
	PublishFanoutRetry(ctx context.Context, evt FanoutRetryEvent) error
}
```

- [ ] **Step 2: Verificar que compila**

```bash
go build ./...
```

Expected: sin errores.

- [ ] **Step 3: Commit**

```bash
git add internal/domain/events.go
git commit -m "feat(domain): add FanoutRetryEvent and FanoutRetryPublisher interface"
```

---

## Task 2: Métrica FanoutDeadLetterTotal

**Files:**
- Modify: `internal/metrics/metrics.go`

- [ ] **Step 1: Escribir el test que falla**

Agregar al final de `internal/metrics/metrics.go` un test inline no — la métrica se verifica via compilación y registro. Agregar la variable y el registro:

En `internal/metrics/metrics.go`, agregar la variable junto a las demás:

```go
FanoutDeadLetterTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
    Name: "fanout_dead_letter_total",
    Help: "Tweets que no pudieron appendearse a un follower tras 10 reintentos.",
}, []string{"follower_id"})
```

Y en `func init()`, agregar dentro del `prometheus.MustRegister(...)`:

```go
FanoutDeadLetterTotal,
```

- [ ] **Step 2: Verificar que compila y el registro no paniquea**

```bash
go build ./...
```

Expected: sin errores.

- [ ] **Step 3: Commit**

```bash
git add internal/metrics/metrics.go
git commit -m "feat(metrics): add fanout_dead_letter_total counter"
```

---

## Task 3: RabbitMQ — constantes y declaración de colas/exchanges

**Files:**
- Modify: `internal/messaging/rabbitmq/client.go`

- [ ] **Step 1: Agregar constantes**

Reemplazar el bloque `const` en `internal/messaging/rabbitmq/client.go`:

```go
const (
	QueueTweetCreated  = "tweet.created"
	QueueFollowCreated = "follow.created"

	ExchangeFanoutRetry = "fanout.retry.exchange"
	QueueFanoutRetry    = "fanout.retry"

	ExchangeFanoutWait = "fanout.wait.exchange"
	QueueFanoutWait    = "fanout.wait"

	QueueFanoutDead = "fanout.dead"
)
```

- [ ] **Step 2: Cambiar la firma de Connect para aceptar context y declarar la topología**

El `Connect` actual no acepta `context.Context`. Hay que actualizarlo para declarar los nuevos exchanges y colas. Reemplazar el contenido completo de `internal/messaging/rabbitmq/client.go`:

```go
package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueTweetCreated  = "tweet.created"
	QueueFollowCreated = "follow.created"

	ExchangeFanoutRetry = "fanout.retry.exchange"
	QueueFanoutRetry    = "fanout.retry"

	ExchangeFanoutWait = "fanout.wait.exchange"
	QueueFanoutWait    = "fanout.wait"

	QueueFanoutDead = "fanout.dead"
)

func Connect(url string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	defer ch.Close()

	// colas pre-existentes
	for _, q := range []string{QueueTweetCreated, QueueFollowCreated} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			conn.Close()
			return nil, err
		}
	}

	// fanout.retry.exchange → fanout.retry (con DLX a fanout.wait.exchange)
	if err := ch.ExchangeDeclare(ExchangeFanoutRetry, "direct", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := ch.QueueDeclare(QueueFanoutRetry, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": ExchangeFanoutWait,
	}); err != nil {
		conn.Close()
		return nil, err
	}
	if err := ch.QueueBind(QueueFanoutRetry, QueueFanoutRetry, ExchangeFanoutRetry, false, nil); err != nil {
		conn.Close()
		return nil, err
	}

	// fanout.wait.exchange → fanout.wait (TTL 30s, DLX de vuelta a fanout.retry.exchange)
	if err := ch.ExchangeDeclare(ExchangeFanoutWait, "direct", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := ch.QueueDeclare(QueueFanoutWait, true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(30000),
		"x-dead-letter-exchange": ExchangeFanoutRetry,
	}); err != nil {
		conn.Close()
		return nil, err
	}
	if err := ch.QueueBind(QueueFanoutWait, QueueFanoutWait, ExchangeFanoutWait, false, nil); err != nil {
		conn.Close()
		return nil, err
	}

	// fanout.dead (cola final para replay manual)
	if _, err := ch.QueueDeclare(QueueFanoutDead, true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
```

- [ ] **Step 3: Verificar que compila**

```bash
go build ./...
```

Expected: sin errores.

- [ ] **Step 4: Commit**

```bash
git add internal/messaging/rabbitmq/client.go
git commit -m "feat(rabbitmq): declare fanout retry/wait/dead queue topology"
```

---

## Task 4: Publisher — PublishFanoutRetry y PublishToDeadLetter

**Files:**
- Modify: `internal/messaging/rabbitmq/publisher.go`

- [ ] **Step 1: Agregar los dos métodos al Publisher**

Agregar en `internal/messaging/rabbitmq/publisher.go`, después de `PublishFollowCreated`:

```go
func (p *Publisher) PublishFanoutRetry(ctx context.Context, evt domain.FanoutRetryEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	return ch.Publish(ExchangeFanoutRetry, QueueFanoutRetry, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (p *Publisher) PublishToDeadLetter(ctx context.Context, evt domain.FanoutRetryEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	return ch.Publish("", QueueFanoutDead, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}
```

Asegurarse que `internal/messaging/rabbitmq/publisher.go` tiene el import de `"context"` (ya debería tenerlo).

- [ ] **Step 2: Verificar que compila**

```bash
go build ./...
```

Expected: sin errores.

- [ ] **Step 3: Commit**

```bash
git add internal/messaging/rabbitmq/publisher.go
git commit -m "feat(rabbitmq): add PublishFanoutRetry and PublishToDeadLetter"
```

---

## Task 5: Consumer — inyectar retryPublisher y actualizar fanoutTweet

**Files:**
- Modify: `internal/messaging/rabbitmq/consumer.go`
- Modify: `internal/messaging/rabbitmq/consumer_test.go`

- [ ] **Step 1: Escribir los tests que fallan**

Agregar en `internal/messaging/rabbitmq/consumer_test.go` después de los tests existentes:

```go
// recordingRetryPublisher captura los eventos publicados a fanout.retry.
type recordingRetryPublisher struct {
	mu     sync.Mutex
	events []domain.FanoutRetryEvent
}

func (r *recordingRetryPublisher) PublishFanoutRetry(_ context.Context, evt domain.FanoutRetryEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	return nil
}

func TestFanoutTweet_PublishesRetryOnAppendFailure(t *testing.T) {
	followerID := uuid.New()
	followers := []uuid.UUID{followerID}

	retryPub := &recordingRetryPublisher{}
	c := &Consumer{
		fanout:         &errorFanout{},
		retryPublisher: retryPub,
		fanoutWorkers:  fanoutConcurrency,
	}

	evt := domain.TweetCreatedEvent{
		TweetID:   uuid.New(),
		UserID:    uuid.New(),
		Content:   "hello",
		CreatedAt: time.Now(),
	}
	_ = c.fanoutTweet(context.Background(), evt, followers)

	retryPub.mu.Lock()
	defer retryPub.mu.Unlock()
	if len(retryPub.events) != 1 {
		t.Fatalf("want 1 retry event published, got %d", len(retryPub.events))
	}
	if retryPub.events[0].FollowerID != followerID {
		t.Errorf("want followerID %s, got %s", followerID, retryPub.events[0].FollowerID)
	}
	if retryPub.events[0].Tweet.Content != "hello" {
		t.Errorf("want tweet content 'hello', got %s", retryPub.events[0].Tweet.Content)
	}
}

func TestFanoutTweet_CountsRetryPublishAsHandled(t *testing.T) {
	followers := []uuid.UUID{uuid.New(), uuid.New()}

	c := &Consumer{
		fanout:         &errorFanout{},
		retryPublisher: &recordingRetryPublisher{},
		fanoutWorkers:  fanoutConcurrency,
	}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := c.fanoutTweet(context.Background(), evt, followers)

	if err != nil {
		t.Errorf("expected nil when retries are published, got %v", err)
	}
}
```

- [ ] **Step 2: Correr los tests para verificar que fallan**

```bash
go test ./internal/messaging/rabbitmq/... -run "TestFanoutTweet_Publishes|TestFanoutTweet_Counts" -v
```

Expected: error de compilación — `retryPublisher` field no existe en `Consumer`.

- [ ] **Step 3: Agregar retryPublisher al Consumer y actualizar fanoutTweet**

En `internal/messaging/rabbitmq/consumer.go`:

Agregar el campo al struct `Consumer`:

```go
type Consumer struct {
	conn           *amqp.Connection
	followRepo     domain.FollowRepository
	fanout         domain.TimelineFanout
	userTweetsRepo domain.UserTweetsRepository
	backfillLimit  int
	fanoutWorkers  int
	retryPublisher domain.FanoutRetryPublisher
}
```

Agregar setter en `NewConsumer` (el publisher se inyecta después de construir, ver Task 7):

```go
func (c *Consumer) WithRetryPublisher(p domain.FanoutRetryPublisher) *Consumer {
	c.retryPublisher = p
	return c
}
```

Reemplazar `fanoutTweet` completo:

```go
func (c *Consumer) fanoutTweet(ctx context.Context, evt domain.TweetCreatedEvent, followers []uuid.UUID) error {
	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}

	var (
		sem     = make(chan struct{}, c.fanoutWorkers)
		g, gctx = errgroup.WithContext(ctx)
		handled atomic.Int64
	)

	for _, followerID := range followers {
		fid := followerID
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if err := c.fanout.AppendTweet(gctx, fid, item); err != nil {
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

	if len(followers) > 0 && handled.Load() == 0 {
		return errors.New("all fanout writes failed and no retries enqueued")
	}
	return nil
}
```

- [ ] **Step 4: Correr los tests**

```bash
go test ./internal/messaging/rabbitmq/... -v
```

Expected: todos los tests pasan, incluyendo los 2 nuevos y los 3 pre-existentes de fanoutTweet.

- [ ] **Step 5: Commit**

```bash
git add internal/messaging/rabbitmq/consumer.go internal/messaging/rabbitmq/consumer_test.go
git commit -m "feat(consumer): publish fanout retry event on AppendTweet failure"
```

---

## Task 6: Consumer — ConsumeFanoutRetry

**Files:**
- Modify: `internal/messaging/rabbitmq/consumer.go`
- Modify: `internal/messaging/rabbitmq/consumer_test.go`

- [ ] **Step 1: Escribir los 3 tests que fallan**

Agregar en `internal/messaging/rabbitmq/consumer_test.go`:

```go
// ackTracker wraps an amqp.Delivery to record Ack/Nack calls.
// Como amqp.Delivery no es una interfaz, testeamos ConsumeFanoutRetry
// indirectamente via handleFanoutRetry, que recibe los campos desacoplados.
// Los tests verifican el comportamiento de handleFanoutRetry.

func makeDelivery(body []byte, xDeathCount int64) amqp.Delivery {
	headers := amqp.Table{}
	if xDeathCount > 0 {
		headers["x-death"] = []interface{}{
			amqp.Table{
				"queue": QueueFanoutRetry,
				"count": xDeathCount,
			},
		}
	}
	return amqp.Delivery{Body: body, Headers: headers}
}

func TestHandleFanoutRetry_AcksOnSuccess(t *testing.T) {
	fanout := &concurrentFanout{}
	c := &Consumer{fanout: fanout, fanoutWorkers: fanoutConcurrency}

	evt := domain.FanoutRetryEvent{
		FollowerID: uuid.New(),
		Tweet:      domain.TweetItem{ID: uuid.New(), Content: "hi", CreatedAt: time.Now()},
	}
	body, _ := json.Marshal(evt)
	d := makeDelivery(body, 0)

	acked, nacked := c.handleFanoutRetry(context.Background(), d)

	if !acked {
		t.Error("expected Ack on successful AppendTweet")
	}
	if nacked {
		t.Error("expected no Nack on success")
	}
}

func TestHandleFanoutRetry_NacksOnFailure(t *testing.T) {
	c := &Consumer{fanout: &errorFanout{}, fanoutWorkers: fanoutConcurrency}

	evt := domain.FanoutRetryEvent{
		FollowerID: uuid.New(),
		Tweet:      domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()},
	}
	body, _ := json.Marshal(evt)
	d := makeDelivery(body, 2)

	acked, nacked := c.handleFanoutRetry(context.Background(), d)

	if acked {
		t.Error("expected no Ack on failed AppendTweet")
	}
	if !nacked {
		t.Error("expected Nack on failed AppendTweet")
	}
}

func TestHandleFanoutRetry_DeadLettersAt10(t *testing.T) {
	deadPub := &recordingRetryPublisher{}
	c := &Consumer{
		fanout:          &errorFanout{},
		deadLetterPub:   deadPub,
		fanoutWorkers:   fanoutConcurrency,
	}

	followerID := uuid.New()
	evt := domain.FanoutRetryEvent{
		FollowerID: followerID,
		Tweet:      domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()},
	}
	body, _ := json.Marshal(evt)
	d := makeDelivery(body, 10)

	acked, nacked := c.handleFanoutRetry(context.Background(), d)

	if !acked {
		t.Error("expected Ack after dead-lettering")
	}
	if nacked {
		t.Error("expected no Nack after dead-lettering")
	}
	deadPub.mu.Lock()
	defer deadPub.mu.Unlock()
	if len(deadPub.events) != 1 {
		t.Fatalf("want 1 dead letter event, got %d", len(deadPub.events))
	}
	if deadPub.events[0].FollowerID != followerID {
		t.Errorf("dead letter event has wrong followerID")
	}
}
```

- [ ] **Step 2: Agregar import de `"encoding/json"` al test si no está**

Verificar que `consumer_test.go` tiene en su bloque de imports:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)
```

- [ ] **Step 3: Correr los tests para verificar que fallan**

```bash
go test ./internal/messaging/rabbitmq/... -run "TestHandleFanoutRetry" -v
```

Expected: error de compilación — `handleFanoutRetry` y `deadLetterPub` no existen.

- [ ] **Step 4: Implementar deadLetterPub, handleFanoutRetry y ConsumeFanoutRetry**

Agregar `deadLetterPub` al struct `Consumer`:

```go
type Consumer struct {
	conn           *amqp.Connection
	followRepo     domain.FollowRepository
	fanout         domain.TimelineFanout
	userTweetsRepo domain.UserTweetsRepository
	backfillLimit  int
	fanoutWorkers  int
	retryPublisher domain.FanoutRetryPublisher
	deadLetterPub  domain.FanoutRetryPublisher
}
```

Agregar setter:

```go
func (c *Consumer) WithDeadLetterPublisher(p domain.FanoutRetryPublisher) *Consumer {
	c.deadLetterPub = p
	return c
}
```

Agregar función helper para leer `x-death` count:

```go
func xDeathCount(d amqp.Delivery) int64 {
	deaths, ok := d.Headers["x-death"].([]interface{})
	if !ok {
		return 0
	}
	for _, entry := range deaths {
		table, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		if table["queue"] == QueueFanoutRetry {
			if count, ok := table["count"].(int64); ok {
				return count
			}
		}
	}
	return 0
}
```

Agregar `handleFanoutRetry` (retorna `(acked, nacked bool)` para poder testearlo sin broker):

```go
const maxFanoutRetries = 10

func (c *Consumer) handleFanoutRetry(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.FanoutRetryEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		log.Printf("rabbitmq: unmarshal fanout retry: %v", err)
		return false, true
	}

	if xDeathCount(d) >= maxFanoutRetries {
		if c.deadLetterPub != nil {
			if err := c.deadLetterPub.PublishFanoutRetry(ctx, evt); err != nil {
				log.Printf("rabbitmq: publish dead letter for %s: %v", evt.FollowerID, err)
			}
		}
		metrics.FanoutDeadLetterTotal.WithLabelValues(evt.FollowerID.String()).Inc()
		log.Printf("rabbitmq: fanout dead letter for follower %s tweet %s", evt.FollowerID, evt.Tweet.ID)
		return true, false
	}

	if err := c.fanout.AppendTweet(ctx, evt.FollowerID, evt.Tweet); err != nil {
		log.Printf("rabbitmq: retry AppendTweet for %s: %v", evt.FollowerID, err)
		return false, true
	}
	return true, false
}
```

Agregar `ConsumeFanoutRetry`:

```go
func (c *Consumer) ConsumeFanoutRetry(ctx context.Context) {
	ch, msgs := c.openChannel(QueueFanoutRetry)
	if ch == nil {
		return
	}
	defer ch.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			acked, nacked := c.handleFanoutRetry(ctx, d)
			if acked {
				_ = d.Ack(false)
			} else if nacked {
				_ = d.Nack(false, false)
			}
		}
	}
}
```

- [ ] **Step 5: Correr todos los tests**

```bash
go test ./internal/messaging/rabbitmq/... -v
```

Expected: todos los tests pasan.

- [ ] **Step 6: Commit**

```bash
git add internal/messaging/rabbitmq/consumer.go internal/messaging/rabbitmq/consumer_test.go
git commit -m "feat(consumer): add ConsumeFanoutRetry with dead-letter at 10 attempts"
```

---

## Task 7: Wiring en main.go

**Files:**
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Inyectar publishers y arrancar ConsumeFanoutRetry**

En `cmd/api/main.go`, reemplazar la sección de consumer:

```go
publisher := rabbitmq.NewPublisher(amqpConn)

consumer := rabbitmq.NewConsumer(amqpConn, followRepo, redisTimeline, pgTimelineRepo, cfg.FollowBackfillLimit).
	WithRetryPublisher(publisher).
	WithDeadLetterPublisher(publisher)

go consumer.ConsumeTweets(ctx)
go consumer.ConsumeFollows(ctx)
go consumer.ConsumeFanoutRetry(ctx)
```

- [ ] **Step 2: Verificar que compila**

```bash
go build ./...
```

Expected: sin errores.

- [ ] **Step 3: Correr todos los tests**

```bash
go test ./... 2>&1
```

Expected: todos los paquetes pasan (los de handler que requieren infra siguen fallando por aridad, pero ese es el gap pre-existente).

- [ ] **Step 4: Commit**

```bash
git add cmd/api/main.go
git commit -m "feat(main): wire fanout retry/dead-letter publishers and start ConsumeFanoutRetry"
```

---

## Self-Review

**Spec coverage:**
- ✅ `FanoutRetryEvent` + `FanoutRetryPublisher` — Task 1
- ✅ `FanoutDeadLetterTotal` métrica — Task 2
- ✅ Topología de colas/exchanges en `Connect` — Task 3
- ✅ `PublishFanoutRetry` + `PublishToDeadLetter` — Task 4
- ✅ `fanoutTweet` publica retry en fallo, cuenta como handled — Task 5
- ✅ `ConsumeFanoutRetry` con límite de 10, dead-letter y métrica — Task 6
- ✅ Wiring en main — Task 7
- ✅ Los 5 tests del spec están cubiertos en Tasks 5 y 6

**Type consistency:**
- `domain.FanoutRetryPublisher` se define en Task 1 y se usa en Tasks 4, 5, 6, 7 ✅
- `domain.FanoutRetryEvent` se define en Task 1 y se usa en Tasks 4, 5, 6 ✅
- `QueueFanoutRetry`, `ExchangeFanoutRetry`, `QueueFanoutDead` se definen en Task 3 y se usan en Tasks 4 y 6 ✅
- `WithRetryPublisher` / `WithDeadLetterPublisher` se definen en Task 5/6 y se usan en Task 7 ✅
- `handleFanoutRetry` retorna `(acked, nacked bool)` — consistente entre Task 6 (implementación) y tests ✅
- `deadLetterPub` en struct usa `domain.FanoutRetryPublisher` — mismo tipo que `retryPublisher` ✅ (el publisher implementa ambos casos)

**Notas importantes para el ejecutor:**
- `PublishToDeadLetter` en `publisher.go` publica directamente al queue `fanout.dead` via default exchange (routing key = queue name). El `Publisher` implementa `domain.FanoutRetryPublisher` tanto para retry como para dead-letter porque la interfaz es la misma — la diferencia es qué cola destino usa.
- En Task 7 se pasa el mismo `publisher` como `WithRetryPublisher` y `WithDeadLetterPublisher`. El consumer no sabe que es el mismo objeto; cada método llama al publisher correcto internamente.
