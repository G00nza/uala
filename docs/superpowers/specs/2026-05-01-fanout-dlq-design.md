# Fanout DLQ por follower fallido

**Fecha:** 2026-05-01  
**Estado:** aprobado

## Problema

Cuando `AppendTweet` falla para un follower específico durante el fanout, el error se loggea y se descarta. No hay mecanismo de reintento: ese follower nunca recibe el tweet aunque Redis se recupere.

## Solución

DLQ por follower usando RabbitMQ native dead-lettering. Cada write fallido se encola como mensaje independiente con el ID del follower y el tweet. RabbitMQ gestiona el backoff (TTL 30s). El código gestiona el límite de reintentos leyendo el header `x-death`.

## Trade-offs anotados

- Config declarativa de exchanges, bindings y TTL en el arranque — más acoplamiento entre infra y código que un contador en el mensaje
- Más difícil de testear sin broker real: la integration test del retry loop requiere un RabbitMQ corriendo
- Lógica distribuida: el backoff lo gestiona RabbitMQ, el límite de 10 lo gestiona el consumer

## Topología de colas

```
fanout.retry ──[Nack, no requeue]──▶ fanout.wait (TTL 30s) ──[expire]──▶ fanout.retry
                                                                           │
                                                              x-death ≥ 10 │
                                                                           ▼
                                                                      fanout.dead
```

| Recurso | Tipo | Config |
|---|---|---|
| `fanout.retry.exchange` | direct exchange | — |
| `fanout.retry` queue | durable | `x-dead-letter-exchange: fanout.wait.exchange` |
| `fanout.wait.exchange` | direct exchange | — |
| `fanout.wait` queue | durable | `x-message-ttl: 30000`, `x-dead-letter-exchange: fanout.retry.exchange` |
| `fanout.dead` queue | durable | replay manual |

## Modelo de datos

```go
// domain/events.go
type FanoutRetryEvent struct {
    FollowerID uuid.UUID `json:"follower_id"`
    Tweet      TweetItem `json:"tweet"`
}

type FanoutRetryPublisher interface {
    PublishFanoutRetry(ctx context.Context, evt FanoutRetryEvent) error
}
```

El contador de reintentos NO va en el payload — lo lleva RabbitMQ en el header `x-death[fanout.retry].count`.

## Flujo productor (fanoutTweet)

Cuando `AppendTweet` falla para un follower:
1. Publicar `FanoutRetryEvent{FollowerID: fid, Tweet: item}` a `fanout.retry.exchange`
2. Si la publicación también falla: loggear, contar como fallo sin retry
3. El mensaje publicado con éxito cuenta como "handled" para la lógica de Nack del evento original

La función `fanoutTweet` retorna error solo si `handled == 0` (ningún write exitoso ni retry encolado).

## Flujo consumer (ConsumeFanoutRetry)

```
recibir mensaje de fanout.retry
  ├── leer x-death count para queue="fanout.retry"
  ├── si count >= 10
  │     publicar a fanout.dead
  │     FanoutDeadLetterTotal.WithLabelValues(followerID).Inc()
  │     Ack
  │     return
  └── intentar AppendTweet(followerID, tweet)
        ├── éxito → Ack
        └── fallo → Nack(false, false)
                     RabbitMQ DLX → fanout.wait (30s TTL) → fanout.retry
```

## Métrica

```go
FanoutDeadLetterTotal = prometheus.NewCounterVec(
    prometheus.CounterOpts{
        Name: "fanout_dead_letter_total",
        Help: "Tweets que no pudieron appendearse a un follower tras 10 reintentos.",
    },
    []string{"follower_id"},
)
```

Permite alertar: `fanout_dead_letter_total > 0` indica followers con problemas persistentes.

## Componentes modificados

| Archivo | Cambio |
|---|---|
| `internal/domain/events.go` | +`FanoutRetryEvent`, +`FanoutRetryPublisher` |
| `internal/messaging/rabbitmq/client.go` | +constantes, +declaración de exchanges y colas en `Connect` |
| `internal/messaging/rabbitmq/publisher.go` | +`PublishFanoutRetry` |
| `internal/messaging/rabbitmq/consumer.go` | +campo `retryPublisher`, cambio en `fanoutTweet`, +`ConsumeFanoutRetry` |
| `internal/metrics/metrics.go` | +`FanoutDeadLetterTotal` |
| `cmd/api/main.go` | +`go consumer.ConsumeFanoutRetry(ctx)` |

## Tests

- `TestFanoutTweet_PublishesRetryOnAppendFailure` — cuando `AppendTweet` falla, `retryPublisher.PublishFanoutRetry` se llama con el follower y tweet correctos
- `TestFanoutTweet_CountsRetryPublishAsHandled` — si retry se publica, `fanoutTweet` retorna nil aunque `AppendTweet` falle para todos
- `TestConsumeFanoutRetry_AcksOnSuccess` — cuando `AppendTweet` tiene éxito, Ack
- `TestConsumeFanoutRetry_NacksOnFailure` — cuando `AppendTweet` falla, Nack sin requeue
- `TestConsumeFanoutRetry_DeadLettersAt10` — cuando `x-death count >= 10`, publica a `fanout.dead`, incrementa métrica, Ack
