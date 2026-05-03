package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/messaging/rabbitmq"
	"uala/internal/metrics"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := infra.LoadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL, func(c *pgxpool.Config) {
		c.ConnConfig.Tracer = &metrics.PgxTracer{}
	})
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer db.Close()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			metrics.DBConnectionsActive.Set(float64(db.Stat().AcquiredConns()))
		}
	}()

	rdb, err := redisrepo.Connect(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatal("redis connect:", err)
	}
	defer rdb.Close()

	amqpConn, err := rabbitmq.Connect(ctx, cfg.AMQPURL)
	if err != nil {
		log.Fatal("rabbitmq connect:", err)
	}
	defer amqpConn.Close()

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		ch, err := amqpConn.Channel()
		if err != nil {
			log.Printf("queue depth sampler: open channel: %v", err)
			return
		}
		defer ch.Close()
		for range ticker.C {
			for _, qName := range []string{rabbitmq.QueueTweetCreated, rabbitmq.QueueFollowCreated} {
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

	redisTimeline := redisrepo.NewTimelineRepository(rdb, pgTimelineRepo, cfg.TimelineLimit).
		WithActivityTTL(cfg.ActivityTTL)

	publisher := rabbitmq.NewPublisher(amqpConn)

	fanoutTweetUC := usecase.NewFanoutTweetUseCase(followRepo, redisTimeline, cfg.ActivityTTL).
		WithRetryPublisher(publisher)
	backfillUC := usecase.NewBackfillTimelineUseCase(pgTimelineRepo, redisTimeline, cfg.FollowBackfillLimit, cfg.ActivityTTL)

	go rabbitmq.NewTweetCreatedConsumer(amqpConn, fanoutTweetUC).Consume(ctx)
	go rabbitmq.NewFollowCreatedConsumer(amqpConn, backfillUC).Consume(ctx)
	go rabbitmq.NewFanoutRetryConsumer(amqpConn, redisTimeline, rabbitmq.NewDeadLetterPublisher(publisher), cfg.ActivityTTL).Consume(ctx)
	go rabbitmq.NewUserActivityConsumer(amqpConn, userRepo).Consume(ctx)

	createUserUseCase := usecase.NewCreateUserUseCase(userRepo)
	createTweetUseCase := usecase.NewCreateTweetUseCase(userRepo, tweetRepo, publisher)
	followUserUseCase := usecase.NewFollowUserUseCase(userRepo, followRepo, publisher)
	getTimelineUseCase := usecase.NewGetTimelineUseCase(userRepo, redisTimeline).
		WithUserActivityPublisher(publisher)

	router := handler.NewRouter(
		handler.NewUserHandler(createUserUseCase),
		handler.NewTweetHandler(createTweetUseCase),
		handler.NewFollowHandler(followUserUseCase),
		handler.NewTimelineHandler(getTimelineUseCase),
	)

	ln, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		log.Fatal("listen:", err)
	}
	log.Printf("listening on :%s", cfg.Port)
	if err := serve(ctx, ln, router); err != nil {
		log.Fatal(err)
	}
}
