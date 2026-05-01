package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/messaging/rabbitmq"
	"uala/internal/metrics"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"
)

func main() {
	cfg := infra.LoadConfig()
	ctx := context.Background()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL, func(c *pgxpool.Config) {
		c.ConnConfig.Tracer = &metrics.PgxTracer{}
	})
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer db.Close()

	if err := postgres.Migrate(ctx, db); err != nil {
		log.Fatal("migrate:", err)
	}

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

	amqpConn, err := rabbitmq.Connect(cfg.AMQPURL)
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

	redisTimeline := redisrepo.NewTimelineRepository(rdb, pgTimelineRepo, cfg.TimelineLimit)

	publisher := rabbitmq.NewPublisher(amqpConn)

	consumer := rabbitmq.NewConsumer(amqpConn, followRepo, redisTimeline, pgTimelineRepo, cfg.FollowBackfillLimit).
		WithRetryPublisher(publisher).
		WithDeadLetterPublisher(rabbitmq.NewDeadLetterPublisher(publisher))

	go consumer.ConsumeTweets(ctx)
	go consumer.ConsumeFollows(ctx)
	go consumer.ConsumeFanoutRetry(ctx)

	userUC := usecase.NewUserUseCase(userRepo)
	tweetUC := usecase.NewTweetUseCase(userRepo, tweetRepo, publisher)
	followUC := usecase.NewFollowUseCase(userRepo, followRepo, publisher)
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
