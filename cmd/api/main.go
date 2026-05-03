package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
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

	consumer := rabbitmq.NewConsumer(amqpConn, followRepo, redisTimeline, pgTimelineRepo, cfg.FollowBackfillLimit).
		WithRetryPublisher(publisher).
		WithDeadLetterPublisher(rabbitmq.NewDeadLetterPublisher(publisher)).
		WithUserActivityRepo(userRepo).
		WithActivityTTL(cfg.ActivityTTL)

	go consumer.ConsumeTweets(ctx)
	go consumer.ConsumeFollows(ctx)
	go consumer.ConsumeFanoutRetry(ctx)
	go consumer.ConsumeUserActivity(ctx)

	userUC := usecase.NewCreateUserUseCase(userRepo)
	tweetUC := usecase.NewCreateTweetUseCase(userRepo, tweetRepo, publisher)
	followUC := usecase.NewFollowUserUseCase(userRepo, followRepo, publisher)
	timelineUC := usecase.NewGetTimelineUseCase(userRepo, redisTimeline).
		WithUserActivityPublisher(publisher)

	router := handler.NewRouter(
		handler.NewUserHandler(userUC),
		handler.NewTweetHandler(tweetUC),
		handler.NewFollowHandler(followUC),
		handler.NewTimelineHandler(timelineUC),
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
