package main

import (
	"context"
	"log"
	"net/http"

	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/messaging/rabbitmq"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"
)

func main() {
	cfg := infra.LoadConfig()
	ctx := context.Background()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer db.Close()

	if err := postgres.Migrate(ctx, db); err != nil {
		log.Fatal("migrate:", err)
	}

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

	userRepo := postgres.NewUserRepository(db)
	tweetRepo := postgres.NewTweetRepository(db)
	followRepo := postgres.NewFollowRepository(db)
	pgTimelineRepo := postgres.NewTimelineRepository(db)

	redisTimeline := redisrepo.NewTimelineRepository(rdb, pgTimelineRepo)

	publisher := rabbitmq.NewPublisher(amqpConn)

	consumer := rabbitmq.NewConsumer(amqpConn, followRepo, redisTimeline, pgTimelineRepo, cfg.FollowBackfillLimit)
	go consumer.ConsumeTweets(ctx)
	go consumer.ConsumeFollows(ctx)

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
