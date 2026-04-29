package main

import (
	"context"
	"log"
	"net/http"

	"uala/internal/handler"
	"uala/internal/infra"
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

	userRepo := postgres.NewUserRepository(db)
	tweetRepo := postgres.NewTweetRepository(db)
	followRepo := postgres.NewFollowRepository(db)
	pgTimelineRepo := postgres.NewTimelineRepository(db)

	redisTimeline := redisrepo.NewTimelineRepository(rdb, pgTimelineRepo)

	userUC := usecase.NewUserUseCase(userRepo)
	tweetUC := usecase.NewTweetUseCase(userRepo, tweetRepo, followRepo, redisTimeline)
	followUC := usecase.NewFollowUseCase(userRepo, followRepo)
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
