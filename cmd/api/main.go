package main

import (
	"context"
	"log"
	"net/http"

	"uala/internal/handler"
	"uala/internal/infra"
	"uala/internal/repository/postgres"
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

	userRepo := postgres.NewUserRepository(db)
	tweetRepo := postgres.NewTweetRepository(db)
	followRepo := postgres.NewFollowRepository(db)
	timelineRepo := postgres.NewTimelineRepository(db)

	userUC := usecase.NewUserUseCase(userRepo)
	tweetUC := usecase.NewTweetUseCase(userRepo, tweetRepo)
	followUC := usecase.NewFollowUseCase(userRepo, followRepo)
	timelineUC := usecase.NewTimelineUseCase(userRepo, timelineRepo)

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
