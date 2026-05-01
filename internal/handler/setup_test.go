package handler_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"uala/internal/handler"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"
)

var (
	testDB  *pgxpool.Pool
	testRDB *redis.Client
)

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(m.Run())
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://uala:uala@localhost:5432/uala"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	pool, err := postgres.Connect(context.Background(), dsn)
	if err != nil {
		panic("connect: " + err.Error())
	}
	if err := postgres.Migrate(context.Background(), pool); err != nil {
		panic("migrate: " + err.Error())
	}
	testDB = pool

	rdb, err := redisrepo.Connect(context.Background(), redisURL)
	if err != nil {
		panic("redis connect: " + err.Error())
	}
	testRDB = rdb

	code := m.Run()
	pool.Close()
	rdb.Close()
	os.Exit(code)
}

func truncate(t *testing.T) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"TRUNCATE TABLE follows, tweets, users RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func flushRedis(t *testing.T) {
	t.Helper()
	if err := testRDB.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
}

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	userRepo := postgres.NewUserRepository(testDB)
	tweetRepo := postgres.NewTweetRepository(testDB)
	followRepo := postgres.NewFollowRepository(testDB)
	pgTimeline := postgres.NewTimelineRepository(testDB)
	redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500)

	router := handler.NewRouter(
		handler.NewUserHandler(usecase.NewUserUseCase(userRepo)),
		handler.NewTweetHandler(usecase.NewTweetUseCase(userRepo, tweetRepo, &noopTweetPublisher{})),
		handler.NewFollowHandler(usecase.NewFollowUseCase(userRepo, followRepo, &noopFollowPublisher{})),
		handler.NewTimelineHandler(usecase.NewTimelineUseCase(userRepo, redisTimeline)),
	)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return srv
}
