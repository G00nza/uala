package handler_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/redis/go-redis/v9"
	"uala/internal/handler"
	"uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"
	"uala/migrations"
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
	testDB = pool

	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		panic("goose dialect: " + err.Error())
	}
	if err := goose.Up(sqlDB, "."); err != nil {
		panic("migrate: " + err.Error())
	}

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
		handler.NewUserHandler(usecase.NewCreateUserUseCase(userRepo)),
		handler.NewTweetHandler(usecase.NewCreateTweetUseCase(userRepo, tweetRepo, &noopTweetPublisher{})),
		handler.NewFollowHandler(usecase.NewFollowUserUseCase(userRepo, followRepo, &noopFollowPublisher{})),
		handler.NewTimelineHandler(usecase.NewGetTimelineUseCase(userRepo, redisTimeline)),
	)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return srv
}
