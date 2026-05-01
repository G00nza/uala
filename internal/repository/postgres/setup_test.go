package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"uala/internal/repository/postgres"
	"uala/migrations"
)

var testDB *pgxpool.Pool

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://uala:uala@localhost:5432/uala"
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

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

type testRepos struct {
	user     *postgres.UserRepository
	tweet    *postgres.TweetRepository
	follow   *postgres.FollowRepository
	timeline *postgres.TimelineRepository
}

func setup(t *testing.T) testRepos {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"TRUNCATE TABLE follows, tweets, users RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return testRepos{
		user:     postgres.NewUserRepository(testDB),
		tweet:    postgres.NewTweetRepository(testDB),
		follow:   postgres.NewFollowRepository(testDB),
		timeline: postgres.NewTimelineRepository(testDB),
	}
}
