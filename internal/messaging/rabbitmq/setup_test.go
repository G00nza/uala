package rabbitmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/redis/go-redis/v9"
	postgresrepo "uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
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

	pool, err := postgresrepo.Connect(context.Background(), dsn)
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

func seedUser(t *testing.T, id uuid.UUID, username string) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"INSERT INTO users (id, username) VALUES ($1, $2)", id, username)
	if err != nil {
		t.Fatalf("seedUser: %v", err)
	}
}

func seedTweet(t *testing.T, id, userID uuid.UUID, content string, createdAt time.Time) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"INSERT INTO tweets (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)", id, userID, content, createdAt)
	if err != nil {
		t.Fatalf("seedTweet: %v", err)
	}
}

func seedFollow(t *testing.T, followerID, followeeID uuid.UUID) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)", followerID, followeeID)
	if err != nil {
		t.Fatalf("seedFollow: %v", err)
	}
}

func setLastActive(t *testing.T, userID uuid.UUID, lastActive time.Time) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		"UPDATE users SET last_active = $1 WHERE id = $2", lastActive, userID)
	if err != nil {
		t.Fatalf("setLastActive: %v", err)
	}
}
