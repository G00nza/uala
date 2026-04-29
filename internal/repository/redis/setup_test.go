package redis_test

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	redisrepo "uala/internal/repository/redis"
)

var testRDB *redis.Client

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		os.Exit(0)
	}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	client, err := redisrepo.Connect(context.Background(), url)
	if err != nil {
		panic("redis connect: " + err.Error())
	}
	testRDB = client
	code := m.Run()
	client.Close()
	os.Exit(code)
}

func flushRedis(t *testing.T) {
	t.Helper()
	if err := testRDB.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
}
