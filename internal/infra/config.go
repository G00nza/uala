package infra

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL         string
	RedisURL            string
	AMQPURL             string
	Port                string
	FollowBackfillLimit int
	TimelineLimit       int
	ActivityTTL         time.Duration
}

func LoadConfig() Config {
	return Config{
		DatabaseURL:         getenv("DATABASE_URL", "postgres://uala:uala@localhost:5432/uala"),
		RedisURL:            getenv("REDIS_URL", "redis://localhost:6379/0"),
		AMQPURL:             getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		Port:                getenv("PORT", "8080"),
		FollowBackfillLimit: getenvInt("FOLLOW_BACKFILL_LIMIT", 20),
		TimelineLimit:       getenvInt("TIMELINE_LIMIT", 500),
		ActivityTTL:         getenvDuration("ACTIVITY_TTL", 24*time.Hour),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
