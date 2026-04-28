package infra

import "os"

type Config struct {
	DatabaseURL string
	Port        string
}

func LoadConfig() Config {
	return Config{
		DatabaseURL: getenv("DATABASE_URL", "postgres://uala:uala@localhost:5432/uala"),
		Port:        getenv("PORT", "8080"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
