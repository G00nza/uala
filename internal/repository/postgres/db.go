package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, dsn string, opts ...func(*pgxpool.Config)) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool parse config: %w", err)
	}
	for _, opt := range opts {
		opt(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.NewWithConfig: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY,
			username TEXT UNIQUE NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS tweets (
			id UUID PRIMARY KEY,
			user_id UUID NOT NULL REFERENCES users(id),
			content TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS follows (
			follower_id UUID NOT NULL REFERENCES users(id),
			followee_id UUID NOT NULL REFERENCES users(id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (follower_id, followee_id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}
