package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type TweetRepository struct {
	db *pgxpool.Pool
}

func NewTweetRepository(db *pgxpool.Pool) *TweetRepository {
	return &TweetRepository{db: db}
}

func (r *TweetRepository) Create(ctx context.Context, t *domain.Tweet) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO tweets (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)`,
		t.ID, t.UserID, t.Content, t.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return domain.ErrNotFound
		}
		return err
	}
	return nil
}
