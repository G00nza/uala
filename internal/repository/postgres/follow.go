package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type FollowRepository struct {
	db *pgxpool.Pool
}

func NewFollowRepository(db *pgxpool.Pool) *FollowRepository {
	return &FollowRepository{db: db}
}

func (r *FollowRepository) Create(ctx context.Context, f *domain.Follow) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO follows (follower_id, followee_id, created_at) VALUES ($1, $2, $3)`,
		f.FollowerID, f.FolloweeID, f.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrAlreadyFollowing
		}
		return err
	}
	return nil
}

func (r *FollowRepository) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM follows WHERE follower_id=$1 AND followee_id=$2)`,
		followerID, followeeID,
	).Scan(&exists)
	return exists, err
}
