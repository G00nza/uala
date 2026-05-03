package postgres

import (
	"context"
	"errors"
	"time"

	"uala/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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

func (r *FollowRepository) GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]domain.FollowerActivity, error) {
	rows, err := r.db.Query(ctx,
		`SELECT f.follower_id, u.last_active
		 FROM follows f
		 JOIN users u ON u.id = f.follower_id
		 WHERE f.followee_id = $1
		   AND u.last_active >= $2`,
		followeeID, activeSince,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var result []domain.FollowerActivity
	for rows.Next() {
		var fa domain.FollowerActivity
		if err := rows.Scan(&fa.ID, &fa.LastActive); err != nil {
			return nil, err
		}
		result = append(result, fa)
	}
	if result == nil {
		result = []domain.FollowerActivity{}
	}

	return result, rows.Err()
}
