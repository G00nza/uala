package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, u *domain.User) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO users (id, username, created_at) VALUES ($1, $2, $3)`,
		u.ID, u.Username, u.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrUsernameConflict
		}
		return err
	}
	return nil
}

func (r *UserRepository) UpdateLastActive(ctx context.Context, userID uuid.UUID, lastActive time.Time) error {
	_, err := r.db.Exec(ctx,
		`UPDATE users SET last_active = $1
		 WHERE id = $2 AND (last_active IS NULL OR last_active < $1)`,
		lastActive, userID,
	)
	return err
}

func (r *UserRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	u := &domain.User{}
	err := r.db.QueryRow(ctx,
		`SELECT id, username, created_at FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return u, nil
}
