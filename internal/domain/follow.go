package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Follow struct {
	FollowerID uuid.UUID
	FolloweeID uuid.UUID
	CreatedAt  time.Time
}

type FollowerActivity struct {
	ID         uuid.UUID
	LastActive time.Time
}

type FollowRepository interface {
	Create(ctx context.Context, f *Follow) error
	Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error)
	GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error)
	GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]FollowerActivity, error)
}
