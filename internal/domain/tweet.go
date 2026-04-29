package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Tweet struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Content   string
	CreatedAt time.Time
}

type TweetRepository interface {
	Create(ctx context.Context, t *Tweet) error
}
