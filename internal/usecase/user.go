package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type UserUseCase struct {
	repo domain.UserRepository
}

func NewUserUseCase(repo domain.UserRepository) *UserUseCase {
	return &UserUseCase{repo: repo}
}

func (uc *UserUseCase) CreateUser(ctx context.Context, username string) (*domain.User, error) {
	if username == "" {
		return nil, domain.ErrEmptyUsername
	}
	u := &domain.User{
		ID:        uuid.New(),
		Username:  username,
		CreatedAt: time.Now().UTC(),
	}
	if err := uc.repo.Create(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}
