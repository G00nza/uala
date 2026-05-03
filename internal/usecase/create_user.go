package usecase

import (
	"context"
	"time"

	"uala/internal/domain"

	"github.com/google/uuid"
)

type CreateUserUseCase struct {
	repo domain.UserRepository
}

func NewCreateUserUseCase(repo domain.UserRepository) *CreateUserUseCase {
	return &CreateUserUseCase{repo: repo}
}

func (uc *CreateUserUseCase) Execute(ctx context.Context, username string) (*domain.User, error) {
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
