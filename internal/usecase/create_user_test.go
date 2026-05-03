package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestUserUseCase_CreateUser_OK(t *testing.T) {
	uc := usecase.NewCreateUserUseCase(&mockUserRepo{})
	user, err := uc.Execute(context.Background(), "alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user.Username != "alice" {
		t.Fatalf("want alice, got %s", user.Username)
	}
	if user.ID == (uuid.UUID{}) {
		t.Fatal("ID must be set")
	}
}

func TestUserUseCase_CreateUser_EmptyUsername(t *testing.T) {
	uc := usecase.NewCreateUserUseCase(&mockUserRepo{})
	_, err := uc.Execute(context.Background(), "")
	if err != domain.ErrEmptyUsername {
		t.Fatalf("want ErrEmptyUsername, got %v", err)
	}
}

func TestUserUseCase_CreateUser_PropagatesRepoError(t *testing.T) {
	repo := &mockUserRepo{createErr: domain.ErrUsernameConflict}
	uc := usecase.NewCreateUserUseCase(repo)
	_, err := uc.Execute(context.Background(), "alice")
	if err != domain.ErrUsernameConflict {
		t.Fatalf("want ErrUsernameConflict, got %v", err)
	}
}
