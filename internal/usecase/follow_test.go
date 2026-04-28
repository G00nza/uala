package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestFollowUseCase_Follow_OK(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: followee}}
	followRepo := &mockFollowRepo{}
	uc := usecase.NewFollowUseCase(userRepo, followRepo)

	if err := uc.Follow(context.Background(), follower, followee); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFollowUseCase_Follow_SelfFollow(t *testing.T) {
	id := uuid.New()
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, &mockFollowRepo{})

	err := uc.Follow(context.Background(), id, id)
	if err != domain.ErrSelfFollow {
		t.Fatalf("want ErrSelfFollow, got %v", err)
	}
}

func TestFollowUseCase_Follow_AlreadyFollowing(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	followRepo := &mockFollowRepo{existsResult: true}
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, followRepo)

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowUseCase_Follow_FolloweeNotFound(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{})

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
