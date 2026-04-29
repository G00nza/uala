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
	publisher := &mockFollowPublisher{}
	uc := usecase.NewFollowUseCase(userRepo, followRepo, publisher)

	if err := uc.Follow(context.Background(), follower, followee); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFollowUseCase_Follow_SelfFollow(t *testing.T) {
	id := uuid.New()
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, &mockFollowRepo{}, &mockFollowPublisher{})

	err := uc.Follow(context.Background(), id, id)
	if err != domain.ErrSelfFollow {
		t.Fatalf("want ErrSelfFollow, got %v", err)
	}
}

func TestFollowUseCase_Follow_AlreadyFollowing(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	followRepo := &mockFollowRepo{existsResult: true}
	uc := usecase.NewFollowUseCase(&mockUserRepo{}, followRepo, &mockFollowPublisher{})

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowUseCase_Follow_FolloweeNotFound(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{}, &mockFollowPublisher{})

	err := uc.Follow(context.Background(), follower, followee)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestFollowUseCase_Follow_PublishesEvent(t *testing.T) {
	follower := uuid.New()
	followee := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: followee}}
	publisher := &mockFollowPublisher{}
	uc := usecase.NewFollowUseCase(userRepo, &mockFollowRepo{}, publisher)

	if err := uc.Follow(context.Background(), follower, followee); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("want 1 publish call, got %d", len(publisher.calls))
	}
	evt := publisher.calls[0]
	if evt.FollowerID != follower {
		t.Fatalf("want followerID %s, got %s", follower, evt.FollowerID)
	}
	if evt.FolloweeID != followee {
		t.Fatalf("want followeeID %s, got %s", followee, evt.FolloweeID)
	}
}
