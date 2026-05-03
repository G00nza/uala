package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestTimelineUseCase_GetTimeline_OK(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{
		{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "hello", CreatedAt: time.Now()},
	}}
	uc := usecase.NewGetTimelineUseCase(userRepo, timelineRepo)

	items, err := uc.Execute(context.Background(), userID, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
}

func TestTimelineUseCase_GetTimeline_UserNotFound(t *testing.T) {
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewGetTimelineUseCase(userRepo, &mockTimelineRepo{})

	_, err := uc.Execute(context.Background(), uuid.New(), nil, nil)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTimelineUseCase_GetTimeline_EmptyWhenNoFollows(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{}}
	uc := usecase.NewGetTimelineUseCase(userRepo, timelineRepo)

	items, err := uc.Execute(context.Background(), userID, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}

func TestTimelineUseCase_GetTimeline_PublishesUserActivity(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{}}
	pub := &mockUserActivityPublisher{}
	uc := usecase.NewGetTimelineUseCase(userRepo, timelineRepo).WithUserActivityPublisher(pub)

	before := time.Now()
	_, err := uc.Execute(context.Background(), userID, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // publish runs in a goroutine
	after := time.Now()

	if len(pub.calls) != 1 {
		t.Fatalf("want 1 UserActivity event, got %d", len(pub.calls))
	}
	if pub.calls[0].UserID != userID {
		t.Errorf("wrong UserID: want %s, got %s", userID, pub.calls[0].UserID)
	}
	if pub.calls[0].LastActive.Before(before) || pub.calls[0].LastActive.After(after) {
		t.Errorf("LastActive %v not in expected window [%v, %v]", pub.calls[0].LastActive, before, after)
	}
}

func TestTimelineUseCase_GetTimeline_NoPublisher_OK(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	timelineRepo := &mockTimelineRepo{items: []domain.TweetItem{}}
	uc := usecase.NewGetTimelineUseCase(userRepo, timelineRepo)

	// No publisher wired — should not panic
	_, err := uc.Execute(context.Background(), userID, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
