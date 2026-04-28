package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func TestTimelineRepository_GetTimeline_Empty(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	alice := &domain.User{ID: uuid.New(), Username: "alice_tl", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), alice)

	repo := postgres.NewTimelineRepository(testDB)
	items, err := repo.GetTimeline(context.Background(), alice.ID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}

func TestTimelineRepository_GetTimeline_WithTweets(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	tweetRepo := postgres.NewTweetRepository(testDB)
	followRepo := postgres.NewFollowRepository(testDB)
	timelineRepo := postgres.NewTimelineRepository(testDB)

	alice := &domain.User{ID: uuid.New(), Username: "alice_tl2", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_tl2", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), alice)
	_ = userRepo.Create(context.Background(), bob)

	_ = followRepo.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = tweetRepo.Create(context.Background(), &domain.Tweet{
		ID: uuid.New(), UserID: bob.ID, Content: "hello from bob", CreatedAt: time.Now().UTC(),
	})

	items, err := timelineRepo.GetTimeline(context.Background(), alice.ID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0].Content != "hello from bob" {
		t.Fatalf("want 'hello from bob', got %s", items[0].Content)
	}
	if items[0].Username != "bob_tl2" {
		t.Fatalf("want 'bob_tl2', got %s", items[0].Username)
	}
}
