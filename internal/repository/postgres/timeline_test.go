package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

func TestTimelineRepository_GetTimeline_Empty(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_tl", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)

	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{UserID: alice.ID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
}

func TestTimelineRepository_GetTimeline_WithTweets(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_tl2", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_tl2", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)

	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{
		ID: uuid.New(), UserID: bob.ID, Content: "hello from bob", CreatedAt: time.Now().UTC(),
	})

	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{UserID: alice.ID, Limit: 20})
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

func TestTimelineRepository_GetTimeline_FirstPage_Limit(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_pg_lim", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_pg_lim", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 5; i++ {
		_ = r.tweet.Create(context.Background(), &domain.Tweet{
			ID:        uuid.New(),
			UserID:    bob.ID,
			Content:   fmt.Sprintf("tweet%d", i),
			CreatedAt: base.Add(time.Duration(i) * time.Second),
		})
	}

	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{UserID: alice.ID, Limit: 3})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("want 3 items, got %d", len(items))
	}
}

func TestTimelineRepository_GetTimeline_AfterCursor(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_pg_after", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_pg_after", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	base := time.Now().UTC().Truncate(time.Second)
	t1ID := uuid.New()
	t2ID := uuid.New()
	t3ID := uuid.New()
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t1ID, UserID: bob.ID, Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t2ID, UserID: bob.ID, Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t3ID, UserID: bob.ID, Content: "tweet3", CreatedAt: base})

	// after=t3ID → tweets older than tweet3 → [tweet2, tweet1] DESC
	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{
		UserID: alice.ID, After: &t3ID, Limit: 20,
	})
	if err != nil {
		t.Fatalf("GetTimeline after: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet2" {
		t.Fatalf("want tweet2 first, got %s", items[0].Content)
	}
	if items[1].Content != "tweet1" {
		t.Fatalf("want tweet1 second, got %s", items[1].Content)
	}
}

func TestTimelineRepository_GetTimeline_BeforeCursor(t *testing.T) {
	r := setup(t)

	alice := &domain.User{ID: uuid.New(), Username: "alice_pg_before", CreatedAt: time.Now().UTC()}
	bob := &domain.User{ID: uuid.New(), Username: "bob_pg_before", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), alice)
	_ = r.user.Create(context.Background(), bob)
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	base := time.Now().UTC().Truncate(time.Second)
	t1ID := uuid.New()
	t2ID := uuid.New()
	t3ID := uuid.New()
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t1ID, UserID: bob.ID, Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t2ID, UserID: bob.ID, Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute)})
	_ = r.tweet.Create(context.Background(), &domain.Tweet{ID: t3ID, UserID: bob.ID, Content: "tweet3", CreatedAt: base})

	// before=t1ID → tweets newer than tweet1 → [tweet3, tweet2] DESC
	items, err := r.timeline.GetTimeline(context.Background(), domain.TimelineQuery{
		UserID: alice.ID, Before: &t1ID, Limit: 20,
	})
	if err != nil {
		t.Fatalf("GetTimeline before: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet3" {
		t.Fatalf("want tweet3 first (newest), got %s", items[0].Content)
	}
	if items[1].Content != "tweet2" {
		t.Fatalf("want tweet2 second, got %s", items[1].Content)
	}
}
