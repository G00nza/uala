package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func seedUser(t *testing.T, repo *postgres.UserRepository, username string) *domain.User {
	t.Helper()
	u := &domain.User{ID: uuid.New(), Username: username, CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), u); err != nil {
		t.Fatalf("seedUser: %v", err)
	}
	return u
}

func TestFollowRepository_Create(t *testing.T) {
	r := setup(t)
	alice := seedUser(t, r.user, "alice")
	bob := seedUser(t, r.user, "bob")

	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	if err := r.follow.Create(context.Background(), f); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestFollowRepository_Create_Duplicate(t *testing.T) {
	r := setup(t)
	alice := seedUser(t, r.user, "alice")
	bob := seedUser(t, r.user, "bob")

	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	_ = r.follow.Create(context.Background(), f)

	err := r.follow.Create(context.Background(), f)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowRepository_Exists_True(t *testing.T) {
	r := setup(t)
	alice := seedUser(t, r.user, "alice")
	bob := seedUser(t, r.user, "bob")

	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	_ = r.follow.Create(context.Background(), f)

	exists, err := r.follow.Exists(context.Background(), alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatal("want exists=true")
	}
}

func TestFollowRepository_Exists_False(t *testing.T) {
	r := setup(t)

	exists, err := r.follow.Exists(context.Background(), uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("want exists=false")
	}
}

func TestFollowRepository_GetFollowers(t *testing.T) {
	r := setup(t)
	alice := seedUser(t, r.user, "alice")
	bob := seedUser(t, r.user, "bob")
	carol := seedUser(t, r.user, "carol")

	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: carol.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	followers, err := r.follow.GetFollowers(context.Background(), bob.ID)
	if err != nil {
		t.Fatalf("GetFollowers: %v", err)
	}
	if len(followers) != 2 {
		t.Fatalf("want 2 followers, got %d", len(followers))
	}
}

func TestFollowRepository_GetFollowers_Empty(t *testing.T) {
	r := setup(t)
	bob := seedUser(t, r.user, "bob_empty")

	followers, err := r.follow.GetFollowers(context.Background(), bob.ID)
	if err != nil {
		t.Fatalf("GetFollowers: %v", err)
	}
	if len(followers) != 0 {
		t.Fatalf("want 0 followers, got %d", len(followers))
	}
}
