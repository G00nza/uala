package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func seedUser(t *testing.T, username string) *domain.User {
	t.Helper()
	repo := postgres.NewUserRepository(testDB)
	u := &domain.User{ID: uuid.New(), Username: username, CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), u); err != nil {
		t.Fatalf("seedUser: %v", err)
	}
	return u
}

func TestFollowRepository_Create(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")

	repo := postgres.NewFollowRepository(testDB)
	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), f); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestFollowRepository_Create_Duplicate(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")

	repo := postgres.NewFollowRepository(testDB)
	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), f)

	err := repo.Create(context.Background(), f)
	if err != domain.ErrAlreadyFollowing {
		t.Fatalf("want ErrAlreadyFollowing, got %v", err)
	}
}

func TestFollowRepository_Exists_True(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")

	repo := postgres.NewFollowRepository(testDB)
	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), f)

	exists, err := repo.Exists(context.Background(), alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatal("want exists=true")
	}
}

func TestFollowRepository_Exists_False(t *testing.T) {
	truncate(t)
	repo := postgres.NewFollowRepository(testDB)

	exists, err := repo.Exists(context.Background(), uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("want exists=false")
	}
}

func TestFollowRepository_GetFollowers(t *testing.T) {
	truncate(t)
	alice := seedUser(t, "alice")
	bob := seedUser(t, "bob")
	carol := seedUser(t, "carol")

	repo := postgres.NewFollowRepository(testDB)
	_ = repo.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = repo.Create(context.Background(), &domain.Follow{
		FollowerID: carol.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	followers, err := repo.GetFollowers(context.Background(), bob.ID)
	if err != nil {
		t.Fatalf("GetFollowers: %v", err)
	}
	if len(followers) != 2 {
		t.Fatalf("want 2 followers, got %d", len(followers))
	}
}

func TestFollowRepository_GetFollowers_Empty(t *testing.T) {
	truncate(t)
	bob := seedUser(t, "bob_empty")

	repo := postgres.NewFollowRepository(testDB)
	followers, err := repo.GetFollowers(context.Background(), bob.ID)
	if err != nil {
		t.Fatalf("GetFollowers: %v", err)
	}
	if len(followers) != 0 {
		t.Fatalf("want 0 followers, got %d", len(followers))
	}
}
