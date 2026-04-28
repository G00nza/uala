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
