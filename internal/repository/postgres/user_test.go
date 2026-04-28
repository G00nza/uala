package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func TestUserRepository_Create(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	u := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC()}
	if err := repo.Create(context.Background(), u); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestUserRepository_Create_DuplicateUsername(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	u1 := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	u2 := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), u1)

	err := repo.Create(context.Background(), u2)
	if err != domain.ErrUsernameConflict {
		t.Fatalf("want ErrUsernameConflict, got %v", err)
	}
}

func TestUserRepository_GetByID(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	u := &domain.User{ID: uuid.New(), Username: "carol", CreatedAt: time.Now().UTC()}
	_ = repo.Create(context.Background(), u)

	got, err := repo.GetByID(context.Background(), u.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Username != "carol" {
		t.Fatalf("want carol, got %s", got.Username)
	}
}

func TestUserRepository_GetByID_NotFound(t *testing.T) {
	truncate(t)
	repo := postgres.NewUserRepository(testDB)

	_, err := repo.GetByID(context.Background(), uuid.New())
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
