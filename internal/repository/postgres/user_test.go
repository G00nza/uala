package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

func TestUserRepository_Create(t *testing.T) {
	r := setup(t)

	u := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	if err := r.user.Create(context.Background(), u); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var gotUsername string
	var gotCreatedAt time.Time
	err := testDB.QueryRow(context.Background(),
		`SELECT username, created_at FROM users WHERE id = $1`, u.ID,
	).Scan(&gotUsername, &gotCreatedAt)
	if err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if gotUsername != u.Username {
		t.Errorf("want username %q, got %q", u.Username, gotUsername)
	}
	if !gotCreatedAt.Equal(u.CreatedAt) {
		t.Errorf("want created_at %v, got %v", u.CreatedAt, gotCreatedAt)
	}
}

func TestUserRepository_Create_DuplicateUsername(t *testing.T) {
	r := setup(t)

	u1 := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	u2 := &domain.User{ID: uuid.New(), Username: "bob", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), u1)

	err := r.user.Create(context.Background(), u2)
	if err != domain.ErrUsernameConflict {
		t.Fatalf("want ErrUsernameConflict, got %v", err)
	}
}

func TestUserRepository_GetByID(t *testing.T) {
	r := setup(t)

	u := &domain.User{ID: uuid.New(), Username: "carol", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), u)

	got, err := r.user.GetByID(context.Background(), u.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Username != "carol" {
		t.Fatalf("want carol, got %s", got.Username)
	}
}

func TestUserRepository_GetByID_NotFound(t *testing.T) {
	r := setup(t)

	_, err := r.user.GetByID(context.Background(), uuid.New())
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestUserRepository_UpdateLastActive_SetsValue(t *testing.T) {
	r := setup(t)
	u := seedUser(t, r.user, "active_user")

	lastActive := time.Now().UTC().Truncate(time.Microsecond)
	if err := r.user.UpdateLastActive(context.Background(), u.ID, lastActive); err != nil {
		t.Fatalf("UpdateLastActive: %v", err)
	}

	var got time.Time
	err := testDB.QueryRow(context.Background(),
		`SELECT last_active FROM users WHERE id = $1`, u.ID,
	).Scan(&got)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !got.Equal(lastActive) {
		t.Errorf("want %v, got %v", lastActive, got)
	}
}

func TestUserRepository_UpdateLastActive_IgnoresOlderTimestamp(t *testing.T) {
	r := setup(t)
	u := seedUser(t, r.user, "active_user2")

	newer := time.Now().UTC().Truncate(time.Microsecond)
	older := newer.Add(-1 * time.Hour)

	_ = r.user.UpdateLastActive(context.Background(), u.ID, newer)
	_ = r.user.UpdateLastActive(context.Background(), u.ID, older)

	var got time.Time
	err := testDB.QueryRow(context.Background(),
		`SELECT last_active FROM users WHERE id = $1`, u.ID,
	).Scan(&got)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !got.Equal(newer) {
		t.Errorf("want newer timestamp %v, got %v", newer, got)
	}
}
