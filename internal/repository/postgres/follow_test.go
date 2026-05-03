package postgres_test

import (
	"context"
	"testing"
	"time"

	"uala/internal/domain"
	"uala/internal/repository/postgres"

	"github.com/google/uuid"
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

	f := &domain.Follow{FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	if err := r.follow.Create(context.Background(), f); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var gotFollowerID, gotFolloweeID uuid.UUID
	var gotCreatedAt time.Time
	err := testDB.QueryRow(context.Background(),
		`SELECT follower_id, followee_id, created_at FROM follows WHERE follower_id = $1 AND followee_id = $2`,
		alice.ID, bob.ID,
	).Scan(&gotFollowerID, &gotFolloweeID, &gotCreatedAt)
	if err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if gotFollowerID != alice.ID {
		t.Errorf("want follower_id %s, got %s", alice.ID, gotFollowerID)
	}
	if gotFolloweeID != bob.ID {
		t.Errorf("want followee_id %s, got %s", bob.ID, gotFolloweeID)
	}
	if !gotCreatedAt.Equal(f.CreatedAt) {
		t.Errorf("want created_at %v, got %v", f.CreatedAt, gotCreatedAt)
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

func setLastActive(t *testing.T, userID uuid.UUID, lastActive time.Time) {
	t.Helper()
	_, err := testDB.Exec(context.Background(),
		`UPDATE users SET last_active = $1 WHERE id = $2`, lastActive, userID)
	if err != nil {
		t.Fatalf("setLastActive: %v", err)
	}
}

func TestFollowRepository_GetActiveFollowers_ReturnsOnlyActive(t *testing.T) {
	r := setup(t)
	alice := seedUser(t, r.user, "alice_active")
	bob := seedUser(t, r.user, "bob_celebrity")
	carol := seedUser(t, r.user, "carol_inactive")

	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: carol.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})

	// alice was active 1h ago — within 24h window
	setLastActive(t, alice.ID, time.Now().UTC().Add(-1*time.Hour))
	// carol was active 48h ago — outside window
	setLastActive(t, carol.ID, time.Now().UTC().Add(-48*time.Hour))

	activeSince := time.Now().UTC().Add(-24 * time.Hour)
	followers, err := r.follow.GetActiveFollowers(context.Background(), bob.ID, activeSince)
	if err != nil {
		t.Fatalf("GetActiveFollowers: %v", err)
	}
	if len(followers) != 1 {
		t.Fatalf("want 1 active follower, got %d", len(followers))
	}
	if followers[0].ID != alice.ID {
		t.Errorf("want alice ID, got %s", followers[0].ID)
	}
	if followers[0].LastActive.IsZero() {
		t.Error("want non-zero LastActive")
	}
}

func TestFollowRepository_GetActiveFollowers_ExcludesNullLastActive(t *testing.T) {
	r := setup(t)
	alice := seedUser(t, r.user, "alice_null")
	bob := seedUser(t, r.user, "bob_null")

	_ = r.follow.Create(context.Background(), &domain.Follow{
		FollowerID: alice.ID, FolloweeID: bob.ID, CreatedAt: time.Now().UTC(),
	})
	// alice has NULL last_active — never recorded activity

	activeSince := time.Now().UTC().Add(-24 * time.Hour)
	followers, err := r.follow.GetActiveFollowers(context.Background(), bob.ID, activeSince)
	if err != nil {
		t.Fatalf("GetActiveFollowers: %v", err)
	}
	if len(followers) != 0 {
		t.Fatalf("want 0 active followers (null last_active), got %d", len(followers))
	}
}
