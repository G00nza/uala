package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"uala/internal/domain"
	postgresrepo "uala/internal/repository/postgres"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegration_UserActivityConsumer_UpdatesLastActive(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)

	// Arrange
	aliceID := uuid.New()
	seedUser(t, aliceID, "alice")
	userRepo := postgresrepo.NewUserRepository(testDB)
	consumer := &UserActivityConsumer{repo: userRepo}

	lastActive := time.Now().UTC().Truncate(time.Millisecond)
	evt := domain.UserActivityEvent{UserID: aliceID, LastActive: lastActive}
	body, _ := json.Marshal(evt)

	// Act
	acked, nacked := consumer.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	// Assert: ack
	if !acked || nacked {
		t.Fatalf("want ack=true nack=false, got ack=%v nack=%v", acked, nacked)
	}

	// Assert: last_active updated in DB
	var stored time.Time
	err := testDB.QueryRow(context.Background(),
		"SELECT last_active FROM users WHERE id = $1", aliceID).Scan(&stored)
	if err != nil {
		t.Fatalf("query last_active: %v", err)
	}
	if !stored.Equal(lastActive) {
		t.Errorf("want last_active %v, got %v", lastActive, stored)
	}
}

type stubUserActivityRepo struct {
	mu     sync.Mutex
	calls  []domain.UserActivityEvent
	retErr error
}

func (s *stubUserActivityRepo) UpdateLastActive(_ context.Context, userID uuid.UUID, lastActive time.Time) error {
	s.mu.Lock()
	s.calls = append(s.calls, domain.UserActivityEvent{UserID: userID, LastActive: lastActive})
	s.mu.Unlock()
	return s.retErr
}

func TestUserActivityConsumer_AcksOnSuccess(t *testing.T) {
	repo := &stubUserActivityRepo{}
	c := &UserActivityConsumer{repo: repo}

	evt := domain.UserActivityEvent{UserID: uuid.New(), LastActive: time.Now()}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if !acked {
		t.Error("expected ack on success")
	}
	if nacked {
		t.Error("expected no nack on success")
	}
}

func TestUserActivityConsumer_NacksOnRepoError(t *testing.T) {
	repo := &stubUserActivityRepo{retErr: errors.New("db error")}
	c := &UserActivityConsumer{repo: repo}

	evt := domain.UserActivityEvent{UserID: uuid.New(), LastActive: time.Now()}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if acked {
		t.Error("expected no ack on repo error")
	}
	if !nacked {
		t.Error("expected nack on repo error")
	}
}

func TestUserActivityConsumer_NacksOnBadJSON(t *testing.T) {
	c := &UserActivityConsumer{repo: &stubUserActivityRepo{}}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
