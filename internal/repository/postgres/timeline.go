package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"uala/internal/domain"
)

type TimelineRepository struct {
	db *pgxpool.Pool
}

func NewTimelineRepository(db *pgxpool.Pool) *TimelineRepository {
	return &TimelineRepository{db: db}
}

func (r *TimelineRepository) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM follows f
		JOIN tweets t ON t.user_id = f.followee_id
		JOIN users u ON u.id = t.user_id
		WHERE f.follower_id = $1
		ORDER BY t.created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []domain.TweetItem
	for rows.Next() {
		var item domain.TweetItem
		if err := rows.Scan(&item.ID, &item.UserID, &item.Username, &item.Content, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if items == nil {
		items = []domain.TweetItem{}
	}
	return items, rows.Err()
}
