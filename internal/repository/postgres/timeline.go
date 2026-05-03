package postgres

import (
	"context"
	"slices"

	"uala/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TimelineRepository struct {
	db *pgxpool.Pool
}

func NewTimelineRepository(db *pgxpool.Pool) *TimelineRepository {
	return &TimelineRepository{db: db}
}

func (r *TimelineRepository) GetLatestByUser(ctx context.Context, userID uuid.UUID, limit int) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM tweets t
		JOIN users u ON u.id = t.user_id
		WHERE t.user_id = $1
		ORDER BY t.created_at DESC
		LIMIT $2
	`, userID, limit)
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

func (r *TimelineRepository) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	switch {
	case q.After != nil:
		return r.getTimelineAfter(ctx, q)
	case q.Before != nil:
		return r.getTimelineBefore(ctx, q)
	default:
		return r.getTimelineFirst(ctx, q)
	}
}

func (r *TimelineRepository) getTimelineAfter(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM tweets t
		JOIN users u ON u.id = t.user_id
		WHERE (t.user_id = $1 OR t.user_id IN (SELECT followee_id FROM follows WHERE follower_id = $1))
		  AND t.created_at < (SELECT created_at FROM tweets WHERE id = $2)
		ORDER BY t.created_at DESC
		LIMIT $3
	`, q.UserID, q.After, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimeline(rows)
}

func (r *TimelineRepository) getTimelineBefore(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM tweets t
		JOIN users u ON u.id = t.user_id
		WHERE (t.user_id = $1 OR t.user_id IN (SELECT followee_id FROM follows WHERE follower_id = $1))
		  AND t.created_at > (SELECT created_at FROM tweets WHERE id = $2)
		ORDER BY t.created_at ASC
		LIMIT $3
	`, q.UserID, q.Before, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanTimeline(rows)
	if err != nil {
		return nil, err
	}
	slices.Reverse(items)
	return items, nil
}

func (r *TimelineRepository) getTimelineFirst(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	rows, err := r.db.Query(ctx, `
		SELECT t.id, t.user_id, u.username, t.content, t.created_at
		FROM tweets t
		JOIN users u ON u.id = t.user_id
		WHERE t.user_id = $1
		   OR t.user_id IN (SELECT followee_id FROM follows WHERE follower_id = $1)
		ORDER BY t.created_at DESC
		LIMIT $2
	`, q.UserID, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTimeline(rows)
}

func scanTimeline(rows pgx.Rows) ([]domain.TweetItem, error) {
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
