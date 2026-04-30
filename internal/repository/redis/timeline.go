package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"uala/internal/domain"
	"uala/internal/metrics"
)

type TimelineRepository struct {
	rdb    *redis.Client
	pgRepo domain.TimelineRepository
	limit  int64
}

func NewTimelineRepository(rdb *redis.Client, pgRepo domain.TimelineRepository, limit int) *TimelineRepository {
	return &TimelineRepository{rdb: rdb, pgRepo: pgRepo, limit: int64(limit)}
}

func timelineKey(userID uuid.UUID) string {
	return fmt.Sprintf("timeline:%s", userID)
}

func (r *TimelineRepository) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	key := timelineKey(userID)

	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return r.pgRepo.GetTimeline(ctx, userID)
	}

	if exists > 0 {
		metrics.TimelineCacheHitsTotal.Inc()
		return r.readFromRedis(ctx, key)
	}

	metrics.TimelineCacheMissesTotal.Inc()
	items, err := r.pgRepo.GetTimeline(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		_ = r.writeToRedis(ctx, key, items)
	}
	return items, nil
}

func (r *TimelineRepository) readFromRedis(ctx context.Context, key string) ([]domain.TweetItem, error) {
	vals, err := r.rdb.ZRevRange(ctx, key, 0, r.limit-1).Result()
	if err != nil {
		return nil, err
	}
	items := make([]domain.TweetItem, 0, len(vals))
	for _, v := range vals {
		var item domain.TweetItem
		if err := json.Unmarshal([]byte(v), &item); err != nil {
			return nil, fmt.Errorf("unmarshal tweet item: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}

func (r *TimelineRepository) writeToRedis(ctx context.Context, key string, items []domain.TweetItem) error {
	members := make([]redis.Z, len(items))
	for i, item := range items {
		data, err := json.Marshal(item)
		if err != nil {
			return err
		}
		members[i] = redis.Z{
			Score:  float64(item.CreatedAt.Unix()),
			Member: string(data),
		}
	}
	return r.rdb.ZAdd(ctx, key, members...).Err()
}

func (r *TimelineRepository) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem) error {
	key := timelineKey(userID)
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(item.CreatedAt.Unix()),
		Member: string(data),
	}).Err()
}
