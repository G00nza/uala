package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

func timelineDataKey(userID uuid.UUID) string {
	return fmt.Sprintf("timeline:data:%s", userID)
}

func (r *TimelineRepository) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	key := timelineKey(userID)

	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return r.pgRepo.GetTimeline(ctx, userID)
	}

	if exists > 0 {
		metrics.TimelineCacheHitsTotal.Inc()
		return r.readFromRedis(ctx, userID)
	}

	metrics.TimelineCacheMissesTotal.Inc()
	items, err := r.pgRepo.GetTimeline(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		_ = r.writeToRedis(ctx, userID, items)
	}
	return items, nil
}

func (r *TimelineRepository) readFromRedis(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	key := timelineKey(userID)
	dataKey := timelineDataKey(userID)

	ids, err := r.rdb.ZRevRange(ctx, key, 0, r.limit-1).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []domain.TweetItem{}, nil
	}

	vals, err := r.rdb.HMGet(ctx, dataKey, ids...).Result()
	if err != nil {
		return nil, err
	}

	items := make([]domain.TweetItem, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var item domain.TweetItem
		if err := json.Unmarshal([]byte(v.(string)), &item); err != nil {
			return nil, fmt.Errorf("unmarshal tweet item: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}

func (r *TimelineRepository) writeToRedis(ctx context.Context, userID uuid.UUID, items []domain.TweetItem) error {
	key := timelineKey(userID)
	dataKey := timelineDataKey(userID)

	members := make([]redis.Z, len(items))
	dataFields := make([]interface{}, 0, len(items)*2)

	for i, item := range items {
		data, err := json.Marshal(item)
		if err != nil {
			return err
		}
		id := item.ID.String()
		members[i] = redis.Z{Score: float64(item.CreatedAt.Unix()), Member: id}
		dataFields = append(dataFields, id, string(data))
	}

	if err := r.rdb.ZAdd(ctx, key, members...).Err(); err != nil {
		return err
	}
	return r.rdb.HSet(ctx, dataKey, dataFields...).Err()
}

func (r *TimelineRepository) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem, ttl time.Duration) error {
	key := timelineKey(userID)
	dataKey := timelineDataKey(userID)

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	id := item.ID.String()

	if err := r.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(item.CreatedAt.Unix()),
		Member: id,
	}).Err(); err != nil {
		return err
	}
	return r.rdb.HSetNX(ctx, dataKey, id, string(data)).Err()
}
