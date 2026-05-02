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
	rdb         *redis.Client
	pgRepo      domain.TimelineRepository
	limit       int64
	activityTTL time.Duration
}

func NewTimelineRepository(rdb *redis.Client, pgRepo domain.TimelineRepository, limit int) *TimelineRepository {
	return &TimelineRepository{rdb: rdb, pgRepo: pgRepo, limit: int64(limit)}
}

func (r *TimelineRepository) WithActivityTTL(ttl time.Duration) *TimelineRepository {
	r.activityTTL = ttl
	return r
}

func timelineKey(userID uuid.UUID) string {
	return fmt.Sprintf("timeline:%s", userID)
}

func timelineDataKey(userID uuid.UUID) string {
	return fmt.Sprintf("timeline:data:%s", userID)
}

func (r *TimelineRepository) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	key := timelineKey(q.UserID)

	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return r.pgRepo.GetTimeline(ctx, q)
	}

	if exists == 0 {
		metrics.TimelineCacheMissesTotal.Inc()
		if q.After != nil || q.Before != nil {
			// Key expired but cursor present — go straight to Postgres
			return r.pgRepo.GetTimeline(ctx, q)
		}
		// First page cache miss: warm Redis with up to r.limit tweets, then paginate
		allItems, err := r.pgRepo.GetTimeline(ctx, domain.TimelineQuery{UserID: q.UserID, Limit: int(r.limit)})
		if err != nil {
			return nil, err
		}
		if len(allItems) == 0 {
			return allItems, nil
		}
		_ = r.writeToRedis(ctx, q.UserID, allItems)
	} else {
		metrics.TimelineCacheHitsTotal.Inc()
	}

	return r.readFromRedis(ctx, q)
}

func (r *TimelineRepository) readFromRedis(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	key := timelineKey(q.UserID)
	dataKey := timelineDataKey(q.UserID)

	var ids []string

	switch {
	case q.After != nil:
		rank, err := r.rdb.ZRevRank(ctx, key, q.After.String()).Result()
		if err == redis.Nil {
			return r.pgRepo.GetTimeline(ctx, q)
		}
		if err != nil {
			return nil, err
		}
		ids, err = r.rdb.ZRevRange(ctx, key, rank+1, rank+int64(q.Limit)).Result()
		if err != nil {
			return nil, err
		}
	case q.Before != nil:
		rank, err := r.rdb.ZRevRank(ctx, key, q.Before.String()).Result()
		if err == redis.Nil {
			return r.pgRepo.GetTimeline(ctx, q)
		}
		if err != nil {
			return nil, err
		}
		start := rank - int64(q.Limit)
		if start < 0 {
			start = 0
		}
		ids, err = r.rdb.ZRevRange(ctx, key, start, rank-1).Result()
		if err != nil {
			return nil, err
		}
	default:
		var err error
		ids, err = r.rdb.ZRevRange(ctx, key, 0, int64(q.Limit)-1).Result()
		if err != nil {
			return nil, err
		}
	}

	if len(ids) == 0 {
		return []domain.TweetItem{}, nil
	}

	vals, err := r.rdb.HMGet(ctx, dataKey, ids...).Result()
	if err != nil {
		return nil, err
	}

	if r.activityTTL > 0 {
		_ = r.rdb.Expire(ctx, key, r.activityTTL)
		_ = r.rdb.Expire(ctx, dataKey, r.activityTTL)
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
	if err := r.rdb.HSet(ctx, dataKey, dataFields...).Err(); err != nil {
		return err
	}
	if r.activityTTL > 0 {
		_ = r.rdb.Expire(ctx, key, r.activityTTL)
		_ = r.rdb.Expire(ctx, dataKey, r.activityTTL)
	}
	return nil
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
	if err := r.rdb.HSetNX(ctx, dataKey, id, string(data)).Err(); err != nil {
		return err
	}
	if ttl > 0 {
		_ = r.rdb.ExpireNX(ctx, key, ttl)
		_ = r.rdb.ExpireNX(ctx, dataKey, ttl)
	}
	return nil
}
