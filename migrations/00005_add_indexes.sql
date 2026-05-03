-- +goose Up
CREATE INDEX idx_follows_followee_id ON follows (followee_id);
CREATE INDEX idx_tweets_user_id ON tweets (user_id);

-- +goose Down
DROP INDEX idx_tweets_user_id;
DROP INDEX idx_follows_followee_id;
