-- +goose Up
ALTER TABLE users ADD COLUMN last_active TIMESTAMPTZ;
CREATE INDEX idx_users_last_active ON users (last_active);

-- +goose Down
DROP INDEX IF EXISTS idx_users_last_active;
ALTER TABLE users DROP COLUMN IF EXISTS last_active;
