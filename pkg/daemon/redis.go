package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PingRedis verifies Redis is reachable before starting consumers.
func PingRedis(ctx context.Context, rdb *redis.Client) error {
	if rdb == nil {
		return fmt.Errorf("redis client is nil")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}
