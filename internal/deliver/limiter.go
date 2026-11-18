package deliver

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter is a token bucket keyed by name.
type Limiter interface {
	// Take spends one token if available. Otherwise it reports how long
	// until one will be.
	Take(ctx context.Context, key string, burst int, every time.Duration) (ok bool, wait time.Duration, err error)
	// Refund returns a token spent on a send that failed: the limit counts
	// messages delivered, not attempts.
	Refund(ctx context.Context, key string, burst int) error
}

// tokenBucket is atomic in Redis: state is (tokens, last refill ms) in a
// hash, refilled lazily on each call. Expires when idle, so inactive users
// cost nothing.
var tokenBucket = redis.NewScript(`
local key, burst, every_ms, now_ms = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local s = redis.call('HMGET', key, 't', 'at')
local tokens, at = tonumber(s[1]), tonumber(s[2])
if tokens == nil then tokens, at = burst, now_ms end
local refill = math.floor((now_ms - at) / every_ms)
if refill > 0 then
  tokens = math.min(burst, tokens + refill)
  at = at + refill * every_ms
end
local ok, wait = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  ok = 1
else
  wait = every_ms - (now_ms - at)
end
if tokens >= burst then at = now_ms end
redis.call('HSET', key, 't', tokens, 'at', at)
redis.call('PEXPIRE', key, every_ms * burst * 2)
return {ok, wait}
`)

var refund = redis.NewScript(`
local t = tonumber(redis.call('HGET', KEYS[1], 't'))
if t ~= nil and t < tonumber(ARGV[1]) then redis.call('HSET', KEYS[1], 't', t + 1) end
return 1
`)

// RedisLimiter implements Limiter on Redis.
type RedisLimiter struct {
	Client *redis.Client
	Now    func() time.Time
}

func (l RedisLimiter) Take(ctx context.Context, key string, burst int, every time.Duration) (bool, time.Duration, error) {
	res, err := tokenBucket.Run(ctx, l.Client, []string{"rl:" + key}, burst, every.Milliseconds(), l.Now().UnixMilli()).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}

func (l RedisLimiter) Refund(ctx context.Context, key string, burst int) error {
	return refund.Run(ctx, l.Client, []string{"rl:" + key}, burst).Err()
}
