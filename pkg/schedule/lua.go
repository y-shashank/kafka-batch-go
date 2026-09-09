package schedule

import "github.com/redis/go-redis/v9"

// Lua mirrors lib/kafka_batch/schedule/redis_store.rb.

// Script wrappers so call sites use EVALSHA (with automatic EVAL fallback on
// NOSCRIPT) instead of shipping the full Lua source on every invocation.
var (
	claimDueScript = redis.NewScript(claimDueLua)
	reclaimScript  = redis.NewScript(reclaimLua)
)

const claimDueLua = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
if #due == 0 then return {} end
for i = 1, #due do
  redis.call('ZREM', KEYS[1], due[i])
  redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), due[i])
end
return due
`

const reclaimLua = `
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i = 1, #expired do
  redis.call('ZREM', KEYS[1], expired[i])
  redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), expired[i])
end
return #expired
`

const (
	pendingKey  = "kafka_batch:sched:pending"
	inflightKey = "kafka_batch:sched:inflight"
	readMissKey = "kafka_batch:sched:read_miss"
)
