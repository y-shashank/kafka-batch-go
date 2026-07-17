package workset

// claimLua atomically claims a job.
// KEYS[1]=job key KEYS[2]=by_consumer SET KEYS[3]=zindex KEYS[4]=live prefix
// ARGV[1]=job_id ARGV[2]=consumer_id ARGV[3]=fence ARGV[4]=json
// ARGV[5]=lease_ttl_sec ARGV[6]=now_unix ARGV[7]=steal_grace_sec ARGV[8]=heartbeat_ttl_sec
// Returns 1 if won, 2 if resumed, 0 if another live (or not-yet-stealable) consumer owns it.
const claimLua = `
local jobKey = KEYS[1]
local byCons = KEYS[2]
local index = KEYS[3]
local livePrefix = KEYS[4]
local jobID = ARGV[1]
local consumerID = ARGV[2]
local fence = ARGV[3]
local payload = ARGV[4]
local ttl = tonumber(ARGV[5])
local now = tonumber(ARGV[6])
local grace = tonumber(ARGV[7]) or 0
local hbTTL = tonumber(ARGV[8])
if not hbTTL or hbTTL < 1 then hbTTL = ttl end

local cur = redis.call('GET', jobKey)
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and type(obj) == 'table' then
    local owner = obj['consumer_id']
    if owner and owner ~= '' and owner ~= consumerID then
      local alive = redis.call('EXISTS', livePrefix .. owner)
      if alive == 1 then
        return 0
      end
      local claimedUnix = tonumber(obj['claimed_at_unix'] or 0) or 0
      if grace > 0 and claimedUnix > 0 and (now - claimedUnix) < grace then
        return 0
      end
      redis.call('SREM', 'kafka_batch:work:by_consumer:' .. owner, jobID)
    elseif owner == consumerID then
      -- Same consumer already owns (crash between claim and kafka ack). Resume.
      redis.call('EXPIRE', jobKey, ttl)
      local liveKey = livePrefix .. consumerID
      -- Seed only when missing so we do not stomp heartbeat JSON (Ruby /live
      -- used to crash on Oj.load("1") → Integer then hash[:key]).
      if redis.call('EXISTS', liveKey) == 0 then
        redis.call('SET', liveKey, '{"consumer_id":"' .. consumerID .. '"}', 'EX', hbTTL)
      else
        redis.call('EXPIRE', liveKey, hbTTL)
      end
      local claimedUnix = tonumber(obj['claimed_at_unix'] or 0) or now
      redis.call('ZADD', index, claimedUnix, jobID)
      return 2
    end
  end
end

redis.call('SET', jobKey, payload, 'EX', ttl)
redis.call('SADD', byCons, jobID)
redis.call('ZADD', index, now, jobID)
local liveKey = livePrefix .. consumerID
if redis.call('EXISTS', liveKey) == 0 then
  redis.call('SET', liveKey, '{"consumer_id":"' .. consumerID .. '"}', 'EX', hbTTL)
else
  redis.call('EXPIRE', liveKey, hbTTL)
end
return 1
`

const renewLua = `
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local ok, obj = pcall(cjson.decode, cur)
if not ok or type(obj) ~= 'table' then return 0 end
if obj['consumer_id'] ~= ARGV[1] or obj['fence'] ~= ARGV[2] then return 0 end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`

const completeLua = `
local cur = redis.call('GET', KEYS[1])
if not cur then
  redis.call('SREM', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[3], ARGV[1])
  return 0
end
local ok, obj = pcall(cjson.decode, cur)
if not ok or type(obj) ~= 'table' then return 0 end
if obj['consumer_id'] ~= ARGV[2] or obj['fence'] ~= ARGV[3] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`

// finishReclaimLua deletes the job if fence matches (or entry missing) and clears reclaim lock + produced marker.
// KEYS[1]=job KEYS[2]=by_consumer KEYS[3]=zindex KEYS[4]=reclaiming KEYS[5]=produced
// ARGV[1]=job_id ARGV[2]=fence
const finishReclaimLua = `
local cur = redis.call('GET', KEYS[1])
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and type(obj) == 'table' then
    if obj['fence'] ~= ARGV[2] then
      redis.call('DEL', KEYS[4])
      return 0
    end
    local owner = obj['consumer_id']
    if owner and owner ~= '' then
      redis.call('SREM', 'kafka_batch:work:by_consumer:' .. owner, ARGV[1])
    end
  end
  redis.call('DEL', KEYS[1])
end
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('DEL', KEYS[4])
redis.call('DEL', KEYS[5])
return 1
`
