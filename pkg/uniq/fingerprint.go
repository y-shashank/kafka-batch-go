package uniq

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
)

// KeyPrefix is the Redis key prefix for uniqueness locks (wire-compatible with the Ruby
// gem's KafkaBatch::Uniqueness keys). Exported so callers that need to build/inspect the
// key directly (e.g. tests) don't have to hardcode the literal.
const KeyPrefix = "kafka_batch:uniq:"

const keyPrefix = KeyPrefix

const releaseLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// Locker manages per-worker uniqueness locks (Ruby KafkaBatch::Uniqueness).
type Locker struct {
	client *redis.Client
	ttl    time.Duration
}

func NewLocker(client *redis.Client, ttl time.Duration) *Locker {
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	return &Locker{client: client, ttl: ttl}
}

// Claim tries to acquire the uniq lock. Returns true when acquired.
func (l *Locker) Claim(ctx context.Context, workerClassName string, payload map[string]interface{}, jobID string) (bool, error) {
	if l == nil || l.client == nil {
		return true, nil
	}
	key := redisKey(workerClassName, payload)
	ok, err := l.client.SetNX(ctx, key, jobID, l.ttl).Result()
	if err != nil {
		// Fail open like Ruby when Redis is unavailable.
		return true, nil
	}
	return ok, nil
}

// ClaimInput is one row for ClaimMany (bulk push_many / enqueue_many).
type ClaimInput struct {
	WorkerClassName string
	Payload         map[string]interface{}
	JobID           string
}

// ClaimMany acquires many uniq locks in pipelined Redis round-trips. Returns a
// parallel []bool (true = acquired). Order is preserved so within-batch duplicate
// payloads dedupe like sequential Claim calls (first wins). Fails open when Redis
// is unavailable, matching Claim.
func (l *Locker) ClaimMany(ctx context.Context, inputs []ClaimInput) []bool {
	out := make([]bool, len(inputs))
	if len(inputs) == 0 {
		return out
	}
	if l == nil || l.client == nil {
		for i := range out {
			out[i] = true
		}
		return out
	}

	pipe := l.client.Pipeline()
	cmds := make([]*redis.BoolCmd, len(inputs))
	for i, in := range inputs {
		key := redisKey(in.WorkerClassName, in.Payload)
		cmds[i] = pipe.SetNX(ctx, key, in.JobID, l.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		for i := range out {
			out[i] = true
		}
		return out
	}
	for i, cmd := range cmds {
		ok, err := cmd.Result()
		if err != nil {
			out[i] = true
		} else {
			out[i] = ok
		}
	}
	return out
}

// Release drops a lock by fingerprint hex from the wire message.
func (l *Locker) Release(ctx context.Context, fpHex, jobID string) error {
	if l == nil || l.client == nil {
		return nil
	}
	return ReleaseLock(ctx, l.client, fpHex, jobID)
}

// ReleaseLock drops a per-worker uniqueness lock given the wire fingerprint hex and the
// owning job id. This is the single implementation of the release path: pkg/client (via
// Locker.Release, after a failed produce) and pkg/store (via RedisStore.ReleaseUniqLock,
// after a job finishes or expires) both call into this instead of each keeping their own
// copy of the Lua script and key prefix — two independent copies previously existed and
// could silently drift out of sync (e.g. TTL handling, key prefix) without either caller
// noticing.
func ReleaseLock(ctx context.Context, client *redis.Client, fpHex, jobID string) error {
	if client == nil || fpHex == "" || jobID == "" {
		return nil
	}
	bin, err := hex.DecodeString(fpHex)
	if err != nil || len(bin) != 16 {
		return nil
	}
	key := keyPrefix + string(bin)
	return client.Eval(ctx, releaseLua, []string{key}, jobID).Err()
}

// DigestHex returns the 32-char hex fingerprint for _uniq_fp on the wire.
func DigestHex(workerClassName string, payload map[string]interface{}) string {
	return hex.EncodeToString(fingerprint(workerClassName, payload))
}

func redisKey(workerClassName string, payload map[string]interface{}) string {
	return keyPrefix + string(fingerprint(workerClassName, payload))
}

func fingerprint(workerClassName string, payload map[string]interface{}) []byte {
	material := workerClassName + "\x00" + canonicalPayload(payload)
	h1 := xxhash.Sum64String(material)
	h2 := xxhash.Sum64String(material + "\x00uniq_salt_v1")
	buf := make([]byte, 16)
	for i, h := range []uint64{h1, h2} {
		for j := 0; j < 8; j++ {
			buf[i*8+j] = byte(h >> (8 * j))
		}
	}
	return buf
}

// canonicalPayload serializes the deep-key-sorted payload to the exact byte
// sequence the Ruby gem produces with Oj.dump(mode: :compat). This must match
// byte-for-byte across runtimes: the fingerprint is hashed from it, so any
// divergence silently breaks cross-runtime uniqueness dedup (a Ruby-enqueued
// and a Go-enqueued job with identical payloads would compute different
// _uniq_fp values and both run).
//
// The critical detail is HTML escaping: encoding/json escapes '<', '>', '&'
// (and U+2028/U+2029) by default, whereas Oj :compat emits them verbatim. We
// disable escaping via json.Encoder so payloads containing those characters
// (e.g. names, URLs with query strings, HTML fragments) fingerprint identically
// to Ruby. json.Encoder appends a trailing newline that Marshal does not, so we
// trim it.
func canonicalPayload(payload map[string]interface{}) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(deepSortKeys(payload)); err != nil {
		return ""
	}
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func deepSortKeys(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]interface{}, len(t))
		for _, k := range keys {
			out[k] = deepSortKeys(t[k])
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = deepSortKeys(e)
		}
		return out
	default:
		return v
	}
}
