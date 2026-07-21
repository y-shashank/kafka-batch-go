module github.com/y-shashank/kafka-batch-go

go 1.25.0

// Stale tags deleted from GitHub still live forever on proxy.golang.org.
// v0.0.7 publishes these retractions so go get / @latest stop selecting them.
retract [v0.0.1, v0.0.6] // Reset line; proxy still serves these. Use v0.0.7+.

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/go-sql-driver/mysql v1.10.0
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/twmb/franz-go v1.21.5
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)
