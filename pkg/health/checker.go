package health

import "context"

// Checker reports whether the process is healthy enough to serve traffic / stay alive.
type Checker interface {
	Healthy(ctx context.Context) (ok bool, detail string)
}
