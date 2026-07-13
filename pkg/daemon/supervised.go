package daemon

import (
	"context"
	"fmt"
	"log"
	"time"
)

// runLoopSupervised runs fn in a restart loop with panic recovery and exponential backoff.
// fn should return nil on clean shutdown (ctx cancelled) or a non-nil error to trigger restart.
func runLoopSupervised(ctx context.Context, name string, health *LoopHealth, fn func(context.Context) error) {
	if health != nil {
		health.Register(name)
	}
	backoff := consumerRestartInitial
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := runLoopOnce(ctx, name, fn)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] loop %s error=%v — restarting in %s", name, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if time.Since(started) >= 30*time.Second {
			backoff = consumerRestartInitial
		} else if backoff < consumerRestartMax {
			backoff *= 2
			if backoff > consumerRestartMax {
				backoff = consumerRestartMax
			}
		}
	}
}

func runLoopOnce(ctx context.Context, name string, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[kbatch] loop %s panic: %v", name, r)
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx)
}
