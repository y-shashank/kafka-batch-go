package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// A transient MySQL blip on MarkDispatched must be retried in-call: a row left
// 'pending' is re-enqueued by the recovery sweep, which is only deduped for
// uniq handlers — surrendering the mark on the first error double-fires
// non-uniq recurring jobs (e.g. a campaign launch).
func TestEnqueueRetriesMarkDispatched(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	blip := errors.New("mysql gone away")
	mock.ExpectExec("UPDATE kafka_batch_recurring_fires").WillReturnError(blip)
	mock.ExpectExec("UPDATE kafka_batch_recurring_fires").WillReturnError(blip)
	mock.ExpectExec("UPDATE kafka_batch_recurring_fires").WillReturnResult(sqlmock.NewResult(0, 1))

	tk := &Ticker{Store: NewStoreDB(db), Enqueuer: &stubEnqueuer{}}
	cf := ClaimedFire{ScheduleID: 5, Name: "launch", JobType: "campaign.launch",
		FireAt: at("2026-08-01T10:00:00Z")}
	tk.enqueue(context.Background(), cf)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("MarkDispatched not retried to success: %v", err)
	}
}

// uniqAwareEnqueuer implements both Enqueuer and UniqChecker.
type uniqAwareEnqueuer struct {
	stubEnqueuer
	uniq       bool
	known      bool
	uniqChecks atomic.Int32
}

func (u *uniqAwareEnqueuer) HandlerUniq(string) (bool, bool) {
	u.uniqChecks.Add(1)
	return u.uniq, u.known
}

// The non-uniq warning fires once per job type, not per fire.
func TestWarnNonUniqHandlerOncePerJobType(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	for i := 0; i < 2; i++ {
		mock.ExpectExec("UPDATE kafka_batch_recurring_fires").WillReturnResult(sqlmock.NewResult(0, 1))
	}

	enq := &uniqAwareEnqueuer{uniq: false, known: true}
	tk := &Ticker{Store: NewStoreDB(db), Enqueuer: enq, RecoverGrace: 2 * time.Minute}
	cf := ClaimedFire{ScheduleID: 7, Name: "launch", JobType: "campaign.launch",
		FireAt: at("2026-08-01T10:00:00Z")}

	tk.enqueue(context.Background(), cf)
	cf.FireAt = at("2026-08-01T10:01:00Z")
	tk.enqueue(context.Background(), cf)

	if got := enq.uniqChecks.Load(); got != 1 {
		t.Fatalf("HandlerUniq consulted %d times, want 1 (warn once per job type)", got)
	}
}
