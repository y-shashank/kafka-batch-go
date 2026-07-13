package store

import "strconv"

func hashToBatch(h map[string]string) *Batch {
	if len(h) == 0 {
		return nil
	}
	b := &Batch{
		ID: h["id"], Status: h["status"], OnSuccess: h["on_success"], OnComplete: h["on_complete"],
		Meta: h["meta"], CallbackArgs: h["callback_args"],
		Description: h["description"], TenantID: h["tenant_id"],
		LockedAt: h["locked_at"], FinishedAt: h["finished_at"],
		ReconcilerRefiredAt: h["reconciler_refired_at"],
	}
	b.TotalJobs, _ = strconv.ParseInt(h["total_jobs"], 10, 64)
	b.CompletedCount, _ = strconv.ParseInt(h["completed_count"], 10, 64)
	b.FailedCount, _ = strconv.ParseInt(h["failed_count"], 10, 64)
	b.CallbackClaimed = h["callback_dispatched_at"] != ""
	return b
}
