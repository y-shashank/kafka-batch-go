package fairness

import "fmt"

// Redis namespace: kafka_batch:fair_{time|throughput}
func ns(lane Lane) string { return "kafka_batch:fair_" + lane.String() }

func ringKey(lane Lane) string            { return ns(lane) + ":ring" }
func vtimeKey(lane Lane) string           { return ns(lane) + ":vtime" }
func weightKey(lane Lane) string          { return ns(lane) + ":weight" }
func readyPrefix(lane Lane) string         { return ns(lane) + ":ready:" }
func leasesKey(lane Lane) string          { return ns(lane) + ":leases" }
func leasePrefix(lane Lane) string        { return ns(lane) + ":lease:" }
func forwardingKey(lane Lane) string      { return ns(lane) + ":forwarding" }
func forwardingMetaKey(lane Lane) string  { return ns(lane) + ":forwarding_meta" }
func slotDedupPrefix(lane Lane) string    { return ns(lane) + ":slot_dedup:" }
func reclaimLockKey(lane Lane) string     { return ns(lane) + ":reclaim_lock" }

// ReadyKey returns the per-tenant ready list key.
func ReadyKey(lane Lane, tenantID string) string {
	return readyPrefix(lane) + tenantID
}

// TenantLeaseKey returns the per-tenant in-flight lease ZSET.
func TenantLeaseKey(lane Lane, tenantID string) string {
	return leasePrefix(lane) + tenantID
}

// SlotDedupKey returns the ready-topic dedup key for a slot.
func SlotDedupKey(lane Lane, slotID string) string {
	return slotDedupPrefix(lane) + slotID
}

func ValidateLane(lane Lane) error {
	switch lane {
	case LaneTime, LaneThroughput:
		return nil
	default:
		return fmt.Errorf("unknown fairness lane %q", lane)
	}
}
