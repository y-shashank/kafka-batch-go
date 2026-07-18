package config

import (
	"fmt"
	"sort"
	"strings"
)

// ResolveJobType maps a user-entered handler identifier to the canonical
// manifest job_type. It accepts either a job_type already in the manifest
// (e.g. "hello.go", "hello.ruby") or a worker_class name (e.g. "HelloRubyWorker"
// → "hello.ruby"). ok is false when it resolves to neither — callers should
// reject it early rather than store a value that only fails at enqueue/fire time
// with "unknown job_type". A job_type match wins over a worker_class match.
func (m Manifest) ResolveJobType(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	if _, ok := m.Handlers[name]; ok {
		return name, true
	}
	for jt, h := range m.Handlers {
		if h.WorkerClass == name {
			return jt, true
		}
	}
	return "", false
}

// JobTypes returns the manifest's job_types sorted (for error messages / listing).
func (m Manifest) JobTypes() []string {
	out := make([]string, 0, len(m.Handlers))
	for jt := range m.Handlers {
		out = append(out, jt)
	}
	sort.Strings(out)
	return out
}

// RuntimeFor returns the normalized runtime for a job_type.
func (m Manifest) RuntimeFor(jobType string) string {
	if h, ok := m.Handlers[jobType]; ok {
		return strings.ToLower(strings.TrimSpace(h.Runtime))
	}
	return ""
}

// JobTopicsForRuntime lists plain (non-fair-ingest) topics for one runtime.
func (m Manifest) JobTopicsForRuntime(runtime, defaultTopic string) []string {
	rt := strings.ToLower(strings.TrimSpace(runtime))
	seen := map[string]struct{}{}
	var out []string
	for _, h := range m.Handlers {
		if strings.ToLower(strings.TrimSpace(h.Runtime)) != rt {
			continue
		}
		if fairnessLane(h.FairnessType) != "" {
			continue
		}
		t := h.Topic
		if t == "" {
			t = defaultTopic
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func fairnessLane(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "time" || s == "throughput" {
		return s
	}
	return ""
}

// TopicRuntime returns the runtime that owns a Kafka topic ("" if unknown).
func (m Manifest) TopicRuntime(topic, defaultTopic string) string {
	topic = strings.TrimSpace(topic)
	owners := map[string]string{}
	for _, h := range m.Handlers {
		if fairnessLane(h.FairnessType) != "" {
			continue
		}
		t := h.Topic
		if t == "" {
			t = defaultTopic
		}
		rt := strings.ToLower(strings.TrimSpace(h.Runtime))
		if rt != "" {
			owners[t] = rt
		}
	}
	return owners[topic]
}

// FilterTopicsForRuntime keeps topics assigned to one runtime in the manifest.
func (m Manifest) FilterTopicsForRuntime(runtime string, topics []string, defaultTopic string) []string {
	rt := strings.ToLower(strings.TrimSpace(runtime))
	var out []string
	for _, t := range topics {
		if m.TopicRuntime(t, defaultTopic) == rt {
			out = append(out, t)
		}
	}
	return out
}

// HasFairHandlersForRuntime reports whether any handler uses a fairness lane for a runtime.
func (m Manifest) HasFairHandlersForRuntime(runtime, lane string) bool {
	want := strings.ToLower(strings.TrimSpace(runtime))
	for _, h := range m.Handlers {
		if fairnessLane(h.FairnessType) != lane {
			continue
		}
		if strings.ToLower(strings.TrimSpace(h.Runtime)) == want {
			return true
		}
	}
	return false
}

// ValidateFairReadySplit requires per-runtime ready topics when both runtimes use fairness.
func (c Daemon) ValidateFairReadySplit(m Manifest) error {
	if !c.FairnessEnabled {
		return nil
	}
	for _, lane := range []string{"time", "throughput"} {
		goFair := m.HasFairHandlersForRuntime(RuntimeGo, lane)
		rubyFair := m.HasFairHandlersForRuntime(RuntimeRuby, lane)
		if !goFair || !rubyFair {
			continue
		}
		if !c.RuntimeSplitFairReady(lane) {
			return fmt.Errorf(
				"hybrid fairness on %s lane requires split ready topics (fairness_%s_ready_go and fairness_%s_ready_ruby)",
				lane, lane, lane,
			)
		}
	}
	return nil
}

// ValidateTopicRuntimeExclusivity ensures each Kafka topic belongs to at most one runtime.
func (m Manifest) ValidateTopicRuntimeExclusivity(defaultTopic string) error {
	owners := map[string]string{}
	for jobType, h := range m.Handlers {
		if fairnessLane(h.FairnessType) != "" {
			continue
		}
		t := h.Topic
		if t == "" {
			t = defaultTopic
		}
		rt := strings.ToLower(strings.TrimSpace(h.Runtime))
		if prev, ok := owners[t]; ok && prev != rt {
			return fmt.Errorf("topic %q is shared by %s and %s handlers (one topic per runtime)", t, prev, rt)
		}
		owners[t] = rt
		if rt == "" {
			return fmt.Errorf("handler %q missing runtime", jobType)
		}
	}
	return nil
}

