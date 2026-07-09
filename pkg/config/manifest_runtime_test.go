package config

import "testing"

func TestValidateFairReadySplit(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.FairnessEnabled = true
	cfg.FairnessTimeReadyGo = ""
	cfg.FairnessTimeReadyRuby = ""
	m := Manifest{
		Handlers: map[string]HandlerEntry{
			"go.fair":   {Runtime: "go", FairnessType: "time"},
			"ruby.fair": {Runtime: "ruby", FairnessType: "time"},
		},
	}
	if err := cfg.ValidateFairReadySplit(m); err == nil {
		t.Fatal("expected hybrid fair split error")
	}
	cfg.FairnessTimeReadyGo = "go.ready"
	cfg.FairnessTimeReadyRuby = "ruby.ready"
	if err := cfg.ValidateFairReadySplit(m); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestFilterTopicsForRuntime(t *testing.T) {
	m := Manifest{
		Handlers: map[string]HandlerEntry{
			"go.p0":   {Runtime: "go", Topic: "p0"},
			"ruby.p1": {Runtime: "ruby", Topic: "p1"},
		},
	}
	got := m.FilterTopicsForRuntime(RuntimeGo, []string{"p0", "p1"}, "default")
	if len(got) != 1 || got[0] != "p0" {
		t.Fatalf("filter go = %v", got)
	}
}

func TestValidateTopicRuntimeExclusivity(t *testing.T) {
	m := Manifest{
		Handlers: map[string]HandlerEntry{
			"go.job":   {Runtime: "go", Topic: "shared"},
			"ruby.job": {Runtime: "ruby", Topic: "shared"},
		},
	}
	if err := m.ValidateTopicRuntimeExclusivity("default.jobs"); err == nil {
		t.Fatal("expected error for shared topic")
	}

	m = Manifest{
		Handlers: map[string]HandlerEntry{
			"go.job":   {Runtime: "go", Topic: "go.jobs"},
			"ruby.job": {Runtime: "ruby", Topic: "ruby.jobs"},
		},
	}
	if err := m.ValidateTopicRuntimeExclusivity("default.jobs"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFairReadyForRuntime(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.FairnessTimeReady = "legacy"
	cfg.FairnessTimeReadyGo = "ready.go"
	cfg.FairnessTimeReadyRuby = "ready.ruby"

	if got := cfg.FairReadyForRuntime("time", RuntimeGo); got != "ready.go" {
		t.Fatalf("go ready = %q", got)
	}
	if got := cfg.FairReadyForRuntime("time", RuntimeRuby); got != "ready.ruby" {
		t.Fatalf("ruby ready = %q", got)
	}
}

func TestRuntimeFor(t *testing.T) {
	m := Manifest{Handlers: map[string]HandlerEntry{
		"test.job": {Runtime: "Ruby"},
	}}
	if got := m.RuntimeFor("test.job"); got != RuntimeRuby {
		t.Fatalf("runtime = %q", got)
	}
}

func TestValidateGoHandlersRegistered(t *testing.T) {
	SetHandlerLookup(func(s string) bool { return s == "registered.go" })
	defer SetHandlerLookup(func(string) bool { return true })

	m := Manifest{
		Handlers: map[string]HandlerEntry{
			"registered.go": {Runtime: "go"},
			"ruby.job":      {Runtime: "ruby"},
		},
	}
	if err := m.ValidateGoHandlersRegistered(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	m.Handlers["missing.go"] = HandlerEntry{Runtime: "go"}
	if err := m.ValidateGoHandlersRegistered(); err == nil {
		t.Fatal("expected missing handler error")
	}
}

func TestValidateRoutingWithoutRegistration(t *testing.T) {
	SetHandlerLookup(func(string) bool { return false })
	defer SetHandlerLookup(func(string) bool { return true })

	m := Manifest{
		Handlers: map[string]HandlerEntry{
			"go.job":   {Runtime: "go", Topic: "go.jobs"},
			"ruby.job": {Runtime: "ruby", Topic: "ruby.jobs"},
		},
	}
	if err := m.ValidateRouting("default.jobs"); err != nil {
		t.Fatalf("routing should not require registration: %v", err)
	}
}
