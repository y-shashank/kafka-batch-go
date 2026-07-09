package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubChecker struct {
	ok     bool
	detail string
}

func (s stubChecker) Healthy(context.Context) (bool, string) {
	return s.ok, s.detail
}

func TestServerProbeHealthy(t *testing.T) {
	s := &Server{Process: "test", Checker: stubChecker{ok: true, detail: "fine"}}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleProbe(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["ok"] != true {
		t.Fatalf("body %+v", body)
	}
}

func TestServerProbeUnhealthy(t *testing.T) {
	s := &Server{Process: "test", Checker: stubChecker{ok: false, detail: "stale"}}
	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	w := httptest.NewRecorder()
	s.handleProbe(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", w.Code)
	}
}

func TestServerProbeNoChecker(t *testing.T) {
	s := &Server{Process: "test"}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleProbe(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
}
