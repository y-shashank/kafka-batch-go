package health

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestListenAndServeShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	s := &Server{
		Addr:    addr,
		Process: "listen-test",
		Checker: stubChecker{ok: true, detail: "ok"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.ListenAndServe(ctx)
	}()

	var lastErr error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status=%d body=%s", resp.StatusCode, body)
			}
			var m map[string]interface{}
			if err := json.Unmarshal(body, &m); err != nil {
				t.Fatal(err)
			}
			if m["process"] != "listen-test" || m["ok"] != true {
				t.Fatalf("body=%v", m)
			}
			lastErr = nil
			break
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("health never became ready: %v", lastErr)
	}

	resp, err := http.Get("http://" + addr + "/live")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("live status=%d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ListenAndServe did not exit after cancel")
	}
}

func TestListenAndServeEmptyAddrDefaultsThenBindError(t *testing.T) {
	// Hold :8080 so ListenAndServe fails after applying the empty-Addr default.
	ln, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Skipf("cannot bind :8080: %v", err)
	}
	defer ln.Close()

	s := &Server{Process: "default-addr"}
	err = s.ListenAndServe(context.Background())
	if err == nil {
		t.Fatal("expected listen error")
	}
	if s.Addr != ":8080" {
		t.Fatalf("addr=%q want :8080", s.Addr)
	}
}
