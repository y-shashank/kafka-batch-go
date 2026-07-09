package health

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Server exposes HTTP liveness/readiness endpoints for Kubernetes probes.
type Server struct {
	Addr    string
	Process string
	Checker Checker // optional — when set, /health and /live reflect consumer activity
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.Addr == "" {
		s.Addr = ":8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleProbe)
	mux.HandleFunc("/live", s.handleProbe)
	srv := &http.Server{Addr: s.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("kbatch %s health listening on %s", s.Process, s.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("health server: %w", err)
	}
	return nil
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ok, detail := true, ""
	if s.Checker != nil {
		ok, detail = s.Checker.Healthy(r.Context())
	}
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":      ok,
		"process": s.Process,
		"detail":  detail,
	})
}
