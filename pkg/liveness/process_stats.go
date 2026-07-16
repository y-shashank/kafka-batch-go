package liveness

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Process stats piggybacked on consumer heartbeats (Ruby ProcessStats parity).
// Sampled at most once per StatsInterval (default 15s).

const defaultStatsInterval = 15 * time.Second

type processSampler struct {
	mu       sync.Mutex
	interval time.Duration
	cache    map[string]interface{}
	sampled  time.Time

	lastWall time.Time
	lastCPU  time.Duration // process CPU time
}

func newProcessSampler(interval time.Duration) *processSampler {
	if interval <= 0 {
		interval = defaultStatsInterval
	}
	return &processSampler{interval: interval}
}

func (s *processSampler) sample() map[string]interface{} {
	if s == nil {
		return nil
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache != nil && now.Sub(s.sampled) < s.interval {
		return s.cache
	}
	out := map[string]interface{}{}
	if rss := readRSSBytes(); rss > 0 {
		out["rss_bytes"] = rss
	}
	if cpu, ok := s.readCPUPercent(now); ok {
		// One decimal place, matching Ruby ProcessStats.
		out["cpu_pct"] = float64(int(cpu*10+0.5)) / 10
	}
	s.cache = out
	s.sampled = now
	return out
}

func (s *processSampler) readCPUPercent(now time.Time) (float64, bool) {
	cpu, ok := readProcessCPUTime()
	if !ok {
		return 0, false
	}
	prevWall := s.lastWall
	prevCPU := s.lastCPU
	s.lastWall = now
	s.lastCPU = cpu
	if prevWall.IsZero() || now.Sub(prevWall) <= 0 {
		return 0, false
	}
	wall := now.Sub(prevWall).Seconds()
	delta := (cpu - prevCPU).Seconds()
	if wall <= 0 || delta < 0 {
		return 0, false
	}
	return (delta / wall) * 100.0, true
}

func readRSSBytes() int64 {
	switch runtime.GOOS {
	case "linux":
		return readLinuxRSS()
	case "darwin":
		return readDarwinRSS()
	default:
		return 0
	}
}

func readLinuxRSS() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

func readDarwinRSS() int64 {
	// Throttled by sampler interval — occasional `ps` is acceptable.
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil || kb <= 0 {
		return 0
	}
	return kb * 1024
}

func readProcessCPUTime() (time.Duration, bool) {
	switch runtime.GOOS {
	case "linux":
		return readLinuxCPUTime()
	case "darwin":
		return readDarwinCPUTime()
	default:
		return 0, false
	}
}

func readLinuxCPUTime() (time.Duration, bool) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	// comm can contain spaces/parens — find last ')' then split the rest.
	idx := bytes.LastIndexByte(raw, ')')
	if idx < 0 || idx+2 >= len(raw) {
		return 0, false
	}
	fields := strings.Fields(string(raw[idx+2:]))
	// After ')': state(1) ... utime(12) stime(13) — 0-based in this slice: 11, 12
	if len(fields) < 13 {
		return 0, false
	}
	utime, err1 := strconv.ParseInt(fields[11], 10, 64)
	stime, err2 := strconv.ParseInt(fields[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	// USER_HZ is typically 100 on Linux.
	ticks := utime + stime
	return time.Duration(ticks) * (time.Second / 100), true
}

func readDarwinCPUTime() (time.Duration, bool) {
	// ps %cpu is instantaneous; use cumulative time "time" field (mm:ss.ss) if available.
	out, err := exec.Command("ps", "-o", "time=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0, false
	}
	d, err := parsePSTime(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, false
	}
	return d, true
}

// parsePSTime parses ps TIME formats like "0:01.23", "1:02:03", or "01:02:03.45".
func parsePSTime(s string) (time.Duration, error) {
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("bad time %q", s)
	}
	var h, m int64
	var sec float64
	var err error
	switch len(parts) {
	case 2:
		m, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
		sec, err = strconv.ParseFloat(parts[1], 64)
	case 3:
		h, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
		m, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, err
		}
		sec, err = strconv.ParseFloat(parts[2], 64)
	}
	if err != nil {
		return 0, err
	}
	total := float64(h*3600+m*60) + sec
	return time.Duration(total * float64(time.Second)), nil
}
