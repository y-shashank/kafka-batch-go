// Package dsn normalizes MySQL connection strings so callers may supply either
// the native go-sql-driver DSN form
//
//	user:pass@tcp(host:port)/dbname?parseTime=true&loc=UTC
//
// or a Rails-style URL (as found in DATABASE_URL / database.yml)
//
//	mysql2://user:pass@host:port/dbname?parseTime=true&loc=UTC
//
// Both KAFKA_BATCH_SCHEDULE_MYSQL_DSN and KAFKA_BATCH_STORE_MYSQL_DSN accept
// either form; the URL form is converted to the driver DSN before sql.Open.
package dsn

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// urlSchemes are recognized as the Rails-style connection-URL form. Anything
// else is treated as an already-valid go-sql-driver DSN and passed through.
var urlSchemes = map[string]bool{"mysql": true, "mysql2": true}

// Normalize returns a go-sql-driver/mysql DSN. A native DSN is returned
// unchanged; a mysql:// or mysql2:// URL is converted to the driver form.
// Empty input returns "" (callers gate on that to mean "not configured").
func Normalize(conn string) (string, error) {
	conn = strings.TrimSpace(conn)
	if conn == "" {
		return "", nil
	}

	scheme, _, ok := strings.Cut(conn, "://")
	if !ok || !urlSchemes[strings.ToLower(scheme)] {
		// Native go-sql-driver DSN (the historical form) — pass through untouched.
		return conn, nil
	}

	u, err := url.Parse(conn)
	if err != nil {
		return "", fmt.Errorf("parse mysql connection url: %w", err)
	}

	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("mysql connection url %q: missing host", scheme+"://…")
	}
	port := u.Port()
	if port == "" {
		port = "3306"
	}

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(host, port)
	cfg.DBName = strings.TrimPrefix(u.Path, "/")
	if u.User != nil {
		cfg.User = u.User.Username()
		if pw, set := u.User.Password(); set {
			cfg.Passwd = pw
		}
	}
	// Carry query params (parseTime, loc, tls, charset, timeout, …) verbatim.
	// FormatDSN re-serializes them and the driver re-parses on sql.Open, so the
	// driver's special handling of parseTime/loc/tls still applies.
	if q := u.Query(); len(q) > 0 {
		cfg.Params = make(map[string]string, len(q))
		for k := range q {
			cfg.Params[k] = q.Get(k)
		}
	}

	return cfg.FormatDSN(), nil
}
