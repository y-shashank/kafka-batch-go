package config

import (
	"os"
	"regexp"
)

// envRef matches ${VAR} and ${VAR:-default}. Only the braced form is expanded —
// a bare $VAR is left untouched so a literal '$' in a DSN password survives.
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// ExpandEnv replaces ${VAR} / ${VAR:-default} references with environment values.
// An unset VAR with no default expands to the empty string. Used so config can
// reference deployment env vars — e.g. store_mysql_dsn: ${KB_MYSQL_URL} — and one
// variable can feed the client, control, and execution roles alike.
func ExpandEnv(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(match string) string {
		m := envRef.FindStringSubmatch(match)
		name, def := m[1], m[2]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return def
	})
}
