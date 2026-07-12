package config

import "testing"

func TestExpandEnv(t *testing.T) {
	t.Setenv("KB_MYSQL_URL", "mysql2://u:p@mysql:3306/kb")
	t.Setenv("EMPTY_VAR", "")

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"simple ref", "${KB_MYSQL_URL}", "mysql2://u:p@mysql:3306/kb"},
		{"embedded ref", "store_mysql_dsn: ${KB_MYSQL_URL}", "store_mysql_dsn: mysql2://u:p@mysql:3306/kb"},
		{"unset no default", "${KB_MISSING}", ""},
		{"unset with default", "${KB_MISSING:-localhost:3306}", "localhost:3306"},
		{"set beats default", "${KB_MYSQL_URL:-fallback}", "mysql2://u:p@mysql:3306/kb"},
		{"set-but-empty is honored", "${EMPTY_VAR:-fallback}", ""},
		{"bare dollar preserved", "user:pa$$w0rd@tcp(h)/db", "user:pa$$w0rd@tcp(h)/db"},
		{"literal no braces", "$KB_MYSQL_URL", "$KB_MYSQL_URL"},
		{"two refs", "${KB_MYSQL_URL}|${KB_MISSING:-x}", "mysql2://u:p@mysql:3306/kb|x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ExpandEnv(c.in); got != c.want {
				t.Fatalf("ExpandEnv(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
