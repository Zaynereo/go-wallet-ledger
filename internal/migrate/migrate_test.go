package migrate

import (
	"strings"
	"testing"
)

func TestSplitSQL(t *testing.T) {
	t.Parallel()
	script := `
-- header
CREATE TABLE t (note VARCHAR(20));
INSERT INTO t (note) VALUES ('a;b');
`
	got := splitSQL(script)
	if len(got) != 2 {
		t.Fatalf("got %d statements: %#v", len(got), got)
	}
	if strings.Contains(got[0], "header") {
		t.Fatalf("comment leaked: %s", got[0])
	}
	if !strings.Contains(got[1], "'a;b'") {
		t.Fatalf("string semicolon was split: %#v", got)
	}
}

func TestEmbeddedMigrationParses(t *testing.T) {
	t.Parallel()
	body, err := files.ReadFile("migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	stmts := splitSQL(string(body))
	if len(stmts) < 5 {
		t.Fatalf("expected the core tables, got %d statements", len(stmts))
	}
	joined := strings.ToLower(strings.Join(stmts, "\n"))
	for _, table := range []string{"accounts", "transactions", "ledger_entries", "idempotency_keys", "outbox"} {
		if !strings.Contains(joined, "create table if not exists "+table) {
			t.Fatalf("missing table %s", table)
		}
	}
}
