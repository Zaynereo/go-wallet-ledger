// Package migrate applies versioned SQL files embedded in the binary.
// MySQL implicitly commits DDL, so each statement runs on its own and the
// version row is recorded only after every statement succeeds. Files use
// CREATE TABLE IF NOT EXISTS so a crash between DDL and the version insert
// is safe to retry.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed migrations/*.sql
var files embed.FS

// Up applies any migration that is not yet recorded in schema_migrations.
func Up(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) NOT NULL,
			applied_at DATETIME(6) NOT NULL,
			PRIMARY KEY (version)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(files, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, name).Scan(&n); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if n > 0 {
			continue
		}
		body, err := files.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		for i, stmt := range splitSQL(string(body)) {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("migration %s statement %d: %w", name, i+1, err)
			}
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
			name, time.Now().UTC().Truncate(time.Microsecond),
		); err != nil {
			if isDuplicate(err) {
				continue
			}
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}
	return nil
}

func isDuplicate(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// splitSQL splits a script on semicolons that are not inside single-quoted
// strings. Full-line -- comments are stripped.
func splitSQL(sqlText string) []string {
	var stmts []string
	var b strings.Builder
	inString := false
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		if c == '\'' {
			if inString && i+1 < len(sqlText) && sqlText[i+1] == '\'' {
				b.WriteByte('\'')
				b.WriteByte('\'')
				i++
				continue
			}
			inString = !inString
			b.WriteByte(c)
			continue
		}
		if c == ';' && !inString {
			if stmt := stripComments(b.String()); stmt != "" {
				stmts = append(stmts, stmt)
			}
			b.Reset()
			continue
		}
		b.WriteByte(c)
	}
	if stmt := stripComments(b.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}

func stripComments(s string) string {
	lines := strings.Split(s, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}
