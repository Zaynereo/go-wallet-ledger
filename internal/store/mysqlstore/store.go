// Package mysqlstore persists the ledger in MySQL 8 (InnoDB).
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
)

// Store is a MySQL-backed ledger.
type Store struct {
	db  *sql.DB
	opt Options
}

// Options tunes locking and logging. Zero values get safe defaults.
type Options struct {
	LockWaitTimeout time.Duration
	Logger          *slog.Logger
	MaxOpenConns    int
	MaxIdleConns    int
}

// Posting is one idempotent money movement. Account ids and the hash are
// already canonical.
type Posting struct {
	Type           string
	IdempotencyKey string
	RequestHash    string
	Currency       string
	AmountMinor    int64
	FromAccountID  string
	ToAccountID    string
}

// Open validates the DSN, configures the pool, and pings the database.
func Open(dsn string, opt Options) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	maxOpen := opt.MaxOpenConns
	if maxOpen < 1 {
		maxOpen = 25
	}
	maxIdle := opt.MaxIdleConns
	if maxIdle < 0 {
		maxIdle = 0
	}
	if maxIdle == 0 {
		maxIdle = 10
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(30 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return New(db, opt), nil
}

// New wraps an existing pool. Used by tests that migrate a shared database.
func New(db *sql.DB, opt Options) *Store {
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.LockWaitTimeout < time.Second {
		opt.LockWaitTimeout = 10 * time.Second
	}
	return &Store{db: db, opt: opt}
}

// Close closes the pool.
func (s *Store) Close() error { return s.db.Close() }

// Ping checks that the database accepts a connection.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// DB exposes the pool for the migrator and the seed command's owner.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) logger() *slog.Logger { return s.opt.Logger }

func (s *Store) lockWaitSeconds() int {
	secs := int(s.opt.LockWaitTimeout / time.Second)
	if secs < 1 {
		return 10
	}
	if secs > 120 {
		return 120
	}
	return secs
}

func nowUTC() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func isDuplicate(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

func isRetryable(err error) bool {
	var contention *contentionError
	if errors.As(err, &contention) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1213, 1205: // deadlock, lock wait timeout
			return true
		}
	}
	return false
}

// contentionError is an internal signal that the posting should be retried.
type contentionError struct{ cause error }

func (e *contentionError) Error() string {
	if e.cause == nil {
		return "ledger contention"
	}
	return "ledger contention: " + e.cause.Error()
}

func (e *contentionError) Unwrap() error { return e.cause }

func (s *Store) withRetry(ctx context.Context, fn func() error) error {
	const maxAttempts = 3
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		if err == nil || !isRetryable(err) {
			return err
		}
		s.logger().Warn("retrying ledger transaction", "attempt", attempt, "err", err)
		delay := time.Duration(attempt*attempt) * 5 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func mapPostError(err error) error {
	if err == nil {
		return nil
	}
	if isRetryable(err) {
		return domain.TemporarilyUnavailable()
	}
	return err
}

func rowsAffectedOne(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return &contentionError{cause: fmt.Errorf("expected 1 row affected, got %d", n)}
	}
	return nil
}

func inPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}
