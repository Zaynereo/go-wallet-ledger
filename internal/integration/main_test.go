package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/ledger"
	"github.com/Zaynereo/go-wallet-ledger/internal/migrate"
	"github.com/Zaynereo/go-wallet-ledger/internal/store/mysqlstore"
)

var (
	testDB *sql.DB
	dbMu   sync.Mutex
)

func TestMain(m *testing.M) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		os.Exit(m.Run())
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = db.PingContext(ctx)
		if pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		fmt.Fprintln(os.Stderr, pingErr)
		os.Exit(1)
	}
	if err := migrate.Up(ctx, db); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := migrate.Up(ctx, db); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testDB = db
	code := m.Run()
	_ = db.Close()
	os.Exit(code)
}

type fixture struct {
	svc   *ledger.Service
	store *mysqlstore.Store
	ctx   context.Context
}

func setup(t *testing.T) *fixture {
	t.Helper()
	if testDB == nil {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatal("REQUIRE_INTEGRATION=1 but MYSQL_DSN is not set")
		}
		t.Skip("MYSQL_DSN not set")
	}
	dbMu.Lock()
	t.Cleanup(func() { dbMu.Unlock() })
	truncate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := mysqlstore.New(testDB, mysqlstore.Options{
		LockWaitTimeout: 5 * time.Second,
		Logger:          logger,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return &fixture{svc: ledger.New(store), store: store, ctx: ctx}
}

func truncate(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	conn, err := testDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1")
	}()
	for _, table := range []string{"outbox", "idempotency_keys", "ledger_entries", "transactions", "accounts"} {
		if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

func mustCode(t *testing.T, err error, code string) {
	t.Helper()
	var de *domain.Error
	if !errors.As(err, &de) || de.Code != code {
		t.Fatalf("got %v, want code %s", err, code)
	}
}

func mustCreate(t *testing.T, f *fixture, currency string) domain.Account {
	t.Helper()
	acct, err := f.svc.CreateAccount(f.ctx, currency)
	if err != nil {
		t.Fatal(err)
	}
	return acct
}

func mustBalance(t *testing.T, f *fixture, id string, want int64) {
	t.Helper()
	acct, err := f.svc.GetAccount(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if acct.BalanceMinor != want {
		t.Fatalf("account %s balance %d, want %d", id, acct.BalanceMinor, want)
	}
}

func mustReconcile(t *testing.T, f *fixture) {
	t.Helper()
	report, err := f.svc.Reconcile(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK {
		t.Fatalf("invariants failed: %+v", report)
	}
	var sum int64
	if err := testDB.QueryRow("SELECT COALESCE(SUM(balance_minor), 0) FROM accounts").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum != 0 {
		t.Fatalf("sum of cached balances = %d, want 0", sum)
	}
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testDB.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
