package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/id"
	"github.com/Zaynereo/go-wallet-ledger/internal/outbox"
	"github.com/Zaynereo/go-wallet-ledger/internal/seed"
)

func TestDepositTransferWithdrawAndReplay(t *testing.T) {
	f := setup(t)
	alice := mustCreate(t, f, "USD")
	bob := mustCreate(t, f, "USD")
	eur := mustCreate(t, f, "EUR")

	dep, replayed, err := f.svc.Deposit(f.ctx, "dep-alice", alice.ID, 1000, "USD")
	if err != nil || replayed {
		t.Fatalf("deposit replayed=%v err=%v", replayed, err)
	}
	if len(dep.Entries) != 2 || dep.Entries[0].Direction != domain.DirectionDebit || dep.Entries[1].Direction != domain.DirectionCredit {
		t.Fatalf("entries: %+v", dep.Entries)
	}
	if dep.Entries[0].AmountMinor != dep.Entries[1].AmountMinor {
		t.Fatal("unbalanced entries")
	}
	mustBalance(t, f, alice.ID, 1000)
	mustBalance(t, f, id.SystemAccountID("USD"), -1000)

	again, replayed, err := f.svc.Deposit(f.ctx, "dep-alice", alice.ID, 1000, "USD")
	if err != nil || !replayed || again.ID != dep.ID {
		t.Fatalf("replay id %s/%s replayed=%v err=%v", again.ID, dep.ID, replayed, err)
	}
	if !again.CreatedAt.Equal(dep.CreatedAt) {
		t.Fatalf("replay timestamp %s != %s", again.CreatedAt, dep.CreatedAt)
	}
	mustBalance(t, f, alice.ID, 1000)

	_, _, err = f.svc.Deposit(f.ctx, "dep-alice", alice.ID, 1001, "USD")
	mustCode(t, err, "idempotency_conflict")
	mustBalance(t, f, alice.ID, 1000)

	xfer, _, err := f.svc.Transfer(f.ctx, "xfer-1", alice.ID, bob.ID, 400, "USD")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := f.svc.GetTransaction(f.ctx, xfer.ID)
	if err != nil || loaded.AmountMinor != 400 || len(loaded.Entries) != 2 {
		t.Fatalf("%+v %v", loaded, err)
	}
	mustBalance(t, f, alice.ID, 600)
	mustBalance(t, f, bob.ID, 400)

	if _, _, err := f.svc.Withdraw(f.ctx, "wd-1", alice.ID, 100, "USD"); err != nil {
		t.Fatal(err)
	}
	mustBalance(t, f, alice.ID, 500)
	mustBalance(t, f, id.SystemAccountID("USD"), -900)

	_, _, err = f.svc.Transfer(f.ctx, "bad-ccy", alice.ID, eur.ID, 1, "USD")
	mustCode(t, err, "currency_mismatch")
	_, _, err = f.svc.Deposit(f.ctx, "bad-ccy-dep", alice.ID, 1, "EUR")
	mustCode(t, err, "currency_mismatch")
	_, _, err = f.svc.Transfer(f.ctx, "same", alice.ID, alice.ID, 1, "USD")
	mustCode(t, err, "same_account")
	_, _, err = f.svc.Withdraw(f.ctx, "too-much", alice.ID, 10_000, "USD")
	mustCode(t, err, "insufficient_funds")
	mustBalance(t, f, alice.ID, 500)

	before := countRows(t, "SELECT COUNT(*) FROM outbox")
	_, _, err = f.svc.Withdraw(f.ctx, "still-too-much", alice.ID, 10_000, "USD")
	mustCode(t, err, "insufficient_funds")
	if countRows(t, "SELECT COUNT(*) FROM outbox") != before {
		t.Fatal("failed withdrawal wrote an outbox event")
	}
	if _, _, err := f.svc.Deposit(f.ctx, "top-up", alice.ID, 50, "USD"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.svc.Withdraw(f.ctx, "still-too-much", alice.ID, 10, "USD"); err != nil {
		t.Fatal("idempotency key was consumed by the earlier insufficient-funds attempt")
	}

	_, err = f.svc.GetAccount(f.ctx, "99999999-9999-4999-8999-999999999999")
	mustCode(t, err, "not_found")
	_, _, err = f.svc.Transfer(f.ctx, "missing", alice.ID, "99999999-9999-4999-8999-999999999999", 1, "USD")
	mustCode(t, err, "not_found")

	mustReconcile(t, f)
}

func TestEntryPagination(t *testing.T) {
	f := setup(t)
	acct := mustCreate(t, f, "USD")
	var ids []string
	for i := 0; i < 5; i++ {
		txn, _, err := f.svc.Deposit(f.ctx, "page-"+string(rune('a'+i)), acct.ID, 1, "USD")
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range txn.Entries {
			if entry.AccountID == acct.ID {
				ids = append(ids, entry.ID)
			}
		}
		time.Sleep(time.Millisecond)
	}
	seen := map[string]struct{}{}
	var cursor *domain.EntryCursor
	pages := 0
	var ordered []string
	for {
		page, err := f.svc.ListEntries(f.ctx, acct.ID, domain.EntryQuery{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if len(page.Entries) == 0 || len(page.Entries) > 2 {
			t.Fatalf("page size %d", len(page.Entries))
		}
		for _, entry := range page.Entries {
			if _, ok := seen[entry.ID]; ok {
				t.Fatalf("duplicate entry %s", entry.ID)
			}
			seen[entry.ID] = struct{}{}
			ordered = append(ordered, entry.ID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d entries, want 5", len(seen))
	}
	// Newest deposit is last created, so the first page starts at the last id.
	if ordered[0] != ids[len(ids)-1] {
		t.Fatalf("order %v, created %v", ordered, ids)
	}
	mustBalance(t, f, acct.ID, 5)
}

func TestOutboxDrain(t *testing.T) {
	f := setup(t)
	acct := mustCreate(t, f, "USD")
	for i := 0; i < 3; i++ {
		if _, _, err := f.svc.Deposit(f.ctx, "ob-"+string(rune('a'+i)), acct.ID, 10, "USD"); err != nil {
			t.Fatal(err)
		}
	}
	if countRows(t, "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL") != 3 {
		t.Fatal("expected 3 unpublished events")
	}
	pub := &capturePublisher{}
	relay := outbox.NewRelay(f.store, pub, 2, time.Second, time.Second, nil)
	if err := relay.Drain(f.ctx); err != nil {
		t.Fatal(err)
	}
	if len(pub.events) != 3 {
		t.Fatalf("published %d", len(pub.events))
	}
	if countRows(t, "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL") != 0 {
		t.Fatal("events left unpublished")
	}
	if err := relay.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.events) != 3 {
		t.Fatal("drain republished events")
	}
}

func TestSeedIsIdempotent(t *testing.T) {
	f := setup(t)
	first, err := seed.Run(f.ctx, f.svc)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || first[0].BalanceMinor != 100_000 || first[1].BalanceMinor != 25_000 || first[2].BalanceMinor != 50_000 {
		t.Fatalf("%+v", first)
	}
	txns := countRows(t, "SELECT COUNT(*) FROM transactions")
	second, err := seed.Run(f.ctx, f.svc)
	if err != nil {
		t.Fatal(err)
	}
	if countRows(t, "SELECT COUNT(*) FROM transactions") != txns {
		t.Fatal("seed credited money twice")
	}
	for i := range first {
		if second[i].BalanceMinor != first[i].BalanceMinor || second[i].ID != first[i].ID {
			t.Fatalf("seed changed %+v -> %+v", first[i], second[i])
		}
	}
	mustReconcile(t, f)
}

type capturePublisher struct {
	events []outbox.Event
}

func (c *capturePublisher) Publish(_ context.Context, events []outbox.Event) error {
	c.events = append(c.events, events...)
	return nil
}

func (c *capturePublisher) Close() error { return nil }
