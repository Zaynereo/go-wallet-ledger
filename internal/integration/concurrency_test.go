package integration

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/id"
)

func TestConcurrentTransfersConserveMoney(t *testing.T) {
	f := setup(t)
	alice := mustCreate(t, f, "USD")
	bob := mustCreate(t, f, "USD")
	const n = 40
	const amt = int64(10)
	if _, _, err := f.svc.Deposit(f.ctx, "fund", alice.ID, n*amt, "USD"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, errs[i] = f.svc.Transfer(f.ctx, fmt.Sprintf("parallel-%d", i), alice.ID, bob.ID, amt, "USD")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}
	mustBalance(t, f, alice.ID, 0)
	mustBalance(t, f, bob.ID, n*amt)
	mustBalance(t, f, id.SystemAccountID("USD"), -n*amt)
	if countRows(t, "SELECT COUNT(*) FROM transactions") != n+1 {
		t.Fatalf("transaction count %d", countRows(t, "SELECT COUNT(*) FROM transactions"))
	}
	mustReconcile(t, f)
}

func TestConcurrentSameKeyRetries(t *testing.T) {
	f := setup(t)
	alice := mustCreate(t, f, "USD")
	bob := mustCreate(t, f, "USD")
	if _, _, err := f.svc.Deposit(f.ctx, "fund", alice.ID, 500, "USD"); err != nil {
		t.Fatal(err)
	}
	const workers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	txns := make([]domain.Transaction, workers)
	replayed := make([]bool, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			txns[i], replayed[i], errs[i] = f.svc.Transfer(f.ctx, "same-key", alice.ID, bob.ID, 25, "USD")
		}(i)
	}
	close(start)
	wg.Wait()

	var idSeen string
	replays := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if idSeen == "" {
			idSeen = txns[i].ID
		} else if txns[i].ID != idSeen {
			t.Fatalf("workers observed different transactions %s and %s", idSeen, txns[i].ID)
		}
		if replayed[i] {
			replays++
		}
	}
	if replays == 0 {
		t.Fatal("expected at least one replayed response")
	}
	mustBalance(t, f, alice.ID, 475)
	mustBalance(t, f, bob.ID, 25)
	if countRows(t, "SELECT COUNT(*) FROM transactions WHERE idempotency_key = ?", "same-key") != 1 {
		t.Fatal("same key posted more than once")
	}
	mustReconcile(t, f)
}

func TestConcurrentSameKeyDifferentPayload(t *testing.T) {
	f := setup(t)
	alice := mustCreate(t, f, "USD")
	bob := mustCreate(t, f, "USD")
	if _, _, err := f.svc.Deposit(f.ctx, "fund", alice.ID, 500, "USD"); err != nil {
		t.Fatal(err)
	}
	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	type result struct {
		txn domain.Transaction
		err error
	}
	results := make([]result, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			amount := int64(10)
			if i%2 == 1 {
				amount = 11
			}
			txn, _, err := f.svc.Transfer(f.ctx, "contested-key", alice.ID, bob.ID, amount, "USD")
			results[i] = result{txn: txn, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	ids := map[string]struct{}{}
	var winner int64
	conflicts := 0
	for _, res := range results {
		if res.err != nil {
			mustCode(t, res.err, "idempotency_conflict")
			conflicts++
			continue
		}
		ids[res.txn.ID] = struct{}{}
		winner = res.txn.AmountMinor
	}
	if conflicts == 0 || len(ids) != 1 || (winner != 10 && winner != 11) {
		t.Fatalf("conflicts=%d ids=%d winner=%d", conflicts, len(ids), winner)
	}
	mustBalance(t, f, alice.ID, 500-winner)
	mustBalance(t, f, bob.ID, winner)
	mustReconcile(t, f)
}

func TestConcurrentOverdraftAllowsOneWinner(t *testing.T) {
	f := setup(t)
	alice := mustCreate(t, f, "USD")
	if _, _, err := f.svc.Deposit(f.ctx, "fund", alice.ID, 100, "USD"); err != nil {
		t.Fatal(err)
	}
	const workers = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	wins := 0
	var mu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := f.svc.Withdraw(f.ctx, fmt.Sprintf("race-%d", i), alice.ID, 100, "USD")
			if err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
				return
			}
			mustCode(t, err, "insufficient_funds")
		}(i)
	}
	close(start)
	wg.Wait()
	if wins != 1 {
		t.Fatalf("wins = %d, want 1", wins)
	}
	mustBalance(t, f, alice.ID, 0)
	mustBalance(t, f, id.SystemAccountID("USD"), 0)
	mustReconcile(t, f)
}

func TestOpposingTransfers(t *testing.T) {
	f := setup(t)
	alice := mustCreate(t, f, "USD")
	bob := mustCreate(t, f, "USD")
	if _, _, err := f.svc.Deposit(f.ctx, "fund-a", alice.ID, 1_000, "USD"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.svc.Deposit(f.ctx, "fund-b", bob.ID, 1_000, "USD"); err != nil {
		t.Fatal(err)
	}
	const n = 30
	const amt = int64(10)
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from, to := alice.ID, bob.ID
			if i%2 == 1 {
				from, to = bob.ID, alice.ID
			}
			_, _, errs[i] = f.svc.Transfer(f.ctx, fmt.Sprintf("opp-%d", i), from, to, amt, "USD")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}
	mustBalance(t, f, alice.ID, 1_000)
	mustBalance(t, f, bob.ID, 1_000)
	mustReconcile(t, f)
}
