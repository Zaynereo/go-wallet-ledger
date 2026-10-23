// Package seed creates the demo wallets used in the README.
package seed

import (
	"context"
	"fmt"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/ledger"
)

// Account is one demo wallet and its opening deposit.
type Account struct {
	Name           string
	ID             string
	Currency       string
	DepositMinor   int64
	IdempotencyKey string
}

// DemoAccounts are stable ids so curl examples can target them.
func DemoAccounts() []Account {
	return []Account{
		{Name: "alice", ID: "11111111-1111-4111-8111-111111111111", Currency: "USD", DepositMinor: 100_000, IdempotencyKey: "seed:v1:alice"},
		{Name: "bob", ID: "22222222-2222-4222-8222-222222222222", Currency: "USD", DepositMinor: 25_000, IdempotencyKey: "seed:v1:bob"},
		{Name: "carol", ID: "33333333-3333-4333-8333-333333333333", Currency: "EUR", DepositMinor: 50_000, IdempotencyKey: "seed:v1:carol"},
	}
}

// Run creates the demo accounts and posts their opening deposits.
// It is safe to run twice: account creation is idempotent on id, and each
// deposit uses a fixed idempotency key so money is not credited again.
func Run(ctx context.Context, svc *ledger.Service) ([]domain.Account, error) {
	out := make([]domain.Account, 0, 3)
	for _, demo := range DemoAccounts() {
		if _, err := svc.CreateAccountWithID(ctx, demo.ID, demo.Currency); err != nil {
			return nil, fmt.Errorf("create %s: %w", demo.Name, err)
		}
		if _, _, err := svc.Deposit(ctx, demo.IdempotencyKey, demo.ID, demo.DepositMinor, demo.Currency); err != nil {
			return nil, fmt.Errorf("deposit %s: %w", demo.Name, err)
		}
		acct, err := svc.GetAccount(ctx, demo.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, acct)
	}
	return out, nil
}
