// Package ledger is the application API over the MySQL ledger.
package ledger

import (
	"context"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/store/mysqlstore"
)

// Service validates commands and posts them through the store.
type Service struct {
	store *mysqlstore.Store
}

func New(store *mysqlstore.Store) *Service {
	return &Service{store: store}
}

func (s *Service) CreateAccount(ctx context.Context, currency string) (domain.Account, error) {
	currency, err := domain.NormalizeCurrency(currency)
	if err != nil {
		return domain.Account{}, err
	}
	return s.store.CreateAccount(ctx, "", currency)
}

// CreateAccountWithID is used by the seed command so demo accounts have
// stable ids. The HTTP API always generates ids.
func (s *Service) CreateAccountWithID(ctx context.Context, accountID, currency string) (domain.Account, error) {
	currency, err := domain.NormalizeCurrency(currency)
	if err != nil {
		return domain.Account{}, err
	}
	accountID, err = domain.NormalizeAccountID(accountID)
	if err != nil {
		return domain.Account{}, err
	}
	return s.store.CreateAccount(ctx, accountID, currency)
}

func (s *Service) GetAccount(ctx context.Context, accountID string) (domain.Account, error) {
	accountID, err := domain.NormalizeAccountID(accountID)
	if err != nil {
		return domain.Account{}, err
	}
	return s.store.GetAccount(ctx, accountID)
}

func (s *Service) Transfer(ctx context.Context, key, from, to string, amount int64, currency string) (domain.Transaction, bool, error) {
	from, to, currency, err := normalizePair(key, from, to, amount, currency)
	if err != nil {
		return domain.Transaction{}, false, err
	}
	if from == to {
		return domain.Transaction{}, false, domain.SameAccount()
	}
	hash, err := domain.RequestHash(domain.TypeTransfer, domain.TransferHash{
		FromAccountID: from,
		ToAccountID:   to,
		AmountMinor:   amount,
		Currency:      currency,
	})
	if err != nil {
		return domain.Transaction{}, false, err
	}
	return s.store.Post(ctx, mysqlstore.Posting{
		Type:           domain.TypeTransfer,
		IdempotencyKey: key,
		RequestHash:    hash,
		Currency:       currency,
		AmountMinor:    amount,
		FromAccountID:  from,
		ToAccountID:    to,
	})
}

func (s *Service) Deposit(ctx context.Context, key, accountID string, amount int64, currency string) (domain.Transaction, bool, error) {
	accountID, currency, err := normalizeOne(key, accountID, amount, currency)
	if err != nil {
		return domain.Transaction{}, false, err
	}
	hash, err := domain.RequestHash(domain.TypeDeposit, domain.AccountAmountHash{
		AccountID:   accountID,
		AmountMinor: amount,
		Currency:    currency,
	})
	if err != nil {
		return domain.Transaction{}, false, err
	}
	return s.store.Post(ctx, mysqlstore.Posting{
		Type:           domain.TypeDeposit,
		IdempotencyKey: key,
		RequestHash:    hash,
		Currency:       currency,
		AmountMinor:    amount,
		ToAccountID:    accountID,
	})
}

func (s *Service) Withdraw(ctx context.Context, key, accountID string, amount int64, currency string) (domain.Transaction, bool, error) {
	accountID, currency, err := normalizeOne(key, accountID, amount, currency)
	if err != nil {
		return domain.Transaction{}, false, err
	}
	hash, err := domain.RequestHash(domain.TypeWithdrawal, domain.AccountAmountHash{
		AccountID:   accountID,
		AmountMinor: amount,
		Currency:    currency,
	})
	if err != nil {
		return domain.Transaction{}, false, err
	}
	return s.store.Post(ctx, mysqlstore.Posting{
		Type:           domain.TypeWithdrawal,
		IdempotencyKey: key,
		RequestHash:    hash,
		Currency:       currency,
		AmountMinor:    amount,
		FromAccountID:  accountID,
	})
}

func (s *Service) GetTransaction(ctx context.Context, transactionID string) (domain.Transaction, error) {
	transactionID, err := domain.NormalizeAccountID(transactionID)
	if err != nil {
		return domain.Transaction{}, domain.InvalidRequest("transaction id must be a UUID")
	}
	return s.store.GetTransaction(ctx, transactionID)
}

func (s *Service) ListEntries(ctx context.Context, accountID string, q domain.EntryQuery) (domain.EntryPage, error) {
	accountID, err := domain.NormalizeAccountID(accountID)
	if err != nil {
		return domain.EntryPage{}, err
	}
	if q.Limit < 1 || q.Limit > 200 {
		return domain.EntryPage{}, domain.InvalidRequest("limit must be between 1 and 200")
	}
	return s.store.ListEntries(ctx, accountID, q)
}

func (s *Service) Reconcile(ctx context.Context) (domain.Reconciliation, error) {
	return s.store.Reconcile(ctx)
}

func normalizeOne(key, accountID string, amount int64, currency string) (string, string, error) {
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		return "", "", err
	}
	accountID, err := domain.NormalizeAccountID(accountID)
	if err != nil {
		return "", "", err
	}
	if err := domain.ValidateAmount(amount); err != nil {
		return "", "", err
	}
	currency, err = domain.NormalizeCurrency(currency)
	if err != nil {
		return "", "", err
	}
	return accountID, currency, nil
}

func normalizePair(key, from, to string, amount int64, currency string) (string, string, string, error) {
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		return "", "", "", err
	}
	from, err := domain.NormalizeAccountID(from)
	if err != nil {
		return "", "", "", err
	}
	to, err = domain.NormalizeAccountID(to)
	if err != nil {
		return "", "", "", err
	}
	if err := domain.ValidateAmount(amount); err != nil {
		return "", "", "", err
	}
	currency, err = domain.NormalizeCurrency(currency)
	if err != nil {
		return "", "", "", err
	}
	return from, to, currency, nil
}
