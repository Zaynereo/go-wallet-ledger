package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/id"
)

func (s *Store) CreateAccount(ctx context.Context, accountID, currency string) (domain.Account, error) {
	if accountID == "" {
		var err error
		accountID, err = id.New()
		if err != nil {
			return domain.Account{}, err
		}
	}
	now := nowUTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO accounts (id, kind, currency, balance_minor, version, created_at, updated_at)
		VALUES (?, ?, ?, 0, 0, ?, ?)`,
		accountID, domain.KindUser, currency, now, now,
	)
	if isDuplicate(err) {
		existing, gerr := s.GetAccount(ctx, accountID)
		if gerr != nil {
			return domain.Account{}, gerr
		}
		if existing.Currency != currency || existing.Kind != domain.KindUser {
			return domain.Account{}, domain.AccountExists()
		}
		return existing, nil
	}
	if err != nil {
		return domain.Account{}, fmt.Errorf("insert account: %w", err)
	}
	return s.GetAccount(ctx, accountID)
}

func (s *Store) GetAccount(ctx context.Context, accountID string) (domain.Account, error) {
	acct, err := scanAccount(s.db.QueryRowContext(ctx, accountSelect+` WHERE id = ?`, accountID))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Account{}, domain.NotFound("account not found")
	}
	if err != nil {
		return domain.Account{}, fmt.Errorf("get account: %w", err)
	}
	return acct, nil
}

func (s *Store) GetTransaction(ctx context.Context, transactionID string) (domain.Transaction, error) {
	txn, err := loadTransaction(ctx, s.db, transactionID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Transaction{}, domain.NotFound("transaction not found")
	}
	if err != nil {
		return domain.Transaction{}, err
	}
	return txn, nil
}

func (s *Store) ListEntries(ctx context.Context, accountID string, q domain.EntryQuery) (domain.EntryPage, error) {
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return domain.EntryPage{}, err
	}
	limit := q.Limit
	if limit < 1 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	args := []any{accountID}
	cursorSQL := ""
	if q.Cursor != nil {
		cursorSQL = " AND (created_at < ? OR (created_at = ? AND id < ?))"
		ts := q.Cursor.CreatedAt.UTC().Truncate(time.Microsecond)
		args = append(args, ts, ts, q.Cursor.ID)
	}
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, transaction_id, account_id, currency, direction, amount_minor, created_at
		FROM ledger_entries
		WHERE account_id = ?`+cursorSQL+`
		ORDER BY created_at DESC, id DESC
		LIMIT ?`, args...)
	if err != nil {
		return domain.EntryPage{}, fmt.Errorf("list entries: %w", err)
	}
	defer func() { _ = rows.Close() }()

	entries := make([]domain.Entry, 0)
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return domain.EntryPage{}, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return domain.EntryPage{}, err
	}
	page := domain.EntryPage{Entries: entries}
	if len(entries) > limit {
		page.Entries = entries[:limit]
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = &domain.EntryCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if page.Entries == nil {
		page.Entries = []domain.Entry{}
	}
	return page, nil
}

const accountSelect = `
	SELECT id, kind, currency, balance_minor, version, created_at, updated_at
	FROM accounts`

func scanAccount(row interface{ Scan(dest ...any) error }) (domain.Account, error) {
	var acct domain.Account
	err := row.Scan(&acct.ID, &acct.Kind, &acct.Currency, &acct.BalanceMinor, &acct.Version, &acct.CreatedAt, &acct.UpdatedAt)
	if err != nil {
		return domain.Account{}, err
	}
	acct.CreatedAt = acct.CreatedAt.UTC()
	acct.UpdatedAt = acct.UpdatedAt.UTC()
	return acct, nil
}

type scanner interface{ Scan(dest ...any) error }

func scanEntry(row scanner) (domain.Entry, error) {
	var entry domain.Entry
	if err := row.Scan(&entry.ID, &entry.TransactionID, &entry.AccountID, &entry.Currency, &entry.Direction, &entry.AmountMinor, &entry.CreatedAt); err != nil {
		return domain.Entry{}, err
	}
	entry.CreatedAt = entry.CreatedAt.UTC()
	return entry, nil
}

func loadTransaction(ctx context.Context, q querier, transactionID string) (domain.Transaction, error) {
	var txn domain.Transaction
	err := q.QueryRowContext(ctx, `
		SELECT id, type, currency, amount_minor, from_account_id, to_account_id, idempotency_key, created_at
		FROM transactions WHERE id = ?`, transactionID).Scan(
		&txn.ID, &txn.Type, &txn.Currency, &txn.AmountMinor, &txn.FromAccountID, &txn.ToAccountID, &txn.IdempotencyKey, &txn.CreatedAt,
	)
	if err != nil {
		return domain.Transaction{}, err
	}
	txn.CreatedAt = txn.CreatedAt.UTC()
	rows, err := q.QueryContext(ctx, `
		SELECT id, transaction_id, account_id, currency, direction, amount_minor, created_at
		FROM ledger_entries
		WHERE transaction_id = ?
		ORDER BY entry_index ASC`, transactionID)
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("load entries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	txn.Entries = []domain.Entry{}
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return domain.Transaction{}, err
		}
		txn.Entries = append(txn.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return domain.Transaction{}, err
	}
	return txn, nil
}
