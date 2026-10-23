package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/id"
)

// Post commits one balanced movement exactly once for an idempotency key.
//
// Locking: the idempotency row is claimed first, then account rows are locked
// with SELECT ... FOR UPDATE one id at a time in sortedUnique order. Same-key
// retries block on the idempotency unique index until the winner commits, then
// read the original transaction. Different keys that touch the same accounts
// acquire those row locks in the same order, so they wait instead of deadlocking.
// InnoDB can still report a deadlock (1213) or lock-wait timeout (1205) from
// secondary-index inserts; those errors retry the whole posting.
//
// The idempotency insert, ledger rows, balance updates, and outbox event commit
// or roll back together. A retry after a rollback does not observe a half-claimed key.
func (s *Store) Post(ctx context.Context, p Posting) (domain.Transaction, bool, error) {
	if p.Type == domain.TypeDeposit || p.Type == domain.TypeWithdrawal {
		// Create the clearing account in its own committed statement so the
		// money transaction can lock it in id order. Doing the insert inside
		// the money transaction would take a lock before the ordered FOR UPDATE
		// and can deadlock with a transfer that already holds a user row.
		if err := s.ensureSystemAccount(ctx, p.Currency); err != nil {
			return domain.Transaction{}, false, err
		}
	}

	var (
		out      domain.Transaction
		replayed bool
	)
	err := s.withRetry(ctx, func() error {
		txn, replay, err := s.postOnce(ctx, p)
		if err != nil {
			return err
		}
		out = txn
		replayed = replay
		return nil
	})
	if err != nil {
		return domain.Transaction{}, false, mapPostError(err)
	}
	return out, replayed, nil
}

func (s *Store) ensureSystemAccount(ctx context.Context, currency string) error {
	accountID := id.SystemAccountID(currency)
	now := nowUTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO accounts (id, kind, currency, balance_minor, version, created_at, updated_at)
		VALUES (?, ?, ?, 0, 0, ?, ?)
		ON DUPLICATE KEY UPDATE id = id`,
		accountID, domain.KindSystem, currency, now, now,
	)
	if err != nil {
		return fmt.Errorf("ensure system account: %w", err)
	}
	acct, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return err
	}
	if acct.Kind != domain.KindSystem || acct.Currency != currency {
		return fmt.Errorf("system account %s has kind %s currency %s", accountID, acct.Kind, acct.Currency)
	}
	return nil
}

func (s *Store) postOnce(ctx context.Context, p Posting) (domain.Transaction, bool, error) {
	// Pin a connection so the session lock-wait timeout applies to this transaction.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return domain.Transaction{}, false, fmt.Errorf("checkout connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET innodb_lock_wait_timeout = %d", s.lockWaitSeconds())); err != nil {
		return domain.Transaction{}, false, fmt.Errorf("set lock wait: %w", err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.Transaction{}, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	replay, txn, done, err := s.claimIdempotency(ctx, tx, p)
	if err != nil || done {
		return txn, replay, err
	}

	fromID, toID := p.FromAccountID, p.ToAccountID
	switch p.Type {
	case domain.TypeDeposit:
		fromID = id.SystemAccountID(p.Currency)
	case domain.TypeWithdrawal:
		toID = id.SystemAccountID(p.Currency)
	}
	if fromID == "" || toID == "" {
		return domain.Transaction{}, false, domain.InvalidRequest("from_account_id and to_account_id are required")
	}
	if fromID == toID {
		return domain.Transaction{}, false, domain.SameAccount()
	}

	accounts, err := lockAccounts(ctx, tx, []string{fromID, toID})
	if err != nil {
		return domain.Transaction{}, false, err
	}
	from := accounts[fromID]
	to := accounts[toID]
	if from.Currency != p.Currency || to.Currency != p.Currency || from.Currency != to.Currency {
		return domain.Transaction{}, false, domain.CurrencyMismatch()
	}
	if err := checkKinds(p.Type, from, to); err != nil {
		return domain.Transaction{}, false, err
	}

	newFrom, err := domain.Apply(from.BalanceMinor, -p.AmountMinor)
	if err != nil {
		return domain.Transaction{}, false, err
	}
	newTo, err := domain.Apply(to.BalanceMinor, p.AmountMinor)
	if err != nil {
		return domain.Transaction{}, false, err
	}
	if from.Kind == domain.KindUser && newFrom < 0 {
		return domain.Transaction{}, false, domain.InsufficientFunds()
	}
	if to.Kind == domain.KindUser && newTo < 0 {
		return domain.Transaction{}, false, domain.InsufficientFunds()
	}

	now := nowUTC()
	txnID, err := id.New()
	if err != nil {
		return domain.Transaction{}, false, err
	}
	debitID, err := id.New()
	if err != nil {
		return domain.Transaction{}, false, err
	}
	creditID, err := id.New()
	if err != nil {
		return domain.Transaction{}, false, err
	}
	eventID, err := id.New()
	if err != nil {
		return domain.Transaction{}, false, err
	}

	posted := domain.Transaction{
		ID:             txnID,
		Type:           p.Type,
		Currency:       p.Currency,
		AmountMinor:    p.AmountMinor,
		FromAccountID:  fromID,
		ToAccountID:    toID,
		IdempotencyKey: p.IdempotencyKey,
		CreatedAt:      now,
		Entries: []domain.Entry{
			{
				ID: debitID, TransactionID: txnID, AccountID: fromID, Currency: p.Currency,
				Direction: domain.DirectionDebit, AmountMinor: p.AmountMinor, CreatedAt: now,
			},
			{
				ID: creditID, TransactionID: txnID, AccountID: toID, Currency: p.Currency,
				Direction: domain.DirectionCredit, AmountMinor: p.AmountMinor, CreatedAt: now,
			},
		},
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transactions (
			id, type, currency, amount_minor, from_account_id, to_account_id, idempotency_key, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		posted.ID, posted.Type, posted.Currency, posted.AmountMinor, posted.FromAccountID, posted.ToAccountID, posted.IdempotencyKey, posted.CreatedAt,
	); err != nil {
		return domain.Transaction{}, false, fmt.Errorf("insert transaction: %w", err)
	}
	if err := insertEntry(ctx, tx, posted.Entries[0], 0); err != nil {
		return domain.Transaction{}, false, err
	}
	if err := insertEntry(ctx, tx, posted.Entries[1], 1); err != nil {
		return domain.Transaction{}, false, err
	}
	if err := updateBalance(ctx, tx, from.ID, newFrom, from.Version, now); err != nil {
		return domain.Transaction{}, false, err
	}
	if err := updateBalance(ctx, tx, to.ID, newTo, to.Version, now); err != nil {
		return domain.Transaction{}, false, err
	}

	payload, err := json.Marshal(struct {
		EventID     string             `json:"event_id"`
		EventType   string             `json:"event_type"`
		OccurredAt  time.Time          `json:"occurred_at"`
		Transaction domain.Transaction `json:"transaction"`
	}{
		EventID:     eventID,
		EventType:   domain.EventTransactionPosted,
		OccurredAt:  now,
		Transaction: posted,
	})
	if err != nil {
		return domain.Transaction{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox (event_id, event_type, aggregate_id, payload, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		eventID, domain.EventTransactionPosted, posted.ID, payload, now,
	); err != nil {
		return domain.Transaction{}, false, fmt.Errorf("insert outbox: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE idempotency_keys SET transaction_id = ? WHERE idempotency_key = ? AND transaction_id IS NULL`,
		posted.ID, p.IdempotencyKey,
	)
	if err != nil {
		return domain.Transaction{}, false, fmt.Errorf("finish idempotency: %w", err)
	}
	if err := rowsAffectedOne(res); err != nil {
		return domain.Transaction{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Transaction{}, false, fmt.Errorf("commit: %w", err)
	}
	return posted, false, nil
}

// claimIdempotency inserts the key or locks the existing row. done is true when
// the caller should return txn (a replay) or err without posting again.
func (s *Store) claimIdempotency(ctx context.Context, tx *sql.Tx, p Posting) (replay bool, txn domain.Transaction, done bool, err error) {
	now := nowUTC()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys (idempotency_key, request_hash, created_at)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE idempotency_key = idempotency_key`,
		p.IdempotencyKey, p.RequestHash, now,
	); err != nil {
		return false, domain.Transaction{}, true, fmt.Errorf("claim idempotency: %w", err)
	}

	var (
		hash  string
		txnID sql.NullString
	)
	if err = tx.QueryRowContext(ctx, `
		SELECT request_hash, transaction_id
		FROM idempotency_keys
		WHERE idempotency_key = ?
		FOR UPDATE`, p.IdempotencyKey).Scan(&hash, &txnID); err != nil {
		return false, domain.Transaction{}, true, fmt.Errorf("lock idempotency: %w", err)
	}
	if hash != p.RequestHash {
		return false, domain.Transaction{}, true, domain.IdempotencyConflict()
	}
	if !txnID.Valid {
		return false, domain.Transaction{}, false, nil
	}
	loaded, err := loadTransaction(ctx, tx, txnID.String)
	if err != nil {
		return false, domain.Transaction{}, true, fmt.Errorf("load replayed transaction: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, domain.Transaction{}, true, fmt.Errorf("commit replay: %w", err)
	}
	return true, loaded, true, nil
}

func lockAccounts(ctx context.Context, tx *sql.Tx, ids []string) (map[string]domain.Account, error) {
	out := make(map[string]domain.Account, len(ids))
	for _, accountID := range sortedUnique(ids) {
		acct, err := scanAccount(tx.QueryRowContext(ctx, accountSelect+` WHERE id = ? FOR UPDATE`, accountID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NotFound("account " + accountID + " not found")
		}
		if err != nil {
			return nil, fmt.Errorf("lock account %s: %w", accountID, err)
		}
		out[accountID] = acct
	}
	return out, nil
}

func checkKinds(movement string, from, to domain.Account) error {
	switch movement {
	case domain.TypeTransfer:
		if from.Kind != domain.KindUser || to.Kind != domain.KindUser {
			return domain.InvalidAccountKind("transfers are only allowed between user accounts; use deposit or withdrawal for clearing")
		}
	case domain.TypeDeposit:
		if from.Kind != domain.KindSystem || to.Kind != domain.KindUser {
			return domain.InvalidAccountKind("deposits credit a user account from the system clearing account")
		}
	case domain.TypeWithdrawal:
		if from.Kind != domain.KindUser || to.Kind != domain.KindSystem {
			return domain.InvalidAccountKind("withdrawals debit a user account into the system clearing account")
		}
	default:
		return domain.InvalidRequest("unsupported movement type")
	}
	return nil
}

func insertEntry(ctx context.Context, tx *sql.Tx, entry domain.Entry, index int) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (
			id, transaction_id, account_id, currency, direction, amount_minor, entry_index, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID, entry.TransactionID, entry.AccountID, entry.Currency, entry.Direction, entry.AmountMinor, index, entry.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert entry: %w", err)
	}
	return nil
}

func updateBalance(ctx context.Context, tx *sql.Tx, accountID string, balance, version int64, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE accounts
		SET balance_minor = ?, version = version + 1, updated_at = ?
		WHERE id = ? AND version = ?`,
		balance, now, accountID, version,
	)
	if err != nil {
		return fmt.Errorf("update balance %s: %w", accountID, err)
	}
	return rowsAffectedOne(res)
}
