package domain

import "time"

const (
	KindUser   = "user"
	KindSystem = "system"

	DirectionDebit  = "debit"
	DirectionCredit = "credit"

	TypeTransfer   = "transfer"
	TypeDeposit    = "deposit"
	TypeWithdrawal = "withdrawal"

	EventTransactionPosted = "ledger.transaction.posted"

	// MaxAmountMinor caps a single posting so a bug cannot walk a balance
	// to the edge of int64 in one call. 10^15 minor units is far above any
	// realistic wallet transfer (10^13 major units at 2 decimal places).
	MaxAmountMinor int64 = 1_000_000_000_000_000
)

// Account is a user liability wallet or a per-currency system clearing account.
// BalanceMinor is a cache of credits minus debits, updated in the same
// database transaction as the ledger entries.
type Account struct {
	ID           string    `json:"id"`
	Kind         string    `json:"kind"`
	Currency     string    `json:"currency"`
	BalanceMinor int64     `json:"balance_minor"`
	Version      int64     `json:"version"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Entry is one side of a double-entry posting. AmountMinor is always positive.
type Entry struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Currency      string    `json:"currency"`
	Direction     string    `json:"direction"`
	AmountMinor   int64     `json:"amount_minor"`
	CreatedAt     time.Time `json:"created_at"`
}

// Transaction is an immutable, balanced movement of value.
type Transaction struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	Currency       string    `json:"currency"`
	AmountMinor    int64     `json:"amount_minor"`
	FromAccountID  string    `json:"from_account_id"`
	ToAccountID    string    `json:"to_account_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Entries        []Entry   `json:"entries"`
	CreatedAt      time.Time `json:"created_at"`
}

// EntryCursor is the position of the last entry returned by a list call.
type EntryCursor struct {
	CreatedAt time.Time
	ID        string
}

// EntryQuery pages entries newest-first.
type EntryQuery struct {
	Limit  int
	Cursor *EntryCursor
}

// EntryPage is one page of ledger entries.
type EntryPage struct {
	Entries    []Entry
	NextCursor *EntryCursor
}

// CurrencyTotal is the signed sum of all entries in one currency.
// Credits are positive and debits are negative, so a balanced ledger is zero.
type CurrencyTotal struct {
	Currency   string `json:"currency"`
	NetMinor   int64  `json:"net_minor"`
	EntryCount int64  `json:"entry_count"`
}

// BalanceMismatch is an account whose cached balance differs from its entries.
type BalanceMismatch struct {
	AccountID    string `json:"account_id"`
	CachedMinor  int64  `json:"cached_minor"`
	DerivedMinor int64  `json:"derived_minor"`
}

// Reconciliation is a point-in-time invariant report.
type Reconciliation struct {
	OK                     bool              `json:"ok"`
	Currencies             []CurrencyTotal   `json:"currencies"`
	BalanceMismatches      []BalanceMismatch `json:"balance_mismatches"`
	UnbalancedTransactions []string          `json:"unbalanced_transactions"`
	IncompleteTransactions []string          `json:"incomplete_transactions"`
	OrphanIdempotencyKeys  int64             `json:"orphan_idempotency_keys"`
	CheckedAt              time.Time         `json:"checked_at"`
}
