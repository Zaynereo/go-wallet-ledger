package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
)

// Reconcile checks ledger invariants on a single REPEATABLE READ snapshot so
// the queries cannot observe a transaction that commits between statements.
func (s *Store) Reconcile(ctx context.Context) (domain.Reconciliation, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return domain.Reconciliation{}, fmt.Errorf("begin reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	report := domain.Reconciliation{
		Currencies:             []domain.CurrencyTotal{},
		BalanceMismatches:      []domain.BalanceMismatch{},
		UnbalancedTransactions: []string{},
		IncompleteTransactions: []string{},
		CheckedAt:              nowUTC(),
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT currency,
		       SUM(CASE WHEN direction = 'credit' THEN amount_minor ELSE -amount_minor END) AS net_minor,
		       COUNT(*) AS entry_count
		FROM ledger_entries
		GROUP BY currency
		ORDER BY currency`)
	if err != nil {
		return domain.Reconciliation{}, fmt.Errorf("sum entries: %w", err)
	}
	for rows.Next() {
		var row domain.CurrencyTotal
		if err := rows.Scan(&row.Currency, &row.NetMinor, &row.EntryCount); err != nil {
			_ = rows.Close()
			return domain.Reconciliation{}, err
		}
		report.Currencies = append(report.Currencies, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return domain.Reconciliation{}, err
	}
	if err := rows.Close(); err != nil {
		return domain.Reconciliation{}, err
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT a.id, a.balance_minor,
		       COALESCE(SUM(CASE WHEN e.direction = 'credit' THEN e.amount_minor ELSE -e.amount_minor END), 0) AS derived
		FROM accounts a
		LEFT JOIN ledger_entries e ON e.account_id = a.id
		GROUP BY a.id, a.balance_minor
		HAVING a.balance_minor <> COALESCE(SUM(CASE WHEN e.direction = 'credit' THEN e.amount_minor ELSE -e.amount_minor END), 0)
		ORDER BY a.id`)
	if err != nil {
		return domain.Reconciliation{}, fmt.Errorf("compare balances: %w", err)
	}
	for rows.Next() {
		var row domain.BalanceMismatch
		if err := rows.Scan(&row.AccountID, &row.CachedMinor, &row.DerivedMinor); err != nil {
			_ = rows.Close()
			return domain.Reconciliation{}, err
		}
		report.BalanceMismatches = append(report.BalanceMismatches, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return domain.Reconciliation{}, err
	}
	if err := rows.Close(); err != nil {
		return domain.Reconciliation{}, err
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT transaction_id
		FROM ledger_entries
		GROUP BY transaction_id
		HAVING SUM(CASE WHEN direction = 'credit' THEN amount_minor ELSE -amount_minor END) <> 0
		ORDER BY transaction_id`)
	if err != nil {
		return domain.Reconciliation{}, fmt.Errorf("unbalanced transactions: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return domain.Reconciliation{}, err
		}
		report.UnbalancedTransactions = append(report.UnbalancedTransactions, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return domain.Reconciliation{}, err
	}
	if err := rows.Close(); err != nil {
		return domain.Reconciliation{}, err
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT t.id
		FROM transactions t
		LEFT JOIN ledger_entries e ON e.transaction_id = t.id
		GROUP BY t.id
		HAVING COUNT(e.id) <> 2
		ORDER BY t.id`)
	if err != nil {
		return domain.Reconciliation{}, fmt.Errorf("incomplete transactions: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return domain.Reconciliation{}, err
		}
		report.IncompleteTransactions = append(report.IncompleteTransactions, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return domain.Reconciliation{}, err
	}
	if err := rows.Close(); err != nil {
		return domain.Reconciliation{}, err
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM idempotency_keys WHERE transaction_id IS NULL`).Scan(&report.OrphanIdempotencyKeys); err != nil {
		return domain.Reconciliation{}, fmt.Errorf("orphan idempotency keys: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return domain.Reconciliation{}, fmt.Errorf("commit reconciliation: %w", err)
	}
	report.OK = reconciliationOK(report)
	return report, nil
}

func reconciliationOK(report domain.Reconciliation) bool {
	if len(report.BalanceMismatches) != 0 || len(report.UnbalancedTransactions) != 0 || len(report.IncompleteTransactions) != 0 {
		return false
	}
	if report.OrphanIdempotencyKeys != 0 {
		return false
	}
	for _, currency := range report.Currencies {
		if currency.NetMinor != 0 {
			return false
		}
	}
	return true
}
