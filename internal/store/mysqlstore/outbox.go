package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/outbox"
)

// ClaimPending locks a batch of unpublished events, stamps a lease, and commits
// before the publisher is called so a slow broker does not hold row locks.
func (s *Store) ClaimPending(ctx context.Context, limit int, lockUntil time.Time) ([]outbox.Event, error) {
	if limit < 1 {
		limit = 1
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowUTC()
	rows, err := tx.QueryContext(ctx, `
		SELECT id, event_id, event_type, aggregate_id, payload, created_at
		FROM outbox
		WHERE published_at IS NULL
		  AND (locked_until IS NULL OR locked_until <= ?)
		ORDER BY id
		LIMIT ?
		FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	events := make([]outbox.Event, 0)
	for rows.Next() {
		var event outbox.Event
		if err := rows.Scan(&event.ID, &event.EventID, &event.EventType, &event.AggregateID, &event.Payload, &event.CreatedAt); err != nil {
			return nil, err
		}
		event.CreatedAt = event.CreatedAt.UTC()
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	until := lockUntil.UTC().Truncate(time.Microsecond)
	for _, event := range events {
		if _, err := tx.ExecContext(ctx, `
			UPDATE outbox SET attempts = attempts + 1, locked_until = ? WHERE id = ?`, until, event.ID); err != nil {
			return nil, fmt.Errorf("lease outbox %d: %w", event.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

// MarkPublished records a successful publish. A crash before this update
// redelivers the event after the lease expires (at-least-once).
func (s *Store) MarkPublished(ctx context.Context, ids []int64, publishedAt time.Time) error {
	return s.updateOutboxIDs(ctx, `
		UPDATE outbox SET published_at = ?, locked_until = NULL
		WHERE published_at IS NULL AND id IN (`, publishedAt.UTC().Truncate(time.Microsecond), ids)
}

// Release drops the lease after a failed publish so the next poll can retry.
func (s *Store) Release(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	query := `UPDATE outbox SET locked_until = NULL WHERE published_at IS NULL AND id IN (` + inPlaceholders(len(ids)) + `)`
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("release outbox: %w", err)
	}
	return nil
}

func (s *Store) updateOutboxIDs(ctx context.Context, prefix string, publishedAt time.Time, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, publishedAt)
	for _, id := range ids {
		args = append(args, id)
	}
	query := prefix + inPlaceholders(len(ids)) + `)`
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}
