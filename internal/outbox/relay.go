package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Store is the slice of persistence the relay needs.
type Store interface {
	ClaimPending(ctx context.Context, limit int, lockUntil time.Time) ([]Event, error)
	MarkPublished(ctx context.Context, ids []int64, publishedAt time.Time) error
	Release(ctx context.Context, ids []int64) error
}

// Relay polls the outbox and publishes claimed batches.
type Relay struct {
	store     Store
	publisher Publisher
	batch     int
	interval  time.Duration
	lease     time.Duration
	logger    *slog.Logger
	now       func() time.Time
}

func NewRelay(store Store, publisher Publisher, batch int, interval, lease time.Duration, logger *slog.Logger) *Relay {
	if batch < 1 {
		batch = 50
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{
		store:     store,
		publisher: publisher,
		batch:     batch,
		interval:  interval,
		lease:     lease,
		logger:    logger,
		now:       time.Now,
	}
}

// Run publishes until ctx is cancelled. A failed batch is logged and retried
// on the next tick; the lease is released so the rows are eligible again.
func (r *Relay) Run(ctx context.Context) {
	r.publishAvailable(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.publishAvailable(ctx)
		}
	}
}

// Drain publishes until the outbox is empty or ctx ends. Used on shutdown
// after the HTTP server has stopped accepting new posts.
func (r *Relay) Drain(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.publishBatch(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func (r *Relay) publishAvailable(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		batchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		n, err := r.publishBatch(batchCtx)
		cancel()
		if err != nil {
			r.logger.Error("outbox publish failed", "err", err)
			return
		}
		if n == 0 {
			return
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	events, err := r.store.ClaimPending(ctx, r.batch, r.now().Add(r.lease))
	if err != nil || len(events) == 0 {
		return 0, err
	}
	ids := make([]int64, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	if err := r.publisher.Publish(ctx, events); err != nil {
		if releaseErr := r.store.Release(ctx, ids); releaseErr != nil {
			return 0, errors.Join(err, releaseErr)
		}
		return 0, err
	}
	if err := r.store.MarkPublished(ctx, ids, r.now()); err != nil {
		return len(events), err
	}
	return len(events), nil
}
