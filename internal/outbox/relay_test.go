package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeStore struct {
	pending   []Event
	published []int64
	released  []int64
}

func (f *fakeStore) ClaimPending(context.Context, int, time.Time) ([]Event, error) {
	if len(f.pending) == 0 {
		return nil, nil
	}
	out := f.pending
	f.pending = nil
	return out, nil
}

func (f *fakeStore) MarkPublished(_ context.Context, ids []int64, _ time.Time) error {
	f.published = append(f.published, ids...)
	return nil
}

func (f *fakeStore) Release(_ context.Context, ids []int64) error {
	f.released = append(f.released, ids...)
	return nil
}

type fakePublisher struct {
	err    error
	events []Event
}

func (f *fakePublisher) Publish(_ context.Context, events []Event) error {
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, events...)
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func testRelay(store *fakeStore, pub *fakePublisher) *Relay {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	relay := NewRelay(store, pub, 10, time.Second, time.Second, logger)
	relay.now = func() time.Time { return time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC) }
	return relay
}

func TestDrainMarksPublished(t *testing.T) {
	t.Parallel()
	store := &fakeStore{pending: []Event{{ID: 7, EventID: "e1"}, {ID: 8, EventID: "e2"}}}
	pub := &fakePublisher{}
	if err := testRelay(store, pub).Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.events) != 2 || len(store.published) != 2 || len(store.released) != 0 {
		t.Fatalf("published=%v released=%v events=%d", store.published, store.released, len(pub.events))
	}
}

func TestDrainReleasesOnPublishError(t *testing.T) {
	t.Parallel()
	store := &fakeStore{pending: []Event{{ID: 3, EventID: "e"}}}
	pub := &fakePublisher{err: errors.New("broker down")}
	err := testRelay(store, pub).Drain(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if len(store.published) != 0 || len(store.released) != 1 || store.released[0] != 3 {
		t.Fatalf("published=%v released=%v", store.published, store.released)
	}
}
