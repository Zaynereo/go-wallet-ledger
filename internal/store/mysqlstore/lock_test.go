package mysqlstore

import (
	"reflect"
	"testing"
)

func TestSortedUniqueLockOrder(t *testing.T) {
	t.Parallel()
	got := sortedUnique([]string{
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		"",
	})
	want := []string{
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
	// Opposite call order still locks the lower id first.
	again := sortedUnique([]string{want[1], want[0]})
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("order not stable: %#v", again)
	}
}
