package id

import (
	"regexp"
	"testing"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var uuidV5Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIsUUIDv7(t *testing.T) {
	t.Parallel()
	seen := map[string]struct{}{}
	for i := 0; i < 20; i++ {
		got, err := New()
		if err != nil {
			t.Fatal(err)
		}
		if !uuidPattern.MatchString(got) {
			t.Fatalf("not a uuid v7: %s", got)
		}
		seen[got] = struct{}{}
	}
	if len(seen) != 20 {
		t.Fatalf("expected 20 unique ids, got %d", len(seen))
	}
}

func TestSystemAccountIDStable(t *testing.T) {
	t.Parallel()
	a := SystemAccountID("USD")
	b := SystemAccountID("USD")
	c := SystemAccountID("EUR")
	if a != b {
		t.Fatalf("not stable: %s %s", a, b)
	}
	if a == c {
		t.Fatal("currency did not change system id")
	}
	if !uuidV5Pattern.MatchString(a) {
		t.Fatalf("not a uuid v5: %s", a)
	}
	// Frozen so a namespace change is deliberate. These are the clearing accounts.
	if a != "cdbed40e-ca6e-5226-8528-3f4c2a7842f9" {
		t.Fatalf("USD system account id changed: %s", a)
	}
	if c != "a2bf6d2c-939a-5e65-b6c9-2ad521812d60" {
		t.Fatalf("EUR system account id changed: %s", c)
	}
}
