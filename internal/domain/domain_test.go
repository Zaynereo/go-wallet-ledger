package domain

import (
	"errors"
	"math"
	"testing"
)

func TestNormalizeCurrency(t *testing.T) {
	t.Parallel()
	got, err := NormalizeCurrency("USD")
	if err != nil || got != "USD" {
		t.Fatalf("USD: got %q %v", got, err)
	}
	for _, bad := range []string{"", "us", "usd", "USDX", "US1", "U$D", " US"} {
		if _, err := NormalizeCurrency(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestNormalizeAccountID(t *testing.T) {
	t.Parallel()
	got, err := NormalizeAccountID("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE")
	if err != nil {
		t.Fatal(err)
	}
	if got != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Fatalf("got %s", got)
	}
	if _, err := NormalizeAccountID("not-a-uuid"); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateAmount(t *testing.T) {
	t.Parallel()
	if err := ValidateAmount(1); err != nil {
		t.Fatal(err)
	}
	if err := ValidateAmount(0); err == nil {
		t.Fatal("zero should fail")
	}
	if err := ValidateAmount(-5); err == nil {
		t.Fatal("negative should fail")
	}
	if err := ValidateAmount(MaxAmountMinor + 1); err == nil {
		t.Fatal("huge amount should fail")
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	t.Parallel()
	if err := ValidateIdempotencyKey("retry:1"); err != nil {
		t.Fatal(err)
	}
	err := ValidateIdempotencyKey("   ")
	var de *Error
	if !errors.As(err, &de) || de.Code != "idempotency_key_required" {
		t.Fatalf("got %v", err)
	}
	if err := ValidateIdempotencyKey("has space"); err == nil {
		t.Fatal("expected invalid key")
	}
	if err := ValidateIdempotencyKey(string(make([]byte, 129))); err == nil {
		t.Fatal("expected length error")
	}
}

func TestApply(t *testing.T) {
	t.Parallel()
	got, err := Apply(10, -4)
	if err != nil || got != 6 {
		t.Fatalf("got %d %v", got, err)
	}
	got, err = Apply(-3, -1)
	if err != nil || got != -4 {
		t.Fatalf("got %d %v", got, err)
	}
	if _, err := Apply(math.MaxInt64, 1); err == nil {
		t.Fatal("expected overflow")
	}
	if _, err := Apply(math.MinInt64, -1); err == nil {
		t.Fatal("expected underflow")
	}
	var de *Error
	_, err = Apply(math.MaxInt64, 1)
	if !errors.As(err, &de) || de.Code != "overflow" {
		t.Fatalf("got %v", err)
	}
}

func TestRequestHashStableAndSensitive(t *testing.T) {
	t.Parallel()
	body := TransferHash{
		FromAccountID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		ToAccountID:   "11111111-2222-3333-4444-555555555555",
		AmountMinor:   42,
		Currency:      "USD",
	}
	a, err := RequestHash(TypeTransfer, body)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RequestHash(TypeTransfer, body)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || len(a) != 64 {
		t.Fatalf("hash not stable: %s %s", a, b)
	}
	body.AmountMinor = 43
	c, err := RequestHash(TypeTransfer, body)
	if err != nil {
		t.Fatal(err)
	}
	if c == a {
		t.Fatal("amount change did not change hash")
	}
	deposit, err := RequestHash(TypeDeposit, AccountAmountHash{
		AccountID:   body.ToAccountID,
		AmountMinor: 42,
		Currency:    "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if deposit == a {
		t.Fatal("different operations collided")
	}
}
