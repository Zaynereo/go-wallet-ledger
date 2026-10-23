package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"unicode"
)

var (
	accountIDPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:~-]{1,128}$`)
)

// NormalizeCurrency requires a 3-letter uppercase code. Values are not checked
// against the ISO catalogue; the ledger treats the code as an opaque unit.
func NormalizeCurrency(currency string) (string, error) {
	currency = strings.TrimSpace(currency)
	if len(currency) != 3 || currency != strings.ToUpper(currency) || !allLetters(currency) {
		return "", validation("currency must be a 3-letter uppercase code", map[string]string{
			"currency": "must match ^[A-Z]{3}$",
		})
	}
	return currency, nil
}

func allLetters(s string) bool {
	for _, r := range s {
		if !unicode.IsLetter(r) || r > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// NormalizeAccountID canonicalizes a UUID so lock order and hashes do not
// depend on hex case.
func NormalizeAccountID(id string) (string, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if !accountIDPattern.MatchString(id) {
		return "", validation("account id must be a UUID", map[string]string{
			"account_id": "must be a UUID",
		})
	}
	return id, nil
}

// ValidateAmount rejects zero, negative, and absurdly large postings.
func ValidateAmount(amount int64) error {
	if amount <= 0 {
		return validation("amount_minor must be a positive integer", map[string]string{
			"amount_minor": "must be > 0",
		})
	}
	if amount > MaxAmountMinor {
		return validation("amount_minor exceeds the maximum posting size", map[string]string{
			"amount_minor": "too large",
		})
	}
	return nil
}

// ValidateIdempotencyKey restricts keys to a short printable token so they
// can be stored in a case-sensitive index and logged safely.
func ValidateIdempotencyKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return IdempotencyKeyRequired()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return validation("Idempotency-Key has an invalid format", map[string]string{
			"Idempotency-Key": "use 1-128 characters from [A-Za-z0-9._:~-]",
		})
	}
	return nil
}

// Apply adds delta to balance and rejects signed overflow.
func Apply(balance, delta int64) (int64, error) {
	if delta > 0 && balance > math.MaxInt64-delta {
		return 0, Overflow()
	}
	if delta < 0 && balance < math.MinInt64-delta {
		return 0, Overflow()
	}
	return balance + delta, nil
}

// RequestHash is the identity of a money-movement payload. The idempotency
// key itself is intentionally excluded: it is the lookup key, not part of
// the body being protected. Encoding is encoding/json of a fixed struct, so
// field order is stable.
func RequestHash(operation string, payload any) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(operation + "\n" + string(body)))
	return hex.EncodeToString(sum[:]), nil
}

// TransferHash is the canonical transfer payload.
type TransferHash struct {
	FromAccountID string `json:"from_account_id"`
	ToAccountID   string `json:"to_account_id"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
}

// AccountAmountHash is the canonical deposit and withdrawal payload.
type AccountAmountHash struct {
	AccountID   string `json:"account_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}
