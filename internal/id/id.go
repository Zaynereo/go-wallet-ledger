// Package id generates UUIDv7 identifiers and deterministic UUIDv5 ids for
// per-currency system accounts. The implementation is stdlib-only.
package id

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// systemNamespace is the UUIDv5 namespace for system clearing accounts.
// Name format: "system:<CURRENCY>".
var systemNamespace = mustParse("6f1b0c3e-9a4d-4e2b-8c71-5a0d9e8b7c61")

// New returns a UUIDv7 (time-ordered, RFC 9562 layout).
func New() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("read random uuid bytes: %w", err)
	}
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return format(b[:]), nil
}

// SystemAccountID is the stable clearing account id for a currency.
func SystemAccountID(currency string) string {
	return uuidV5(systemNamespace, "system:"+currency)
}

func uuidV5(namespace [16]byte, name string) string {
	h := sha1.New()
	h.Write(namespace[:])
	h.Write([]byte(name))
	sum := h.Sum(nil)[:16]
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	return format(sum)
}

func format(b []byte) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

func mustParse(s string) [16]byte {
	raw := strings.ReplaceAll(s, "-", "")
	b, err := hex.DecodeString(raw)
	if err != nil || len(b) != 16 {
		panic("invalid namespace uuid")
	}
	var out [16]byte
	copy(out[:], b)
	return out
}
