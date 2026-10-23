package httpapi

import (
	"encoding/base64"
	"strings"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
)

// EncodeCursor packs a page position into an opaque URL token.
func EncodeCursor(c domain.EntryCursor) string {
	raw := c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor reverses EncodeCursor.
func DecodeCursor(token string) (domain.EntryCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return domain.EntryCursor{}, domain.InvalidRequest("cursor is not valid")
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok || ts == "" || id == "" {
		return domain.EntryCursor{}, domain.InvalidRequest("cursor is not valid")
	}
	created, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return domain.EntryCursor{}, domain.InvalidRequest("cursor is not valid")
	}
	id, err = domain.NormalizeAccountID(id)
	if err != nil {
		return domain.EntryCursor{}, domain.InvalidRequest("cursor is not valid")
	}
	return domain.EntryCursor{CreatedAt: created.UTC(), ID: id}, nil
}
