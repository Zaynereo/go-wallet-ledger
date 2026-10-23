package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
)

func requireJSON(r *http.Request) error {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return domain.UnsupportedMediaType()
	}
	return nil
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return jsonError(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.InvalidRequest("request body must contain a single JSON object")
	}
	return nil
}

func jsonError(err error) error {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return domain.InvalidRequest("request body is too large")
	}
	var syn *json.SyntaxError
	if errors.As(err, &syn) {
		return domain.InvalidRequest("request body is not valid JSON")
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		field := typeErr.Field
		if field == "" {
			field = "body"
		}
		return domain.InvalidRequest("field " + field + " has the wrong JSON type")
	}
	msg := err.Error()
	if strings.Contains(msg, "unknown field") {
		return domain.InvalidRequest(msg)
	}
	if errors.Is(err, io.EOF) || strings.Contains(msg, "EOF") {
		return domain.InvalidRequest("request body is empty or truncated")
	}
	return domain.InvalidRequest("request body is not valid JSON")
}

func idempotencyKey(r *http.Request) (string, error) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		return "", err
	}
	return key, nil
}
