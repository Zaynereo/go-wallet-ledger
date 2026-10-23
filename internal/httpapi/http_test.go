package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	in := domain.EntryCursor{
		CreatedAt: time.Date(2026, 10, 23, 1, 2, 3, 456000000, time.UTC),
		ID:        "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
	}
	token := EncodeCursor(in)
	if strings.Contains(token, "+") || strings.Contains(token, "/") {
		t.Fatalf("cursor is not url-safe: %s", token)
	}
	out, err := DecodeCursor(token)
	if err != nil {
		t.Fatal(err)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) || out.ID != in.ID {
		t.Fatalf("got %+v", out)
	}
	if _, err := DecodeCursor("@@@"); err == nil {
		t.Fatal("expected invalid cursor")
	}
}

func TestDecodeRejectsUnknownFieldAndFloat(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/v1/transfers", strings.NewReader(`{"from_account_id":"x","extra":1}`))
	var body transferRequest
	err := decodeJSON(req, &body)
	var de *domain.Error
	if err == nil || !errors.As(err, &de) || de.Code != "validation_error" {
		t.Fatalf("unknown field: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/transfers", strings.NewReader(`{"amount_minor":1.5}`))
	err = decodeJSON(req, &body)
	if err == nil || !errors.As(err, &de) {
		t.Fatalf("float: %v", err)
	}
}

func TestRequestIDPreserved(t *testing.T) {
	t.Parallel()
	h := New(nil, nil, nil)
	var seen string
	handler := h.requestIDs(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	req.Header.Set("X-Request-Id", "trace-1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if seen != "trace-1" || rr.Header().Get("X-Request-Id") != "trace-1" {
		t.Fatalf("seen %q header %q", seen, rr.Header().Get("X-Request-Id"))
	}
}

func TestNotFoundJSON(t *testing.T) {
	t.Parallel()
	h := New(nil, nil, nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), `"not_found"`) {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
}
