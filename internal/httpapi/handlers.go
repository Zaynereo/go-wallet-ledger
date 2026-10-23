package httpapi

import (
	"net/http"
	"strconv"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
)

type createAccountRequest struct {
	Currency string `json:"currency"`
}

type transferRequest struct {
	FromAccountID string `json:"from_account_id"`
	ToAccountID   string `json:"to_account_id"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
}

type accountAmountRequest struct {
	AccountID   string `json:"account_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type entryListResponse struct {
	Entries    []domain.Entry `json:"entries"`
	NextCursor *string        `json:"next_cursor"`
}

func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	if err := requireJSON(r); err != nil {
		h.writeError(w, r, err)
		return
	}
	var req createAccountRequest
	if err := decodeJSON(r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	acct, err := h.svc.CreateAccount(r.Context(), req.Currency)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/accounts/"+acct.ID)
	h.writeJSON(w, http.StatusCreated, acct)
}

func (h *Handler) getAccount(w http.ResponseWriter, r *http.Request) {
	acct, err := h.svc.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, acct)
}

func (h *Handler) listEntries(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			h.writeError(w, r, domain.InvalidRequest("limit must be an integer from 1 to 200"))
			return
		}
		limit = n
	}
	q := domain.EntryQuery{Limit: limit}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		cursor, err := DecodeCursor(raw)
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		q.Cursor = &cursor
	}
	page, err := h.svc.ListEntries(r.Context(), r.PathValue("id"), q)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	resp := entryListResponse{Entries: page.Entries}
	if resp.Entries == nil {
		resp.Entries = []domain.Entry{}
	}
	if page.NextCursor != nil {
		token := EncodeCursor(*page.NextCursor)
		resp.NextCursor = &token
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) transfer(w http.ResponseWriter, r *http.Request) {
	key, req, ok := decodeTransfer(w, r, h)
	if !ok {
		return
	}
	txn, replayed, err := h.svc.Transfer(r.Context(), key, req.FromAccountID, req.ToAccountID, req.AmountMinor, req.Currency)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writePosted(w, txn, replayed)
}

func (h *Handler) deposit(w http.ResponseWriter, r *http.Request) {
	key, req, ok := decodeAccountAmount(w, r, h)
	if !ok {
		return
	}
	txn, replayed, err := h.svc.Deposit(r.Context(), key, req.AccountID, req.AmountMinor, req.Currency)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writePosted(w, txn, replayed)
}

func (h *Handler) withdrawal(w http.ResponseWriter, r *http.Request) {
	key, req, ok := decodeAccountAmount(w, r, h)
	if !ok {
		return
	}
	txn, replayed, err := h.svc.Withdraw(r.Context(), key, req.AccountID, req.AmountMinor, req.Currency)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writePosted(w, txn, replayed)
}

func (h *Handler) getTransaction(w http.ResponseWriter, r *http.Request) {
	txn, err := h.svc.GetTransaction(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, txn)
}

func (h *Handler) reconcile(w http.ResponseWriter, r *http.Request) {
	report, err := h.svc.Reconcile(r.Context())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.writeJSON(w, http.StatusOK, report)
}

func (h *Handler) writePosted(w http.ResponseWriter, txn domain.Transaction, replayed bool) {
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	w.Header().Set("Cache-Control", "no-store")
	h.writeJSON(w, http.StatusCreated, txn)
}

func decodeTransfer(w http.ResponseWriter, r *http.Request, h *Handler) (string, transferRequest, bool) {
	if err := requireJSON(r); err != nil {
		h.writeError(w, r, err)
		return "", transferRequest{}, false
	}
	key, err := idempotencyKey(r)
	if err != nil {
		h.writeError(w, r, err)
		return "", transferRequest{}, false
	}
	var req transferRequest
	if err := decodeJSON(r, &req); err != nil {
		h.writeError(w, r, err)
		return "", transferRequest{}, false
	}
	return key, req, true
}

func decodeAccountAmount(w http.ResponseWriter, r *http.Request, h *Handler) (string, accountAmountRequest, bool) {
	if err := requireJSON(r); err != nil {
		h.writeError(w, r, err)
		return "", accountAmountRequest{}, false
	}
	key, err := idempotencyKey(r)
	if err != nil {
		h.writeError(w, r, err)
		return "", accountAmountRequest{}, false
	}
	var req accountAmountRequest
	if err := decodeJSON(r, &req); err != nil {
		h.writeError(w, r, err)
		return "", accountAmountRequest{}, false
	}
	return key, req, true
}
