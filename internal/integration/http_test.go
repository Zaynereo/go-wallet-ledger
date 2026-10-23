package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/httpapi"
)

func TestHTTPFlow(t *testing.T) {
	f := setup(t)
	srv := httptest.NewServer(httpapi.New(f.svc, f.store.Ping, nil).Routes())
	t.Cleanup(srv.Close)
	client := srv.Client()

	health := getJSON(t, client, srv.URL+"/v1/health")
	if health["status"] != "ok" {
		t.Fatalf("health %+v", health)
	}
	ready := getJSON(t, client, srv.URL+"/v1/ready")
	if ready["status"] != "ok" {
		t.Fatalf("ready %+v", ready)
	}

	created := postJSON(t, client, srv.URL+"/v1/accounts", "", map[string]any{"currency": "USD"}, http.StatusCreated)
	id, _ := created["id"].(string)
	if id == "" || created["balance_minor"].(float64) != 0 {
		t.Fatalf("created %+v", created)
	}
	got := getJSON(t, client, srv.URL+"/v1/accounts/"+id)
	if got["id"] != id {
		t.Fatalf("get %+v", got)
	}

	missingKey := postJSON(t, client, srv.URL+"/v1/deposits", "", map[string]any{
		"account_id": id, "amount_minor": 100, "currency": "USD",
	}, http.StatusBadRequest)
	if errCode(missingKey) != "idempotency_key_required" {
		t.Fatalf("%+v", missingKey)
	}

	unknown := postJSON(t, client, srv.URL+"/v1/deposits", "k-unknown", map[string]any{
		"account_id": id, "amount_minor": 100, "currency": "USD", "extra": true,
	}, http.StatusBadRequest)
	if errCode(unknown) != "validation_error" {
		t.Fatalf("%+v", unknown)
	}

	dep := postJSON(t, client, srv.URL+"/v1/deposits", "k-dep", map[string]any{
		"account_id": id, "amount_minor": 250, "currency": "USD",
	}, http.StatusCreated)
	replayReq, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/deposits", bytes.NewReader(mustJSON(t, map[string]any{
		"account_id": id, "amount_minor": 250, "currency": "USD",
	})))
	if err != nil {
		t.Fatal(err)
	}
	replayReq.Header.Set("Content-Type", "application/json")
	replayReq.Header.Set("Idempotency-Key", "k-dep")
	replayRes, err := client.Do(replayReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replayRes.Body.Close() }()
	replayBody := decodeBody(t, replayRes)
	if replayRes.StatusCode != http.StatusCreated || replayRes.Header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("status %d header %q body %s", replayRes.StatusCode, replayRes.Header.Get("Idempotency-Replayed"), replayBody)
	}
	if replayBody["id"] != dep["id"] {
		t.Fatalf("replay id changed")
	}

	conflict := postJSON(t, client, srv.URL+"/v1/deposits", "k-dep", map[string]any{
		"account_id": id, "amount_minor": 251, "currency": "USD",
	}, http.StatusConflict)
	if errCode(conflict) != "idempotency_conflict" {
		t.Fatalf("%+v", conflict)
	}

	other := postJSON(t, client, srv.URL+"/v1/accounts", "", map[string]any{"currency": "USD"}, http.StatusCreated)
	otherID, _ := other["id"].(string)
	postJSON(t, client, srv.URL+"/v1/transfers", "k-xfer", map[string]any{
		"from_account_id": id, "to_account_id": otherID, "amount_minor": 40, "currency": "USD",
	}, http.StatusCreated)
	postJSON(t, client, srv.URL+"/v1/withdrawals", "k-wd", map[string]any{
		"account_id": id, "amount_minor": 10, "currency": "USD",
	}, http.StatusCreated)

	page := getJSON(t, client, srv.URL+"/v1/accounts/"+id+"/entries?limit=1")
	entries, _ := page["entries"].([]any)
	if len(entries) != 1 || page["next_cursor"] == nil {
		t.Fatalf("page %+v", page)
	}
	next, _ := page["next_cursor"].(string)
	page2 := getJSON(t, client, srv.URL+"/v1/accounts/"+id+"/entries?limit=1&cursor="+next)
	entries2, _ := page2["entries"].([]any)
	if len(entries2) != 1 {
		t.Fatalf("page2 %+v", page2)
	}

	txnID, _ := dep["id"].(string)
	txn := getJSON(t, client, srv.URL+"/v1/transactions/"+txnID)
	if txn["id"] != txnID {
		t.Fatalf("txn %+v", txn)
	}

	report := getJSON(t, client, srv.URL+"/v1/reconciliation")
	if report["ok"] != true {
		t.Fatalf("reconcile %+v", report)
	}

	missing := getJSONStatus(t, client, srv.URL+"/v1/accounts/99999999-9999-4999-8999-999999999999", http.StatusNotFound)
	if errCode(missing) != "not_found" {
		t.Fatalf("%+v", missing)
	}
	_ = domain.KindUser
}

func postJSON(t *testing.T, client *http.Client, url, key string, payload any, want int) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(mustJSON(t, payload)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body := decodeBody(t, res)
	if res.StatusCode != want {
		t.Fatalf("%s: status %d body %+v", url, res.StatusCode, body)
	}
	return body
}

func getJSON(t *testing.T, client *http.Client, url string) map[string]any {
	t.Helper()
	return getJSONStatus(t, client, url, http.StatusOK)
}

func getJSONStatus(t *testing.T, client *http.Client, url string, want int) map[string]any {
	t.Helper()
	res, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body := decodeBody(t, res)
	if res.StatusCode != want {
		t.Fatalf("%s: status %d body %+v", url, res.StatusCode, body)
	}
	return body
}

func decodeBody(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return body
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func errCode(body map[string]any) string {
	errObj, _ := body["error"].(map[string]any)
	code, _ := errObj["code"].(string)
	return code
}
