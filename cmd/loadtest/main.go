// Command loadtest is a small closed-loop client for the transfer API.
// It prints the counts and latencies it observed. It does not claim those
// numbers are a capacity result.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type account struct {
	ID           string `json:"id"`
	BalanceMinor int64  `json:"balance_minor"`
}

func main() {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	base := fs.String("base-url", "http://127.0.0.1:8080", "API base URL")
	requests := fs.Int("requests", 200, "number of transfers")
	concurrency := fs.Int("concurrency", 20, "parallel workers")
	amount := fs.Int64("amount", 1, "transfer amount in minor units")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if *requests < 1 || *concurrency < 1 || *amount < 1 {
		fmt.Fprintln(os.Stderr, "requests, concurrency, and amount must be positive")
		os.Exit(2)
	}

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency,
			MaxIdleConnsPerHost: *concurrency,
		},
	}

	from, err := createAccount(client, *base, "USD")
	if err != nil {
		fail(err)
	}
	to, err := createAccount(client, *base, "USD")
	if err != nil {
		fail(err)
	}
	if _, err := postMoney(client, *base, "/v1/deposits", "load-deposit", map[string]any{
		"account_id":   from.ID,
		"amount_minor": int64(*requests) * *amount,
		"currency":     "USD",
	}); err != nil {
		fail(err)
	}

	jobs := make(chan int)
	latencies := make([]time.Duration, *requests)
	errs := make([]error, *requests)
	var wg sync.WaitGroup
	workerN := *concurrency
	if workerN > *requests {
		workerN = *requests
	}
	for w := 0; w < workerN; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				start := time.Now()
				_, err := postMoney(client, *base, "/v1/transfers", fmt.Sprintf("load-%d", i), map[string]any{
					"from_account_id": from.ID,
					"to_account_id":   to.ID,
					"amount_minor":    *amount,
					"currency":        "USD",
				})
				latencies[i] = time.Since(start)
				errs[i] = err
			}
		}()
	}
	started := time.Now()
	for i := 0; i < *requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(started)

	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
	from, err = getAccount(client, *base, from.ID)
	if err != nil {
		fail(err)
	}
	to, err = getAccount(client, *base, to.ID)
	if err != nil {
		fail(err)
	}
	samples := append([]time.Duration(nil), latencies...)
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	fmt.Printf("requests %d\n", *requests)
	fmt.Printf("concurrency %d\n", *concurrency)
	fmt.Printf("success %d\n", ok)
	fmt.Printf("errors %d\n", *requests-ok)
	fmt.Printf("elapsed_ms %d\n", elapsed.Milliseconds())
	fmt.Printf("latency_ms_p50 %d\n", percentile(samples, 50).Milliseconds())
	fmt.Printf("latency_ms_p95 %d\n", percentile(samples, 95).Milliseconds())
	fmt.Printf("latency_ms_max %d\n", samples[len(samples)-1].Milliseconds())
	fmt.Printf("from_balance %d\n", from.BalanceMinor)
	fmt.Printf("to_balance %d\n", to.BalanceMinor)
	if ok != *requests || from.BalanceMinor != 0 || to.BalanceMinor != int64(*requests)**amount {
		os.Exit(1)
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)*p + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func createAccount(client *http.Client, base, currency string) (account, error) {
	var out account
	body, err := json.Marshal(map[string]string{"currency": currency})
	if err != nil {
		return out, err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/accounts", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return out, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return out, err
	}
	if res.StatusCode != http.StatusCreated {
		return out, fmt.Errorf("create account: %s %s", res.Status, raw)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func getAccount(client *http.Client, base, id string) (account, error) {
	var out account
	res, err := client.Get(base + "/v1/accounts/" + id)
	if err != nil {
		return out, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return out, err
	}
	if res.StatusCode != http.StatusOK {
		return out, fmt.Errorf("get account: %s %s", res.Status, raw)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func postMoney(client *http.Client, base, path, key string, payload map[string]any) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("%s %s: %s", path, res.Status, raw)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
	os.Exit(1)
}
