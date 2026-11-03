package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type testEnvironment struct {
	db       *pgxpool.Pool
	server   *httptest.Server
	walletID uuid.UUID
}

type requestResult struct {
	amount int64
	status int
	body   []byte
	err    error
}

// Creates an isolated wallet backed by real PostgreSQL.
func setupTestEnvironment(
	t *testing.T,
	initialBalance int64,
) *testEnvironment {
	t.Helper()

	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	// Registered first; executes last.
	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping: %v", err)
	}

	walletID := uuid.New()

	_, err = db.Exec(ctx, `
		INSERT INTO wallets (id, balance)
		VALUES ($1, $2)
	`, walletID, initialBalance)

	if err != nil {
		t.Fatalf("create test wallet: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		if _, err := db.Exec(
			cleanupCtx,
			"DELETE FROM wallets WHERE id = $1",
			walletID,
		); err != nil {
			t.Errorf("cleanup wallet: %v", err)
		}
	})

	repo := NewPostgresWalletRepository(db)
	service := NewWalletService(repo)
	handler := NewWalletHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /wallets/{id}/withdraw",
		handler.Withdraw,
	)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return &testEnvironment{
		db:       db,
		server:   server,
		walletID: walletID,
	}
}

func sendWithdrawal(
	ctx context.Context,
	env *testEnvironment,
	amount int64,
) requestResult {
	payload, err := json.Marshal(
		WithdrawRequest{Amount: amount},
	)
	if err != nil {
		return requestResult{amount: amount, err: err}
	}

	url := fmt.Sprintf(
		"%s/wallets/%s/withdraw",
		env.server.URL,
		env.walletID,
	)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(payload),
	)
	if err != nil {
		return requestResult{amount: amount, err: err}
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return requestResult{amount: amount, err: err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	return requestResult{
		amount: amount,
		status: resp.StatusCode,
		body:   body,
		err:    err,
	}
}

func getWalletState(
	ctx context.Context,
	env *testEnvironment,
) (int64, int64, error) {
	var balance, version int64

	err := env.db.QueryRow(ctx, `
		SELECT balance, version
		FROM wallets
		WHERE id = $1
	`, env.walletID).Scan(&balance, &version)

	return balance, version, err
}

// Releases all workers using a shared start channel.
// Each result is written to a distinct slice index.
func runConcurrentWithdrawals(
	ctx context.Context,
	env *testEnvironment,
	amounts []int64,
) []requestResult {
	results := make([]requestResult, len(amounts))
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i, amount := range amounts {
		wg.Add(1)

		go func(index int, value int64) {
			defer wg.Done()
			<-start

			results[index] = sendWithdrawal(
				ctx,
				env,
				value,
			)
		}(i, amount)
	}

	close(start)
	wg.Wait()

	return results
}

// Test 1: Both concurrent withdrawals must be applied.
func TestConcurrentAtomicWithdrawals(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	results := runConcurrentWithdrawals(
		ctx,
		env,
		[]int64{100, 200},
	)

	for _, result := range results {
		if result.err != nil {
			t.Fatalf(
				"withdrawal %d failed: %v",
				result.amount,
				result.err,
			)
		}

		if result.status != http.StatusOK {
			t.Fatalf(
				"withdrawal %d: HTTP %d, body=%s",
				result.amount,
				result.status,
				result.body,
			)
		}

		var response WithdrawResult
		if err := json.Unmarshal(
			result.body,
			&response,
		); err != nil {
			t.Fatal(err)
		}

		if response.WalletID != env.walletID {
			t.Error("incorrect wallet ID")
		}

		t.Logf(
			"Withdrawal=%d, committed balance=%d, version=%d",
			result.amount,
			response.Balance,
			response.Version,
		)
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	if balance != 700 {
		t.Errorf("expected balance 700, got %d", balance)
	}

	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}

	t.Logf(
		"Final balance=%d, version=%d",
		balance,
		version,
	)
}

// Test 2: Two withdrawals of 700 must not both succeed.
func TestAtomicOverspendingPrevention(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	results := runConcurrentWithdrawals(
		ctx,
		env,
		[]int64{700, 700},
	)

	successes := 0
	conflicts := 0

	for _, result := range results {
		if result.err != nil {
			t.Fatalf("request failed: %v", result.err)
		}

		switch result.status {
		case http.StatusOK:
			successes++

		case http.StatusConflict:
			conflicts++

		default:
			t.Errorf(
				"unexpected HTTP %d: %s",
				result.status,
				result.body,
			)
		}
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	if successes != 1 || conflicts != 1 {
		t.Errorf(
			"expected 1 success and 1 conflict, got %d and %d",
			successes,
			conflicts,
		)
	}

	if balance != 300 {
		t.Errorf("expected balance 300, got %d", balance)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	t.Logf(
		"Successes=%d, conflicts=%d, balance=%d, version=%d",
		successes,
		conflicts,
		balance,
		version,
	)
}

// Test 3: Run 100 concurrent withdrawals against one wallet.
func TestHighConcurrencyAtomicUpdates(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()

	const requestCount = 100

	amounts := make([]int64, requestCount)
	for i := range amounts {
		amounts[i] = 1
	}

	started := time.Now()

	results := runConcurrentWithdrawals(ctx, env, amounts)

	elapsed := time.Since(started)

	successes := 0

	for i, result := range results {
		if result.err != nil {
			t.Errorf(
				"request %d failed: %v",
				i,
				result.err,
			)
			continue
		}

		if result.status != http.StatusOK {
			t.Errorf(
				"request %d: HTTP %d, body=%s",
				i,
				result.status,
				result.body,
			)
			continue
		}

		successes++
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	expectedBalance := int64(1000 - requestCount)

	if successes != requestCount {
		t.Errorf(
			"expected %d successes, got %d",
			requestCount,
			successes,
		)
	}

	if balance != expectedBalance {
		t.Errorf(
			"expected balance %d, got %d",
			expectedBalance,
			balance,
		)
	}

	if version != requestCount {
		t.Errorf(
			"expected version %d, got %d",
			requestCount,
			version,
		)
	}

	t.Logf("Concurrent requests: %d", requestCount)
	t.Logf("Successful requests: %d", successes)
	t.Logf("Final balance: %d", balance)
	t.Logf("Final version: %d", version)
	t.Logf("Elapsed time: %s", elapsed)
}

// Test 4: An atomic UPDATE must wait while another
// transaction owns a conflicting wallet row lock.
func TestAtomicUpdateUnderLockContention(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	tx, err := env.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer rollbackCancel()

		_ = tx.Rollback(rollbackCtx)
	})

	// Explicitly lock the wallet row.
	var originalBalance int64

	err = tx.QueryRow(ctx, `
		SELECT balance
		FROM wallets
		WHERE id = $1
		FOR UPDATE
	`, env.walletID).Scan(&originalBalance)

	if err != nil {
		t.Fatal(err)
	}

	if originalBalance != 1000 {
		t.Fatalf(
			"expected balance 1000, got %d",
			originalBalance,
		)
	}

	t.Log("Transaction A acquired row lock")

	results := make(chan requestResult, 1)

	go func() {
		results <- sendWithdrawal(ctx, env, 100)
	}()

	// While A holds the lock, the conflicting withdrawal
	// must not finish. This is a bounded observation test.
	select {
	case result := <-results:
		t.Fatalf(
			"withdrawal completed before lock release: "+
				"HTTP %d, err=%v, body=%s",
			result.status,
			result.err,
			result.body,
		)

	case <-time.After(300 * time.Millisecond):
		t.Log("Withdrawal remains pending while row is locked")

	case <-ctx.Done():
		t.Fatalf("test timed out: %v", ctx.Err())
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("release lock: %v", err)
	}

	t.Log("Transaction A committed; lock released")

	select {
	case result := <-results:
		if result.err != nil {
			t.Fatal(result.err)
		}

		if result.status != http.StatusOK {
			t.Fatalf(
				"expected HTTP 200, got %d: %s",
				result.status,
				result.body,
			)
		}

	case <-ctx.Done():
		t.Fatalf(
			"withdrawal did not complete: %v",
			ctx.Err(),
		)
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	if balance != 900 || version != 1 {
		t.Errorf(
			"expected balance=900 version=1, got balance=%d version=%d",
			balance,
			version,
		)
	}
}

// Test 5: A rejected withdrawal must not modify
// either balance or version.
func TestAtomicRollbackOnInsufficientFunds(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result := sendWithdrawal(ctx, env, 1500)

	if result.err != nil {
		t.Fatal(result.err)
	}

	if result.status != http.StatusConflict {
		t.Fatalf(
			"expected HTTP 409, got %d: %s",
			result.status,
			result.body,
		)
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	if balance != 1000 {
		t.Errorf(
			"expected unchanged balance 1000, got %d",
			balance,
		)
	}

	if version != 0 {
		t.Errorf(
			"expected unchanged version 0, got %d",
			version,
		)
	}

	t.Log("Insufficient funds: no database mutation")
}
