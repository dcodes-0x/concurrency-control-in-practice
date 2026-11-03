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
	status int
	body   []byte
	err    error
}

// setupTestEnvironment creates a real PostgreSQL wallet,
// initializes the application and registers test cleanup.
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
		t.Fatal(err)
	}

	// Registered first, so database closes last.
	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping failed: %v", err)
	}

	walletID := uuid.New()

	_, err = db.Exec(ctx, `
		INSERT INTO wallets (id, balance)
		VALUES ($1, $2)
	`, walletID, initialBalance)

	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	// Clean up the isolated wallet before closing the pool.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := db.Exec(
			cleanupCtx,
			"DELETE FROM wallets WHERE id = $1",
			walletID,
		)
		if err != nil {
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

// sendWithdrawal sends a real HTTP request to the test server.
func sendWithdrawal(
	ctx context.Context,
	env *testEnvironment,
	amount int64,
) requestResult {
	payload, err := json.Marshal(
		WithdrawRequest{Amount: amount},
	)
	if err != nil {
		return requestResult{err: err}
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
		return requestResult{err: err}
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return requestResult{err: err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	return requestResult{
		status: resp.StatusCode,
		body:   body,
		err:    err,
	}
}

// getWalletState reads the committed wallet state.
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

// runConcurrently releases all request goroutines using
// a shared start signal.
func runConcurrently(
	ctx context.Context,
	env *testEnvironment,
	amounts ...int64,
) []requestResult {
	results := make([]requestResult, len(amounts))
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i, amount := range amounts {
		wg.Add(1)

		go func(index int, withdrawal int64) {
			defer wg.Done()
			<-start

			results[index] = sendWithdrawal(
				ctx,
				env,
				withdrawal,
			)
		}(i, amount)
	}

	close(start)
	wg.Wait()

	return results
}

// Test 1: Both valid withdrawals should be reflected
// in the final wallet balance.
func TestConcurrentWithdrawals(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	results := runConcurrently(ctx, env, 100, 200)

	for i, result := range results {
		if result.err != nil {
			t.Fatalf(
				"withdrawal %d failed: %v",
				i, result.err,
			)
		}

		if result.status != http.StatusOK {
			t.Fatalf(
				"expected HTTP 200, got %d: %s",
				result.status, result.body,
			)
		}

		var response WithdrawResult
		if err := json.Unmarshal(
			result.body, &response,
		); err != nil {
			t.Fatal(err)
		}

		if response.WalletID != env.walletID {
			t.Errorf("unexpected wallet ID")
		}

		t.Logf(
			"Withdrawal %d: committed balance = %d",
			i+1,
			response.Balance,
		)
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Final balance: %d", balance)
	t.Logf("Wallet version: %d", version)

	if balance != 700 {
		t.Errorf("expected balance 700, got %d", balance)
	}

	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}
}

// Test 2: Two withdrawals of 700 must not both
// succeed against a wallet containing only 1000.
func TestConcurrentInsufficientFunds(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	results := runConcurrently(ctx, env, 700, 700)

	successCount := 0
	conflictCount := 0

	for _, result := range results {
		if result.err != nil {
			t.Fatalf("request failed: %v", result.err)
		}

		switch result.status {
		case http.StatusOK:
			successCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Errorf(
				"unexpected HTTP %d: %s",
				result.status, result.body,
			)
		}
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Successful withdrawals: %d", successCount)
	t.Logf("Rejected withdrawals: %d", conflictCount)
	t.Logf("Final balance: %d", balance)
	t.Logf("Wallet version: %d", version)

	if successCount != 1 || conflictCount != 1 {
		t.Errorf(
			"expected 1 success and 1 conflict; got %d and %d",
			successCount, conflictCount,
		)
	}

	if balance != 300 {
		t.Errorf("expected balance 300, got %d", balance)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}

// Test 3: Explicitly acquire a row lock and observe
// that another withdrawal cannot complete until release.
func TestRowLockContention(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	// Transaction A acquires the wallet lock.
	tx, err := env.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer rollbackCancel()
		_ = tx.Rollback(rollbackCtx)
	})

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

	t.Log("Transaction A acquired the wallet row lock")

	// Start a withdrawal while the lock remains held.
	resultCh := make(chan requestResult, 1)

	go func() {
		resultCh <- sendWithdrawal(ctx, env, 100)
	}()

	// Observe that the withdrawal cannot complete
	// while Transaction A is holding the lock.
	select {
	case result := <-resultCh:
		t.Fatalf(
			"withdrawal completed before lock release: "+
				"status=%d error=%v body=%s",
			result.status, result.err, result.body,
		)

	case <-time.After(300 * time.Millisecond):
		t.Log("Withdrawal still pending while row is locked")

	case <-ctx.Done():
		t.Fatalf("test timeout: %v", ctx.Err())
	}

	// Release the row lock.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transaction A: %v", err)
	}

	t.Log("Transaction A committed and released the lock")

	// The pending withdrawal can now complete.
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatal(result.err)
		}

		if result.status != http.StatusOK {
			t.Fatalf(
				"expected HTTP 200, got %d: %s",
				result.status, result.body,
			)
		}

		var response WithdrawResult
		if err := json.Unmarshal(
			result.body, &response,
		); err != nil {
			t.Fatal(err)
		}

		if response.Balance != 900 {
			t.Errorf(
				"expected response balance 900, got %d",
				response.Balance,
			)
		}

	case <-ctx.Done():
		t.Fatalf(
			"withdrawal did not complete after unlock: %v",
			ctx.Err(),
		)
	}

	balance, version, err := getWalletState(ctx, env)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Final balance: %d", balance)
	t.Logf("Wallet version: %d", version)

	if balance != 900 {
		t.Errorf("expected balance 900, got %d", balance)
	}

	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}
