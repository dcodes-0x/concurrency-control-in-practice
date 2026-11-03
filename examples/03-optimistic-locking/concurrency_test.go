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
	"sync/atomic"
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

// setupTestEnvironment creates an isolated wallet and
// HTTP server. An optional WalletStore can be injected
// later for deterministic concurrency tests.
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

	// Registered first; closes after wallet cleanup.
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

	env := &testEnvironment{
		db:       db,
		walletID: walletID,
	}

	// Default server uses the real PostgreSQL repository.
	env.useRepository(t, NewPostgresWalletRepository(db))

	return env
}

// useRepository creates or replaces the HTTP test server
// using the provided repository implementation.
//
// Used to inject a synchronization barrier in tests.
func (e *testEnvironment) useRepository(
	t *testing.T,
	repo WalletStore,
) {
	t.Helper()

	if e.server != nil {
		e.server.Close()
	}

	service := NewWalletService(repo)
	handler := NewWalletHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /wallets/{id}/withdraw",
		handler.Withdraw,
	)

	e.server = httptest.NewServer(mux)

	// Every created server gets its own cleanup.
	server := e.server
	t.Cleanup(server.Close)
}

func (e *testEnvironment) walletState(
	ctx context.Context,
) (Wallet, error) {
	return NewPostgresWalletRepository(e.db).
		GetWallet(ctx, e.walletID)
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

// readBarrier releases all participating requests
// only after every request has completed its read.
type readBarrier struct {
	arrived atomic.Int32
	parties int32
	ready   chan struct{}
}

func newReadBarrier(parties int32) *readBarrier {
	return &readBarrier{
		parties: parties,
		ready:   make(chan struct{}),
	}
}

func (b *readBarrier) wait(ctx context.Context) error {
	if b.arrived.Add(1) == b.parties {
		close(b.ready)
	}

	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// barrierWalletStore delegates SQL operations to the
// real repository but pauses after GetWallet returns.
//
// This is a test-only wrapper; it is not production code.
type barrierWalletStore struct {
	repo    WalletStore
	barrier *readBarrier
}

func (r *barrierWalletStore) GetWallet(
	ctx context.Context,
	walletID uuid.UUID,
) (Wallet, error) {
	wallet, err := r.repo.GetWallet(ctx, walletID)
	if err != nil {
		return Wallet{}, err
	}

	if err := r.barrier.wait(ctx); err != nil {
		return Wallet{}, err
	}

	return wallet, nil
}

func (r *barrierWalletStore) CompareAndSetBalance(
	ctx context.Context,
	walletID uuid.UUID,
	expectedVersion int64,
	newBalance int64,
) (Wallet, bool, error) {
	return r.repo.CompareAndSetBalance(
		ctx,
		walletID,
		expectedVersion,
		newBalance,
	)
}

// runConcurrentWithdrawals starts all request
// goroutines using the same start signal.
func runConcurrentWithdrawals(
	ctx context.Context,
	env *testEnvironment,
	amounts ...int64,
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

// Test 1: Verify a normal withdrawal updates
// both the balance and version.
func TestSingleWithdrawal(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	result := sendWithdrawal(ctx, env, 100)

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

	var response WithdrawResult
	if err := json.Unmarshal(
		result.body, &response,
	); err != nil {
		t.Fatal(err)
	}

	if response.WalletID != env.walletID ||
		response.Balance != 900 ||
		response.Version != 1 {
		t.Errorf("unexpected response: %+v", response)
	}

	wallet, err := env.walletState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance != 900 || wallet.Version != 1 {
		t.Errorf("unexpected database state: %+v", wallet)
	}

	t.Logf(
		"Withdrawal successful: balance=%d version=%d",
		wallet.Balance,
		wallet.Version,
	)
}

// Test 2: Verify a stale version cannot overwrite
// an update that has already been committed.
func TestStaleVersionRejected(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	repo := NewPostgresWalletRepository(env.db)

	// Both operations initially observe version 0.
	original, err := repo.GetWallet(ctx, env.walletID)
	if err != nil {
		t.Fatal(err)
	}

	// First CAS succeeds.
	first, updated, err := repo.CompareAndSetBalance(
		ctx,
		env.walletID,
		original.Version,
		900,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("first CAS should have succeeded")
	}

	// Second CAS uses the stale version 0.
	_, updated, err = repo.CompareAndSetBalance(
		ctx,
		env.walletID,
		original.Version,
		800,
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("stale CAS unexpectedly succeeded")
	}

	wallet, err := repo.GetWallet(ctx, env.walletID)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance != 900 || wallet.Version != 1 {
		t.Errorf("unexpected state: %+v", wallet)
	}

	t.Logf(
		"First CAS succeeded: balance=%d version=%d",
		first.Balance,
		first.Version,
	)
	t.Log("Stale CAS rejected without overwriting data")
}

// Test 3: Force both requests to read version 0.
// Exactly one conditional UPDATE must succeed.
func TestConcurrentVersionConflict(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	realRepo := NewPostgresWalletRepository(env.db)

	env.useRepository(t, &barrierWalletStore{
		repo:    realRepo,
		barrier: newReadBarrier(2),
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	results := runConcurrentWithdrawals(
		ctx,
		env,
		100,
		200,
	)

	successes := 0
	conflicts := 0
	var successfulAmount int64

	for _, result := range results {
		if result.err != nil {
			t.Fatalf("request failed: %v", result.err)
		}

		switch result.status {
		case http.StatusOK:
			successes++
			successfulAmount = result.amount

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

	if successes != 1 || conflicts != 1 {
		t.Fatalf(
			"expected 1 success and 1 conflict; got %d and %d",
			successes,
			conflicts,
		)
	}

	wallet, err := env.walletState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	expectedBalance := int64(1000) - successfulAmount

	if wallet.Balance != expectedBalance {
		t.Errorf(
			"expected balance %d, got %d",
			expectedBalance,
			wallet.Balance,
		)
	}

	if wallet.Version != 1 {
		t.Errorf(
			"expected version 1, got %d",
			wallet.Version,
		)
	}

	t.Logf("Successful withdrawal: %d", successfulAmount)
	t.Logf("Final balance: %d", wallet.Balance)
	t.Logf("Final version: %d", wallet.Version)
	t.Log("Version conflict detected successfully")
}

// Test 4: Two requests each attempt to withdraw
// more than half of the available balance.
//
// The database must never reflect both deductions.
func TestConcurrentInsufficientFunds(t *testing.T) {
	env := setupTestEnvironment(t, 1000)

	realRepo := NewPostgresWalletRepository(env.db)

	env.useRepository(t, &barrierWalletStore{
		repo:    realRepo,
		barrier: newReadBarrier(2),
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	results := runConcurrentWithdrawals(
		ctx,
		env,
		700,
		700,
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

	wallet, err := env.walletState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if successes != 1 || conflicts != 1 {
		t.Errorf(
			"expected 1 success and 1 conflict; got %d and %d",
			successes,
			conflicts,
		)
	}

	if wallet.Balance != 300 {
		t.Errorf(
			"expected balance 300, got %d",
			wallet.Balance,
		)
	}

	if wallet.Version != 1 {
		t.Errorf(
			"expected version 1, got %d",
			wallet.Version,
		)
	}

	t.Logf("Successful requests: %d", successes)
	t.Logf("Conflict responses: %d", conflicts)
	t.Logf("Final balance: %d", wallet.Balance)
	t.Logf("Final version: %d", wallet.Version)
}
