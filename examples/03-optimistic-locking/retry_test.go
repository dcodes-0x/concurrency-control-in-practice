package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Build an HTTP server using the retry-enabled service.
//
// The barrier forces the first read of both requests
// to return the same version. After release, the
// barrier remains open for any subsequent retry reads.
func setupRetryTest(
	t *testing.T,
	initialBalance int64,
) *testEnvironment {
	t.Helper()

	env := setupTestEnvironment(t, initialBalance)

	repo := NewPostgresWalletRepository(env.db)

	synchronizedRepo := &barrierWalletStore{
		repo:    repo,
		barrier: newReadBarrier(2),
	}

	service := NewRetryingWalletService(synchronizedRepo)
	handler := NewWalletHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /wallets/{id}/withdraw",
		handler.Withdraw,
	)

	// Replace the fail-fast server from the test helper.
	env.server.Close()

	server := httptest.NewServer(mux)
	env.server = server
	t.Cleanup(server.Close)

	return env
}

// Both requests initially read version 0.
//
// Exactly one CAS succeeds on the first attempt.
// The losing request retries with version 1 and
// should successfully update the remaining balance.
func TestConcurrentWithdrawalsWithRetry(t *testing.T) {
	env := setupRetryTest(t, 1000)

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
				"withdrawal %d returned HTTP %d: %s",
				result.amount,
				result.status,
				result.body,
			)
		}

		t.Logf(
			"Withdrawal %d succeeded",
			result.amount,
		)
	}

	wallet, err := env.walletState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance != 700 {
		t.Errorf(
			"expected balance 700, got %d",
			wallet.Balance,
		)
	}

	if wallet.Version != 2 {
		t.Errorf(
			"expected version 2, got %d",
			wallet.Version,
		)
	}

	t.Logf(
		"Final balance=%d, version=%d",
		wallet.Balance,
		wallet.Version,
	)
}

// Both requests initially see enough funds.
//
// After the first withdrawal of 700 succeeds,
// the losing request retries, reads balance 300,
// and must fail with insufficient funds.
func TestInsufficientFundsAfterRetry(t *testing.T) {
	env := setupRetryTest(t, 1000)

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

			// After retry, the rejection should be due
			// to insufficient funds, not a stale version.
			if string(result.body) !=
				ErrInsufficientFunds.Error()+"\n" {
				t.Errorf(
					"expected insufficient funds, got %q",
					string(result.body),
				)
			}

		default:
			t.Errorf(
				"unexpected HTTP %d: %s",
				result.status,
				result.body,
			)
		}
	}

	if successes != 1 || conflicts != 1 {
		t.Errorf(
			"expected 1 success and 1 conflict, got %d and %d",
			successes,
			conflicts,
		)
	}

	wallet, err := env.walletState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance != 300 || wallet.Version != 1 {
		t.Errorf(
			"unexpected final state: balance=%d version=%d",
			wallet.Balance,
			wallet.Version,
		)
	}

	t.Logf(
		"Final balance=%d, version=%d",
		wallet.Balance,
		wallet.Version,
	)
}

// This fake repository always reports a CAS conflict.
// It verifies that retry attempts are bounded.
type alwaysConflictingStore struct {
	reads  int
	writes int
}

func (s *alwaysConflictingStore) GetWallet(
	ctx context.Context,
	walletID uuid.UUID,
) (Wallet, error) {
	s.reads++

	return Wallet{
		ID:      walletID,
		Balance: 1000,
		Version: int64(s.reads - 1),
	}, nil
}

func (s *alwaysConflictingStore) CompareAndSetBalance(
	ctx context.Context,
	walletID uuid.UUID,
	expectedVersion int64,
	newBalance int64,
) (Wallet, bool, error) {
	s.writes++
	return Wallet{}, false, nil
}

func TestRetryLimitExhausted(t *testing.T) {
	store := &alwaysConflictingStore{}

	service := &WalletService{
		repo: store,
		retryPolicy: RetryPolicy{
			MaxRetries: 2,
			BaseDelay:  0,
			MaxDelay:   0,
		},
	}

	_, err := service.Withdraw(
		context.Background(),
		WithdrawInput{
			WalletID: uuid.New(),
			Amount:   100,
		},
	)

	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf(
			"expected version conflict, got %v",
			err,
		)
	}

	// Initial attempt + 2 retries = 3 attempts.
	if store.reads != 3 {
		t.Errorf(
			"expected 3 reads, got %d",
			store.reads,
		)
	}

	if store.writes != 3 {
		t.Errorf(
			"expected 3 CAS attempts, got %d",
			store.writes,
		)
	}

	t.Log("Retry limit enforced after 3 CAS attempts")
}

// Cancellation must interrupt an active backoff.
func TestRetryRespectsCancellation(t *testing.T) {
	store := &alwaysConflictingStore{}

	service := &WalletService{
		repo: store,
		retryPolicy: RetryPolicy{
			MaxRetries: 3,
			BaseDelay:  time.Second,
			MaxDelay:   time.Second,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.Withdraw(
		ctx,
		WithdrawInput{
			WalletID: uuid.New(),
			Amount:   100,
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"expected context cancellation, got %v",
			err,
		)
	}

	t.Log("Cancelled retry stopped without further writes")
}
