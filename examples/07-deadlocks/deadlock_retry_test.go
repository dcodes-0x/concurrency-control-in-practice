package deadlocks

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDeadlockRetry_BothTransfersEventuallySucceed(
	t *testing.T,
) {
	db := openTestDB(t)
	walletA, walletB := createWalletPair(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	policy := DeadlockRetryPolicy{
		MaxRetries: 3,
		BaseDelay:  5 * time.Millisecond,
		MaxDelay:   40 * time.Millisecond,
	}

	// Force the initial deadlock only.
	barrier := newTwoPartyBarrier()

	type result struct {
		name     string
		attempts int
		err      error
	}

	results := make(chan result, 2)

	runTransfer := func(
		name string,
		transfer Transfer,
	) {
		attempts := 0

		err := RetryDeadlockedTransaction(
			ctx,
			policy,
			func(ctx context.Context) error {
				attempts++

				var attemptBarrier *twoPartyBarrier

				// Both initial attempts acquire their
				// source locks before requesting the
				// destination locks.
				if attempts == 1 {
					attemptBarrier = barrier
				}

				// Each invocation begins and ends
				// its own database transaction.
				return transferWithSourceFirstLocking(
					ctx,
					db,
					transfer,
					attemptBarrier,
				)
			},
		)

		results <- result{
			name:     name,
			attempts: attempts,
			err:      err,
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		runTransfer("A to B", Transfer{
			From:   walletA,
			To:     walletB,
			Amount: 100,
		})
	}()

	go func() {
		defer wg.Done()
		runTransfer("B to A", Transfer{
			From:   walletB,
			To:     walletA,
			Amount: 200,
		})
	}()

	wg.Wait()
	close(results)

	successes := 0
	totalAttempts := 0
	retriedOperations := 0

	for r := range results {
		if r.err != nil {
			t.Errorf(
				"%s failed: %v",
				r.name,
				r.err,
			)
			continue
		}

		successes++
		totalAttempts += r.attempts

		if r.attempts > 1 {
			retriedOperations++
		}

		t.Logf(
			"%s committed after %d attempt(s)",
			r.name,
			r.attempts,
		)
	}

	if successes != 2 {
		t.Fatalf(
			"expected 2 successful transfers, got %d",
			successes,
		)
	}

	if totalAttempts != 3 {
		t.Errorf(
			"expected exactly 3 attempts, got %d",
			totalAttempts,
		)
	}

	if retriedOperations != 1 {
		t.Errorf(
			"expected exactly 1 retried operation, got %d",
			retriedOperations,
		)
	}

	var balanceA, balanceB int64
	var versionA, versionB int64

	err := db.QueryRow(ctx, `
		SELECT balance, version
		FROM wallets
		WHERE id = $1
	`, walletA).Scan(&balanceA, &versionA)
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow(ctx, `
		SELECT balance, version
		FROM wallets
		WHERE id = $1
	`, walletB).Scan(&balanceB, &versionB)
	if err != nil {
		t.Fatal(err)
	}

	if balanceA != 1100 || balanceB != 900 {
		t.Errorf(
			"expected A=1100 B=900, got A=%d B=%d",
			balanceA,
			balanceB,
		)
	}

	if versionA != 2 || versionB != 2 {
		t.Errorf(
			"expected both versions=2, got A=%d B=%d",
			versionA,
			versionB,
		)
	}

	if balanceA+balanceB != 2000 {
		t.Fatal("fund conservation invariant violated")
	}

	t.Logf(
		"Final state: A=%d B=%d, total=%d",
		balanceA,
		balanceB,
		balanceA+balanceB,
	)
}

func TestDeadlockRetry_Exhaustion(t *testing.T) {
	policy := DeadlockRetryPolicy{
		MaxRetries: 2,
		BaseDelay:  0,
		MaxDelay:   0,
	}

	attempts := 0

	err := RetryDeadlockedTransaction(
		context.Background(),
		policy,
		func(ctx context.Context) error {
			attempts++

			// Simulate PostgreSQL deadlock failure
			// to test policy behavior deterministically.
			return &pgconn.PgError{
				Code:    "40P01",
				Message: "deadlock detected",
			}
		},
	)

	if !IsDeadlockError(err) {
		t.Fatalf(
			"expected wrapped SQLSTATE 40P01, got %v",
			err,
		)
	}

	// Initial attempt + 2 retries.
	if attempts != 3 {
		t.Fatalf(
			"expected 3 attempts, got %d",
			attempts,
		)
	}
}

func TestDeadlockRetry_NonRetryableError(t *testing.T) {
	policy := DeadlockRetryPolicy{
		MaxRetries: 3,
	}

	attempts := 0

	err := RetryDeadlockedTransaction(
		context.Background(),
		policy,
		func(ctx context.Context) error {
			attempts++
			return ErrInsufficientFunds
		},
	)

	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf(
			"expected insufficient funds, got %v",
			err,
		)
	}

	if attempts != 1 {
		t.Fatalf(
			"business error was retried %d times",
			attempts,
		)
	}
}

func TestDeadlockRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	policy := DeadlockRetryPolicy{
		MaxRetries: 10,
		BaseDelay:  time.Second,
		MaxDelay:   2 * time.Second,
	}

	attempts := 0

	err := RetryDeadlockedTransaction(
		ctx,
		policy,
		func(ctx context.Context) error {
			attempts++

			// Cancel after the first deadlock.
			cancel()

			return &pgconn.PgError{
				Code: "40P01",
			}
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"expected context cancellation, got %v",
			err,
		)
	}

	if attempts != 1 {
		t.Fatalf(
			"expected 1 attempt, got %d",
			attempts,
		)
	}
}

func TestDeadlockRetry_InvalidConfiguration(t *testing.T) {
	policy := DeadlockRetryPolicy{
		MaxRetries: -1,
	}

	err := RetryDeadlockedTransaction(
		context.Background(),
		policy,
		func(context.Context) error {
			return nil
		},
	)

	if err == nil {
		t.Fatal("expected validation error")
	}
}

// Compile-time assertion that the UUID type used
// by the transfer remains compatible with our
// existing wallet model.
var _ uuid.UUID = Transfer{}.From
