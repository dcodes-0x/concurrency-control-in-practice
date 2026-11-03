package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// Each integration test uses a real PostgreSQL pool.
func openBenchmarkDB(t *testing.T) *pgxpool.Pool {
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

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database config: %v", err)
	}

	config.MaxConns = 16

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping: %v", err)
	}

	return db
}

// Creates one isolated wallet and registers cleanup.
//
// The database pool must remain open until the
// cleanup callback finishes.
func createBenchmarkWallet(
	t *testing.T,
	db *pgxpool.Pool,
	balance int64,
) uuid.UUID {
	t.Helper()

	walletID := uuid.New()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := db.Exec(ctx, `
		INSERT INTO wallets (id, balance)
		VALUES ($1, $2)
	`, walletID, balance)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := db.Exec(cleanupCtx, `
			DELETE FROM wallets WHERE id = $1
		`, walletID)
		if err != nil {
			t.Errorf("cleanup wallet: %v", err)
		}
	})

	return walletID
}

func readBenchmarkWallet(
	t *testing.T,
	db *pgxpool.Pool,
	walletID uuid.UUID,
) (int64, int64) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	var balance, version int64

	err := db.QueryRow(ctx, `
		SELECT balance, version
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(&balance, &version)

	if err != nil {
		t.Fatalf("read wallet: %v", err)
	}

	return balance, version
}

// Verifies that a successful, conflict-free workload
// applies every withdrawal correctly.
func TestBenchmark_SafeStrategiesPreserveBalance(
	t *testing.T,
) {
	db := openBenchmarkDB(t)

	cfg := Config{
		Workers:      4,
		OpsPerWorker: 15,
		Amount:       1,
		MaxRetries:   1000,
		RequestLimit: 15 * time.Second,
	}

	tests := []struct {
		name     string
		strategy StrategyFunc
	}{
		{
			name:     "pessimistic",
			strategy: pessimisticWithdraw,
		},
		{
			name:     "optimistic",
			strategy: optimisticWithdraw(cfg.MaxRetries),
		},
		{
			name:     "atomic",
			strategy: atomicWithdraw,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, err := runBenchmark(
				db,
				tt.name,
				tt.strategy,
				cfg,
			)
			if err != nil {
				t.Fatalf("run benchmark: %v", err)
			}

			if !report.Correct {
				t.Fatalf(
					"wallet consistency violated: "+
						"expected=%d actual=%d "+
						"successes=%d version=%d",
					report.ExpectedBalance,
					report.ActualBalance,
					report.Successes,
					report.FinalVersion,
				)
			}

			if report.Attempts !=
				cfg.Workers*cfg.OpsPerWorker {
				t.Fatalf(
					"unexpected attempts: %d",
					report.Attempts,
				)
			}

			if report.Successes+report.Failures !=
				report.Attempts {
				t.Fatal(
					"success and failure counts do not add up",
				)
			}

			// The two locking strategies should
			// complete every request because the
			// wallet has enough funds.
			//
			// Optimistic CAS can fail if its retry
			// budget is exhausted; that is not a
			// consistency violation.
			if tt.name != "optimistic" &&
				report.Failures != 0 {
				t.Fatalf(
					"unexpected failed withdrawals: %d",
					report.Failures,
				)
			}

			if report.P95Latency <= 0 ||
				report.P99Latency <= 0 {
				t.Fatal("invalid latency measurements")
			}

			if report.P99Latency <
				report.P95Latency {
				t.Fatal("p99 cannot be below p95")
			}

			t.Logf(
				"%s: attempts=%d successes=%d "+
					"failures=%d conflicts=%d "+
					"balance=%d p95=%s p99=%s",
				tt.name,
				report.Attempts,
				report.Successes,
				report.Failures,
				report.Conflicts,
				report.ActualBalance,
				report.P95Latency,
				report.P99Latency,
			)
		})
	}
}

// Runs optimistic locking with zero retry budget.
//
// Under contention, some CAS attempts may fail.
// Regardless of how many fail, only successful
// withdrawals are allowed to change the balance.
func TestOptimisticCAS_ZeroRetryBudgetPreservesConsistency(
	t *testing.T,
) {
	const (
		workers      = 8
		opsPerWorker = 20
	)

	db := openBenchmarkDB(t)

	initialBalance := int64(1000)
	walletID := createBenchmarkWallet(
		t, db, initialBalance,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	strategy := optimisticWithdraw(0)

	type result struct {
		conflicts int
		err       error
	}

	results := make(
		chan result,
		workers*opsPerWorker,
	)

	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			select {
			case <-start:
			case <-ctx.Done():
				return
			}

			for j := 0; j < opsPerWorker; j++ {
				conflicts, err := strategy(
					ctx,
					db,
					walletID,
					1,
				)

				results <- result{
					conflicts: conflicts,
					err:       err,
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	successes := 0
	exhausted := 0
	conflicts := 0

	for r := range results {
		conflicts += r.conflicts

		switch {
		case r.err == nil:
			successes++

		case errors.Is(r.err, ErrRetriesExhausted):
			exhausted++

		default:
			t.Fatalf(
				"unexpected optimistic error: %v",
				r.err,
			)
		}
	}

	if successes+exhausted != workers*opsPerWorker {
		t.Fatalf(
			"missing results: successes=%d exhausted=%d",
			successes,
			exhausted,
		)
	}

	// With zero retries, every exhausted operation
	// must have encountered exactly one CAS conflict.
	if conflicts != exhausted {
		t.Fatalf(
			"expected conflicts=%d, got %d",
			exhausted,
			conflicts,
		)
	}

	balance, version := readBenchmarkWallet(
		t, db, walletID,
	)

	expectedBalance := initialBalance - int64(successes)

	if balance != expectedBalance {
		t.Fatalf(
			"incorrect balance: expected=%d actual=%d",
			expectedBalance,
			balance,
		)
	}

	if version != int64(successes) {
		t.Fatalf(
			"incorrect version: expected=%d actual=%d",
			successes,
			version,
		)
	}

	t.Logf(
		"successes=%d exhausted=%d conflicts=%d "+
			"balance=%d version=%d",
		successes,
		exhausted,
		conflicts,
		balance,
		version,
	)
}

// Reproduces the same vulnerable SQL sequence as
// unsafeWithdraw:
//
// SELECT balance
// calculate new balance in Go
// UPDATE balance = calculated value
//
// The barrier forces both requests to read the same
// balance before either performs its UPDATE.
//
// This makes the lost update deterministic.
func TestUnsafeReadModifyWrite_LosesUpdate(
	t *testing.T,
) {
	db := openBenchmarkDB(t)

	walletID := createBenchmarkWallet(
		t, db, 1000,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	readsCompleted := make(chan struct{}, 2)
	allowUpdates := make(chan struct{})
	results := make(chan error, 2)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var balance int64

			err := db.QueryRow(ctx, `
				SELECT balance
				FROM wallets
				WHERE id = $1
			`, walletID).Scan(&balance)

			if err != nil {
				results <- err
				return
			}

			// Both goroutines must reach this point
			// before either is allowed to write.
			readsCompleted <- struct{}{}

			select {
			case <-allowUpdates:
			case <-ctx.Done():
				results <- ctx.Err()
				return
			}

			_, err = db.Exec(ctx, `
				UPDATE wallets
				SET balance = $1,
				    version = version + 1,
				    updated_at = NOW()
				WHERE id = $2
			`, balance-100, walletID)

			results <- err
		}()
	}

	// Wait for both reads. If a query unexpectedly
	// fails, the context deadline prevents the test
	// from hanging indefinitely.
	for i := 0; i < 2; i++ {
		select {
		case <-readsCompleted:
		case <-ctx.Done():
			close(allowUpdates)
			wg.Wait()
			t.Fatal(ctx.Err())
		}
	}

	close(allowUpdates)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("unsafe update failed: %v", err)
		}
	}

	balance, version := readBenchmarkWallet(
		t, db, walletID,
	)

	// Both requests withdrew 100 based on 1000.
	// Both wrote 900, so one withdrawal was lost.
	if balance != 900 {
		t.Fatalf(
			"expected lost-update balance 900, got %d",
			balance,
		)
	}

	// Both UPDATE statements committed.
	if version != 2 {
		t.Fatalf(
			"expected version=2, got %d",
			version,
		)
	}

	// Correct balance would be 800.
	if balance == 800 {
		t.Fatal("lost update was not reproduced")
	}

	t.Log(
		"Lost update reproduced: two successful " +
			"withdrawals, actual=900, expected=800",
	)
}

func TestPercentileCalculation(t *testing.T) {
	values := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}

	tests := []struct {
		name     string
		p        int
		expected time.Duration
	}{
		{
			name:     "p50",
			p:        50,
			expected: 30 * time.Millisecond,
		},
		{
			name:     "p95",
			p:        95,
			expected: 50 * time.Millisecond,
		},
		{
			name:     "p99",
			p:        99,
			expected: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := percentile(values, tt.p)

			if actual != tt.expected {
				t.Fatalf(
					"expected %s, got %s",
					tt.expected,
					actual,
				)
			}
		})
	}

	if averageLatency(values) != 30*time.Millisecond {
		t.Fatal("incorrect average latency")
	}

	if percentile(nil, 95) != 0 {
		t.Fatal("empty latency series should return zero")
	}
}
