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

// ReadBarrier synchronizes two concurrent operations.
//
// Both requests must complete GetBalance before either
// request is allowed to execute SetBalance.
type ReadBarrier struct {
	count   atomic.Int32
	ready   chan struct{}
	parties int32
}

func NewReadBarrier(parties int32) *ReadBarrier {
	return &ReadBarrier{
		ready:   make(chan struct{}),
		parties: parties,
	}
}

func (b *ReadBarrier) Wait(ctx context.Context) error {
	if b.count.Add(1) == b.parties {
		close(b.ready)
	}

	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BarrierWalletRepository wraps the real PostgreSQL
// repository without changing its implementation.
//
// It forces the unsafe read-modify-write interleaving.
type BarrierWalletRepository struct {
	repo    *PostgresWalletRepository
	barrier *ReadBarrier
}

func (r *BarrierWalletRepository) GetBalance(
	ctx context.Context,
	walletID uuid.UUID,
) (int64, error) {

	balance, err := r.repo.GetBalance(ctx, walletID)
	if err != nil {
		return 0, err
	}

	// Pause AFTER reading the database balance.
	// Both requests must reach this point.
	if err := r.barrier.Wait(ctx); err != nil {
		return 0, err
	}

	return balance, nil
}

func (r *BarrierWalletRepository) SetBalance(
	ctx context.Context,
	walletID uuid.UUID,
	balance int64,
) error {
	return r.repo.SetBalance(ctx, walletID, balance)
}

func TestLostUpdateWithConcurrentWithdrawals(t *testing.T) {
	// ---------------------------------------
	// 1. Initialize PostgreSQL
	// ---------------------------------------

	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}

	// Close the pool after test data cleanup.
	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping failed: %v", err)
	}

	// ---------------------------------------
	// 2. Create an isolated test wallet
	// ---------------------------------------

	walletID := uuid.New()

	const initialBalance int64 = 1000

	_, err = db.Exec(
		ctx,
		`INSERT INTO wallets (id, balance)
		 VALUES ($1, $2)`,
		walletID,
		initialBalance,
	)
	if err != nil {
		t.Fatalf("create test wallet: %v", err)
	}

	// Registered after pool cleanup,
	// so this runs before db.Close().
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := db.Exec(
			cleanupCtx,
			`DELETE FROM wallets WHERE id = $1`,
			walletID,
		)
		if err != nil {
			t.Errorf("cleanup wallet: %v", err)
		}
	})

	// ---------------------------------------
	// 3. Initialize application dependencies
	// ---------------------------------------

	realRepo := NewPostgresWalletRepository(db)

	barrierRepo := &BarrierWalletRepository{
		repo:    realRepo,
		barrier: NewReadBarrier(2),
	}

	service := NewWalletService(barrierRepo)
	handler := NewWalletHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /wallets/{id}/withdraw",
		handler.Withdraw,
	)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// ---------------------------------------
	// 4. Prepare concurrent withdrawals
	// ---------------------------------------

	type withdrawalResult struct {
		amount     int64
		statusCode int
		body       string
		err        error
	}

	results := make(
		chan withdrawalResult,
		2,
	)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// This channel releases both request goroutines
	// at approximately the same time.
	start := make(chan struct{})

	var wg sync.WaitGroup

	withdraw := func(amount int64) {
		defer wg.Done()

		<-start

		payload, err := json.Marshal(
			WithdrawRequest{
				Amount: amount,
			},
		)
		if err != nil {
			results <- withdrawalResult{
				amount: amount,
				err:    err,
			}
			return
		}

		url := fmt.Sprintf(
			"%s/wallets/%s/withdraw",
			server.URL,
			walletID,
		)

		req, err := http.NewRequest(
			http.MethodPost,
			url,
			bytes.NewReader(payload),
		)
		if err != nil {
			results <- withdrawalResult{
				amount: amount,
				err:    err,
			}
			return
		}

		req.Header.Set(
			"Content-Type",
			"application/json",
		)

		resp, err := client.Do(req)
		if err != nil {
			results <- withdrawalResult{
				amount: amount,
				err:    err,
			}
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			results <- withdrawalResult{
				amount: amount,
				err:    err,
			}
			return
		}

		results <- withdrawalResult{
			amount:     amount,
			statusCode: resp.StatusCode,
			body:       string(body),
		}
	}

	// Two legitimate withdrawals.
	wg.Add(2)

	go withdraw(100)
	go withdraw(200)

	// Start both requests.
	close(start)

	wg.Wait()
	close(results)

	// ---------------------------------------
	// 5. Verify HTTP results
	// ---------------------------------------

	successCount := 0

	for result := range results {
		if result.err != nil {
			t.Errorf(
				"withdrawal %d failed: %v",
				result.amount,
				result.err,
			)
			continue
		}

		if result.statusCode != http.StatusNoContent {
			t.Errorf(
				"withdrawal %d returned HTTP %d: %s",
				result.amount,
				result.statusCode,
				result.body,
			)
			continue
		}

		successCount++

		t.Logf(
			"Withdrawal of %d succeeded",
			result.amount,
		)
	}

	if successCount != 2 {
		t.Fatalf(
			"expected 2 successful withdrawals, got %d",
			successCount,
		)
	}

	// ---------------------------------------
	// 6. Verify final database balance
	// ---------------------------------------

	var finalBalance int64

	err = db.QueryRow(
		ctx,
		`SELECT balance
		 FROM wallets
		 WHERE id = $1`,
		walletID,
	).Scan(&finalBalance)
	if err != nil {
		t.Fatalf("read final balance: %v", err)
	}

	expectedBalance := initialBalance - 100 - 200

	t.Logf("Initial balance: %d", initialBalance)
	t.Logf("Expected balance: %d", expectedBalance)
	t.Logf("Actual balance: %d", finalBalance)

	// ---------------------------------------
	// 7. Assert the intentionally broken state
	// ---------------------------------------

	// The last absolute UPDATE wins.
	//
	// Both requests calculated their new balances
	// using the same original value: 1000.
	//
	// Depending on write order, final balance
	// should be 800 or 900, never 700.

	if finalBalance != 800 && finalBalance != 900 {
		t.Errorf(
			"expected lost-update result 800 or 900, got %d",
			finalBalance,
		)
	}

	if finalBalance == expectedBalance {
		t.Errorf(
			"lost update was not reproduced: "+
				"expected an incorrect balance, got %d",
			finalBalance,
		)
	}

	t.Log("Lost-update anomaly reproduced successfully")
}
