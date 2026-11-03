package isolation

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// openTestDB initializes a connection pool for
// integration tests against the real PostgreSQL instance.
func openTestDB(t *testing.T) *pgxpool.Pool {
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

	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	return db
}

// createTestWallet inserts a wallet unique to the test.
// Cleanup runs before the database pool is closed.
func createTestWallet(
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

		if _, err := db.Exec(
			cleanupCtx,
			"DELETE FROM wallets WHERE id = $1",
			walletID,
		); err != nil {
			t.Errorf("cleanup wallet: %v", err)
		}
	})

	return walletID
}

// readBalance executes the same SELECT across tests.
// pgx.Tx guarantees the read uses the transaction's
// connection and isolation context.
func readBalance(
	ctx context.Context,
	tx pgx.Tx,
	walletID uuid.UUID,
) (int64, error) {
	var balance int64

	err := tx.QueryRow(ctx, `
		SELECT balance
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(&balance)

	return balance, err
}

// runNonRepeatableReadScenario controls this schedule:
//
//  1. Transaction A starts and reads the wallet.
//  2. Transaction B updates the wallet and commits.
//  3. Transaction A reads the wallet again.
//
// Transaction A is read-only; Transaction B is a writer.
func runNonRepeatableReadScenario(
	t *testing.T,
	level pgx.TxIsoLevel,
) (int64, int64) {
	t.Helper()

	db := openTestDB(t)
	walletID := createTestWallet(t, db, 1000)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	// Transaction A: choose the isolation level
	// explicitly and allow only read operations.
	txA, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   level,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		t.Fatalf("begin transaction A: %v", err)
	}

	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer rollbackCancel()

		_ = txA.Rollback(rollbackCtx)
	}()

	// First read establishes A's snapshot.
	firstBalance, err := readBalance(ctx, txA, walletID)
	if err != nil {
		t.Fatalf("transaction A first read: %v", err)
	}

	t.Logf(
		"Transaction A first read: %d",
		firstBalance,
	)

	// Transaction B uses a separate transaction and
	// database connection from Transaction A.
	txB, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		t.Fatalf("begin transaction B: %v", err)
	}

	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer rollbackCancel()

		_ = txB.Rollback(rollbackCtx)
	}()

	_, err = txB.Exec(ctx, `
		UPDATE wallets
		SET balance = 900,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $1
	`, walletID)

	if err != nil {
		t.Fatalf("transaction B update: %v", err)
	}

	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("transaction B commit: %v", err)
	}

	t.Log("Transaction B committed balance=900")

	// Transaction A performs the same SELECT again.
	secondBalance, err := readBalance(
		ctx,
		txA,
		walletID,
	)
	if err != nil {
		t.Fatalf("transaction A second read: %v", err)
	}

	t.Logf(
		"Transaction A second read: %d",
		secondBalance,
	)

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("transaction A commit: %v", err)
	}

	return firstBalance, secondBalance
}

// READ COMMITTED allows a non-repeatable read because
// each statement uses a new snapshot.
func TestReadCommitted_NonRepeatableRead(t *testing.T) {
	first, second := runNonRepeatableReadScenario(
		t,
		pgx.ReadCommitted,
	)

	if first != 1000 {
		t.Fatalf("expected first balance 1000, got %d", first)
	}

	if second != 900 {
		t.Fatalf("expected second balance 900, got %d", second)
	}

	t.Log("Non-repeatable read reproduced successfully")
}

// REPEATABLE READ uses a stable transaction snapshot,
// so A cannot observe B's later committed update.
func TestRepeatableRead_StableSnapshot(t *testing.T) {
	first, second := runNonRepeatableReadScenario(
		t,
		pgx.RepeatableRead,
	)

	if first != 1000 || second != 1000 {
		t.Fatalf(
			"expected stable snapshot (1000, 1000), got (%d, %d)",
			first,
			second,
		)
	}

	t.Log("Repeatable Read preserved the original snapshot")
}

// SERIALIZABLE also gives a stable snapshot here.
//
// This example alone does not demonstrate Serializable's
// additional protection against serialization anomalies.
func TestSerializable_StableSnapshot(t *testing.T) {
	first, second := runNonRepeatableReadScenario(
		t,
		pgx.Serializable,
	)

	if first != 1000 || second != 1000 {
		t.Fatalf(
			"expected stable snapshot (1000, 1000), got (%d, %d)",
			first,
			second,
		)
	}

	t.Log("Serializable preserved the original snapshot")
}
