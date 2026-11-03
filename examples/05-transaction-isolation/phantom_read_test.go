package isolation

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// countMatchingWallets counts only our test wallets.
//
// The second wallet does not initially exist.
// It will be inserted by Transaction B.
func countMatchingWallets(
	ctx context.Context,
	tx pgx.Tx,
	firstID uuid.UUID,
	secondID uuid.UUID,
) (int64, error) {
	var count int64

	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM wallets
		WHERE id IN ($1, $2)
		  AND balance >= 500
	`, firstID, secondID).Scan(&count)

	return count, err
}

// runPhantomReadScenario executes this schedule:
//
// 1. Transaction A reads the count of matching wallets.
// 2. Transaction B inserts another matching wallet.
// 3. Transaction B commits.
// 4. Transaction A executes the same COUNT query.
//
// Only Transaction A's isolation level changes.
func runPhantomReadScenario(
	t *testing.T,
	level pgx.TxIsoLevel,
) (int64, int64, int64) {
	t.Helper()

	db := openTestDB(t)

	// First wallet is committed before A begins.
	firstID := createTestWallet(t, db, 1000)

	// Second wallet does not exist yet.
	secondID := uuid.New()

	// Register cleanup for the wallet inserted by B.
	// Transaction B will commit before the test ends.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_, err := db.Exec(ctx, `
			DELETE FROM wallets
			WHERE id = $1
		`, secondID)

		if err != nil {
			t.Errorf("cleanup second wallet: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	// Transaction A uses the selected isolation level.
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

	// Step 1: A executes the count query.
	firstCount, err := countMatchingWallets(
		ctx,
		txA,
		firstID,
		secondID,
	)
	if err != nil {
		t.Fatalf("first count query: %v", err)
	}

	t.Logf(
		"Transaction A first count: %d",
		firstCount,
	)

	// Step 2: Transaction B starts independently.
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

	// Insert a wallet that satisfies A's query.
	_, err = txB.Exec(ctx, `
		INSERT INTO wallets (
			id,
			balance
		)
		VALUES ($1, $2)
	`, secondID, 800)

	if err != nil {
		t.Fatalf("transaction B insert: %v", err)
	}

	// Step 3: B commits its new wallet.
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("transaction B commit: %v", err)
	}

	t.Log("Transaction B inserted and committed a matching wallet")

	// Step 4: A executes the identical query again.
	secondCount, err := countMatchingWallets(
		ctx,
		txA,
		firstID,
		secondID,
	)
	if err != nil {
		t.Fatalf("second count query: %v", err)
	}

	t.Logf(
		"Transaction A second count: %d",
		secondCount,
	)

	// A finishes its read-only transaction.
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("transaction A commit: %v", err)
	}

	// Independently check the committed database state
	// outside Transaction A's snapshot.
	var committedCount int64

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM wallets
		WHERE id IN ($1, $2)
		  AND balance >= 500
	`, firstID, secondID).Scan(&committedCount)

	if err != nil {
		t.Fatalf("read committed count: %v", err)
	}

	t.Logf(
		"Actual committed wallet count: %d",
		committedCount,
	)

	return firstCount, secondCount, committedCount
}

// READ COMMITTED permits phantom reads.
// A new statement can see B's committed insert.
func TestReadCommitted_PhantomRead(t *testing.T) {
	first, second, committed := runPhantomReadScenario(
		t,
		pgx.ReadCommitted,
	)

	if first != 1 || second != 2 {
		t.Fatalf(
			"expected counts (1, 2), got (%d, %d)",
			first,
			second,
		)
	}

	if committed != 2 {
		t.Fatalf(
			"expected committed count 2, got %d",
			committed,
		)
	}

	t.Log("Phantom read reproduced successfully")
}

// PostgreSQL REPEATABLE READ preserves a stable snapshot.
// The newly inserted row is not visible inside A.
func TestRepeatableRead_NoPhantom(t *testing.T) {
	first, second, committed := runPhantomReadScenario(
		t,
		pgx.RepeatableRead,
	)

	if first != 1 || second != 1 {
		t.Fatalf(
			"expected stable counts (1, 1), got (%d, %d)",
			first,
			second,
		)
	}

	if committed != 2 {
		t.Fatalf(
			"expected committed count 2, got %d",
			committed,
		)
	}

	t.Log("Repeatable Read prevented phantom visibility")
}

// SERIALIZABLE also prevents the phantom in this
// read-only transaction schedule.
func TestSerializable_NoPhantom(t *testing.T) {
	first, second, committed := runPhantomReadScenario(
		t,
		pgx.Serializable,
	)

	if first != 1 || second != 1 {
		t.Fatalf(
			"expected stable counts (1, 1), got (%d, %d)",
			first,
			second,
		)
	}

	if committed != 2 {
		t.Fatalf(
			"expected committed count 2, got %d",
			committed,
		)
	}

	t.Log("Serializable preserved snapshot consistency")
}
