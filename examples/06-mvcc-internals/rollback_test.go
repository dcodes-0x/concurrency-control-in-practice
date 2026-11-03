package mvcc

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestMVCC_RollbackPreservesOriginalVersion(t *testing.T) {
	db := openTestDB(t)
	walletID := createTestWallet(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	// Transaction A: ordinary reader.
	reader, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()
		_ = reader.Rollback(cleanupCtx)
	}()

	original, err := readTuple(ctx, reader, walletID)
	if err != nil {
		t.Fatal(err)
	}

	if original.Balance != 1000 || original.Version != 0 {
		t.Fatalf("unexpected original state: %+v", original)
	}

	t.Logf(
		"Original: balance=%d xmin=%s ctid=%s",
		original.Balance,
		original.Xmin,
		original.CTID,
	)

	// Transaction B: update without committing.
	writer, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()
		_ = writer.Rollback(cleanupCtx)
	}()

	var changed tupleState

	err = writer.QueryRow(ctx, `
		UPDATE wallets
		SET balance = 500,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $1
		RETURNING
		    balance,
		    version,
		    xmin::text,
		    xmax::text,
		    ctid::text
	`, walletID).Scan(
		&changed.Balance,
		&changed.Version,
		&changed.Xmin,
		&changed.Xmax,
		&changed.CTID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if changed.Balance != 500 || changed.Version != 1 {
		t.Fatalf("unexpected uncommitted state: %+v", changed)
	}

	if changed.CTID == original.CTID {
		t.Fatal("expected a new tuple location after UPDATE")
	}

	t.Logf(
		"Writer sees uncommitted version: balance=%d xmin=%s ctid=%s",
		changed.Balance,
		changed.Xmin,
		changed.CTID,
	)

	// A must not see B's uncommitted update.
	duringUpdate, err := readTuple(ctx, reader, walletID)
	if err != nil {
		t.Fatal(err)
	}

	if duringUpdate.Balance != 1000 {
		t.Fatalf(
			"dirty read: expected 1000, got %d",
			duringUpdate.Balance,
		)
	}

	t.Log("Reader does not see uncommitted balance=500")

	// B aborts its changes.
	if err := writer.Rollback(ctx); err != nil {
		t.Fatalf("writer rollback: %v", err)
	}

	t.Log("Writer rolled back")

	// A should still see the original row version.
	afterRollback, err := readTuple(ctx, reader, walletID)
	if err != nil {
		t.Fatal(err)
	}

	if afterRollback.Balance != 1000 ||
		afterRollback.Version != 0 {
		t.Errorf(
			"rollback changed committed state: %+v",
			afterRollback,
		)
	}

	if afterRollback.Xmin != original.Xmin ||
		afterRollback.CTID != original.CTID {
		t.Error("expected the original tuple version to remain visible")
	}

	if err := reader.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// A completely fresh read must agree.
	fresh, err := readCommittedTuple(ctx, db, walletID)
	if err != nil {
		t.Fatal(err)
	}

	if fresh.Balance != 1000 || fresh.Version != 0 {
		t.Errorf("unexpected final state: %+v", fresh)
	}

	t.Logf(
		"Final committed balance=%d version=%d",
		fresh.Balance,
		fresh.Version,
	)
}
