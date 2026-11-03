package deadlocks

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// transferWithOrderedLocks is the safe implementation.
//
// It locks wallets by UUID order, not by transfer
// direction, avoiding the circular dependency.
func transferWithOrderedLocks(
	ctx context.Context,
	db *pgxpool.Pool,
	transfer Transfer,
) error {
	if transfer.Amount <= 0 || transfer.From == transfer.To {
		return ErrInvalidTransfer
	}

	firstID := transfer.From
	secondID := transfer.To

	if bytes.Compare(firstID[:], secondID[:]) > 0 {
		firstID, secondID = secondID, firstID
	}

	tx, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return err
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	first, err := lockWallet(ctx, tx, firstID)
	if err != nil {
		return err
	}

	second, err := lockWallet(ctx, tx, secondID)
	if err != nil {
		return err
	}

	// Determine the source balance independently
	// of the lock acquisition order.
	sourceBalance := first.Balance

	if transfer.From == second.ID {
		sourceBalance = second.Balance
	}

	if err := applyTransfer(
		ctx,
		tx,
		transfer,
		sourceBalance,
	); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func TestTransfersWithConsistentLockOrder(t *testing.T) {
	db := openTestDB(t)
	walletA, walletB := createWalletPair(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	start := make(chan struct{})
	results := make(chan error, 2)

	transfers := []Transfer{
		{From: walletA, To: walletB, Amount: 100},
		{From: walletB, To: walletA, Amount: 200},
	}

	var wg sync.WaitGroup

	for _, tr := range transfers {
		wg.Add(1)

		go func(transfer Transfer) {
			defer wg.Done()
			<-start

			results <- transferWithOrderedLocks(
				ctx, db, transfer,
			)
		}(tr)
	}

	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("ordered transfer failed: %v", err)
		}
	}

	var balanceA, balanceB int64

	if err := db.QueryRow(ctx, `
		SELECT balance FROM wallets WHERE id = $1
	`, walletA).Scan(&balanceA); err != nil {
		t.Fatal(err)
	}

	if err := db.QueryRow(ctx, `
		SELECT balance FROM wallets WHERE id = $1
	`, walletB).Scan(&balanceB); err != nil {
		t.Fatal(err)
	}

	// A sends 100 and receives 200.
	// B receives 100 and sends 200.
	if balanceA != 1100 || balanceB != 900 {
		t.Fatalf(
			"expected A=1100 B=900, got A=%d B=%d",
			balanceA, balanceB,
		)
	}

	if balanceA+balanceB != 2000 {
		t.Fatal("total funds invariant violated")
	}

	t.Logf("Wallet A final balance: %d", balanceA)
	t.Logf("Wallet B final balance: %d", balanceB)
	t.Log("Both transfers committed without deadlock")
}
