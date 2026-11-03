package deadlocks

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

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

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}

	config.MaxConns = 5

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func createWalletPair(
	t *testing.T,
	db *pgxpool.Pool,
) (uuid.UUID, uuid.UUID) {
	t.Helper()

	walletA := uuid.New()
	walletB := uuid.New()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := db.Exec(ctx, `
		INSERT INTO wallets (id, balance)
		VALUES ($1, 1000), ($2, 1000)
	`, walletA, walletB)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := db.Exec(cleanupCtx, `
			DELETE FROM wallets
			WHERE id IN ($1, $2)
		`, walletA, walletB)
		if err != nil {
			t.Errorf("cleanup wallets: %v", err)
		}
	})

	return walletA, walletB
}

type twoPartyBarrier struct {
	arrived atomic.Int32
	ready   chan struct{}
}

func newTwoPartyBarrier() *twoPartyBarrier {
	return &twoPartyBarrier{
		ready: make(chan struct{}),
	}
}

func (b *twoPartyBarrier) Wait(
	ctx context.Context,
) error {
	if b.arrived.Add(1) == 2 {
		close(b.ready)
	}

	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == "40P01"
}

// transferWithSourceFirstLocking deliberately locks
// the source wallet first, regardless of its UUID.
//
// The barrier is test-only. It forces both transactions
// to hold their first lock before acquiring the second.
func transferWithSourceFirstLocking(
	ctx context.Context,
	db *pgxpool.Pool,
	transfer Transfer,
	barrier *twoPartyBarrier,
) error {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return err
	}

	// Rollback on every unsuccessful path.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	// First lock: source wallet.
	source, err := lockWallet(ctx, tx, transfer.From)
	if err != nil {
		return err
	}

	// Wait until the competing transaction has
	// acquired its own source wallet lock.
	if barrier != nil {
		if err := barrier.Wait(ctx); err != nil {
			return err
		}
	}

	// Second lock: destination wallet.
	_, err = lockWallet(ctx, tx, transfer.To)
	if err != nil {
		return err
	}

	if err := applyTransfer(
		ctx, tx, transfer, source.Balance,
	); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func TestDeadlock_OppositeLockOrder(t *testing.T) {
	db := openTestDB(t)
	walletA, walletB := createWalletPair(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	barrier := newTwoPartyBarrier()

	type result struct {
		name string
		err  error
	}

	results := make(chan result, 2)
	var wg sync.WaitGroup

	transfers := []struct {
		name     string
		transfer Transfer
	}{
		{
			name: "A to B",
			transfer: Transfer{
				From:   walletA,
				To:     walletB,
				Amount: 100,
			},
		},
		{
			name: "B to A",
			transfer: Transfer{
				From:   walletB,
				To:     walletA,
				Amount: 200,
			},
		},
	}

	for _, item := range transfers {
		wg.Add(1)

		go func(name string, tr Transfer) {
			defer wg.Done()

			err := transferWithSourceFirstLocking(
				ctx, db, tr, barrier,
			)

			results <- result{name: name, err: err}
		}(item.name, item.transfer)
	}

	wg.Wait()
	close(results)

	commits := 0
	deadlocks := 0
	var committedTransfer string

	for r := range results {
		switch {
		case r.err == nil:
			commits++
			committedTransfer = r.name
			t.Logf("%s: committed", r.name)

		case isDeadlock(r.err):
			deadlocks++
			t.Logf("%s: deadlock detected (40P01)", r.name)

		default:
			t.Errorf("%s: unexpected error: %v", r.name, r.err)
		}
	}

	if commits != 1 || deadlocks != 1 {
		t.Fatalf(
			"expected 1 commit and 1 deadlock; got commits=%d deadlocks=%d",
			commits, deadlocks,
		)
	}

	var balanceA, balanceB int64

	err := db.QueryRow(ctx, `
		SELECT balance FROM wallets WHERE id = $1
	`, walletA).Scan(&balanceA)
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow(ctx, `
		SELECT balance FROM wallets WHERE id = $1
	`, walletB).Scan(&balanceB)
	if err != nil {
		t.Fatal(err)
	}

	// Exactly one transfer committed.
	if committedTransfer == "A to B" {
		if balanceA != 900 || balanceB != 1100 {
			t.Errorf(
				"unexpected A->B state: A=%d B=%d",
				balanceA, balanceB,
			)
		}
	} else {
		if balanceA != 1200 || balanceB != 800 {
			t.Errorf(
				"unexpected B->A state: A=%d B=%d",
				balanceA, balanceB,
			)
		}
	}

	if balanceA+balanceB != 2000 {
		t.Errorf(
			"total funds changed: A=%d B=%d",
			balanceA, balanceB,
		)
	}

	t.Logf("Final balances: A=%d B=%d", balanceA, balanceB)
	t.Log("Deadlock reproduced successfully")
}
