package mvcc

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

type tupleState struct {
	Balance int64
	Version int64
	Xmin    string
	Xmax    string
	CTID    string
}

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

	// Reader and writer transactions must occupy
	// separate database connections.
	config.MaxConns = 5

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	// Registered first, so the pool closes last.
	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping: %v", err)
	}

	return db
}

func createTestWallet(
	t *testing.T,
	db *pgxpool.Pool,
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
		VALUES ($1, 1000)
	`, walletID)
	if err != nil {
		t.Fatal(err)
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

func readTuple(
	ctx context.Context,
	tx pgx.Tx,
	walletID uuid.UUID,
) (tupleState, error) {
	var state tupleState

	err := tx.QueryRow(ctx, `
		SELECT
			balance,
			version,
			xmin::text,
			xmax::text,
			ctid::text
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(
		&state.Balance,
		&state.Version,
		&state.Xmin,
		&state.Xmax,
		&state.CTID,
	)

	return state, err
}

func readCommittedTuple(
	ctx context.Context,
	db *pgxpool.Pool,
	walletID uuid.UUID,
) (tupleState, error) {
	var state tupleState

	err := db.QueryRow(ctx, `
		SELECT
			balance,
			version,
			xmin::text,
			xmax::text,
			ctid::text
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(
		&state.Balance,
		&state.Version,
		&state.Xmin,
		&state.Xmax,
		&state.CTID,
	)

	return state, err
}

func runMVCCScenario(
	t *testing.T,
	isolation pgx.TxIsoLevel,
	expectedAfterCommit int64,
) {
	t.Helper()

	db := openTestDB(t)
	walletID := createTestWallet(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	// Transaction A: long-running reader.
	reader, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   isolation,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()
		_ = reader.Rollback(cleanupCtx)
	}()

	// First read establishes the reader's visibility.
	initial, err := readTuple(ctx, reader, walletID)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"Reader initial: balance=%d xmin=%s ctid=%s",
		initial.Balance,
		initial.Xmin,
		initial.CTID,
	)

	if initial.Balance != 1000 || initial.Version != 0 {
		t.Fatalf("unexpected initial state: %+v", initial)
	}

	// Transaction B: writer on a separate connection.
	writer, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()
		_ = writer.Rollback(cleanupCtx)
	}()

	var updated tupleState

	err = writer.QueryRow(ctx, `
		UPDATE wallets
		SET balance = 900,
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
		&updated.Balance,
		&updated.Version,
		&updated.Xmin,
		&updated.Xmax,
		&updated.CTID,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"Writer uncommitted: balance=%d xmin=%s ctid=%s",
		updated.Balance,
		updated.Xmin,
		updated.CTID,
	)

	// A new row version was produced by the UPDATE.
	if updated.Xmin == initial.Xmin {
		t.Error("expected a new tuple creation transaction ID")
	}

	if updated.CTID == initial.CTID {
		t.Error("expected UPDATE to create a new tuple location")
	}

	// A plain SELECT must not see B's uncommitted write.
	duringWrite, err := readTuple(ctx, reader, walletID)
	if err != nil {
		t.Fatal(err)
	}

	if duringWrite.Balance != 1000 {
		t.Fatalf(
			"dirty read: expected 1000, got %d",
			duringWrite.Balance,
		)
	}

	t.Logf(
		"Reader during uncommitted write: balance=%d",
		duringWrite.Balance,
	)

	// Commit B while A's transaction remains open.
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	t.Log("Writer committed")

	// Visibility now depends on A's isolation level.
	afterCommit, err := readTuple(ctx, reader, walletID)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"Reader after writer commit: balance=%d xmin=%s",
		afterCommit.Balance,
		afterCommit.Xmin,
	)

	if afterCommit.Balance != expectedAfterCommit {
		t.Errorf(
			"expected balance %d, got %d",
			expectedAfterCommit,
			afterCommit.Balance,
		)
	}

	if isolation == pgx.RepeatableRead {
		if afterCommit.Xmin != initial.Xmin ||
			afterCommit.CTID != initial.CTID {
			t.Error(
				"repeatable-read snapshot changed tuple version",
			)
		}
	}

	if err := reader.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Fresh read outside the old transaction snapshot.
	fresh, err := readCommittedTuple(ctx, db, walletID)
	if err != nil {
		t.Fatal(err)
	}

	if fresh.Balance != 900 || fresh.Version != 1 {
		t.Errorf(
			"fresh read expected balance=900 version=1; got %+v",
			fresh,
		)
	}

	if fresh.Xmin != updated.Xmin ||
		fresh.CTID != updated.CTID {
		t.Error(
			"fresh read did not observe the committed new tuple",
		)
	}

	t.Logf(
		"Fresh transaction: balance=%d version=%d",
		fresh.Balance,
		fresh.Version,
	)
}

func TestMVCC_ReadCommittedVisibility(t *testing.T) {
	runMVCCScenario(
		t,
		pgx.ReadCommitted,
		900,
	)
}

func TestMVCC_RepeatableReadVisibility(t *testing.T) {
	runMVCCScenario(
		t,
		pgx.RepeatableRead,
		1000,
	)
}
