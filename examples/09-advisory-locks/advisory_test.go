package advisory

import (
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// Generates a unique 64-bit advisory lock key
// for each test to avoid interference.
func newLockKey() int64 {
	id := uuid.New()
	return int64(binary.BigEndian.Uint64(id[:8]))
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
		t.Fatalf("parse database config: %v", err)
	}

	// Tests need multiple independent PostgreSQL
	// sessions for lock contention.
	config.MaxConns = 5

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}

	// Registered before connection cleanup, so
	// the pool closes after connections are released.
	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping: %v", err)
	}

	return db
}

// Acquire a dedicated connection so advisory locks
// remain associated with a known PostgreSQL session.
func acquireConn(
	t *testing.T,
	ctx context.Context,
	db *pgxpool.Pool,
) *pgxpool.Conn {
	t.Helper()

	conn, err := db.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire database connection: %v", err)
	}

	t.Cleanup(conn.Release)

	return conn
}

// Ensure unfinished transactions are rolled back
// before their connections are released.
func rollbackOnCleanup(
	t *testing.T,
	tx pgx.Tx,
) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = tx.Rollback(ctx)
	})
}

// Test 1:
// Transaction-level advisory locks are automatically
// released when the owning transaction commits.
func TestTransactionLock_ReleasedOnCommit(t *testing.T) {
	db := openTestDB(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	connA := acquireConn(t, ctx, db)
	connB := acquireConn(t, ctx, db)

	key := newLockKey()

	txA, err := connA.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rollbackOnCleanup(t, txA)

	txB, err := connB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rollbackOnCleanup(t, txB)

	// Transaction A acquires the lock.
	acquiredA, err := TryTransactionLock(
		ctx,
		txA,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !acquiredA {
		t.Fatal("transaction A should acquire the lock")
	}

	t.Log("Transaction A acquired advisory lock")

	// Transaction B tries the same key.
	acquiredB, err := TryTransactionLock(
		ctx,
		txB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquiredB {
		t.Fatal(
			"transaction B acquired a lock already held by A",
		)
	}

	t.Log("Transaction B could not acquire the same lock")

	// Committing A automatically releases its
	// transaction-level advisory lock.
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit transaction A: %v", err)
	}

	// Transaction B can now acquire the same key.
	acquiredB, err = TryTransactionLock(
		ctx,
		txB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !acquiredB {
		t.Fatal(
			"transaction B should acquire the lock after A commits",
		)
	}

	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("commit transaction B: %v", err)
	}

	t.Log("Transaction-level lock released on commit")
}

// Test 2:
// Rolling back a transaction automatically releases
// its transaction-level advisory locks.
func TestTransactionLock_ReleasedOnRollback(t *testing.T) {
	db := openTestDB(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	connA := acquireConn(t, ctx, db)
	connB := acquireConn(t, ctx, db)

	key := newLockKey()

	txA, err := connA.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rollbackOnCleanup(t, txA)

	acquiredA, err := TryTransactionLock(
		ctx,
		txA,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !acquiredA {
		t.Fatal("transaction A should acquire the lock")
	}

	// Session B cannot acquire the same key.
	acquiredB, err := TrySessionLock(
		ctx,
		connB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquiredB {
		_, _ = UnlockSession(ctx, connB, key)
		t.Fatal(
			"session B should not acquire the lock held by A",
		)
	}

	// Rollback releases A's transaction lock.
	if err := txA.Rollback(ctx); err != nil {
		t.Fatalf("rollback transaction A: %v", err)
	}

	acquiredB, err = TrySessionLock(
		ctx,
		connB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !acquiredB {
		t.Fatal(
			"session B should acquire after transaction A rolls back",
		)
	}

	// Ensure the session lock is released even if a
	// later assertion fails.
	lockHeld := true

	t.Cleanup(func() {
		if !lockHeld {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		released, err := UnlockSession(
			cleanupCtx,
			connB,
			key,
		)

		if err != nil || !released {
			_ = connB.Conn().Close(cleanupCtx)
		}
	})

	released, err := UnlockSession(
		ctx,
		connB,
		key,
	)
	if err != nil || !released {
		t.Fatalf(
			"session B unlock failed: released=%v err=%v",
			released,
			err,
		)
	}

	lockHeld = false

	t.Log("Rollback released transaction advisory lock")
}

// Test 3:
// Session-level advisory locks survive transaction
// rollback and require explicit unlocking.
func TestSessionLock_SurvivesTransactionRollback(t *testing.T) {
	db := openTestDB(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	connA := acquireConn(t, ctx, db)
	connB := acquireConn(t, ctx, db)

	key := newLockKey()

	txA, err := connA.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rollbackOnCleanup(t, txA)

	// Acquire a session-scoped lock from within
	// transaction A.
	var acquiredA bool

	err = txA.QueryRow(ctx, `
		SELECT pg_try_advisory_lock($1)
	`, key).Scan(&acquiredA)

	if err != nil {
		t.Fatal(err)
	}

	if !acquiredA {
		t.Fatal("session A should acquire advisory lock")
	}

	lockHeld := true

	// Cleanup executes before connA.Release because
	// callbacks run in reverse registration order.
	t.Cleanup(func() {
		if !lockHeld {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		released, err := UnlockSession(
			cleanupCtx,
			connA,
			key,
		)

		if err != nil || !released {
			// Do not leave a possibly lock-owning
			// session available for pool reuse.
			_ = connA.Conn().Close(cleanupCtx)
		}
	})

	t.Log("Session A acquired session-level advisory lock")

	// Rollback ends the database transaction but
	// does not release the session-level lock.
	if err := txA.Rollback(ctx); err != nil {
		t.Fatalf("rollback transaction A: %v", err)
	}

	// Another PostgreSQL session cannot acquire
	// the same advisory lock.
	acquiredB, err := TrySessionLock(
		ctx,
		connB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquiredB {
		_, _ = UnlockSession(ctx, connB, key)
		t.Fatal(
			"session-level lock should survive transaction rollback",
		)
	}

	t.Log("Session-level lock survived transaction rollback")

	// Explicitly release the lock using the same
	// database connection that acquired it.
	released, err := UnlockSession(
		ctx,
		connA,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !released {
		t.Fatal("session A should release its lock")
	}

	// Mark the lock released so cleanup does not
	// perform a second unlock.
	lockHeld = false

	// Another session can now acquire the key.
	acquiredB, err = TrySessionLock(
		ctx,
		connB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !acquiredB {
		t.Fatal(
			"session B should acquire after explicit unlock",
		)
	}

	// Clean up session B's lock even on failure.
	lockBHeld := true

	t.Cleanup(func() {
		if !lockBHeld {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		released, err := UnlockSession(
			cleanupCtx,
			connB,
			key,
		)

		if err != nil || !released {
			_ = connB.Conn().Close(cleanupCtx)
		}
	})

	released, err = UnlockSession(
		ctx,
		connB,
		key,
	)
	if err != nil || !released {
		t.Fatalf(
			"session B unlock failed: released=%v err=%v",
			released,
			err,
		)
	}

	lockBHeld = false

	t.Log("Explicit unlocking released session-level lock")
}

// Test 4:
// Session-level advisory locks support reentrant
// acquisition on the same database session.
//
// Every successful acquisition must be matched by
// a corresponding unlock.
func TestSessionLock_ReentrantAcquisitions(t *testing.T) {
	db := openTestDB(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	connA := acquireConn(t, ctx, db)
	connB := acquireConn(t, ctx, db)

	key := newLockKey()

	// Cleanup any remaining session locks on connA.
	// This connection is dedicated to the test.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_, err := connA.Exec(
			cleanupCtx,
			"SELECT pg_advisory_unlock_all()",
		)

		if err != nil {
			_ = connA.Conn().Close(cleanupCtx)
		}
	})

	// Acquire the same key twice on session A.
	for i := 0; i < 2; i++ {
		acquired, err := TrySessionLock(
			ctx,
			connA,
			key,
		)

		if err != nil {
			t.Fatal(err)
		}

		if !acquired {
			t.Fatalf(
				"session A acquisition %d failed",
				i+1,
			)
		}
	}

	t.Log("Session A acquired the same lock twice")

	// Releasing once leaves one acquisition active.
	released, err := UnlockSession(
		ctx,
		connA,
		key,
	)
	if err != nil || !released {
		t.Fatalf(
			"first unlock failed: released=%v err=%v",
			released,
			err,
		)
	}

	acquiredB, err := TrySessionLock(
		ctx,
		connB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquiredB {
		_, _ = UnlockSession(ctx, connB, key)
		t.Fatal(
			"session B acquired lock after only one unlock",
		)
	}

	t.Log("One unlock did not release both acquisitions")

	// Release A's second acquisition.
	released, err = UnlockSession(
		ctx,
		connA,
		key,
	)
	if err != nil || !released {
		t.Fatalf(
			"second unlock failed: released=%v err=%v",
			released,
			err,
		)
	}

	// Now session B should acquire the lock.
	acquiredB, err = TrySessionLock(
		ctx,
		connB,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !acquiredB {
		t.Fatal(
			"session B should acquire after both unlocks",
		)
	}

	// Ensure B's lock is cleaned up on assertion failure.
	lockBHeld := true

	t.Cleanup(func() {
		if !lockBHeld {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		released, err := UnlockSession(
			cleanupCtx,
			connB,
			key,
		)

		if err != nil || !released {
			_ = connB.Conn().Close(cleanupCtx)
		}
	})

	released, err = UnlockSession(
		ctx,
		connB,
		key,
	)
	if err != nil || !released {
		t.Fatalf(
			"session B unlock failed: released=%v err=%v",
			released,
			err,
		)
	}

	lockBHeld = false

	t.Log("Reentrant advisory-lock behavior verified")
}
