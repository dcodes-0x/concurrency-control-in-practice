package advisory

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// advisoryLockCount returns the number of granted
// exclusive advisory locks matching a specific
// backend PID and 64-bit advisory key.
//
// Filtering by PID and key isolates the observation
// from other database sessions and tests.
func advisoryLockCount(
	ctx context.Context,
	db *pgxpool.Pool,
	pid uint32,
	key int64,
) (int, error) {
	var count int

	err := db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND mode = 'ExclusiveLock'
		  AND granted = TRUE
		  AND pid = $1
		  AND database = (
		      SELECT oid
		      FROM pg_database
		      WHERE datname = current_database()
		  )
		  AND objsubid = 1
		  AND (
		      (classid::bigint << 32)
		      | objid::bigint
		  ) = $2
	`, int32(pid), key).Scan(&count)

	return count, err
}

// openDedicatedConnection creates a PostgreSQL
// session separate from the connection pool.
//
// This is especially important for the termination
// test: only this test-owned session will be killed.
func openDedicatedConnection(
	t *testing.T,
) *pgx.Conn {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open dedicated connection: %v", err)
	}

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer closeCancel()

		// The connection may already be terminated
		// by PostgreSQL in the termination test.
		_ = conn.Close(closeCtx)
	})

	return conn
}

// Test 1:
//
// Acquire a transaction-scoped advisory lock,
// inspect it through pg_locks from another session,
// then roll back and verify that it disappears.
func TestAdvisoryLock_VisibleInPgLocks(t *testing.T) {
	db := openTestDB(t)
	owner := openDedicatedConnection(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	key := newLockKey()
	ownerPID := owner.PgConn().PID()

	tx, err := owner.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer rollbackCancel()

		_ = tx.Rollback(rollbackCtx)
	}()

	acquired, err := TryTransactionLock(
		ctx, tx, key,
	)
	if err != nil {
		t.Fatalf("acquire advisory lock: %v", err)
	}

	if !acquired {
		t.Fatal("expected advisory lock acquisition")
	}

	t.Logf(
		"Transaction acquired advisory lock: pid=%d key=%d",
		ownerPID,
		key,
	)

	// Inspect the lock using a different connection.
	count, err := advisoryLockCount(
		ctx, db, ownerPID, key,
	)
	if err != nil {
		t.Fatalf("inspect pg_locks: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected 1 advisory lock in pg_locks, got %d",
			count,
		)
	}

	t.Log("Advisory lock is visible in pg_locks")

	// Rollback automatically releases the
	// transaction-level advisory lock.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}

	count, err = advisoryLockCount(
		ctx, db, ownerPID, key,
	)
	if err != nil {
		t.Fatalf("inspect pg_locks after rollback: %v", err)
	}

	if count != 0 {
		t.Fatalf(
			"expected lock to disappear after rollback, got %d",
			count,
		)
	}

	t.Log("Transaction rollback removed the advisory lock")
}

// Test 2:
//
// Create a dedicated session, acquire a session-level
// advisory lock, terminate that exact backend, and
// verify another session can acquire the same key.
//
// Never terminate an arbitrary database backend.
func TestSessionLock_ReleasedWhenBackendTerminates(
	t *testing.T,
) {
	db := openTestDB(t)
	owner := openDedicatedConnection(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	key := newLockKey()
	ownerPID := owner.PgConn().PID()

	// Session A acquires a session-level advisory lock.
	var ownerAcquired bool

	err := owner.QueryRow(ctx, `
		SELECT pg_try_advisory_lock($1)
	`, key).Scan(&ownerAcquired)
	if err != nil {
		t.Fatalf("owner acquire lock: %v", err)
	}

	if !ownerAcquired {
		t.Fatal("owner could not acquire advisory lock")
	}

	t.Logf(
		"Dedicated session acquired lock: pid=%d key=%d",
		ownerPID,
		key,
	)

	// Confirm PostgreSQL exposes the lock.
	count, err := advisoryLockCount(
		ctx, db, ownerPID, key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf(
			"expected owner's lock in pg_locks, got %d",
			count,
		)
	}

	// Session B must be a different physical
	// connection and remain checked out.
	contender, err := db.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire contender connection: %v", err)
	}
	defer contender.Release()

	var contenderPID int

	err = contender.QueryRow(ctx, `
		SELECT pg_backend_pid()
	`).Scan(&contenderPID)
	if err != nil {
		t.Fatal(err)
	}

	if contenderPID == int(ownerPID) {
		t.Fatal("contender unexpectedly uses owner session")
	}

	// The contender cannot acquire the held key.
	contenderAcquired, err := TrySessionLock(
		ctx, contender, key,
	)
	if err != nil {
		t.Fatalf("contender first acquisition: %v", err)
	}

	if contenderAcquired {
		_, _ = UnlockSession(ctx, contender, key)
		t.Fatal("contender acquired a conflicting lock")
	}

	t.Log("Contender cannot acquire lock while owner is alive")

	// The PostgreSQL backend to terminate is the
	// dedicated session created by this test.
	//
	// The CASE expression guards against accidentally
	// terminating the calling backend.
	//
	// PostgreSQL 16 supports a timeout argument:
	// 5000 ms requests confirmation of termination.
	var terminated bool

	err = db.QueryRow(ctx, `
		SELECT CASE
		    WHEN $1 = pg_backend_pid() THEN FALSE
		    ELSE pg_terminate_backend($1, 5000)
		END
	`, int32(ownerPID)).Scan(&terminated)

	if err != nil {
		t.Fatalf("terminate owner backend: %v", err)
	}

	if !terminated {
		t.Fatal("PostgreSQL did not confirm backend termination")
	}

	t.Logf("Terminated dedicated backend PID %d", ownerPID)

	// Ending the session releases its advisory locks.
	count, err = advisoryLockCount(
		ctx, db, ownerPID, key,
	)
	if err != nil {
		t.Fatalf("inspect locks after termination: %v", err)
	}

	if count != 0 {
		t.Fatalf(
			"terminated backend still holds %d locks",
			count,
		)
	}

	// Session B should now acquire the same key.
	contenderAcquired, err = TrySessionLock(
		ctx, contender, key,
	)
	if err != nil {
		t.Fatalf("contender acquire after termination: %v", err)
	}

	if !contenderAcquired {
		t.Fatal(
			"contender could not acquire after owner terminated",
		)
	}

	// Ensure the new lock is released before
	// returning the contender connection to the pool.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		released, unlockErr := UnlockSession(
			cleanupCtx, contender, key,
		)

		if unlockErr != nil || !released {
			// Do not reuse a session that might
			// still own an advisory lock.
			_ = contender.Conn().Close(cleanupCtx)
		}
	}()

	t.Log("Contender acquired lock after owner termination")
}
