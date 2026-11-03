package advisory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidConnection = errors.New(
	"a dedicated database connection is required",
)

// TryTransactionLock attempts to acquire an exclusive
// advisory lock scoped to the current transaction.
//
// PostgreSQL automatically releases it on COMMIT
// or ROLLBACK.
//
// The caller must own the transaction lifecycle.
func TryTransactionLock(
	ctx context.Context,
	tx pgx.Tx,
	key int64,
) (bool, error) {
	if tx == nil {
		return false, errors.New("transaction is nil")
	}

	var acquired bool

	err := tx.QueryRow(ctx, `
		SELECT pg_try_advisory_xact_lock($1)
	`, key).Scan(&acquired)

	if err != nil {
		return false, fmt.Errorf(
			"try transaction advisory lock: %w",
			err,
		)
	}

	return acquired, nil
}

// LockTransaction waits until the transaction-level
// advisory lock can be acquired or the context ends.
func LockTransaction(
	ctx context.Context,
	tx pgx.Tx,
	key int64,
) error {
	if tx == nil {
		return errors.New("transaction is nil")
	}

	var ignored any

	err := tx.QueryRow(ctx, `
		SELECT pg_advisory_xact_lock($1)
	`, key).Scan(&ignored)

	if err != nil {
		return fmt.Errorf(
			"acquire transaction advisory lock: %w",
			err,
		)
	}

	return nil
}

// TrySessionLock acquires a session-level advisory
// lock on the specific underlying PostgreSQL session.
//
// Keep this same connection until the lock is released.
func TrySessionLock(
	ctx context.Context,
	conn *pgxpool.Conn,
	key int64,
) (bool, error) {
	if conn == nil {
		return false, ErrInvalidConnection
	}

	var acquired bool

	err := conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock($1)
	`, key).Scan(&acquired)

	if err != nil {
		return false, fmt.Errorf(
			"try session advisory lock: %w",
			err,
		)
	}

	return acquired, nil
}

// UnlockSession releases one acquisition of the
// session-level advisory lock.
//
// Session lock acquisitions are reference-counted;
// repeated acquisitions require matching unlocks.
func UnlockSession(
	ctx context.Context,
	conn *pgxpool.Conn,
	key int64,
) (bool, error) {
	if conn == nil {
		return false, ErrInvalidConnection
	}

	var released bool

	err := conn.QueryRow(ctx, `
		SELECT pg_advisory_unlock($1)
	`, key).Scan(&released)

	if err != nil {
		return false, fmt.Errorf(
			"release session advisory lock: %w",
			err,
		)
	}

	return released, nil
}
