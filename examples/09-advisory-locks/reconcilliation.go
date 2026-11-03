package advisory

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrWarehouseBusy = errors.New(
		"warehouse reconciliation already running",
	)
	ErrInvalidWarehouse = errors.New(
		"warehouse ID cannot be empty",
	)
)

// ReconciliationFunc represents short, transactional
// database work performed while holding the lock.
//
// Do not perform irreversible external side effects
// inside the callback.
type ReconciliationFunc func(
	ctx context.Context,
	tx pgx.Tx,
) error

type ReconciliationCoordinator struct {
	db *pgxpool.Pool
}

func NewReconciliationCoordinator(
	db *pgxpool.Pool,
) (*ReconciliationCoordinator, error) {
	if db == nil {
		return nil, errors.New("database pool is nil")
	}

	return &ReconciliationCoordinator{db: db}, nil
}

// WarehouseLockKey maps a warehouse identifier
// into the advisory-lock namespace.
//
// Domain separation prevents unrelated application
// features using the same hash convention from
// accidentally sharing keys.
//
// IMPORTANT: Hash collisions are theoretically
// possible. This is not a globally collision-free
// mapping of arbitrary strings.
func WarehouseLockKey(
	warehouseID string,
) (int64, error) {
	if strings.TrimSpace(warehouseID) == "" {
		return 0, ErrInvalidWarehouse
	}

	h := fnv.New64a()

	_, _ = h.Write([]byte(
		"concurrency-control/reconciliation/warehouse/v1/",
	))
	_, _ = h.Write([]byte(warehouseID))

	return int64(h.Sum64()), nil
}

// TryReconcile acquires an exclusive, nonblocking
// transaction-level advisory lock for the warehouse.
//
// Return values:
//   - executed=true: callback ran and committed
//   - executed=false, ErrWarehouseBusy: lock unavailable
//   - other error: transaction failed and rolled back
//
// A successful callback is not considered successful
// until COMMIT returns successfully.
func (c *ReconciliationCoordinator) TryReconcile(
	ctx context.Context,
	warehouseID string,
	fn ReconciliationFunc,
) (executed bool, err error) {
	if c == nil || c.db == nil {
		return false, errors.New("coordinator has no database")
	}

	if fn == nil {
		return false, errors.New(
			"reconciliation callback is nil",
		)
	}

	key, err := WarehouseLockKey(warehouseID)
	if err != nil {
		return false, err
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	tx, err := c.db.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		return false, fmt.Errorf(
			"begin reconciliation transaction: %w",
			err,
		)
	}

	// Roll back all uncommitted paths, including
	// errors and unavailable advisory locks.
	//
	// Use a fresh bounded context in case the
	// request context has already been cancelled.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = tx.Rollback(cleanupCtx)
	}()

	acquired, err := TryTransactionLock(
		ctx,
		tx,
		key,
	)
	if err != nil {
		return false, fmt.Errorf(
			"acquire warehouse lock: %w",
			err,
		)
	}

	if !acquired {
		return false, ErrWarehouseBusy
	}

	// The callback must not commit, roll back, or
	// retain the supplied transaction after return.
	if err := fn(ctx, tx); err != nil {
		return false, fmt.Errorf(
			"warehouse %s reconciliation: %w",
			warehouseID,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf(
			"commit reconciliation: %w",
			err,
		)
	}

	return true, nil
}
