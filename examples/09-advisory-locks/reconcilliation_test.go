package advisory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The first instance holds the warehouse lock
// while the second attempts the same key.
//
// We use channels to control the execution order
// without relying on sleeps.
func TestReconciliation_SameWarehouseOnlyOneExecutes(
	t *testing.T,
) {
	db := openTestDB(t)

	instanceA, err := NewReconciliationCoordinator(db)
	if err != nil {
		t.Fatal(err)
	}

	instanceB, err := NewReconciliationCoordinator(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	warehouseID := "warehouse-104"

	lockAcquired := make(chan struct{})
	releaseWork := make(chan struct{})
	firstResult := make(chan error, 1)

	// Ensure the first goroutine can always finish,
	// including if the test encounters an assertion
	// failure before normal release.
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseWork)
		})
	}
	defer release()

	go func() {
		executed, err := instanceA.TryReconcile(
			ctx,
			warehouseID,
			func(ctx context.Context, tx pgx.Tx) error {
				close(lockAcquired)

				select {
				case <-releaseWork:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		)

		if err == nil && !executed {
			err = errors.New(
				"instance A did not execute",
			)
		}

		firstResult <- err
	}()

	// Wait until A has acquired the advisory lock.
	select {
	case <-lockAcquired:
	case <-ctx.Done():
		t.Fatal("instance A did not acquire lock")
	}

	// Instance B targets the same warehouse.
	// Its callback must not run.
	secondCallbackCalled := false

	executedB, err := instanceB.TryReconcile(
		ctx,
		warehouseID,
		func(context.Context, pgx.Tx) error {
			secondCallbackCalled = true
			return nil
		},
	)

	if !errors.Is(err, ErrWarehouseBusy) {
		t.Fatalf(
			"expected ErrWarehouseBusy, got %v",
			err,
		)
	}

	if executedB {
		t.Fatal(
			"instance B executed despite lock contention",
		)
	}

	if secondCallbackCalled {
		t.Fatal(
			"instance B callback ran without ownership",
		)
	}

	t.Log("Instance B rejected while A owns warehouse lock")

	// Release A and allow its transaction to commit.
	release()

	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("instance A failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("instance A did not finish")
	}

	// The same warehouse can be reconciled again
	// after A's transaction commits.
	calledAgain := false

	executedB, err = instanceB.TryReconcile(
		ctx,
		warehouseID,
		func(context.Context, pgx.Tx) error {
			calledAgain = true
			return nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if !executedB || !calledAgain {
		t.Fatal(
			"warehouse lock was not released after commit",
		)
	}

	t.Log("Instance B executed after A committed")
}

// Distinct warehouse identifiers use distinct
// advisory keys, allowing independent transactions
// to hold their locks simultaneously.
func TestReconciliation_DifferentWarehousesRunConcurrently(
	t *testing.T,
) {
	db := openTestDB(t)

	coordinator, err := NewReconciliationCoordinator(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	warehouseA := "warehouse-104"
	warehouseB := "warehouse-205"

	keyA, err := WarehouseLockKey(warehouseA)
	if err != nil {
		t.Fatal(err)
	}

	keyB, err := WarehouseLockKey(warehouseB)
	if err != nil {
		t.Fatal(err)
	}

	if keyA == keyB {
		t.Fatal("test warehouses have colliding lock keys")
	}

	type result struct {
		warehouse string
		executed  bool
		err       error
	}

	entered := make(chan string, 2)
	releaseWork := make(chan struct{})
	results := make(chan result, 2)

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseWork)
		})
	}
	defer release()

	var wg sync.WaitGroup

	for _, warehouse := range []string{
		warehouseA,
		warehouseB,
	} {
		wg.Add(1)

		go func(id string) {
			defer wg.Done()

			executed, err := coordinator.TryReconcile(
				ctx,
				id,
				func(ctx context.Context, tx pgx.Tx) error {
					// Signal that this warehouse has
					// acquired its own transaction lock.
					entered <- id

					select {
					case <-releaseWork:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			)

			results <- result{
				warehouse: id,
				executed:  executed,
				err:       err,
			}
		}(warehouse)
	}

	// Both callbacks must enter before either exits.
	// This proves that the warehouse locks coexist.
	observed := make(map[string]bool)

	for len(observed) < 2 {
		select {
		case id := <-entered:
			observed[id] = true

		case <-ctx.Done():
			release()
			wg.Wait()
			t.Fatal(
				"both warehouse reconciliations did not enter concurrently",
			)
		}
	}

	t.Log("Both warehouses held advisory locks concurrently")

	release()
	wg.Wait()
	close(results)

	successes := 0

	for r := range results {
		if r.err != nil {
			t.Errorf(
				"%s: %v",
				r.warehouse,
				r.err,
			)
			continue
		}

		if !r.executed {
			t.Errorf(
				"%s unexpectedly skipped",
				r.warehouse,
			)
			continue
		}

		successes++
	}

	if successes != 2 {
		t.Fatalf(
			"expected 2 successful reconciliations, got %d",
			successes,
		)
	}
}

// A callback error rolls back the transaction,
// releasing the transaction-scoped advisory lock.
func TestReconciliation_CallbackErrorReleasesLock(
	t *testing.T,
) {
	db := openTestDB(t)

	coordinator, err := NewReconciliationCoordinator(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	warehouseID := "warehouse-rollback"
	reconciliationFailure := errors.New(
		"inventory validation failed",
	)

	executed, err := coordinator.TryReconcile(
		ctx,
		warehouseID,
		func(context.Context, pgx.Tx) error {
			return reconciliationFailure
		},
	)

	if !errors.Is(err, reconciliationFailure) {
		t.Fatalf(
			"expected callback error, got %v",
			err,
		)
	}

	if executed {
		t.Fatal("failed transaction reported as executed")
	}

	// The failed transaction has rolled back.
	// A fresh transaction must acquire the same key.
	executed, err = coordinator.TryReconcile(
		ctx,
		warehouseID,
		func(ctx context.Context, tx pgx.Tx) error {
			var value int

			return tx.QueryRow(
				ctx,
				"SELECT 1",
			).Scan(&value)
		},
	)

	if err != nil || !executed {
		t.Fatalf(
			"expected successful retry after rollback: executed=%v err=%v",
			executed,
			err,
		)
	}

	t.Log("Callback error rolled back and released lock")
}

func TestWarehouseLockKey_Deterministic(t *testing.T) {
	first, err := WarehouseLockKey("warehouse-104")
	if err != nil {
		t.Fatal(err)
	}

	second, err := WarehouseLockKey("warehouse-104")
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("same warehouse produced different keys")
	}

	other, err := WarehouseLockKey("warehouse-205")
	if err != nil {
		t.Fatal(err)
	}

	if first == other {
		t.Fatal("different test warehouses collided")
	}

	_, err = WarehouseLockKey("   ")
	if !errors.Is(err, ErrInvalidWarehouse) {
		t.Fatalf(
			"expected invalid warehouse error, got %v",
			err,
		)
	}

	t.Log("Lock-key mapping is deterministic")
}
