package isolation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type firstAttemptBarrier struct {
	count atomic.Int32
	ready chan struct{}
}

func newFirstAttemptBarrier() *firstAttemptBarrier {
	return &firstAttemptBarrier{
		ready: make(chan struct{}),
	}
}

func (b *firstAttemptBarrier) Wait(
	ctx context.Context,
) error {
	if b.count.Add(1) == 2 {
		close(b.ready)
	}

	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// This test forces both transactions to establish
// snapshots while two doctors are still on call.
//
// A serialization conflict causes one operation to
// retry from the beginning, observe the new count,
// and reject going off call.
func TestSerializableRetryPreservesInvariant(
	t *testing.T,
) {
	db := openTestDB(t)

	shiftID, aliceID, bobID := setupWriteSkewDoctors(
		t,
		db,
	)

	executor := &SerializableExecutor{
		DB:         db,
		MaxRetries: 3,
		BaseDelay:  5 * time.Millisecond,
		MaxDelay:   40 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	barrier := newFirstAttemptBarrier()

	type operationResult struct {
		doctor   string
		attempts int
		err      error
	}

	results := make(chan operationResult, 2)

	run := func(
		doctor string,
		doctorID uuid.UUID,
	) {
		attempts := 0

		err := executor.Execute(
			ctx,
			func(ctx context.Context, tx pgx.Tx) error {
				attempts++

				var count int
				err := tx.QueryRow(ctx, `
					SELECT COUNT(*)
					FROM on_call_doctors
					WHERE shift_id = $1
					  AND is_on_call = TRUE
				`, shiftID).Scan(&count)
				if err != nil {
					return err
				}

				// On the first attempt, both transactions
				// must read the original snapshot before
				// either can update a doctor.
				if attempts == 1 {
					if err := barrier.Wait(ctx); err != nil {
						return err
					}
				}

				if count <= 1 {
					return ErrLastDoctorOnCall
				}

				tag, err := tx.Exec(ctx, `
					UPDATE on_call_doctors
					SET is_on_call = FALSE
					WHERE id = $1
					  AND shift_id = $2
					  AND is_on_call = TRUE
				`, doctorID, shiftID)
				if err != nil {
					return err
				}

				if tag.RowsAffected() != 1 {
					return ErrDoctorNotFound
				}

				return nil
			},
		)

		results <- operationResult{
			doctor:   doctor,
			attempts: attempts,
			err:      err,
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		run("Alice", aliceID)
	}()

	go func() {
		defer wg.Done()
		run("Bob", bobID)
	}()

	wg.Wait()
	close(results)

	successes := 0
	businessRejections := 0
	totalAttempts := 0

	for result := range results {
		totalAttempts += result.attempts

		switch {
		case result.err == nil:
			successes++
			t.Logf(
				"%s successfully went off call after %d attempt(s)",
				result.doctor,
				result.attempts,
			)

		case errors.Is(
			result.err,
			ErrLastDoctorOnCall,
		):
			businessRejections++
			t.Logf(
				"%s rejected after %d attempt(s): %v",
				result.doctor,
				result.attempts,
				result.err,
			)

		default:
			t.Errorf(
				"%s failed unexpectedly: %v",
				result.doctor,
				result.err,
			)
		}
	}

	if successes != 1 {
		t.Errorf(
			"expected 1 successful operation, got %d",
			successes,
		)
	}

	if businessRejections != 1 {
		t.Errorf(
			"expected 1 business rejection, got %d",
			businessRejections,
		)
	}

	// Two initial attempts plus at least one retry.
	if totalAttempts < 3 {
		t.Errorf(
			"expected at least 3 total attempts, got %d",
			totalAttempts,
		)
	}

	var finalOnCall int

	err := db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM on_call_doctors
		WHERE shift_id = $1
		  AND is_on_call = TRUE
	`, shiftID).Scan(&finalOnCall)
	if err != nil {
		t.Fatal(err)
	}

	if finalOnCall != 1 {
		t.Errorf(
			"business invariant violated: "+
				"expected 1 doctor on call, got %d",
			finalOnCall,
		)
	}

	t.Logf("Total transaction attempts: %d", totalAttempts)
	t.Logf("Successful operations: %d", successes)
	t.Logf("Business rejections: %d", businessRejections)
	t.Logf("Final on-call doctors: %d", finalOnCall)
}

// A cancelled context must not start another
// serializable transaction.
func TestSerializableExecutorRespectsCancellation(
	t *testing.T,
) {
	db := openTestDB(t)

	executor := &SerializableExecutor{
		DB:         db,
		MaxRetries: 3,
		BaseDelay:  5 * time.Millisecond,
		MaxDelay:   40 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	called := false

	err := executor.Execute(
		ctx,
		func(ctx context.Context, tx pgx.Tx) error {
			called = true
			return nil
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"expected context.Canceled, got %v",
			err,
		)
	}

	if called {
		t.Fatal("transaction callback ran after cancellation")
	}
}
