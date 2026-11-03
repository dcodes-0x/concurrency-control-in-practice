package isolation

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrLastDoctorOnCall = errors.New(
		"at least one doctor must remain on call",
	)
	ErrDoctorNotFound = errors.New("doctor not found")
)

type SerializableExecutor struct {
	DB         *pgxpool.Pool
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// A TransactionFunc must contain all reads,
// validations and writes required by the operation.
//
// It may be called more than once; avoid external
// side effects inside this callback.
type TransactionFunc func(
	ctx context.Context,
	tx pgx.Tx,
) error

func isSerializationError(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == "40001"
}

func (e *SerializableExecutor) Execute(
	ctx context.Context,
	fn TransactionFunc,
) error {
	if e.DB == nil {
		return errors.New("database pool is nil")
	}

	for attempt := 0; attempt <= e.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := e.executeOnce(ctx, fn)
		if err == nil {
			return nil
		}

		// Business errors and unexpected database errors
		// are not automatically retried.
		if !isSerializationError(err) {
			return err
		}

		if attempt == e.MaxRetries {
			return fmt.Errorf(
				"serialization retry budget exhausted "+
					"after %d attempts: %w",
				attempt+1,
				err,
			)
		}

		if err := e.waitBeforeRetry(ctx, attempt); err != nil {
			return err
		}
	}

	return errors.New("unreachable retry state")
}

func (e *SerializableExecutor) executeOnce(
	ctx context.Context,
	fn TransactionFunc,
) error {
	tx, err := e.DB.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Ensure every uncommitted transaction is cleaned
	// up before returning to the retry loop.
	//
	// Use a fresh bounded context because the request
	// context may already have been cancelled.
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()

		_ = tx.Rollback(rollbackCtx)
	}()

	// Re-executed on every serialization retry.
	if err := fn(ctx, tx); err != nil {
		return err
	}

	// Serialization failures can happen at COMMIT.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (e *SerializableExecutor) waitBeforeRetry(
	ctx context.Context,
	attempt int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	delay := e.BaseDelay
	maxDelay := e.MaxDelay

	if delay < 0 || maxDelay < 0 {
		return errors.New("retry delays must be nonnegative")
	}

	// Capped exponential backoff.
	for i := 0; i < attempt; i++ {
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}

	if delay > maxDelay {
		delay = maxDelay
	}

	// Full jitter.
	if delay > 0 {
		delay = time.Duration(
			rand.Int63n(int64(delay) + 1),
		)
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// TakeDoctorOffCall contains the business operation.
//
// Every attempt re-counts available doctors using
// the current transaction's snapshot.
func TakeDoctorOffCall(
	ctx context.Context,
	tx pgx.Tx,
	shiftID uuid.UUID,
	doctorID uuid.UUID,
) error {
	var onCallCount int

	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM on_call_doctors
		WHERE shift_id = $1
		  AND is_on_call = TRUE
	`, shiftID).Scan(&onCallCount)
	if err != nil {
		return fmt.Errorf("count doctors: %w", err)
	}

	if onCallCount <= 1 {
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
		return fmt.Errorf("update doctor: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return ErrDoctorNotFound
	}

	return nil
}
