package deadlocks

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type DeadlockRetryPolicy struct {
	// MaxRetries excludes the initial attempt.
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

type TransferOperation func(context.Context) error

func (p DeadlockRetryPolicy) validate() error {
	if p.MaxRetries < 0 {
		return errors.New("MaxRetries cannot be negative")
	}

	if p.BaseDelay < 0 || p.MaxDelay < 0 {
		return errors.New("retry delays cannot be negative")
	}

	if p.MaxDelay < p.BaseDelay {
		return errors.New("MaxDelay must be >= BaseDelay")
	}

	return nil
}

func (p DeadlockRetryPolicy) backoff(attempt int) time.Duration {
	delay := p.BaseDelay

	// Exponential backoff capped at MaxDelay.
	// Avoid overflowing time.Duration.
	for i := 0; i < attempt; i++ {
		if delay >= p.MaxDelay/2 {
			delay = p.MaxDelay
			break
		}

		delay *= 2
	}

	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}

	// Full jitter: random delay in [0, delay).
	if delay <= 0 {
		return 0
	}

	return time.Duration(rand.Int63n(int64(delay)))
}

func waitForRetry(
	ctx context.Context,
	delay time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func IsDeadlockError(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == "40P01"
}

// RetryDeadlockedTransaction retries the entire
// transaction operation when PostgreSQL reports
// SQLSTATE 40P01.
//
// The operation must begin and finish a complete
// transaction on every invocation. Failed attempts
// must roll back and release their connections.
//
// External side effects must not be performed
// inside the retried operation.
func RetryDeadlockedTransaction(
	ctx context.Context,
	policy DeadlockRetryPolicy,
	operation TransferOperation,
) error {
	if err := policy.validate(); err != nil {
		return fmt.Errorf("invalid retry policy: %w", err)
	}

	if operation == nil {
		return errors.New("transaction operation is nil")
	}

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := operation(ctx)
		if err == nil {
			return nil
		}

		// Do not retry insufficient funds, missing
		// wallets, validation errors, or other
		// non-deadlock failures.
		if !IsDeadlockError(err) {
			return err
		}

		if attempt == policy.MaxRetries {
			return fmt.Errorf(
				"deadlock retry exhausted after %d attempts: %w",
				attempt+1,
				err,
			)
		}

		delay := policy.backoff(attempt)

		if err := waitForRetry(ctx, delay); err != nil {
			return err
		}
	}

	return errors.New("unreachable retry state")
}
