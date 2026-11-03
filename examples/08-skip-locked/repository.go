package skiplocked

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db            *pgxpool.Pool
	leaseDuration time.Duration
	maxAttempts   int
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{
		db:            db,
		leaseDuration: DefaultLeaseDuration,
		maxAttempts:   DefaultMaxAttempts,
	}
}

// Useful for tests and queue-specific configurations.
// Duration must be >= 1 ms and maxAttempts >= 1.
func NewRepositoryWithConfig(
	db *pgxpool.Pool,
	leaseDuration time.Duration,
	maxAttempts int,
) (*Repository, error) {
	if db == nil ||
		leaseDuration < time.Millisecond ||
		maxAttempts < 1 {
		return nil, ErrBadRequest
	}

	return &Repository{
		db:            db,
		leaseDuration: leaseDuration,
		maxAttempts:   maxAttempts,
	}, nil
}

func (r *Repository) leaseMilliseconds() int64 {
	return r.leaseDuration.Milliseconds()
}

func (r *Repository) Enqueue(
	ctx context.Context,
	queueName string,
	payload string,
) (uuid.UUID, error) {
	if strings.TrimSpace(queueName) == "" {
		return uuid.Nil, ErrBadRequest
	}

	var id uuid.UUID

	err := r.db.QueryRow(ctx, `
		INSERT INTO queue_jobs (queue_name, payload)
		VALUES ($1, $2)
		RETURNING id
	`, queueName, payload).Scan(&id)

	if err != nil {
		return uuid.Nil, fmt.Errorf("enqueue job: %w", err)
	}

	return id, nil
}

// Claim picks one available job.
//
// Eligibility:
//   - pending and available_at has passed, OR
//   - processing with an expired lease
//
// Jobs that reached maxAttempts are not claimed.
//
// PostgreSQL's row lock protects selection and
// ownership transition. A new token fences off
// the previous owner's completion/heartbeat calls.
func (r *Repository) Claim(
	ctx context.Context,
	queueName string,
	workerID string,
) (Job, error) {
	if strings.TrimSpace(queueName) == "" ||
		strings.TrimSpace(workerID) == "" {
		return Job{}, ErrBadRequest
	}

	newToken := uuid.New()

	var job Job

	err := r.db.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM queue_jobs
			WHERE queue_name = $1
			  AND attempt_count < $4
			  AND (
			      (
			          status = 'pending'
			          AND available_at <= clock_timestamp()
			      )
			      OR (
			          status = 'processing'
			          AND lease_expires_at <= clock_timestamp()
			      )
			  )
			ORDER BY
			    CASE
			        WHEN status = 'processing'
			        THEN lease_expires_at
			        ELSE available_at
			    END,
			    created_at,
			    id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE queue_jobs AS j
		SET status = 'processing',
		    locked_by = $2,
		    claim_token = $3,
		    claimed_at = clock_timestamp(),
		    lease_expires_at =
		        clock_timestamp()
		        + ($5 * INTERVAL '1 millisecond'),
		    attempt_count = attempt_count + 1,
		    updated_at = clock_timestamp()
		FROM candidate
		WHERE j.id = candidate.id
		RETURNING
		    j.id,
		    j.queue_name,
		    j.payload,
		    j.status,
		    j.locked_by,
		    j.claim_token,
		    j.attempt_count,
		    j.claimed_at,
		    j.lease_expires_at
	`,
		queueName,
		workerID,
		newToken,
		r.maxAttempts,
		r.leaseMilliseconds(),
	).Scan(
		&job.ID,
		&job.QueueName,
		&job.Payload,
		&job.Status,
		&job.LockedBy,
		&job.ClaimToken,
		&job.AttemptCount,
		&job.ClaimedAt,
		&job.LeaseExpiresAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJob
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim job: %w", err)
	}

	return job, nil
}

// Complete requires both worker identity and the
// exact token returned by the current Claim.
//
// An expired claim cannot complete a job.
func (r *Repository) Complete(
	ctx context.Context,
	jobID uuid.UUID,
	workerID string,
	claimToken uuid.UUID,
) error {
	if jobID == uuid.Nil ||
		claimToken == uuid.Nil ||
		strings.TrimSpace(workerID) == "" {
		return ErrBadRequest
	}

	tag, err := r.db.Exec(ctx, `
		UPDATE queue_jobs
		SET status = 'completed',
		    locked_by = NULL,
		    claim_token = NULL,
		    claimed_at = NULL,
		    lease_expires_at = NULL,
		    updated_at = clock_timestamp()
		WHERE id = $1
		  AND locked_by = $2
		  AND claim_token = $3
		  AND status = 'processing'
		  AND lease_expires_at > clock_timestamp()
	`, jobID, workerID, claimToken)

	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return ErrNotOwner
	}

	return nil
}

// Heartbeat renews an active claim.
//
// It fails if the claim has expired, even if another
// worker has not reclaimed the job yet.
func (r *Repository) Heartbeat(
	ctx context.Context,
	jobID uuid.UUID,
	workerID string,
	claimToken uuid.UUID,
) (time.Time, error) {
	if jobID == uuid.Nil ||
		claimToken == uuid.Nil ||
		strings.TrimSpace(workerID) == "" {
		return time.Time{}, ErrBadRequest
	}

	var expiresAt time.Time

	err := r.db.QueryRow(ctx, `
		UPDATE queue_jobs
		SET lease_expires_at =
		        clock_timestamp()
		        + ($4 * INTERVAL '1 millisecond'),
		    updated_at = clock_timestamp()
		WHERE id = $1
		  AND locked_by = $2
		  AND claim_token = $3
		  AND status = 'processing'
		  AND lease_expires_at > clock_timestamp()
		RETURNING lease_expires_at
	`,
		jobID,
		workerID,
		claimToken,
		r.leaseMilliseconds(),
	).Scan(&expiresAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrNotOwner
	}
	if err != nil {
		return time.Time{},
			fmt.Errorf("heartbeat job: %w", err)
	}

	return expiresAt, nil
}

// ReapExhausted marks expired processing jobs as
// terminally failed once they reach maxAttempts.
//
// It is safe to call from multiple processes:
// row-level locks serialize competing updates.
func (r *Repository) ReapExhausted(
	ctx context.Context,
	queueName string,
) (int64, error) {
	if strings.TrimSpace(queueName) == "" {
		return 0, ErrBadRequest
	}

	tag, err := r.db.Exec(ctx, `
		UPDATE queue_jobs
		SET status = 'failed',
		    locked_by = NULL,
		    claim_token = NULL,
		    claimed_at = NULL,
		    lease_expires_at = NULL,
		    updated_at = clock_timestamp()
		WHERE queue_name = $1
		  AND status = 'processing'
		  AND attempt_count >= $2
		  AND lease_expires_at <= clock_timestamp()
	`, queueName, r.maxAttempts)

	if err != nil {
		return 0, fmt.Errorf("reap exhausted jobs: %w", err)
	}

	return tag.RowsAffected(), nil
}
