package skiplocked

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func expireLease(
	t *testing.T,
	ctx context.Context,
	db *pgxpool.Pool,
	jobID uuid.UUID,
) {
	t.Helper()

	tag, err := db.Exec(ctx, `
		UPDATE queue_jobs
		SET lease_expires_at =
		    clock_timestamp() - INTERVAL '1 second'
		WHERE id = $1
		  AND status = 'processing'
	`, jobID)

	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	if tag.RowsAffected() != 1 {
		t.Fatalf("expected one lease to expire")
	}
}

// Worker A claims a job, crashes, and loses its lease.
// Worker B reclaims it with a different claim token.
//
// Worker A must not be able to complete or heartbeat
// Worker B's claim.
func TestExpiredLease_ReclaimAndFenceStaleWorker(
	t *testing.T,
) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo, err := NewRepositoryWithConfig(
		db, 30*time.Second, 3,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	jobID, err := repo.Enqueue(ctx, queue, "send-notification")
	if err != nil {
		t.Fatal(err)
	}

	first, err := repo.Claim(ctx, queue, "worker-A")
	if err != nil {
		t.Fatal(err)
	}

	if first.ID != jobID || first.AttemptCount != 1 {
		t.Fatalf("unexpected first claim: %+v", first)
	}

	t.Logf(
		"Worker A claimed job with token %s",
		first.ClaimToken,
	)

	// Simulate A crashing and its lease expiring.
	expireLease(t, ctx, db, jobID)

	// Expired owner may no longer acknowledge.
	err = repo.Complete(
		ctx, jobID, "worker-A", first.ClaimToken,
	)
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf(
			"expected stale completion rejection, got %v",
			err,
		)
	}

	// A different worker takes ownership.
	second, err := repo.Claim(ctx, queue, "worker-B")
	if err != nil {
		t.Fatal(err)
	}

	if second.ID != first.ID {
		t.Fatal("expected reclamation of the same job")
	}

	if second.ClaimToken == first.ClaimToken {
		t.Fatal("claim token was not rotated")
	}

	if second.AttemptCount != 2 {
		t.Fatalf(
			"expected attempt 2, got %d",
			second.AttemptCount,
		)
	}

	t.Logf(
		"Worker B reclaimed job with token %s",
		second.ClaimToken,
	)

	// Old worker A resumes after B has reclaimed.
	err = repo.Complete(
		ctx, jobID, "worker-A", first.ClaimToken,
	)
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf(
			"stale worker completed new claim: %v",
			err,
		)
	}

	_, err = repo.Heartbeat(
		ctx, jobID, "worker-A", first.ClaimToken,
	)
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf(
			"stale worker renewed new claim: %v",
			err,
		)
	}

	// Only the current owner can complete.
	err = repo.Complete(
		ctx, jobID, "worker-B", second.ClaimToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	var status string
	var attempts int

	err = db.QueryRow(ctx, `
		SELECT status, attempt_count
		FROM queue_jobs
		WHERE id = $1
	`, jobID).Scan(&status, &attempts)
	if err != nil {
		t.Fatal(err)
	}

	if status != "completed" || attempts != 2 {
		t.Fatalf(
			"unexpected final state: status=%s attempts=%d",
			status, attempts,
		)
	}

	t.Log("Expired job recovered; stale worker fenced")
}

func TestHeartbeat_ValidClaimCanRenew(t *testing.T) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo, err := NewRepositoryWithConfig(
		db, 30*time.Second, 3,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err = repo.Enqueue(ctx, queue, "long-running-job")
	if err != nil {
		t.Fatal(err)
	}

	job, err := repo.Claim(ctx, queue, "worker-A")
	if err != nil {
		t.Fatal(err)
	}

	// Move the deadline close to now, while still
	// keeping this claim valid.
	_, err = db.Exec(ctx, `
		UPDATE queue_jobs
		SET lease_expires_at =
		    clock_timestamp() + INTERVAL '10 seconds'
		WHERE id = $1
	`, job.ID)
	if err != nil {
		t.Fatal(err)
	}

	var previousExpiry time.Time
	err = db.QueryRow(ctx, `
		SELECT lease_expires_at
		FROM queue_jobs
		WHERE id = $1
	`, job.ID).Scan(&previousExpiry)
	if err != nil {
		t.Fatal(err)
	}

	newExpiry, err := repo.Heartbeat(
		ctx,
		job.ID,
		"worker-A",
		job.ClaimToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !newExpiry.After(previousExpiry) {
		t.Fatalf(
			"heartbeat did not extend lease: old=%s new=%s",
			previousExpiry,
			newExpiry,
		)
	}

	if err := repo.Complete(
		ctx, job.ID, "worker-A", job.ClaimToken,
	); err != nil {
		t.Fatal(err)
	}

	t.Log("Active worker renewed and completed its claim")
}

func TestExpiredLease_MaxAttemptsBecomesFailed(
	t *testing.T,
) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo, err := NewRepositoryWithConfig(
		db, 30*time.Second, 3,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	jobID, err := repo.Enqueue(ctx, queue, "permanently-failing-job")
	if err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		job, err := repo.Claim(
			ctx, queue, "worker-A",
		)
		if err != nil {
			t.Fatalf(
				"attempt %d claim: %v",
				attempt, err,
			)
		}

		if job.ID != jobID || job.AttemptCount != attempt {
			t.Fatalf(
				"unexpected claim on attempt %d: %+v",
				attempt, job,
			)
		}

		// Simulate a crash after every claim.
		expireLease(t, ctx, db, jobID)
	}

	// Attempt budget is exhausted.
	_, err = repo.Claim(ctx, queue, "worker-B")
	if !errors.Is(err, ErrNoJob) {
		t.Fatalf(
			"expected no reclaimable job, got %v",
			err,
		)
	}

	reaped, err := repo.ReapExhausted(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}

	if reaped != 1 {
		t.Fatalf(
			"expected 1 failed job, got %d",
			reaped,
		)
	}

	var status string
	var attempts int

	err = db.QueryRow(ctx, `
		SELECT status, attempt_count
		FROM queue_jobs
		WHERE id = $1
	`, jobID).Scan(&status, &attempts)
	if err != nil {
		t.Fatal(err)
	}

	if status != "failed" || attempts != 3 {
		t.Fatalf(
			"unexpected state: status=%s attempts=%d",
			status, attempts,
		)
	}

	// Failed jobs are terminal in this protocol.
	_, err = repo.Claim(ctx, queue, "worker-C")
	if !errors.Is(err, ErrNoJob) {
		t.Fatalf(
			"failed job was reclaimed: %v",
			err,
		)
	}

	t.Log("Job reached maximum attempts and became failed")
}

func TestClaimToken_RejectsWrongToken(t *testing.T) {
	db := openTestDB(t)
	queue := setupQueue(t, db)
	repo := NewRepository(db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := repo.Enqueue(ctx, queue, "job")
	if err != nil {
		t.Fatal(err)
	}

	job, err := repo.Claim(ctx, queue, "worker-A")
	if err != nil {
		t.Fatal(err)
	}

	wrongToken := uuid.New()

	err = repo.Complete(
		ctx, job.ID, "worker-A", wrongToken,
	)
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf(
			"completion with wrong token succeeded: %v",
			err,
		)
	}

	_, err = repo.Heartbeat(
		ctx, job.ID, "worker-A", wrongToken,
	)
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf(
			"heartbeat with wrong token succeeded: %v",
			err,
		)
	}

	if err := repo.Complete(
		ctx, job.ID, "worker-A", job.ClaimToken,
	); err != nil {
		t.Fatal(err)
	}
}
