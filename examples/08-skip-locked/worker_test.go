package skiplocked

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorker_ProcessOnceCompletesJob(t *testing.T) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo := NewRepository(db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	jobID, err := repo.Enqueue(
		ctx, queue, "generate-report",
	)
	if err != nil {
		t.Fatal(err)
	}

	processed := 0

	worker, err := NewWorker(
		repo,
		queue,
		"worker-A",
		10*time.Millisecond,
		5*time.Second,
		func(ctx context.Context, job Job) error {
			processed++

			if job.ID != jobID {
				t.Errorf(
					"unexpected job: %s",
					job.ID,
				)
			}

			if job.ClaimToken == [16]byte{} {
				t.Error("missing claim token")
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := worker.ProcessOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !claimed {
		t.Fatal("expected an eligible job")
	}

	if processed != 1 {
		t.Fatalf(
			"expected one invocation, got %d",
			processed,
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

	if status != "completed" || attempts != 1 {
		t.Fatalf(
			"unexpected state: status=%s attempts=%d",
			status, attempts,
		)
	}

	// No jobs remain.
	claimed, err = worker.ProcessOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if claimed {
		t.Fatal("expected no available job")
	}
}

func TestWorker_ProcessingFailureRemainsRecoverable(
	t *testing.T,
) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo := NewRepository(db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	jobID, err := repo.Enqueue(
		ctx, queue, "temporary-failure",
	)
	if err != nil {
		t.Fatal(err)
	}

	processFailure := errors.New("temporary downstream failure")

	worker, err := NewWorker(
		repo,
		queue,
		"worker-A",
		10*time.Millisecond,
		5*time.Second,
		func(context.Context, Job) error {
			return processFailure
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := worker.ProcessOnce(ctx)

	if !claimed {
		t.Fatal("expected job to be claimed")
	}

	if !errors.Is(err, processFailure) {
		t.Fatalf(
			"expected processing error, got %v",
			err,
		)
	}

	var status string

	err = db.QueryRow(ctx, `
		SELECT status
		FROM queue_jobs
		WHERE id = $1
	`, jobID).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}

	if status != "processing" {
		t.Fatalf(
			"expected processing, got %s",
			status,
		)
	}

	// Simulate the failed owner's lease expiring.
	expireLease(t, ctx, db, jobID)

	recovered, err := repo.Claim(
		ctx, queue, "worker-B",
	)
	if err != nil {
		t.Fatal(err)
	}

	if recovered.ID != jobID ||
		recovered.AttemptCount != 2 {
		t.Fatalf(
			"unexpected recovered job: %+v",
			recovered,
		)
	}

	if err := repo.Complete(
		ctx,
		recovered.ID,
		"worker-B",
		recovered.ClaimToken,
	); err != nil {
		t.Fatal(err)
	}

	t.Log("Failed job recovered on its second attempt")
}

func TestWorker_RunStopsOnCancellation(t *testing.T) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo := NewRepository(db)

	worker, err := NewWorker(
		repo,
		queue,
		"worker-A",
		10*time.Millisecond,
		5*time.Second,
		func(context.Context, Job) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	results := make(chan error, 1)

	go func() {
		results <- worker.Run(ctx)
	}()

	// Cancellation must interrupt the polling loop.
	cancel()

	select {
	case err := <-results:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"expected context.Canceled, got %v",
				err,
			)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestReaper_MarksExhaustedJobFailed(t *testing.T) {
	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo, err := NewRepositoryWithConfig(
		db,
		30*time.Second,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	jobID, err := repo.Enqueue(
		ctx, queue, "exhausted-job",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = repo.Claim(ctx, queue, "worker-A")
	if err != nil {
		t.Fatal(err)
	}

	expireLease(t, ctx, db, jobID)

	// RunReaper immediately sweeps once before
	// waiting for its next interval.
	reaperCtx, stopReaper := context.WithCancel(ctx)

	results := make(chan error, 1)

	go func() {
		results <- RunReaper(
			reaperCtx,
			repo,
			queue,
			time.Hour,
		)
	}()

	// Wait until the first sweep has marked the job.
	// This is a bounded observation loop, not a
	// timing dependency for the underlying logic.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var status string

		err := db.QueryRow(ctx, `
			SELECT status
			FROM queue_jobs
			WHERE id = $1
		`, jobID).Scan(&status)
		if err != nil {
			stopReaper()
			t.Fatal(err)
		}

		if status == "failed" {
			break
		}

		select {
		case <-ctx.Done():
			stopReaper()
			t.Fatal(ctx.Err())

		case <-ticker.C:
		}
	}

	stopReaper()

	select {
	case err := <-results:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"unexpected reaper error: %v",
				err,
			)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("reaper did not stop")
	}

	t.Log("Expired exhausted job moved to failed")
}
