package skiplocked

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestConcurrentReclaim_OnlyOneWorkerWins(t *testing.T) {
	const contenders = 4

	db := openTestDB(t)
	queue := setupQueue(t, db)

	repo, err := NewRepositoryWithConfig(
		db,
		30*time.Second,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	jobID, err := repo.Enqueue(
		ctx, queue, "recover-this-job",
	)
	if err != nil {
		t.Fatal(err)
	}

	original, err := repo.Claim(
		ctx, queue, "worker-A",
	)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate Worker A crashing.
	expireLease(t, ctx, db, jobID)

	type result struct {
		worker string
		job    Job
		err    error
	}

	results := make(chan result, contenders)
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < contenders; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		wg.Add(1)

		go func(worker string) {
			defer wg.Done()

			select {
			case <-start:
			case <-ctx.Done():
				results <- result{
					worker: worker,
					err:    ctx.Err(),
				}
				return
			}

			job, err := repo.Claim(
				ctx, queue, worker,
			)

			results <- result{
				worker: worker,
				job:    job,
				err:    err,
			}
		}(workerID)
	}

	close(start)
	wg.Wait()
	close(results)

	successes := 0
	noJobResults := 0

	var winner result

	for r := range results {
		switch {
		case r.err == nil:
			successes++
			winner = r
			t.Logf(
				"%s reclaimed job %s",
				r.worker,
				r.job.ID,
			)

		case errors.Is(r.err, ErrNoJob):
			noJobResults++

		default:
			t.Errorf(
				"%s: unexpected claim error: %v",
				r.worker, r.err,
			)
		}
	}

	if successes != 1 {
		t.Fatalf(
			"expected exactly 1 successful reclaim, got %d",
			successes,
		)
	}

	if noJobResults != contenders-1 {
		t.Errorf(
			"expected %d unavailable results, got %d",
			contenders-1,
			noJobResults,
		)
	}

	if winner.job.ID != jobID {
		t.Fatalf(
			"expected job %s, got %s",
			jobID, winner.job.ID,
		)
	}

	if winner.job.ClaimToken == original.ClaimToken ||
		winner.job.ClaimToken == uuid.Nil {
		t.Fatal("reclaimed job must receive a fresh claim token")
	}

	if winner.job.AttemptCount != 2 {
		t.Fatalf(
			"expected attempt 2, got %d",
			winner.job.AttemptCount,
		)
	}

	// Original worker must not acknowledge the
	// recovered job.
	err = repo.Complete(
		ctx,
		jobID,
		"worker-A",
		original.ClaimToken,
	)
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf(
			"expected old owner rejection, got %v",
			err,
		)
	}

	// Current owner can acknowledge it.
	err = repo.Complete(
		ctx,
		jobID,
		winner.worker,
		winner.job.ClaimToken,
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

	t.Log("Exactly one worker reclaimed the expired job")
}
