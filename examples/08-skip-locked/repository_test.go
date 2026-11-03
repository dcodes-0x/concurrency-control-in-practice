package skiplocked

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func openTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	_ = godotenv.Load("../../.env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database config: %v", err)
	}

	// Allow concurrent workers to use independent
	// PostgreSQL connections.
	config.MaxConns = 12

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}

	// Register first so the pool closes after all
	// test-data cleanup callbacks.
	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("database ping: %v", err)
	}

	return db
}

func setupQueue(
	t *testing.T,
	db *pgxpool.Pool,
) string {
	t.Helper()

	// Each test uses an isolated queue name.
	queueName := "test-" + uuid.NewString()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_, err := db.Exec(ctx, `
			DELETE FROM queue_jobs
			WHERE queue_name = $1
		`, queueName)

		if err != nil {
			t.Errorf("cleanup queue: %v", err)
		}
	})

	return queueName
}

// TestClaim_SkipsLockedJob verifies that SKIP LOCKED
// allows a worker to claim another pending job while
// the first job is locked by a separate transaction.
func TestClaim_SkipsLockedJob(t *testing.T) {
	db := openTestDB(t)
	repo := NewRepository(db)
	queue := setupQueue(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	// Enqueue Job 1.
	firstID, err := repo.Enqueue(
		ctx,
		queue,
		"job-1",
	)
	if err != nil {
		t.Fatalf("enqueue first job: %v", err)
	}

	// Enqueue Job 2.
	secondID, err := repo.Enqueue(
		ctx,
		queue,
		"job-2",
	)
	if err != nil {
		t.Fatalf("enqueue second job: %v", err)
	}

	// Transaction A acquires a row lock on Job 1.
	locker, err := db.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		t.Fatalf("begin locking transaction: %v", err)
	}

	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer rollbackCancel()

		_ = locker.Rollback(rollbackCtx)
	}()

	var lockedID uuid.UUID

	err = locker.QueryRow(ctx, `
		SELECT id
		FROM queue_jobs
		WHERE id = $1
		FOR UPDATE
	`, firstID).Scan(&lockedID)

	if err != nil {
		t.Fatalf("lock first job: %v", err)
	}

	if lockedID != firstID {
		t.Fatalf(
			"expected locked job %s, got %s",
			firstID,
			lockedID,
		)
	}

	t.Log("Transaction A holds a lock on Job 1")

	// Worker B uses a different database connection.
	// It should skip locked Job 1 and claim Job 2.
	claimed, err := repo.Claim(
		ctx,
		queue,
		"worker-B",
	)
	if err != nil {
		t.Fatalf("worker B claim: %v", err)
	}

	if claimed.ID != secondID {
		t.Fatalf(
			"expected Job 2 (%s), got %s",
			secondID,
			claimed.ID,
		)
	}

	if claimed.Status != "processing" {
		t.Fatalf(
			"expected processing status, got %s",
			claimed.Status,
		)
	}

	if claimed.LockedBy != "worker-B" {
		t.Fatalf(
			"expected worker-B as owner, got %s",
			claimed.LockedBy,
		)
	}

	if claimed.ClaimToken == uuid.Nil {
		t.Fatal("claim token must not be empty")
	}

	if claimed.AttemptCount != 1 {
		t.Fatalf(
			"expected attempt count 1, got %d",
			claimed.AttemptCount,
		)
	}

	t.Log("Worker B skipped locked Job 1 and claimed Job 2")

	// IMPORTANT:
	// Complete now requires the claim token.
	err = repo.Complete(
		ctx,
		claimed.ID,
		"worker-B",
		claimed.ClaimToken,
	)
	if err != nil {
		t.Fatalf("worker B complete: %v", err)
	}

	// Release Job 1's row lock.
	if err := locker.Commit(ctx); err != nil {
		t.Fatalf("commit locking transaction: %v", err)
	}

	// Job 1 remains pending and can now be claimed.
	next, err := repo.Claim(
		ctx,
		queue,
		"worker-C",
	)
	if err != nil {
		t.Fatalf("worker C claim: %v", err)
	}

	if next.ID != firstID {
		t.Fatalf(
			"expected Job 1 (%s), got %s",
			firstID,
			next.ID,
		)
	}

	if next.ClaimToken == uuid.Nil {
		t.Fatal("second claim token must not be empty")
	}

	err = repo.Complete(
		ctx,
		next.ID,
		"worker-C",
		next.ClaimToken,
	)
	if err != nil {
		t.Fatalf("worker C complete: %v", err)
	}

	var completed int

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM queue_jobs
		WHERE queue_name = $1
		  AND status = 'completed'
	`, queue).Scan(&completed)

	if err != nil {
		t.Fatalf("count completed jobs: %v", err)
	}

	if completed != 2 {
		t.Fatalf(
			"expected 2 completed jobs, got %d",
			completed,
		)
	}

	t.Log("Both jobs completed successfully")
}

// TestConcurrentWorkers_NoDuplicateClaims verifies
// that multiple workers can claim and complete jobs
// without claiming the same job more than once.
func TestConcurrentWorkers_NoDuplicateClaims(t *testing.T) {
	const (
		jobCount    = 40
		workerCount = 8
	)

	db := openTestDB(t)
	repo := NewRepository(db)
	queue := setupQueue(t, db)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	// Enqueue 40 jobs and track their UUIDs.
	expected := make(map[uuid.UUID]struct{}, jobCount)

	for i := 0; i < jobCount; i++ {
		jobID, err := repo.Enqueue(
			ctx,
			queue,
			fmt.Sprintf("job-%d", i),
		)
		if err != nil {
			t.Fatalf("enqueue job %d: %v", i, err)
		}

		expected[jobID] = struct{}{}
	}

	type claimResult struct {
		jobID  uuid.UUID
		worker string
		err    error
	}

	results := make(
		chan claimResult,
		jobCount+workerCount,
	)

	// Start workers simultaneously.
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		wg.Add(1)

		go func(worker string) {
			defer wg.Done()

			select {
			case <-start:
			case <-ctx.Done():
				results <- claimResult{
					worker: worker,
					err:    ctx.Err(),
				}
				return
			}

			for {
				job, err := repo.Claim(
					ctx,
					queue,
					worker,
				)

				// No eligible jobs remain.
				if errors.Is(err, ErrNoJob) {
					return
				}

				if err != nil {
					results <- claimResult{
						worker: worker,
						err:    err,
					}
					return
				}

				if job.ClaimToken == uuid.Nil {
					results <- claimResult{
						worker: worker,
						err: fmt.Errorf(
							"job %s has empty claim token",
							job.ID,
						),
					}
					return
				}

				// The claim statement has committed.
				// Processing occurs outside that
				// statement's row lock.
				err = repo.Complete(
					ctx,
					job.ID,
					worker,
					job.ClaimToken,
				)

				results <- claimResult{
					jobID:  job.ID,
					worker: worker,
					err:    err,
				}

				if err != nil {
					return
				}
			}
		}(workerID)
	}

	close(start)

	wg.Wait()
	close(results)

	// Validate all worker results.
	seen := make(map[uuid.UUID]string)

	successfulCompletions := 0

	for result := range results {
		if result.err != nil {
			t.Errorf(
				"worker %s failed: %v",
				result.worker,
				result.err,
			)
			continue
		}

		successfulCompletions++

		if _, exists := expected[result.jobID]; !exists {
			t.Errorf(
				"unexpected job claimed: %s",
				result.jobID,
			)
		}

		if previousWorker, exists := seen[result.jobID]; exists {
			t.Errorf(
				"duplicate job claim: %s processed by %s and %s",
				result.jobID,
				previousWorker,
				result.worker,
			)
		}

		seen[result.jobID] = result.worker
	}

	if successfulCompletions != jobCount {
		t.Errorf(
			"expected %d completions, got %d",
			jobCount,
			successfulCompletions,
		)
	}

	if len(seen) != jobCount {
		t.Errorf(
			"expected %d unique jobs, got %d",
			jobCount,
			len(seen),
		)
	}

	// Verify the actual persisted database state.
	var completed, pending, processing, failed int

	err := db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE status = 'completed'
			),
			COUNT(*) FILTER (
				WHERE status = 'pending'
			),
			COUNT(*) FILTER (
				WHERE status = 'processing'
			),
			COUNT(*) FILTER (
				WHERE status = 'failed'
			)
		FROM queue_jobs
		WHERE queue_name = $1
	`, queue).Scan(
		&completed,
		&pending,
		&processing,
		&failed,
	)

	if err != nil {
		t.Fatalf("read queue statistics: %v", err)
	}

	if completed != jobCount ||
		pending != 0 ||
		processing != 0 ||
		failed != 0 {
		t.Fatalf(
			"unexpected queue state: completed=%d pending=%d processing=%d failed=%d",
			completed,
			pending,
			processing,
			failed,
		)
	}

	t.Logf(
		"%d workers completed %d unique jobs",
		workerCount,
		len(seen),
	)

	t.Log("No duplicate claims detected")
}
