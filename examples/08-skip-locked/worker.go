package skiplocked

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// JobProcessor contains the business logic executed
// for a claimed job.
//
// It must honor context cancellation. Processing
// may run more than once after crashes or lease loss.
type JobProcessor func(context.Context, Job) error

type Worker struct {
	repo              *Repository
	queueName         string
	workerID          string
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	process           JobProcessor
}

func NewWorker(
	repo *Repository,
	queueName string,
	workerID string,
	pollInterval time.Duration,
	heartbeatInterval time.Duration,
	process JobProcessor,
) (*Worker, error) {
	if repo == nil ||
		repo.db == nil ||
		strings.TrimSpace(queueName) == "" ||
		strings.TrimSpace(workerID) == "" ||
		pollInterval <= 0 ||
		heartbeatInterval <= 0 ||
		heartbeatInterval >= repo.leaseDuration ||
		process == nil {
		return nil, ErrBadRequest
	}

	return &Worker{
		repo:              repo,
		queueName:         queueName,
		workerID:          workerID,
		pollInterval:      pollInterval,
		heartbeatInterval: heartbeatInterval,
		process:           process,
	}, nil
}

// ProcessOnce tries to claim and process one job.
//
// claimed=false means no eligible job was available.
// An error means processing, ownership, or database
// operations failed.
//
// After a processing error, the claim is deliberately
// left in processing until its lease expires.
func (w *Worker) ProcessOnce(
	ctx context.Context,
) (claimed bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	job, err := w.repo.Claim(
		ctx,
		w.queueName,
		w.workerID,
	)

	if errors.Is(err, ErrNoJob) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, w.processClaim(ctx, job)
}

func (w *Worker) processClaim(
	ctx context.Context,
	job Job,
) error {
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeatResult := make(chan error, 1)

	// Heartbeat runs concurrently with processing.
	go func() {
		ticker := time.NewTicker(w.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-processCtx.Done():
				heartbeatResult <- nil
				return

			case <-ticker.C:
				_, err := w.repo.Heartbeat(
					processCtx,
					job.ID,
					w.workerID,
					job.ClaimToken,
				)

				if err != nil {
					// Stop processing when the claim
					// cannot be renewed.
					cancel()
					heartbeatResult <- err
					return
				}
			}
		}
	}()

	// Execute business logic after the claim has
	// committed and released its row lock.
	processErr := w.process(processCtx, job)

	// Stop heartbeats and wait for the goroutine
	// before acknowledging the job.
	cancel()
	heartbeatErr := <-heartbeatResult

	if err := ctx.Err(); err != nil {
		return err
	}

	if heartbeatErr != nil {
		return fmt.Errorf(
			"job %s lost heartbeat: %w",
			job.ID,
			heartbeatErr,
		)
	}

	if processErr != nil {
		return fmt.Errorf(
			"process job %s: %w",
			job.ID,
			processErr,
		)
	}

	// Complete uses the original parent context,
	// not the cancelled heartbeat context.
	//
	// The repository verifies token ownership
	// and checks lease expiry atomically.
	if err := w.repo.Complete(
		ctx,
		job.ID,
		w.workerID,
		job.ClaimToken,
	); err != nil {
		return fmt.Errorf(
			"complete job %s: %w",
			job.ID,
			err,
		)
	}

	return nil
}

// Run continuously claims and processes jobs.
//
// It returns on context cancellation or an error.
// A production supervisor may restart workers with
// an appropriate backoff and error policy.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		claimed, err := w.ProcessOnce(ctx)
		if err != nil {
			return err
		}

		if claimed {
			// Immediately look for the next job.
			continue
		}

		// Nothing eligible: poll again later.
		timer := time.NewTimer(w.pollInterval)

		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()

		case <-timer.C:
		}
	}
}

// RunReaper periodically marks expired claims
// with exhausted attempt budgets as failed.
//
// Other expired jobs are reclaimed directly by
// the normal Claim query.
func RunReaper(
	ctx context.Context,
	repo *Repository,
	queueName string,
	interval time.Duration,
) error {
	if repo == nil ||
		repo.db == nil ||
		strings.TrimSpace(queueName) == "" ||
		interval <= 0 {
		return ErrBadRequest
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if _, err := repo.ReapExhausted(
			ctx, queueName,
		); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
		}
	}
}
