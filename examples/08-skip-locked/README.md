# 08 — PostgreSQL SKIP LOCKED: Concurrent Job Processing

**Building a PostgreSQL-backed job queue with concurrent workers, atomic job claiming, lease-based recovery, heartbeats, and fencing tokens using Go.**

Distributed backend systems frequently need to execute work asynchronously.

Examples include:

- Sending notifications.
- Processing orders.
- Generating reports.
- Reconciling failed workflows.
- Dispatching background tasks.
- Processing scheduled operations.

A common requirement is to allow multiple workers to process jobs concurrently without repeatedly competing for the same database row.

PostgreSQL provides `SELECT FOR UPDATE SKIP LOCKED`, which is particularly useful for queue-like workloads.

This project demonstrates how to build a database-backed worker coordination system and progressively extend it to recover from crashes and reject stale workers.

> **Core principle:** Atomic job claiming prevents concurrent claim conflicts. Reliable job processing additionally requires failure recovery, lease management, and idempotent business operations.

---

## 1. What We're Building

A shared PostgreSQL job queue with the following capabilities:

- Atomic job claiming.
- Concurrent worker coordination.
- Row-level locking with `FOR UPDATE SKIP LOCKED`.
- Unique job ownership tokens.
- Time-limited processing leases.
- Heartbeat-based lease renewal.
- Recovery of abandoned jobs.
- Maximum processing attempts.
- Terminal failed-job state.
- Graceful worker cancellation.
- Integration tests against real PostgreSQL.

The implementation deliberately separates **database claiming** from **business processing**.

Workers do not keep database transactions open while executing the job processor.

## 2. Architecture

```mermaid
flowchart TD
    A["Producer / API"] --> B["PostgreSQL queue_jobs"]
    B --> C["Worker 1"]
    B --> D["Worker 2"]
    B --> E["Worker 3"]

    C --> F["Claim with SKIP LOCKED"]
    D --> F
    E --> F

    F --> G["Persist owner, token and lease"]
    G --> H["Execute job outside transaction"]
    H --> I["Heartbeat while processing"]
    I --> J{"Outcome"}
    J -->|Success| K["Complete using claim token"]
    J -->|Failure or crash| L["Lease expires"]
    L --> M{"Attempts remaining?"}
    M -->|Yes| F
    M -->|No| N["Reaper marks failed"]
```

### Components

| Component | Responsibility |
|---|---|
| Producer | Inserts jobs into PostgreSQL |
| Repository | Atomically claims and updates jobs |
| Worker | Polls and executes business logic |
| Heartbeat | Renews an active processing lease |
| Claim token | Identifies a specific ownership generation |
| Reaper | Marks exhausted, expired jobs as failed |
| PostgreSQL | Stores durable state and coordinates row locks |

## 3. Project Structure

```text
08-skip-locked/
├── job.go
├── repository.go
├── repository_test.go
├── lease_test.go
├── concurrent_reclaim_test.go
├── worker.go
├── worker_test.go
└── README.md
```

Relevant migrations:

```text
migrations/
├── 000003_create_queue_jobs.up.sql
├── 000003_create_queue_jobs.down.sql
├── 000004_add_job_leases.up.sql
└── 000004_add_job_leases.down.sql
```

## 4. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| Database | PostgreSQL 16 |
| PostgreSQL driver | pgx/v5 |
| Connection pooling | pgxpool |
| Job identifiers | UUID |
| Ownership fencing | Unique UUID claim tokens |
| Concurrency testing | Goroutines, channels, WaitGroup |
| Containerization | Docker Compose |
| Testing | Go testing package |

---

# Part I — Understanding PostgreSQL Lock Contention

## 5. What Is Row-Level Locking?

PostgreSQL allows transactions to acquire row-level locks when they need to coordinate modifications to shared data.

A common locking query is:

```sql
SELECT id
FROM queue_jobs
WHERE status = 'pending'
FOR UPDATE;
```

A row selected using `FOR UPDATE` is locked against incompatible concurrent row operations until the transaction ends.

This is useful for preventing competing transactions from modifying the same row simultaneously.

However, a queue has an additional requirement: **workers should be able to claim other available jobs rather than waiting unnecessarily for a locked job.**

## 6. FOR UPDATE vs NOWAIT vs SKIP LOCKED

Suppose Job 1 is locked by Worker A, while Job 2 remains available.

| Locking mode | Worker B's behavior |
|---|---|
| `FOR UPDATE` | May wait for Job 1's lock |
| `FOR UPDATE NOWAIT` | Returns an error if the selected row cannot immediately be locked |
| `FOR UPDATE SKIP LOCKED` | Skips locked Job 1 and can claim Job 2 |

### Normal Locking

```sql
SELECT id
FROM queue_jobs
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE;
```

If the first eligible job is locked, another worker can wait for its lock.

### NOWAIT

```sql
SELECT id
FROM queue_jobs
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE NOWAIT;
```

`NOWAIT` fails immediately rather than waiting for a conflicting row lock.

### SKIP LOCKED

```sql
SELECT id
FROM queue_jobs
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` permits a worker to skip rows currently locked by other transactions and select another eligible row.

This is a trade-off: the query provides a deliberately incomplete view of matching rows.

That behavior is appropriate for work queues, where processing another available job is often preferable to waiting.

It is not appropriate for every business query requiring a consistent view of all matching records.

## 7. Why Ordinary SELECT Followed by UPDATE Is Unsafe

Consider the following application logic:

```text
Worker A                         Worker B
--------                         --------
SELECT first pending job         SELECT first pending job
Returns Job 1                    Returns Job 1

UPDATE Job 1 → processing        UPDATE Job 1 → processing

Execute Job 1                    Execute Job 1
```

Both workers can initially select the same pending job.

Without an appropriate atomic claim protocol, the system risks duplicate ownership and processing.

The solution is to coordinate selection and mutation in one database transaction.

---

# Part II — Atomic Job Claiming

## 8. Queue Schema

The queue stores job lifecycle and ownership information.

```sql
CREATE TABLE queue_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name TEXT NOT NULL,
    payload TEXT NOT NULL,

    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (
            status IN (
                'pending',
                'processing',
                'completed',
                'failed'
            )
        ),

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    locked_by TEXT,
    claimed_at TIMESTAMPTZ,

    attempt_count INT NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The subsequent lease migration adds:

```sql
ALTER TABLE queue_jobs
    ADD COLUMN claim_token UUID,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;
```

### Important Columns

| Column | Meaning |
|---|---|
| `id` | Unique job identifier |
| `queue_name` | Logical queue |
| `payload` | Job data |
| `status` | Current lifecycle state |
| `available_at` | Earliest processing eligibility |
| `locked_by` | Worker identity |
| `claim_token` | Unique token for the current claim |
| `claimed_at` | Time of the current claim |
| `lease_expires_at` | Current ownership deadline |
| `attempt_count` | Number of claim attempts |

## 9. Atomic Claim Query

Our repository uses a CTE to select one eligible job, lock it, and update its ownership state.

The relevant query structure is:

```sql
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
    j.lease_expires_at;
```

### How It Works

**Step 1 — Find an eligible job**

A job is eligible if it is pending and available, or is being processed under an expired lease.

**Step 2 — Exclude exhausted jobs**

The claim query checks the attempt budget.

**Step 3 — Lock the selected job**

`FOR UPDATE SKIP LOCKED` protects the claim against competing workers while permitting them to consider other unlocked rows.

**Step 4 — Assign ownership**

The database updates the job's status, worker identity, claim token, lease deadline, and attempt counter.

**Step 5 — Return the claimed job**

The worker receives the payload and its ownership token.

The claim statement commits before business processing starts.

### Important Distinction

The PostgreSQL row lock does not remain held throughout the job's execution.

After the claim commits, ownership is represented by persisted queue columns.

This avoids keeping long-running database transactions open while workers perform external processing.

## 10. Testing Concurrent Job Claims

Test:

```text
TestConcurrentWorkers_NoDuplicateClaims
```

The test inserts 40 jobs and starts 8 workers.

Each worker repeatedly:

1. Claims a job.
2. Processes it through the test's completion protocol.
3. Marks it completed using its claim token.
4. Attempts another claim.

The test verifies:

```text
Jobs inserted:         40
Concurrent workers:     8
Unique completed jobs: 40
Pending jobs:           0
Processing jobs:        0
Failed jobs:            0
```

No job should be claimed twice during this successful, non-expiring test scenario.

This does not establish exactly-once execution for arbitrary external side effects.

---

# Part III — Worker Leases and Crash Recovery

## 11. Why Job Ownership Needs an Expiration Time

Suppose Worker A claims a job and crashes.

Without a recovery mechanism:

```text
Job status: processing
Owner: Worker A
```

The job remains unavailable to other workers indefinitely.

A lease solves this by giving ownership a deadline.

Example:

```text
claimed_at:       10:00:00
lease_expires_at: 10:00:30
```

If the worker cannot renew or complete its claim before the deadline, the job becomes eligible for recovery.

## 12. Job Lease Lifecycle

```mermaid
flowchart TD
    A["Pending"] --> B["Worker A claims job"]
    B --> C["Processing; lease active"]
    C -->|Heartbeat| C
    C -->|Complete with valid token| D["Completed"]
    C -->|Lease expires| E["Eligible for reclaim"]
    E --> F["Worker B claims with new token"]
    F --> G["Processing; new lease"]
    G -->|Complete| D
```

A lease is a time-limited claim of ownership.

It is not a guarantee that the worker process has stopped executing when the deadline passes.

## 13. Reclaiming Abandoned Jobs

Our `Claim` operation also considers jobs satisfying:

```sql
status = 'processing'
AND lease_expires_at <= clock_timestamp()
```

When a worker claims an expired job, the repository:

- Assigns a new worker identity.
- Generates a new claim token.
- Sets a fresh lease deadline.
- Increments `attempt_count`.
- Keeps the job in `processing`.

This allows recovery without a separate transaction that must first reset each expired job to `pending`.

## 14. Why We Use clock_timestamp()

PostgreSQL distinguishes between transaction-start time and the actual wall-clock time.

`now()` returns the transaction-start timestamp.

`clock_timestamp()` returns the actual current time.

Lease expiry checks need to account for elapsed time during processing and lock waits.

Our repository uses `clock_timestamp()` for lease comparisons and renewal.

---

# Part IV — Claim Tokens and Fencing

## 15. The Stale-Worker Problem

Consider:

```text
Worker A claims Job 1.
Token = T1.

Worker A becomes unresponsive.
Its lease expires.

Worker B reclaims Job 1.
Token = T2.

Worker A resumes and calls Complete.
```

Without ownership validation, Worker A might incorrectly acknowledge the job after Worker B has taken over.

Checking only the worker name is insufficient.

Worker identities can be reused, and a single logical worker may obtain multiple claims over time.

## 16. Unique Claim Tokens

Every successful claim generates a new UUID:

```go
newToken := uuid.New()
```

The token is persisted in PostgreSQL and returned to the worker.

The worker must include this exact token in future ownership-sensitive operations.

### Completion Query

```sql
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
  AND lease_expires_at > clock_timestamp();
```

If the update affects zero rows, completion is rejected.

Possible reasons include:

- The lease expired.
- Another worker reclaimed the job.
- The claim token is incorrect.
- The job is no longer processing.
- The caller is not the current owner.

The repository returns `ErrNotOwner`.

## 17. What Claim Tokens Actually Guarantee

Our token protects ownership updates **inside the PostgreSQL queue**.

A stale worker cannot use its previous token to complete or renew a newly claimed job.

However, the token alone does not prevent external side effects.

For example, Worker A might continue calling an external payment or notification service after losing its lease.

Unless that external system enforces idempotency or a compatible fencing mechanism, duplicate effects remain possible.

A unique UUID token identifies a claim generation, but does not by itself provide an ordered fencing number that every downstream system can compare.

---

# Part V — Heartbeats

## 18. Why Heartbeats Are Necessary

Some jobs take longer than their initial lease.

If processing continues normally but the lease expires, another worker could reclaim the same job.

A heartbeat allows the active owner to extend its lease.

Our worker periodically calls:

```go
repo.Heartbeat(
    ctx,
    job.ID,
    workerID,
    job.ClaimToken,
)
```

The repository updates the lease only if the worker still holds the current, unexpired claim.

### Heartbeat Behavior

| Situation | Result |
|---|---|
| Correct owner and valid token | Lease renewed |
| Wrong claim token | Rejected |
| Lease already expired | Rejected |
| Job already completed | Rejected |
| Job reclaimed by another worker | Rejected |

## 19. Worker Behavior After Heartbeat Failure

A worker should treat a rejected heartbeat as possible loss of ownership.

Our implementation cancels the processor's context when heartbeat renewal fails.

This requests that the processing callback stop.

However, context cancellation is cooperative in Go.

A processor that ignores its context may continue executing.

For production workflows, cancellation behavior and external-operation idempotency must both be considered.

---

# Part VI — Concurrent Lease Reclamation

## 20. Multiple Workers Recover the Same Job

Test:

```text
TestConcurrentReclaim_OnlyOneWorkerWins
```

The scenario is:

1. Worker A claims Job 1.
2. Its lease is manually expired.
3. Four workers simultaneously call `Claim`.
4. One receives the new claim.
5. The others receive `ErrNoJob`.
6. Worker A attempts to complete using its stale token.
7. The stale completion is rejected.
8. The winning worker completes successfully.

### Expected Result

```text
Workers attempting reclaim: 4

Successful reclaim claims: 1
No-job responses:           3

New attempt count:          2
Final job status:           completed
```

The winning worker is not predetermined.

The test uses actual PostgreSQL concurrency, not simulated locking.

The claim transaction is atomic, preventing two successful claims of the same job in this controlled scenario.

---

# Part VII — Worker Lifecycle

## 21. Worker Processing Loop

File:

```text
worker.go
```

The worker exposes:

```go
ProcessOnce(ctx)
Run(ctx)
```

`ProcessOnce` attempts to claim and execute one eligible job.

`Run` repeatedly polls for work.

### Processing Flow

```mermaid
flowchart TD
    A["Worker running"] --> B["Claim eligible job"]
    B --> C{"Job found?"}
    C -->|No| D["Wait poll interval"]
    D --> B
    C -->|Yes| E["Start heartbeat loop"]
    E --> F["Execute processor"]
    F --> G{"Processing outcome"}
    G -->|Success| H["Stop heartbeat"]
    H --> I["Complete with current claim token"]
    I --> B
    G -->|Error or lease loss| J["Stop and return error"]
```

### Processor Interface

```go
type JobProcessor func(context.Context, Job) error
```

This allows the worker to execute different business operations without coupling queue management to one specific job type.

The callback should be designed for repeatable execution because jobs may be retried after failures.

## 22. Processing Happens Outside the Claim Transaction

A critical design decision is that PostgreSQL locks are released before job processing begins.

For example:

```text
BEGIN
Claim job
Assign token and lease
COMMIT

Call external service
Execute business logic
Write output

Complete job using token
```

We do not keep a transaction open across the entire processing operation.

Benefits include:

- Shorter database transactions.
- Reduced lock holding time.
- Lower connection occupancy during slow processing.
- Better concurrency between workers.

The trade-off is that processing and completion are not one atomic transaction.

That is why leases, retries, and idempotency are necessary.

## 23. Graceful Cancellation

Workers must stop when the parent context is cancelled.

The `Run` loop checks context cancellation before claiming more work.

Its polling delay is also cancellable.

During processing, the parent context is passed through to the processor.

This makes shutdown behavior explicit.

A production shutdown procedure should also define how much time active jobs are allowed to finish before cancellation is enforced.

---

# Part VIII — Failure Handling

## 24. Processing Failure

When a processor returns an error, our current worker returns that error and stops its processing loop.

The job remains in `processing` until its lease expires.

After expiry, another claim attempt may recover the job, provided the attempt budget has not been exhausted.

This is a deliberately simple recovery strategy.

An expanded implementation could support immediate retry scheduling, per-job exponential backoff, and structured failure reasons.

## 25. Maximum Attempts

A job cannot be retried indefinitely.

Our repository has a configurable maximum number of claim attempts.

Example:

```go
repo, err := NewRepositoryWithConfig(
    db,
    30*time.Second,
    3,
)
```

A job can be claimed up to three times under this configuration.

After an expired third claim, it is no longer eligible for another claim.

## 26. Terminal Failure and the Reaper

The reaper examines expired processing jobs whose attempt budgets are exhausted.

It marks them as `failed`.

The current implementation exposes:

```go
RunReaper(ctx, repo, queueName, interval)
```

The reaper periodically invokes:

```go
repo.ReapExhausted(ctx, queueName)
```

### Failure Lifecycle

```text
Attempt 1 → lease expires
Attempt 2 → lease expires
Attempt 3 → lease expires

Reaper executes

Status → failed
```

Failed jobs are not automatically reclaimed.

A separate administrative retry policy or dead-letter mechanism would be needed to reprocess them.

## 27. Testing Failure Recovery

The integration suite covers:

| Test | Verified behavior |
|---|---|
| `TestExpiredLease_ReclaimAndFenceStaleWorker` | Recovery and stale-owner rejection |
| `TestHeartbeat_ValidClaimCanRenew` | Lease renewal |
| `TestExpiredLease_MaxAttemptsBecomesFailed` | Attempt limit |
| `TestClaimToken_RejectsWrongToken` | Token validation |
| `TestConcurrentReclaim_OnlyOneWorkerWins` | Concurrent recovery exclusivity |
| `TestWorker_ProcessingFailureRemainsRecoverable` | Processing failure recovery |
| `TestWorker_RunStopsOnCancellation` | Graceful cancellation |
| `TestReaper_MarksExhaustedJobFailed` | Terminal failure cleanup |

The tests manipulate lease deadlines directly where appropriate to avoid long sleeps.

---

# Part IX — Delivery Guarantees and Limitations

## 28. Is This an Exactly-Once Job Queue?

No.

The queue provides atomic database claim ownership, but it does not guarantee that arbitrary job side effects execute exactly once.

Consider:

```text
Worker A claims Job 1

Worker A sends a notification

Notification succeeds

Worker A crashes before marking completed

Lease expires

Worker B reclaims Job 1

Worker B sends the notification again
```

The job was delivered twice, even though the queue's database state transitions were correctly coordinated.

This is a classic at-least-once processing scenario.

## 29. How to Handle Duplicate Execution

Depending on the business operation, useful techniques include:

**Idempotency keys**

Associate a stable business-operation key with each job and reject duplicate effects.

**Unique constraints**

Prevent duplicate database business records through schema-level uniqueness.

**Transactional outbox**

Durably coordinate database changes with downstream event publication.

**Downstream fencing**

Require an external resource to reject operations from older ownership generations.

**Idempotent consumers**

Design consumers so replaying the same event does not duplicate the intended outcome.

The appropriate approach depends on which system owns the side effect.

## 30. Leases Are Not Distributed Mutexes

A lease can expire while the original worker is still running.

Therefore, valid ownership at the beginning of processing does not guarantee ownership throughout execution.

An old worker may continue executing after another worker acquires the job.

The database prevents outdated completion operations, but outside systems need their own protection where duplicate concurrent execution matters.

## 31. Queue Ordering and Fairness

`SKIP LOCKED` prioritizes throughput and progress over a globally consistent FIFO view.

A worker may skip an older job because it is locked and process a newer one.

A busy or repeatedly locked job may also experience delays.

Therefore, the query's `ORDER BY` should be treated as a preference among selectable jobs, not a strict guarantee that every job is processed in exact arrival order.

## 32. Queue Scaling Considerations

A PostgreSQL-backed queue can be a reasonable choice when jobs need to share transactional infrastructure with application data.

However, scaling introduces concerns such as:

- Queue table growth.
- Index maintenance.
- Frequent status updates.
- Autovacuum pressure.
- Database connection consumption.
- Polling load.
- Hot rows and queue contention.
- Large payload storage.
- Job retention and archival.

Completed and failed jobs should generally have a retention strategy.

For larger or more demanding workloads, dedicated messaging systems may provide advantages in throughput, partitioning, retention, and operational independence.

The decision should consider workload characteristics rather than assuming one queue technology is universally superior.

---

# Part X — Running the Project

## 33. Prerequisites

- Go installed.
- Docker Desktop running.
- PostgreSQL 16 available through the repository's Docker Compose configuration.
- Migrations `000003` and `000004` applied.
- Database connection configured.

Start PostgreSQL from the repository root:

```powershell
docker compose up -d
```

Set the connection string in PowerShell if needed:

```powershell
$env:DATABASE_URL = "postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable"
```

Apply pending migrations:

```powershell
migrate -path ./migrations -database $env:DATABASE_URL up
```

Expected current migration version:

```text
4
```

## 34. Running Integration Tests

Run all Demo 08 tests:

```powershell
go test -v ./examples/08-skip-locked -count=1
```

Run the Go race detector:

```powershell
go test -race ./examples/08-skip-locked -count=1
```

Run all repository tests:

```powershell
go test ./... -count=1
```

The race detector identifies Go memory-access races, not SQL concurrency anomalies by itself.

Our PostgreSQL integration tests validate database ownership and state transitions.

## 35. Expected Test Coverage

```text
TestClaim_SkipsLockedJob
TestConcurrentWorkers_NoDuplicateClaims

TestExpiredLease_ReclaimAndFenceStaleWorker
TestHeartbeat_ValidClaimCanRenew
TestExpiredLease_MaxAttemptsBecomesFailed
TestClaimToken_RejectsWrongToken

TestConcurrentReclaim_OnlyOneWorkerWins

TestWorker_ProcessOnceCompletesJob
TestWorker_ProcessingFailureRemainsRecoverable
TestWorker_RunStopsOnCancellation
TestReaper_MarksExhaustedJobFailed
```

These tests cover job claiming, ownership fencing, recovery, worker lifecycle, and retry exhaustion.

---

# Part XI — Engineering Takeaways

**1. `SKIP LOCKED` enables concurrent queue consumers to make progress without waiting on locked job rows.**

**2. Selecting and claiming a job must be atomic.** A separate unprotected read and update can produce duplicate ownership.

**3. Row locks should be held only for the short claim transaction.** Business processing should happen after the claim commits.

**4. Lease expiry enables abandoned-job recovery.** A crashed worker cannot permanently reserve a processing job.

**5. Every claim needs a new ownership generation.** Our UUID claim token prevents stale workers from changing queue ownership state.

**6. Heartbeats extend leases but do not guarantee that a worker is alive or that its external effects are safe.**

**7. A lease can expire while the original worker is still running.** Fencing and idempotency remain necessary for external operations.

**8. Processing must tolerate replay.** At-least-once job execution can duplicate effects unless business operations are idempotent.

**9. Retry budgets limit repeated failures.** Exhausted jobs should enter an observable terminal state.

**10. PostgreSQL is both a storage engine and a coordination mechanism, but queue throughput and maintenance must be monitored.**

---

## What's Next?

### Demo 09 — PostgreSQL Advisory Locks

The next demonstration explores coordination that does not naturally map to locking one existing database row.

We'll compare:

- Session-level advisory locks.
- Transaction-level advisory locks.
- Blocking and nonblocking acquisition.
- Lock ownership and release.
- Connection-pool implications.
- Distributed coordination across application instances.
- When advisory locks are appropriate.
- When advisory locks are dangerous.

We'll implement reproducible Go integration tests using PostgreSQL's advisory-lock functions and inspect lock behavior through `pg_locks`.

---

Part of [Concurrency Control in Practice](../../README.md) — reproducible PostgreSQL concurrency demonstrations using Go, real database transactions, and integration tests.