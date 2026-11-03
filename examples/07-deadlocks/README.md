# 07 — PostgreSQL Deadlocks

**Understanding database deadlocks, circular lock dependencies, SQLSTATE `40P01`, consistent lock ordering, and transaction retries using Go and PostgreSQL.**

Database transactions protect consistency, but concurrent transactions can still block one another while acquiring locks.

When multiple transactions acquire conflicting locks in incompatible orders, they can form a circular dependency where none can proceed.

This condition is called a **deadlock**.

PostgreSQL automatically detects deadlocks and resolves them by aborting one of the participating transactions. However, applications must still handle the resulting transaction failure.

This example demonstrates how to reproduce, prevent, detect, and recover from PostgreSQL deadlocks using real database transactions.

> **Core principle:** Acquiring the correct locks is not enough. The order in which concurrent transactions acquire those locks also matters.

---

## 1. What We're Exploring

This demonstration covers:

- Row-level locking using `SELECT FOR UPDATE`.
- Circular lock dependencies.
- PostgreSQL deadlock detection.
- SQLSTATE `40P01`.
- Atomic wallet transfers.
- Deterministic concurrency testing.
- Consistent lock acquisition ordering.
- Whole-transaction retry handling.
- Exponential backoff and jitter.
- Context cancellation and retry exhaustion.
- Financial invariants under concurrent transfers.

All main concurrency tests run against PostgreSQL rather than mocked database transactions.

## 2. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| Database | PostgreSQL 16 |
| Driver | pgx/v5 |
| Connection pool | pgxpool |
| Containerization | Docker Compose |
| Testing | Go testing package |
| Synchronization | Channels, sync.WaitGroup, atomic counters |
| Error handling | PostgreSQL SQLSTATE codes |

## 3. Project Structure

```text
07-deadlocks/
├── transfer.go
├── deadlock_test.go
├── ordered_locking_test.go
├── deadlock_retry.go
├── deadlock_retry_test.go
└── README.md
```

### File Responsibilities

| File | Responsibility |
|---|---|
| `transfer.go` | Transfer model, wallet locking, debit and credit operations |
| `deadlock_test.go` | Reproduces a circular lock dependency |
| `ordered_locking_test.go` | Demonstrates consistent lock ordering |
| `deadlock_retry.go` | Implements configurable deadlock retries |
| `deadlock_retry_test.go` | Tests recovery, exhaustion, cancellation and error classification |

The demo reuses the existing `wallets` table.

No new migration is required.

---

# Part I — Understanding Deadlocks

## 4. What Is a Deadlock?

A database deadlock occurs when transactions form a circular dependency while waiting for locks.

Consider two wallets:

| Wallet | Balance |
|---|---:|
| Wallet A | 1000 |
| Wallet B | 1000 |

Two concurrent transactions attempt transfers:

**Transaction T1:** Transfer 100 from A to B.

**Transaction T2:** Transfer 200 from B to A.

Both transactions lock their source wallet first.

### Execution Schedule

```text
Transaction T1                 Transaction T2
--------------                 --------------
BEGIN                          BEGIN

LOCK Wallet A
                               LOCK Wallet B

LOCK Wallet B                  LOCK Wallet A
    |                              |
    v                              v
Waiting for T2                 Waiting for T1
    |                              |
    +--------------+---------------+
                   |
              DEADLOCK
```

T1 holds Wallet A and waits for Wallet B.

T2 holds Wallet B and waits for Wallet A.

Neither can acquire its second lock until the other transaction releases its lock.

Since both transactions are waiting, neither can finish normally.

## 5. Understanding the Wait-For Graph

A wait-for graph represents transaction dependencies.

Each node represents a transaction.

A directed edge `T1 → T2` means Transaction T1 is waiting for a resource held by Transaction T2.

For the wallet-transfer scenario:

```mermaid
flowchart LR
    A["Transaction T1<br/>Holds Wallet A"] -->|"Waits for Wallet B"| B["Transaction T2<br/>Holds Wallet B"]
    B -->|"Waits for Wallet A"| A
```

The graph contains a cycle:

```text
T1 → T2 → T1
```

That cycle represents a deadlock.

A transaction waiting for another transaction is not automatically deadlocked.

For example:

```text
T1 → T2
```

is ordinary lock contention.

But:

```text
T1 → T2 → T1
```

is a circular dependency.

### Important Distinction

**Lock contention** means a transaction must wait.

**Deadlock** means the waiting dependencies form a cycle that cannot resolve without intervention.

## 6. PostgreSQL Deadlock Detection

PostgreSQL detects deadlocks and aborts one participating transaction.

The database reports:

```text
SQLSTATE 40P01
deadlock_detected
```

PostgreSQL does not guarantee which transaction will be selected as the deadlock victim.

Applications must not depend on a particular transaction being aborted.

After PostgreSQL resolves the deadlock:

1. One transaction receives the deadlock error.
2. Its transaction is aborted.
3. Its locks are released when the aborted transaction is rolled back.
4. The surviving transaction can acquire the required lock.
5. The surviving transaction can continue toward commit.

### deadlock_timeout

PostgreSQL has a configuration parameter named `deadlock_timeout`.

It controls how long PostgreSQL waits before checking for a deadlock after a lock wait begins.

The default is typically one second.

This is different from `lock_timeout`, which limits how long a statement can wait to acquire a lock.

Our tests do not require changing either configuration.

---

# Part II — Atomic Wallet Transfers

## 7. Why Wallet Transfers Need Transactions

A transfer modifies two wallet balances:

```text
Wallet A: -100
Wallet B: +100
```

The operation must be atomic.

Either both changes commit, or neither change commits.

Otherwise, failures can produce inconsistent financial state.

### Unsafe Approach

```text
UPDATE Wallet A: -100
COMMIT

UPDATE Wallet B: +100
COMMIT
```

If the second operation fails, Wallet A has already been debited.

### Correct Approach

```text
BEGIN

Lock required wallet rows

Validate source balance

Debit source wallet

Credit destination wallet

COMMIT
```

If anything fails before commit, roll back the entire transaction.

## 8. Locking Wallet Rows

Our implementation acquires a row lock using:

```sql
SELECT id, balance
FROM wallets
WHERE id = $1
FOR UPDATE;
```

`FOR UPDATE` requests a row-level lock that conflicts with other transactions attempting incompatible operations on the same row.

The lock is held until the transaction ends.

This allows us to safely validate the source balance before making the transfer.

### Applying the Transfer

Debit:

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2;
```

Credit:

```sql
UPDATE wallets
SET balance = balance + $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2;
```

Both SQL statements execute inside the same transaction.

The transfer validates that the source has sufficient funds before applying changes.

The wallet schema also contains a nonnegative-balance constraint as an additional safeguard.

---

# Part III — Reproducing a Real Deadlock

## 9. Intentionally Unsafe Lock Ordering

Our first implementation acquires locks according to transfer direction:

```text
Lock source wallet
Lock destination wallet
```

For A → B:

```text
LOCK A
LOCK B
```

For B → A:

```text
LOCK B
LOCK A
```

This creates the possibility of circular waiting.

### Why This Is Dangerous

Each transfer's lock order changes depending on which wallet is the source.

Two opposing transfers can therefore hold each other's required locks.

This is not a Go data race.

The issue occurs inside PostgreSQL's lock manager.

## 10. Deterministic Deadlock Reproduction

File:

```text
deadlock_test.go
```

Test:

```text
TestDeadlock_OppositeLockOrder
```

The test uses a two-party synchronization barrier.

### Execution

1. Transaction T1 begins.
2. Transaction T2 begins.
3. T1 locks Wallet A.
4. T2 locks Wallet B.
5. Both transactions reach the barrier.
6. The barrier releases both.
7. T1 requests Wallet B.
8. T2 requests Wallet A.
9. PostgreSQL detects the resulting deadlock.
10. One transaction aborts, while the other can complete.

### Why Use a Barrier?

Without synchronization, the following schedule might occur:

```text
T1 locks A
T1 locks B
T1 commits

T2 locks B
T2 locks A
T2 commits
```

No deadlock occurs because T1 finishes before T2 holds a conflicting lock.

A timing-dependent test would therefore be unreliable.

The barrier forces both transactions to acquire their first locks before either attempts the second.

This makes the circular dependency deterministic without arbitrary sleep durations.

## 11. Detecting SQLSTATE 40P01

The Go PostgreSQL driver exposes server errors through `pgconn.PgError`.

We check the SQLSTATE rather than comparing error-message strings:

```go
func IsDeadlockError(err error) bool {
    var pgErr *pgconn.PgError

    return errors.As(err, &pgErr) &&
        pgErr.Code == "40P01"
}
```

This approach also works when the database error has been wrapped using Go's `%w` formatting.

### Expected Outcome

Exactly one transfer commits.

The other fails with `40P01`.

The successful transfer can be either direction.

If A → B commits:

```text
Wallet A = 900
Wallet B = 1100
```

If B → A commits:

```text
Wallet A = 1200
Wallet B = 800
```

In either case:

```text
Wallet A + Wallet B = 2000
```

The test verifies that the failed transaction did not partially modify the wallets.

### Run the Test

```powershell
go test -v ./examples/07-deadlocks -run TestDeadlock_OppositeLockOrder -count=1
```

---

# Part IV — Deadlock Prevention

## 12. Consistent Lock Ordering

The most important preventative technique demonstrated here is **consistent lock ordering**.

Instead of locking wallets according to transfer direction, every transaction locks wallets according to a shared global order.

In our example, wallet UUIDs are sorted in ascending byte order.

```text
firstID = min(fromID, toID)
secondID = max(fromID, toID)
```

Both transfers acquire locks using:

```text
LOCK firstID
LOCK secondID
```

### Why This Works

Suppose Wallet A sorts before Wallet B.

For A → B:

```text
LOCK A
LOCK B
```

For B → A:

```text
LOCK A
LOCK B
```

Both transactions now acquire the same resources in the same order.

If one transaction already holds A, the other waits for A before attempting to lock B.

This prevents the specific two-wallet circular wait demonstrated earlier.

### Execution Example

```text
Transaction T1                 Transaction T2
--------------                 --------------
LOCK A
                               Attempts LOCK A
                               Waits

LOCK B

Debit A
Credit B

COMMIT
                               Acquires LOCK A
                               Acquires LOCK B

                               Debit B
                               Credit A

                               COMMIT
```

Both transfers can complete without forming a circular dependency.

### Source Balance Validation

The source wallet is not necessarily the first wallet locked.

For a reverse-direction transfer, the source may be the second wallet.

Therefore, the implementation must identify the source wallet independently of lock order before checking available funds.

This distinction is important: **lock acquisition order must not change business semantics.**

## 13. Ordered Locking Integration Test

File:

```text
ordered_locking_test.go
```

Test:

```text
TestTransfersWithConsistentLockOrder
```

Both transfers run concurrently.

Expected results:

| Wallet | Initial | Final |
|---|---:|---:|
| A | 1000 | 1100 |
| B | 1000 | 900 |
| Total | 2000 | 2000 |

Both transfers should commit successfully.

Run:

```powershell
go test -v ./examples/07-deadlocks -run TestTransfersWithConsistentLockOrder -count=1
```

### Limitation of Lock Ordering

Consistent wallet-row ordering prevents deadlocks caused by acquiring those two wallet locks in opposite orders.

It does not prove that a complete production system is deadlock-free.

Other operations, triggers, foreign-key interactions, or inconsistent lock acquisition patterns may introduce additional dependencies.

All relevant code paths must follow compatible locking conventions.

---

# Part V — Recovering from Deadlocks

## 14. Why Retry Deadlocked Transactions?

Even with careful lock ordering, production systems may encounter deadlocks.

PostgreSQL resolves a deadlock by aborting a transaction rather than automatically replaying the application's business operation.

A confirmed `40P01` therefore represents a failed transaction attempt that may be retried safely if the operation can be replayed correctly.

### Incorrect Approach

```text
BEGIN
Lock A
Lock B
Debit A
Credit B

Deadlock detected

Retry only the failed statement
```

Once PostgreSQL aborts the transaction, the previous transaction cannot simply continue normally.

Any successful statements executed earlier in that failed transaction also belong to the aborted attempt.

### Correct Approach

```text
Start a NEW transaction

Acquire required locks again

Read current balances

Revalidate sufficient funds

Debit source

Credit destination

Attempt COMMIT
```

The entire business transaction must execute again.

## 15. The Retry Policy

File:

```text
deadlock_retry.go
```

The implementation uses:

```go
type DeadlockRetryPolicy struct {
    MaxRetries int
    BaseDelay  time.Duration
    MaxDelay   time.Duration
}
```

Example configuration:

```go
policy := DeadlockRetryPolicy{
    MaxRetries: 3,
    BaseDelay:  5 * time.Millisecond,
    MaxDelay:   40 * time.Millisecond,
}
```

`MaxRetries` excludes the initial transaction attempt.

This example permits up to four total attempts:

```text
Attempt 1: Initial attempt
Attempt 2: First retry
Attempt 3: Second retry
Attempt 4: Third retry
```

### Retry Decision

The policy follows:

```mermaid
flowchart TD
    A[Execute full transaction] --> B{Result}
    B -->|Success| C[Return success]
    B -->|Business error| D[Return error]
    B -->|SQLSTATE 40P01| E{Retries remaining?}
    B -->|Other DB error| F[Return error]
    E -->|Yes| G[Exponential backoff with jitter]
    G --> A
    E -->|No| H[Return retry exhaustion]
```

We deliberately do not retry all database errors.

Examples of non-retryable outcomes in this implementation include invalid transfer input, missing wallets, and insufficient funds.

## 16. Exponential Backoff and Jitter

Retries can increase contention if every failed transaction immediately attempts the same operation again.

Our policy introduces bounded exponential backoff.

Example maximum delays:

| Retry | Backoff cap |
|---|---:|
| First retry | 5 ms |
| Second retry | 10 ms |
| Third retry | 20 ms |

Actual delays are randomized using full jitter.

This reduces the chance of many clients retrying at the same instant.

The delay values in this repository are intentionally small for local tests. Production values should be determined using workload characteristics, transaction duration, contention patterns, and latency requirements.

## 17. Context Cancellation

Retry handling must respect the request context.

If a caller cancels its request:

- No further transaction attempt should begin.
- A pending retry delay should stop.
- The executor should return the cancellation error.

This prevents retry work from continuing unnecessarily after the request has been cancelled.

## 18. Why the Barrier Is Only Used Once

The actual deadlock recovery test uses the synchronization barrier only for each operation's initial attempt.

That forces a real deadlock.

After PostgreSQL aborts one transaction, the retry runs without the barrier.

This is essential.

Reusing the barrier on retries would make the test synchronization logic interfere with the recovery process.

The barrier exists solely to reproduce the concurrency anomaly.

It is not part of the production transfer algorithm.

---

# Part VI — Deadlock Recovery Testing

## 19. Integration Test: Both Transfers Eventually Succeed

File:

```text
deadlock_retry_test.go
```

Test:

```text
TestDeadlockRetry_BothTransfersEventuallySucceed
```

Initial balances:

```text
Wallet A = 1000
Wallet B = 1000
```

Concurrent operations:

```text
A → B: 100
B → A: 200
```

### First Attempts

Both transactions acquire their source-wallet locks.

The barrier releases them simultaneously.

They attempt to acquire the opposing wallet locks.

PostgreSQL detects the deadlock.

One transaction aborts with SQLSTATE `40P01`.

The other completes successfully.

### Recovery

The losing operation waits according to the retry policy.

It then starts a new transaction.

It reacquires the wallet locks and reads the current source balance.

If sufficient funds remain, it executes the transfer and commits.

### Expected Results

```text
Successful transfers: 2
Deadlock failures during initial attempts: 1
Total transaction attempts: 3

Final Wallet A = 1100
Final Wallet B = 900

Total funds = 2000
```

Both wallet version counters should be incremented twice because each successful transfer updates both wallets.

The aborted transaction attempt must not contribute a committed balance or version change.

## 20. Retry Exhaustion

Test:

```text
TestDeadlockRetry_Exhaustion
```

The retry policy is configured with:

```go
MaxRetries: 2
```

The simulated transaction operation returns `40P01` every time.

Expected behavior:

```text
Attempt 1 → deadlock
Attempt 2 → deadlock
Attempt 3 → deadlock

Retry budget exhausted
```

The returned error preserves the underlying PostgreSQL deadlock error, allowing callers to inspect its SQLSTATE.

## 21. Non-Retryable Errors

Test:

```text
TestDeadlockRetry_NonRetryableError
```

An insufficient-funds error should not trigger a deadlock retry.

Expected behavior:

```text
Attempt 1 → insufficient funds

Return immediately
```

Retrying a business-rule failure without a reason to expect the state to change only adds unnecessary database work.

## 22. Context Cancellation

Test:

```text
TestDeadlockRetry_ContextCancellation
```

The test cancels the context after an initial simulated deadlock.

The retry executor must stop without starting another attempt.

Expected:

```text
Transaction attempts = 1
Returned error = context.Canceled
```

## 23. Invalid Retry Configuration

Test:

```text
TestDeadlockRetry_InvalidConfiguration
```

The retry executor rejects invalid policy settings such as a negative retry count.

The policy also validates delay settings.

Configuration validation prevents silent or unpredictable retry behavior.

---

# Part VII — Production Engineering

## 24. Prevention vs Recovery

| Property | Consistent lock ordering | Deadlock retry |
|---|---|---|
| Main objective | Prevent circular lock waits | Recover after deadlock detection |
| Requires transaction replay | No | Yes |
| Adds retry latency | No | Potentially |
| Handles deadlocks from unknown code paths | Not necessarily | Can recover from detected `40P01` |
| Requires idempotent or replay-safe design | Not specifically | Yes |
| Recommended production use | Primary prevention | Bounded recovery mechanism |

These approaches are complementary rather than mutually exclusive.

Use consistent ordering to reduce the probability of deadlocks, then handle relevant transient failures where appropriate.

## 25. Deadlocks vs Lock Timeouts vs Serialization Failures

| PostgreSQL SQLSTATE | Error | Meaning |
|---|---|---|
| `40P01` | `deadlock_detected` | PostgreSQL detected a circular lock dependency |
| `55P03` | `lock_not_available` | A requested lock was not available, including lock timeout cases |
| `40001` | `serialization_failure` | Transaction could not complete while preserving required serialization semantics |
| `57014` | `query_canceled` | Statement cancelled, including statement timeout cases |

These errors are not interchangeable.

A retry policy should explicitly classify the errors it intends to handle.

This example retries only `40P01`.

## 26. Ambiguous Commit Outcomes

A confirmed deadlock error means the affected transaction attempt was aborted.

However, consider a different case:

```text
Client sends COMMIT
          |
Connection breaks
          |
Client does not receive a response
```

The client may not know whether the transaction committed.

Blindly retrying a non-idempotent transfer could apply it twice.

Production transfer systems should use an appropriate idempotency mechanism or durable operation record to recover from ambiguous outcomes.

**Deadlock recovery is not a replacement for idempotency.**

## 27. Avoid External Side Effects Inside Retried Transactions

A transaction callback may execute multiple times.

Avoid directly performing irreversible external actions inside it, such as:

- Sending notifications.
- Publishing external events without durable coordination.
- Calling payment providers.
- Triggering irreversible third-party operations.

If the database transaction is rolled back, those external effects may already have occurred.

A transactional outbox is one possible solution when database changes and event publication must be coordinated reliably.

## 28. Keep Transactions Short

Long-running transactions hold locks longer and can increase contention.

Avoid performing slow external calls while row locks are held.

For wallet transfers, a practical sequence is:

```text
Validate request

BEGIN

Acquire wallet locks
Read balances
Validate sufficient funds
Debit source
Credit destination

COMMIT

Perform post-commit processing
```

External processing may need durable coordination when delivery guarantees matter.

## 29. Monitoring Lock Contention

PostgreSQL exposes lock information through `pg_locks` and session information through `pg_stat_activity`.

Example:

```sql
SELECT
    a.pid,
    a.state,
    a.wait_event_type,
    a.wait_event,
    a.xact_start,
    now() - a.xact_start AS transaction_age,
    a.query
FROM pg_stat_activity a
WHERE a.wait_event_type = 'Lock'
ORDER BY a.xact_start;
```

To inspect locks:

```sql
SELECT
    pid,
    locktype,
    mode,
    granted,
    relation::regclass AS relation
FROM pg_locks
WHERE pid IS NOT NULL
ORDER BY pid, granted;
```

These queries help diagnose lock waits but do not, by themselves, reconstruct every dependency in a complex wait-for graph.

For identifying blockers, PostgreSQL provides `pg_blocking_pids()`.

Example:

```sql
SELECT
    pid,
    pg_blocking_pids(pid) AS blocking_pids,
    wait_event_type,
    wait_event,
    query
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0;
```

During an actual deadlock, the window for observing the cycle may be short because PostgreSQL detects it and aborts a participant.

Server logs are therefore also important for diagnosing deadlock incidents.

## 30. Useful Production Metrics

Potential application metrics include:

```text
db_deadlocks_total
db_transaction_retry_attempts_total
db_transaction_retry_exhausted_total
db_lock_wait_duration_seconds
db_transaction_duration_seconds
wallet_transfer_failures_total
```

These are proposed production metrics, not instrumentation currently implemented in this example.

Monitoring should distinguish deadlock failures from other database errors.

---

# Part VIII — Running the Demo

## 31. Prerequisites

- Go installed.
- Docker Desktop running.
- PostgreSQL 16 container available.
- Existing wallet migration applied.
- Database connection configured.

Start the database from the repository root:

```powershell
docker compose up -d
```

Set the database URL if needed:

```powershell
$env:DATABASE_URL = "postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable"
```

The tests also support loading the connection string from the repository's root `.env` file.

## 32. Run Individual Tests

### Reproduce the Deadlock

```powershell
go test -v ./examples/07-deadlocks -run TestDeadlock_OppositeLockOrder -count=1
```

### Verify Consistent Lock Ordering

```powershell
go test -v ./examples/07-deadlocks -run TestTransfersWithConsistentLockOrder -count=1
```

### Verify Retry Recovery

```powershell
go test -v ./examples/07-deadlocks -run TestDeadlockRetry_BothTransfersEventuallySucceed -count=1
```

### Run All Deadlock Tests

```powershell
go test -v ./examples/07-deadlocks -count=1
```

### Run the Full Repository Suite

```powershell
go test ./... -count=1
```

### Run the Go Race Detector

```powershell
go test -race ./examples/07-deadlocks -count=1
```

The Go race detector examines Go memory-access races.

It does not replace database concurrency tests or detect PostgreSQL deadlocks directly.

## 33. Expected Test Coverage

| Test | Validated behavior |
|---|---|
| `TestDeadlock_OppositeLockOrder` | Detects a real PostgreSQL deadlock |
| `TestTransfersWithConsistentLockOrder` | Both transfers complete with global lock ordering |
| `TestDeadlockRetry_BothTransfersEventuallySucceed` | Aborted transfer succeeds after a full retry |
| `TestDeadlockRetry_Exhaustion` | Retry budget is enforced |
| `TestDeadlockRetry_NonRetryableError` | Business errors are not retried |
| `TestDeadlockRetry_ContextCancellation` | Cancellation stops retries |
| `TestDeadlockRetry_InvalidConfiguration` | Invalid policy values are rejected |

The tests validate both concurrency behavior and business invariants.

---

# Part IX — Engineering Takeaways

**1. Correct individual transactions can still deadlock.**

The problem may be lock acquisition order rather than invalid SQL or incorrect business logic.

**2. Deadlock and lock contention are different.**

Waiting for a lock is normal; a circular dependency requires intervention.

**3. PostgreSQL detects deadlocks automatically.**

It aborts a participating transaction and reports SQLSTATE `40P01`.

**4. Victim selection is not predictable.**

Applications should handle either competing transaction being aborted.

**5. Consistent lock ordering is a powerful prevention technique.**

All relevant transactions should acquire shared resources in a compatible order.

**6. Multi-row operations require atomic transactions.**

A wallet transfer must commit both debit and credit together.

**7. A deadlock retry must restart the entire transaction.**

Locks, reads, validation, and writes need to execute again.

**8. Retry policies must be bounded.**

Exponential backoff, jitter, deadlines, and retry limits help control contention and latency.

**9. Confirmed aborts and uncertain commits are different failure modes.**

Idempotency remains necessary when a client cannot determine whether an operation committed.

**10. Production systems benefit from both prevention and recovery.**

Consistent lock ordering reduces deadlocks, while well-designed retries handle those that still occur.

---

## What's Next?

### Demo 08 — PostgreSQL Lock Contention and SKIP LOCKED

The next example explores how multiple concurrent workers safely claim jobs from a shared PostgreSQL-backed queue.

We'll demonstrate:

- Lock contention between workers.
- `SELECT FOR UPDATE`.
- `NOWAIT`.
- `SKIP LOCKED`.
- Atomic job claiming.
- Concurrent worker execution.
- Avoiding duplicate job ownership.
- Job lease expiry and recovery.
- Worker crashes and retry handling.

The goal is to understand how PostgreSQL row locks can coordinate queue consumers without forcing every worker to wait for the same locked row.

---

Part of [Concurrency Control in Practice](../../README.md) — practical demonstrations of PostgreSQL concurrency, transaction isolation, MVCC, locking, deadlocks, and recovery using Go.