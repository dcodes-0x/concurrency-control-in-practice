# 05 — PostgreSQL Transaction Isolation Levels

**Understanding READ COMMITTED, REPEATABLE READ, SERIALIZABLE, MVCC snapshots, write skew, and transaction retries through reproducible Go integration tests.**

Database transactions provide atomicity, but atomicity alone does not guarantee that concurrent transactions produce correct business outcomes.

Two transactions can each execute successfully, make individually valid decisions, and still violate a shared business invariant when their changes are combined.

This example explores how PostgreSQL controls transaction visibility and concurrency using different isolation levels.

Rather than relying exclusively on theoretical descriptions, we reproduce anomalies using real PostgreSQL transactions, controlled execution schedules, and Go integration tests.

> **Core principle:** Transaction isolation determines which concurrent changes a transaction can observe and which combinations of transaction outcomes the database permits.

---

## 1. What We're Exploring

PostgreSQL supports the following SQL isolation-level names:

| Isolation level | PostgreSQL behavior |
|---|---|
| READ UNCOMMITTED | Behaves as READ COMMITTED |
| READ COMMITTED | Statement-level snapshots |
| REPEATABLE READ | Stable transaction snapshot |
| SERIALIZABLE | Serializable Snapshot Isolation (SSI) |

The demonstrations focus on three levels:

- **READ COMMITTED:** A transaction can observe newer committed changes in subsequent statements.
- **REPEATABLE READ:** A transaction generally sees a stable snapshot established by its first non-transaction-control statement.
- **SERIALIZABLE:** PostgreSQL detects dangerous concurrent dependency patterns and prevents nonserializable combinations of committed transactions.

Serializable isolation does not mean all transactions execute one after another physically.

PostgreSQL allows concurrency while ensuring that committed transactions have results equivalent to some serial execution order.

When necessary, PostgreSQL aborts a transaction with a serialization failure.

## 2. Project Structure

```text
05-transaction-isolation/
├── isolation_test.go
├── phantom_read_test.go
├── write_skew_test.go
├── serializable_retry.go
├── serializable_retry_test.go
└── README.md
```

Unlike the earlier wallet APIs, this demonstration interacts directly with PostgreSQL transactions through `pgx`.

This makes the execution order, transaction boundaries, and isolation settings explicit.

### Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| Database | PostgreSQL 16 |
| Driver | pgx/v5 |
| Database pool | pgxpool |
| Migration management | golang-migrate |
| Containerization | Docker Compose |
| Testing | Go testing package |

## 3. Database Setup

The example reuses the existing `wallets` table and introduces an `on_call_doctors` table for testing multi-row business invariants.

### Existing Wallet Table

```sql
CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    balance BIGINT NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### On-Call Doctors Table

Migration: `000002_create_on_call_doctors.up.sql`

```sql
CREATE TABLE on_call_doctors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shift_id UUID NOT NULL,
    doctor_name VARCHAR(100) NOT NULL,
    is_on_call BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_on_call_doctors_shift
    ON on_call_doctors(shift_id);
```

Each test creates uniquely identified wallets or doctor shifts to avoid interference with other tests.

## 4. Running the Demonstrations

Run all commands from the repository root.

### Prerequisites

- Go 1.22 or newer
- Docker Desktop and Docker Compose
- PostgreSQL 16 container
- `golang-migrate` CLI

Start PostgreSQL:

```bash
docker compose up -d
docker compose ps
```

Set the connection string in PowerShell:

```powershell
$env:DATABASE_URL = "postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable"
```

Apply pending migrations:

```powershell
migrate -path ./migrations -database $env:DATABASE_URL up
```

If the database is already at the latest migration version, no additional migration is needed.

Run the entire demo:

```bash
go test -v ./examples/05-transaction-isolation -count=1
```

The examples use real PostgreSQL connections. They are integration tests, not mocked transaction simulations.

---

# Part I — Transaction Visibility Anomalies

## 5. Non-Repeatable Reads

A non-repeatable read occurs when a transaction executes the same row query multiple times and receives different results because another transaction committed an update between those reads.

### Scenario

A wallet initially contains 1000.

Transaction A reads its balance.

Transaction B updates the balance to 900 and commits.

Transaction A reads the same wallet again.

```mermaid
sequenceDiagram
    participant A as Transaction A
    participant DB as PostgreSQL
    participant B as Transaction B

    A->>DB: BEGIN
    A->>DB: SELECT balance
    DB-->>A: 1000

    B->>DB: UPDATE balance = 900
    B->>DB: COMMIT

    A->>DB: SELECT balance
    DB-->>A: Depends on isolation level
```

### Observed Behavior

| Isolation level | First read | Second read |
|---|---:|---:|
| READ COMMITTED | 1000 | 900 |
| REPEATABLE READ | 1000 | 1000 |
| SERIALIZABLE | 1000 | 1000 |

### Why READ COMMITTED Allows This

At `READ COMMITTED`, each SQL statement obtains a fresh snapshot.

Transaction A's second query can see Transaction B's committed update.

```text
Statement 1 snapshot → balance 1000

Transaction B commits

Statement 2 snapshot → balance 900
```

### Why REPEATABLE READ Prevents This

`REPEATABLE READ` establishes a transaction snapshot when the first non-transaction-control statement executes.

Subsequent reads generally continue using that snapshot.

```text
Transaction snapshot → balance 1000

Transaction B commits

Transaction A still sees 1000
```

This does not mean the actual database balance remains 1000.

After Transaction B commits, a new transaction can observe 900.

Transaction A simply retains its earlier view.

### Tests

```text
TestReadCommitted_NonRepeatableRead
TestRepeatableRead_StableSnapshot
TestSerializable_StableSnapshot
```

Run:

```bash
go test -v ./examples/05-transaction-isolation -run "TestReadCommitted_NonRepeatableRead|TestRepeatableRead_StableSnapshot|TestSerializable_StableSnapshot" -count=1
```

## 6. Phantom Reads

A phantom read occurs when a repeated query returns a different set of matching rows due to a concurrent insert, deletion, or qualifying row change.

### Scenario

Transaction A counts matching wallets.

Initially, one wallet matches the query.

Transaction B inserts another matching wallet and commits.

Transaction A repeats the same query.

```sql
SELECT COUNT(*)
FROM wallets
WHERE id IN ($1, $2)
  AND balance >= 500;
```

### Execution

```mermaid
sequenceDiagram
    participant A as Transaction A
    participant DB as PostgreSQL
    participant B as Transaction B

    A->>DB: SELECT COUNT(*)
    DB-->>A: 1

    B->>DB: INSERT matching wallet
    B->>DB: COMMIT

    A->>DB: SELECT COUNT(*)
    DB-->>A: Depends on isolation level
```

### Results

| Isolation level | First count | Second count |
|---|---:|---:|
| READ COMMITTED | 1 | 2 |
| REPEATABLE READ | 1 | 1 |
| SERIALIZABLE | 1 | 1 |

PostgreSQL's `REPEATABLE READ` prevents phantom visibility through its stable transaction snapshot.

This is stronger than the minimum phantom-read guarantee typically associated with the SQL-standard isolation-level descriptions.

### Tests

```text
TestReadCommitted_PhantomRead
TestRepeatableRead_NoPhantom
TestSerializable_NoPhantom
```

Run:

```bash
go test -v ./examples/05-transaction-isolation -run "Phantom|NoPhantom" -count=1
```

## 7. Why Stable Snapshots Are Not Enough

At this point, both `REPEATABLE READ` and `SERIALIZABLE` have produced identical results in our read-only demonstrations.

Both preserve stable snapshots.

Does that mean they provide the same correctness guarantees?

**No.**

A stable snapshot protects what a transaction observes, but it does not automatically guarantee that decisions made by multiple concurrent transactions remain valid when all their changes are committed.

To understand this distinction, we reproduce write skew.

---

# Part II — Write Skew and Serializable Isolation

## 8. The Write-Skew Problem

Consider a hospital with two doctors on call:

| Doctor | On call |
|---|---|
| Alice | Yes |
| Bob | Yes |

Business rule:

**At least one doctor must remain on call during a shift.**

Two concurrent transactions attempt to take different doctors off call.

Each transaction performs:

1. Count doctors currently on call.
2. If more than one is available, permit its doctor to leave.
3. Update that doctor's row.
4. Commit.

### Concurrent Execution

```mermaid
sequenceDiagram
    participant A as Transaction A
    participant DB as PostgreSQL
    participant B as Transaction B

    A->>DB: BEGIN REPEATABLE READ
    B->>DB: BEGIN REPEATABLE READ

    A->>DB: COUNT doctors on call
    DB-->>A: 2

    B->>DB: COUNT doctors on call
    DB-->>B: 2

    A->>DB: UPDATE Alice off call
    B->>DB: UPDATE Bob off call

    A->>DB: COMMIT
    B->>DB: COMMIT

    Note over DB: Zero doctors remain on call
```

Both transactions made their decisions using valid snapshots.

Both modified different database rows.

Under `REPEATABLE READ`, both can commit.

The combined result violates the business invariant.

This is **write skew**.

## 9. Why Ordinary Row Locks Do Not Automatically Prevent Write Skew

Transactions A and B update different rows.

Transaction A modifies Alice.

Transaction B modifies Bob.

There is no direct write/write conflict on the same row.

The actual conflict is between the transactions' reads and the assumptions underlying their updates.

Both depend on the condition:

```sql
SELECT COUNT(*)
FROM on_call_doctors
WHERE shift_id = $1
  AND is_on_call = TRUE;
```

Each transaction assumes another doctor will remain available.

Neither transaction's individual row update expresses the entire multi-row business invariant.

This demonstrates why protecting individual rows is not always enough to protect aggregate business rules.

## 10. SERIALIZABLE Prevents the Anomaly

We execute the same concurrent schedule under PostgreSQL `SERIALIZABLE`.

PostgreSQL uses Serializable Snapshot Isolation (SSI) to detect dependency patterns that would make the combined result nonserializable.

For this controlled write-skew scenario, one transaction is aborted.

### Expected Outcome

| Property | REPEATABLE READ | SERIALIZABLE |
|---|---:|---:|
| Initial on-call doctors | 2 | 2 |
| Transactions attempting updates | 2 | 2 |
| Successful commits | 2 | 1 |
| Serialization failures | 0 | 1 |
| Doctors remaining on call | 0 | 1 |
| Invariant preserved | No | Yes |

The rejected transaction receives a PostgreSQL serialization error.

### SQLSTATE 40001

PostgreSQL reports serialization failures using:

```text
40001 — serialization_failure
```

In Go, we inspect the PostgreSQL error:

```go
func isSerializationError(err error) bool {
    var pgErr *pgconn.PgError

    return errors.As(err, &pgErr) &&
        pgErr.Code == "40001"
}
```

A serialization failure can occur during an SQL statement or when attempting to commit.

The entire failed transaction must be considered aborted.

### Tests

```text
TestRepeatableRead_WriteSkew
TestSerializable_PreventsWriteSkew
```

Run:

```bash
go test -v ./examples/05-transaction-isolation -run "TestRepeatableRead_WriteSkew|TestSerializable_PreventsWriteSkew" -count=1
```

---

# Part III — Production-Style Transaction Retries

## 11. Why Serialization Failure Handling Matters

`SERIALIZABLE` protects correctness, but it does not guarantee that every concurrent transaction will successfully commit.

An application that simply returns an internal server error whenever it receives SQLSTATE `40001` may fail otherwise valid operations unnecessarily.

In many cases, the application can retry the entire transaction.

However, retrying requires more than repeating the final `UPDATE`.

The business decision itself must be reevaluated using a fresh snapshot.

## 12. Correct Transaction Retry Flow

Our executor follows:

```mermaid
flowchart TD
    A[Begin SERIALIZABLE transaction] --> B[Read current state]
    B --> C{Business invariant satisfied?}
    C -->|No| D[Return business rejection]
    C -->|Yes| E[Execute update]
    E --> F[Commit]
    F --> G{Result}
    G -->|Success| H[Return success]
    G -->|SQLSTATE 40001| I{Retries remaining?}
    G -->|Other error| J[Return error]
    I -->|Yes| K[Backoff with jitter]
    K --> A
    I -->|No| L[Return retry exhaustion error]
```

### First Attempt

Both doctors are on call.

Both transactions read the count as 2.

Both decide their doctors can leave.

PostgreSQL detects the nonserializable dependency pattern and aborts one transaction.

### Retried Attempt

The failed operation begins a new transaction.

Its new snapshot sees only one doctor remaining on call.

The business check now rejects the operation:

```text
at least one doctor must remain on call
```

The retry does not attempt the unsafe update.

The final database state preserves the business invariant.

## 13. Serializable Transaction Executor

The implementation lives in:

```text
serializable_retry.go
```

The executor takes a transaction callback:

```go
type TransactionFunc func(
    ctx context.Context,
    tx pgx.Tx,
) error
```

It starts a new serializable transaction for each attempt:

```go
tx, err := db.BeginTx(ctx, pgx.TxOptions{
    IsoLevel: pgx.Serializable,
})
```

The callback executes the full business operation:

```text
Read current state
      |
Validate invariant
      |
Apply changes
      |
Commit
```

If PostgreSQL returns SQLSTATE `40001`, the executor may retry from the beginning.

### Retry Configuration

```go
executor := &SerializableExecutor{
    DB:         db,
    MaxRetries: 3,
    BaseDelay:  5 * time.Millisecond,
    MaxDelay:   40 * time.Millisecond,
}
```

This allows one initial attempt and up to three additional attempts.

Retries are bounded to avoid unlimited contention and unpredictable request latency.

## 14. Exponential Backoff and Jitter

If many transactions retry immediately after conflicting, they may reproduce the same contention.

Exponential backoff increases the waiting window between attempts.

Full jitter randomizes the actual delay within that window.

For the example configuration:

| Retry | Maximum delay before jitter |
|---|---:|
| First retry | 5 ms |
| Second retry | 10 ms |
| Third retry | 20 ms |

Actual delays are randomly selected between zero and the corresponding cap.

The configured 40 ms maximum would be reached if additional retries were permitted.

## 15. Context Cancellation

Every retry checks whether the request context has been cancelled.

The backoff wait is also context-aware.

This prevents the executor from continuing retry work after the caller has cancelled the operation.

For production systems, request deadlines and transaction timeouts should be configured intentionally.

## 16. Why Retry the Entire Transaction?

Consider this incorrect retry strategy:

```text
Begin transaction
Read doctor count = 2
Update doctor
COMMIT fails with 40001

Retry only UPDATE
```

The problem is that the original business decision depended on a snapshot that is no longer current.

The correct strategy is:

```text
Begin new transaction
Read current doctor count
Reevaluate availability
Decide whether update is still allowed
Attempt commit
```

A transaction retry must repeat all SQL reads, business validation, and writes necessary to make a correct decision.

This is a central principle of reliable serializable transaction handling.

## 17. Deterministic Retry Integration Testing

Our test uses a barrier to ensure both transactions establish their snapshots before either performs its update.

The barrier applies only to the first attempt.

When a transaction retries, it must be free to continue with a fresh snapshot.

### Expected Outcome

```text
Initial doctors on call: 2

Alice requests off call
Bob requests off call

One operation commits successfully.
The other encounters a serialization failure.

The failed operation retries.
The new transaction observes one doctor remaining.
The business rule rejects the request.

Final doctors on call: 1
```

### Tests

```text
TestSerializableRetryPreservesInvariant
TestSerializableExecutorRespectsCancellation
```

Run:

```bash
go test -v ./examples/05-transaction-isolation -run "TestSerializableRetryPreservesInvariant|TestSerializableExecutorRespectsCancellation" -count=1
```

The test verifies that the successful operation commits, the losing operation retries and receives a business-rule rejection, and one doctor remains on call.

---

# Part IV — Engineering Trade-offs

## 18. Comparing Isolation Levels

| Property | READ COMMITTED | REPEATABLE READ | SERIALIZABLE |
|---|---|---|---|
| Dirty reads | Prevented | Prevented | Prevented |
| Non-repeatable reads | Possible | Prevented | Prevented |
| Phantom reads | Possible | Prevented | Prevented |
| Stable transaction snapshot | No | Yes | Yes |
| Write skew | Possible | Possible | Prevented for correctly participating serializable transactions |
| Serialization failures | Not normally from this isolation level alone | Possible for conflicting concurrent updates | Possible due to SSI conflicts |
| Whole-transaction retries | Needed for relevant errors | Needed for relevant errors | Essential when handling 40001 |

These characteristics describe PostgreSQL behavior rather than every database system's implementation of the SQL isolation levels.

## 19. When to Use Each Isolation Level

### READ COMMITTED

A suitable default for many backend operations where statement-level committed visibility and explicit concurrency controls are sufficient.

Typical examples include simple record retrieval, independent updates, and atomic conditional SQL operations.

### REPEATABLE READ

Useful when a transaction needs a stable view of data across multiple queries.

Examples include snapshot-based analysis and multi-query calculations where consistent visibility matters.

However, it does not guarantee that every concurrent multi-row business invariant remains valid after commit.

### SERIALIZABLE

Appropriate when multiple concurrent transactions make decisions involving shared predicates or aggregate conditions and serial execution semantics are required.

Examples include scheduling constraints, allocation invariants, and certain multi-record business rules.

Serializable isolation may introduce additional transaction aborts and retry work.

It should be selected based on correctness requirements and measured workload behavior.

## 20. Production Considerations

### Avoid External Side Effects Inside Retried Transactions

The transaction callback may execute multiple times.

Avoid sending emails, charging payment providers, or publishing irreversible external events directly from the callback.

A database rollback cannot undo such side effects.

For reliable event publishing, consider a transactional outbox.

### Keep Transactions Short

Long-running transactions retain resources and can increase contention, database maintenance pressure, and serialization failures.

Do not perform unnecessary network calls while holding an open database transaction.

### Monitor Retry Behavior

Useful production metrics include:

```text
db_serialization_failures_total
db_transaction_retry_attempts_total
db_transaction_retry_exhausted_total
db_transaction_duration_seconds
```

These are suggested metrics, not instrumentation currently implemented by the project.

### Distinguish Business Errors from Database Conflicts

An error such as:

```text
at least one doctor must remain on call
```

is an expected business rejection.

It should not be treated as a transient serialization failure and automatically retried.

### Handle Ambiguous Outcomes Carefully

A confirmed SQLSTATE `40001` indicates a transaction did not successfully commit.

A lost connection during `COMMIT` can instead leave the client uncertain about the outcome.

Blindly repeating a non-idempotent operation after an ambiguous commit can duplicate business effects.

Idempotency or durable operation tracking may be required.

### Mixed Isolation Levels

Serializable guarantees depend on how the participating transactions interact with shared data.

If other code paths modify the same business records using weaker isolation without appropriate concurrency controls, they may undermine application-level assumptions.

Use a consistent strategy across every path that can modify the protected invariant.

### Explicit Locking as an Alternative

Some multi-row invariants can also be protected with explicit locks, suitable data modeling, or database constraints.

`SERIALIZABLE` is powerful, but it is not the only correctness strategy.

The design choice should be driven by the business invariant and operational trade-offs.

## 21. Engineering Takeaways

**1. Atomicity and isolation are different transaction properties.**

Atomic writes can still participate in concurrent executions that violate higher-level business invariants.

**2. READ COMMITTED uses statement-level visibility.**

Two reads in one transaction can observe different committed states.

**3. PostgreSQL REPEATABLE READ provides a stable snapshot.**

It prevents non-repeatable reads and phantom visibility, but does not eliminate all serialization anomalies.

**4. Write skew can occur even when transactions update different rows.**

A shared business invariant may create dependencies that aren't captured by direct row-write conflicts.

**5. SERIALIZABLE protects serial execution semantics through conflict detection.**

It may abort transactions rather than blocking every potentially conflicting operation upfront.

**6. Serialization retries must restart the entire business transaction.**

All relevant reads and validations must be performed again.

**7. Retried transactions must be safe to execute more than once.**

Keep external side effects outside the retry callback or coordinate them through durable mechanisms.

**8. Isolation levels are correctness tools, not simply performance settings.**

Choose the weakest mechanism that fully protects the required invariants, and understand its failure behavior.

## 22. What's Next?

### Demo 06 — PostgreSQL MVCC Internals

Next, we'll explore the storage and visibility mechanisms that support much of the behavior demonstrated here.

Topics include:

- Multi-Version Concurrency Control (MVCC)
- Tuple versions and visibility
- PostgreSQL `xmin` and `xmax`
- Transaction IDs and snapshots
- Why ordinary reads generally do not block writes
- How PostgreSQL stores updated row versions
- Vacuum and dead tuples
- Long-running transactions and database bloat

We'll use actual PostgreSQL queries to inspect tuple metadata and reproduce visibility differences across concurrent transactions.

---

Part of [Concurrency Control in Practice](../../README.md) — exploring database concurrency, PostgreSQL locking, MVCC, and transaction isolation through runnable Go examples.