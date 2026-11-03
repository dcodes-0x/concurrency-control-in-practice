# 09 — PostgreSQL Advisory Locks

**Distributed coordination, mutual exclusion, transaction-scoped locking, session ownership, and failure recovery using Go and PostgreSQL.**

Database concurrency control is often associated with row-level locks, optimistic locking, and transaction isolation.

However, some operations require coordinating access to a logical resource rather than an existing database row.

Examples include:

- Running only one reconciliation per warehouse.
- Preventing overlapping scheduled jobs.
- Coordinating inventory correction tasks.
- Serializing operations for a customer or business entity.
- Avoiding concurrent execution of the same database maintenance operation.

PostgreSQL provides **advisory locks** for these situations.

Unlike ordinary row locks, advisory locks operate on application-defined numeric keys. PostgreSQL manages lock acquisition and release, while the application defines what each key represents.

This demonstration explores how advisory locks work, when they are useful, and the failure modes developers must understand before using them in distributed backend systems.

> **Core principle:** Advisory locks provide cooperative mutual exclusion. They coordinate code paths that participate in the same locking convention, but they do not automatically enforce business invariants or guarantee exclusive external execution.

---

## 1. What We're Building

This demonstration implements:

- Transaction-level advisory locks.
- Session-level advisory locks.
- Blocking and nonblocking lock acquisition.
- Automatic lock release on commit or rollback.
- Explicit session-level unlocking.
- Reentrant session-level lock acquisition.
- Warehouse-scoped reconciliation coordination.
- Concurrent execution across independent warehouse keys.
- Lock inspection using `pg_locks`.
- Session termination and automatic lock release.
- PostgreSQL integration tests using Go and pgx.

The examples use real PostgreSQL sessions and transactions.

No new application table or database migration is required.

## 2. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| Database | PostgreSQL 16 |
| Database driver | pgx/v5 |
| Connection pool | pgxpool |
| Advisory-lock keys | Signed 64-bit integers |
| Key generation | FNV-1a 64-bit hashing |
| Concurrency testing | Go channels, goroutines, WaitGroup |
| Containerization | Docker Compose |
| Testing | Go testing package |

## 3. Project Structure

```text
09-advisory-locks/
├── advisory.go
├── advisory_test.go
├── reconciliation.go
├── reconciliation_test.go
├── lock_inspection_test.go
└── README.md
```

### File Responsibilities

| File | Responsibility |
|---|---|
| `advisory.go` | Acquire and release advisory locks |
| `advisory_test.go` | Test transaction and session lock lifecycles |
| `reconciliation.go` | Coordinate reconciliation by warehouse |
| `reconciliation_test.go` | Test mutual exclusion and independent warehouse execution |
| `lock_inspection_test.go` | Inspect `pg_locks` and test session termination |
| `README.md` | Explain implementation, correctness, and limitations |

---

# Part I — Advisory Lock Fundamentals

## 4. What Is an Advisory Lock?

An advisory lock is a PostgreSQL-managed lock associated with an application-defined resource identifier.

Consider a warehouse reconciliation job.

```text
Warehouse: warehouse-104
        |
        v
Deterministic advisory-lock key
        |
        v
PostgreSQL advisory lock manager
        |
        +---- Instance A acquires lock
        |
        +---- Instance B cannot acquire conflicting lock
```

The database does not know what `warehouse-104` means.

It only knows that concurrent sessions are requesting locks on the same numeric key.

### Row Locks vs Advisory Locks

| Property | Row-level lock | Advisory lock |
|---|---|---|
| Requires an existing database row | Yes | No |
| Uses an application-defined key | Not directly | Yes |
| Protects a row from conflicting updates | Yes | Only if code cooperates |
| Suitable for logical resource coordination | Sometimes | Yes |
| Managed by PostgreSQL | Yes | Yes |
| Visible through lock diagnostics | Yes, with limitations | Yes |

Advisory locks complement ordinary transaction and row-lock mechanisms.

They are not replacements for primary keys, unique constraints, or transactional validation.

## 5. Advisory-Lock Key Formats

PostgreSQL supports two key forms.

### One 64-bit Integer

```sql
SELECT pg_try_advisory_lock(42001::bigint);
```

### Two 32-bit Integers

```sql
SELECT pg_try_advisory_lock(42, 1);
```

The single-`bigint` and two-integer key spaces are separate.

Our demonstration uses the single-`bigint` form.

All application instances must use the same key-generation convention for resources that require mutual exclusion.

---

# Part II — Transaction-Level Advisory Locks

## 6. Transaction-Scoped Locking

A transaction-level advisory lock remains held until the transaction commits or rolls back.

It cannot be manually released before the transaction ends.

### Nonblocking Acquisition

```sql
SELECT pg_try_advisory_xact_lock(42001::bigint);
```

Result:

```text
true  → lock acquired
false → conflicting lock currently held
```

### Blocking Acquisition

```sql
SELECT pg_advisory_xact_lock(42001::bigint);
```

This waits until the conflicting advisory lock becomes available, unless the operation is cancelled or fails.

### Typical Transaction

```sql
BEGIN;

SELECT pg_try_advisory_xact_lock(42001::bigint);

-- Execute protected database operations.

COMMIT;
```

The advisory lock is automatically released at commit.

If the transaction rolls back, it is also released.

## 7. Go Implementation

File:

```text
advisory.go
```

Transaction acquisition:

```go
func TryTransactionLock(
    ctx context.Context,
    tx pgx.Tx,
    key int64,
) (bool, error)
```

Blocking acquisition:

```go
func LockTransaction(
    ctx context.Context,
    tx pgx.Tx,
    key int64,
) error
```

The API requires the caller to provide an active transaction.

This makes the ownership boundary explicit.

A transaction-level advisory lock is generally the simpler option for short database operations requiring logical mutual exclusion.

## 8. Transaction Lock Lifecycle

```mermaid
sequenceDiagram
    participant A as Transaction A
    participant DB as PostgreSQL
    participant B as Transaction B

    A->>DB: BEGIN
    A->>DB: Try lock key K
    DB-->>A: true

    B->>DB: BEGIN
    B->>DB: Try lock key K
    DB-->>B: false

    A->>DB: COMMIT
    Note over DB: Key K released

    B->>DB: Try lock key K
    DB-->>B: true
```

### Automated Tests

```text
TestTransactionLock_ReleasedOnCommit
TestTransactionLock_ReleasedOnRollback
```

The tests verify that:

- Another transaction cannot acquire the conflicting key while the first owns it.
- Commit releases the original transaction's lock.
- Rollback also releases the original transaction's lock.
- A competing transaction or session can then acquire that key.

---

# Part III — Session-Level Advisory Locks

## 9. How Session Locks Differ

Session-level advisory locks belong to a PostgreSQL database session.

Their lifetime is independent of an individual transaction.

Acquire:

```sql
SELECT pg_try_advisory_lock(42001::bigint);
```

Release:

```sql
SELECT pg_advisory_unlock(42001::bigint);
```

A session-level lock remains held until explicitly released or until the owning PostgreSQL session ends.

### Behavior Comparison

| Event | Transaction lock | Session lock |
|---|---|---|
| Transaction commits | Released | Still held |
| Transaction rolls back | Released | Still held |
| Manual unlock | Not available | Supported |
| Database session ends | Released | Released |
| Multiple acquisitions on same session | Held through transaction | Acquisitions stack |

## 10. Session Locks Survive Rollback

Consider:

```sql
BEGIN;

SELECT pg_try_advisory_lock(42001::bigint);

ROLLBACK;
```

The session-level advisory lock remains held.

The rollback ends the database transaction, but it does not undo the session-level lock acquisition.

This is a critical difference from ordinary transaction-scoped locking.

To release the lock:

```sql
SELECT pg_advisory_unlock(42001::bigint);
```

### Automated Test

```text
TestSessionLock_SurvivesTransactionRollback
```

The test:

1. Acquires a session-level advisory lock.
2. Rolls back the transaction.
3. Verifies another PostgreSQL session cannot acquire the lock.
4. Explicitly unlocks from the original session.
5. Verifies another session can now acquire the key.

## 11. Reentrant Session Locks

PostgreSQL allows one session to acquire the same advisory lock more than once.

Example:

```sql
SELECT pg_try_advisory_lock(42001::bigint);
SELECT pg_try_advisory_lock(42001::bigint);
```

Both calls can succeed on the same owning session.

The acquisition count is effectively stacked.

One unlock releases only one acquisition:

```sql
SELECT pg_advisory_unlock(42001::bigint);
```

A second unlock is required to release the remaining acquisition.

### Automated Test

```text
TestSessionLock_ReentrantAcquisitions
```

The test verifies that one unlock does not release both acquisitions and that another session can acquire the key only after the matching unlocks have occurred.

---

# Part IV — Connection Pooling

## 12. Why Session Locks Are Dangerous with Connection Pools

Consider:

```go
db.QueryRow(
    ctx,
    "SELECT pg_try_advisory_lock($1)",
    key,
)
```

Here, `db` is a `pgxpool.Pool`.

The pool may execute the query on any available PostgreSQL session.

After the query finishes, that session can return to the pool.

However, the session-level advisory lock may remain held.

Later:

```go
db.QueryRow(
    ctx,
    "SELECT pg_advisory_unlock($1)",
    key,
)
```

The pool might choose a different connection.

The unlock would not release the lock held by the original session.

### Incorrect Ownership Model

```text
Application request
      |
      v
Pool selects Connection A
      |
      v
Acquire session lock
      |
      v
Connection A returns to pool
      |
      v
Later unlock uses Connection B
      |
      v
Original session lock remains held
```

This is a connection ownership problem.

## 13. Correct Session Ownership

Our implementation uses a dedicated pooled connection:

```go
conn, err := db.Acquire(ctx)
```

Acquire the session-level lock through that connection:

```go
acquired, err := TrySessionLock(
    ctx,
    conn,
    key,
)
```

Then release using the same connection:

```go
released, err := UnlockSession(
    ctx,
    conn,
    key,
)
```

Only after the lock is fully released should the connection return to normal pool use.

If unlocking fails, the connection may need to be discarded rather than reused.

### Recommended Rule

**Never assume that two separate pool-level queries execute on the same PostgreSQL session.**

A session-level advisory lock requires explicit connection lifecycle management.

---

# Part V — Warehouse Reconciliation Coordination

## 14. Business Scenario

Suppose a supply-chain system runs inventory reconciliation for multiple warehouses.

The reconciliation operation may inspect mismatches and apply corrective database updates.

Two service instances could independently receive requests for the same warehouse.

Without coordination:

```text
Instance A: Reconcile Warehouse 104
Instance B: Reconcile Warehouse 104
```

Both operations may read overlapping inconsistent records and attempt conflicting corrections.

We want to permit only one participating reconciliation transaction for a given warehouse at a time.

However, independent warehouses should remain concurrent.

## 15. Warehouse-Scoped Lock Keys

We deterministically map each warehouse identifier to a 64-bit advisory-lock key.

File:

```text
reconciliation.go
```

Function:

```go
func WarehouseLockKey(
    warehouseID string,
) (int64, error)
```

The implementation uses FNV-1a hashing with a domain-specific prefix:

```text
concurrency-control/reconciliation/warehouse/v1/
```

This helps distinguish reconciliation lock keys from other independently designed application lock namespaces.

### Determinism

The same warehouse identifier produces the same lock key:

```text
warehouse-104 → Key K1
warehouse-104 → Key K1
```

Different identifiers generally produce different keys:

```text
warehouse-104 → Key K1
warehouse-205 → Key K2
```

### Hash Collision Limitation

FNV-1a does not guarantee collision-free mapping.

Different strings can theoretically produce the same 64-bit value.

A collision would cause two unrelated warehouse operations to contend for the same advisory key.

For systems requiring stronger key guarantees, a controlled stable numeric identifier mapping may be preferable.

Identifier normalization must also be consistent across application instances.

## 16. Reconciliation Coordinator

The coordinator exposes:

```go
func (c *ReconciliationCoordinator) TryReconcile(
    ctx context.Context,
    warehouseID string,
    fn ReconciliationFunc,
) (executed bool, err error)
```

The callback receives the active database transaction:

```go
type ReconciliationFunc func(
    ctx context.Context,
    tx pgx.Tx,
) error
```

### Execution Flow

```mermaid
flowchart TD
    A[Reconciliation requested] --> B[Generate warehouse key]
    B --> C[Begin PostgreSQL transaction]
    C --> D[Try transaction advisory lock]
    D --> E{Acquired?}
    E -->|No| F[Rollback and return WarehouseBusy]
    E -->|Yes| G[Execute reconciliation callback]
    G --> H{Callback result}
    H -->|Error| I[Rollback]
    H -->|Success| J[Commit]
    J --> K[Automatic lock release]
```

The coordinator owns the transaction lifecycle.

A successful callback is not considered completed until the transaction commits successfully.

## 17. Same-Warehouse Mutual Exclusion

Test:

```text
TestReconciliation_SameWarehouseOnlyOneExecutes
```

Scenario:

```text
Instance A:
  Acquire lock for Warehouse 104
  Execute reconciliation callback
  Hold transaction open

Instance B:
  Attempt lock for Warehouse 104
  Receive ErrWarehouseBusy

Instance A:
  Commit and release lock

Instance B:
  Retry acquisition
  Execute successfully
```

The test uses channels to control the transaction schedule.

It waits until Instance A has acquired the lock before Instance B attempts acquisition.

This avoids arbitrary timing assumptions.

## 18. Independent Warehouses

Test:

```text
TestReconciliation_DifferentWarehousesRunConcurrently
```

Scenario:

```text
Instance A → Warehouse 104 → Key K1
Instance B → Warehouse 205 → Key K2
```

Both transactions can hold their respective advisory locks simultaneously.

The test requires both reconciliation callbacks to begin before either is permitted to finish.

This establishes actual overlap rather than merely observing two successful sequential executions.

## 19. Error Handling and Automatic Cleanup

Test:

```text
TestReconciliation_CallbackErrorReleasesLock
```

If the reconciliation callback returns an error, the coordinator rolls back its transaction.

The transaction-level advisory lock is automatically released.

A later reconciliation for the same warehouse should therefore be able to acquire the key.

The implementation distinguishes a busy warehouse from a failed reconciliation:

```text
ErrWarehouseBusy
```

means the current attempt did not obtain ownership.

A callback or database error means an operation failed after or during transaction execution.

These cases should be handled differently by callers.

---

# Part VI — Inspecting PostgreSQL Advisory Locks

## 20. The pg_locks System View

PostgreSQL exposes active lock information through:

```sql
SELECT *
FROM pg_locks;
```

To inspect advisory locks:

```sql
SELECT
    pid,
    locktype,
    mode,
    granted,
    classid,
    objid,
    objsubid
FROM pg_locks
WHERE locktype = 'advisory';
```

### Important Columns

| Column | Meaning |
|---|---|
| `pid` | Backend process holding or requesting the lock |
| `locktype` | Advisory lock classification |
| `mode` | Shared or exclusive lock mode |
| `granted` | Whether the lock is currently granted |
| `classid` | High 32 bits of a 64-bit advisory key |
| `objid` | Low 32 bits |
| `objsubid` | Distinguishes advisory key forms |

## 21. Reconstructing the Advisory Key

For the single-`bigint` advisory-lock form, PostgreSQL stores the key in two halves.

The key can be reconstructed using:

```sql
SELECT
    pid,
    mode,
    granted,
    (classid::bigint << 32)
        | objid::bigint AS advisory_key
FROM pg_locks
WHERE locktype = 'advisory'
  AND objsubid = 1
  AND database = (
      SELECT oid
      FROM pg_database
      WHERE datname = current_database()
  );
```

`objsubid = 1` identifies the 64-bit-key representation.

Advisory locks are scoped to a database, so the database filter matters when interpreting results.

## 22. Inspecting Lock Ownership

Test:

```text
TestAdvisoryLock_VisibleInPgLocks
```

The test:

1. Creates a dedicated PostgreSQL connection.
2. Begins a transaction.
3. Acquires a transaction-level advisory lock.
4. Queries `pg_locks` from another connection.
5. Verifies the owning backend and key are visible.
6. Rolls back.
7. Confirms the lock entry is gone.

The observation uses PostgreSQL's lock manager rather than relying on an application-maintained flag.

---

# Part VII — Session Termination

## 23. What Happens When the Owning Session Dies?

Session-level advisory locks are associated with the database session.

When that session ends, PostgreSQL releases its advisory locks.

This happens even when the session ends unexpectedly.

### Scenario

```mermaid
sequenceDiagram
    participant A as Session A
    participant DB as PostgreSQL
    participant B as Session B

    A->>DB: Acquire session lock K
    DB-->>A: true

    B->>DB: Try lock K
    DB-->>B: false

    Note over A,DB: Session A terminates

    DB->>DB: Release session locks

    B->>DB: Try lock K
    DB-->>B: true
```

## 24. Backend Termination Test

Test:

```text
TestSessionLock_ReleasedWhenBackendTerminates
```

The test creates a dedicated PostgreSQL connection and obtains its backend PID.

It acquires a session-level advisory lock.

Another connection confirms that the key is unavailable.

The test then terminates the dedicated backend:

```sql
SELECT pg_terminate_backend($1, 5000);
```

The second argument is a timeout in milliseconds supported by PostgreSQL 16.

After successful termination, PostgreSQL releases the advisory lock.

The competing connection can acquire the key.

### Safety

The test targets only the PostgreSQL session it created.

It should be run against the local development database, not a shared production environment.

It also requires sufficient database permissions to terminate that backend.

---

# Part VIII — Production Limitations

## 25. Advisory Locks Are Cooperative

An advisory lock does not automatically block SQL changes to an associated warehouse.

For example:

```text
Service A:
  Acquires advisory lock for Warehouse 104

Service B:
  Ignores advisory lock
  Updates Warehouse 104 inventory
```

PostgreSQL does not automatically prohibit Service B's operation.

Every participating code path must follow the same coordination protocol.

For invariants requiring database enforcement, use appropriate transactions, constraints, and row-locking strategies in addition to any advisory locking.

## 26. Connection Loss Does Not Necessarily Stop Application Work

Suppose a service acquires a session-level advisory lock before invoking an external API.

Then:

```text
Service A acquires lock.

Service A starts external processing.

Database connection is lost.

PostgreSQL releases the lock.

Service B acquires the same key.

Service B starts equivalent processing.

Service A may still be running.
```

Both services may now execute external work simultaneously.

The database lock is behaving correctly, but it cannot guarantee that application execution stops when ownership is lost.

This is a critical distributed-systems limitation.

## 27. Why Advisory Locks Are Not Durable Leases

Advisory locks are stored in PostgreSQL's lock manager.

They are not durable ownership records with a persisted expiration timestamp and independently enforceable fencing generation.

They are released when their owning transaction or session ends.

For long-running asynchronous workflows, a durable ownership model may be more appropriate.

Possible components include:

- A persisted job state.
- A lease expiration timestamp.
- An ownership generation or claim token.
- Heartbeat renewal.
- A recovery process.
- Idempotent downstream operations.

Our earlier `SKIP LOCKED` job queue demonstrates several of these mechanisms.

## 28. Short Database Work vs Long-Running Workflows

### Appropriate Transaction-Level Usage

```text
BEGIN

Acquire advisory lock

Read inconsistent inventory

Apply corrective SQL updates

COMMIT
```

The lock protects a short database transaction.

### Potentially Problematic Usage

```text
BEGIN

Acquire advisory lock

Call external transport service

Wait for third-party response

Start asynchronous workflow

Wait several minutes

COMMIT
```

This holds a PostgreSQL transaction and connection open for an extended period.

Potential consequences include:

- Increased connection-pool utilization.
- Longer lock holding times.
- Higher contention.
- MVCC cleanup interference.
- More difficult failure recovery.

For long-running reconciliation, durable coordination is generally a better design than an open database transaction.

## 29. Advisory-Lock Resource Consumption

Advisory locks consume PostgreSQL lock-manager resources.

They are not unlimited.

Applications should avoid acquiring very large numbers of unnecessary advisory locks or leaking session-level acquisitions.

In particular, session-level locks can remain held unexpectedly when connection pooling is used incorrectly.

## 30. Blocking vs Nonblocking Acquisition

Our warehouse coordinator uses:

```sql
SELECT pg_try_advisory_xact_lock($1);
```

This returns immediately with `false` when a conflicting lock cannot be acquired.

That is useful when the caller can report that reconciliation is already running.

Another approach is:

```sql
SELECT pg_advisory_xact_lock($1);
```

This waits for the lock.

Blocking may be appropriate when the operation must eventually proceed, but it introduces wait-time and cancellation considerations.

The choice depends on the business contract.

## 31. Avoid Hash-Key Collisions

When strings are hashed into 64-bit advisory keys, different logical resources can theoretically collide.

For correctness-sensitive systems, consider stable database-assigned integer identifiers or a carefully managed collision-free mapping for the expected resource domain.

Also document the namespace conventions across services.

A lock key has no built-in semantic meaning to PostgreSQL.

---

# Part IX — Running the Demo

## 32. Prerequisites

- Go installed.
- Docker Desktop running.
- PostgreSQL 16 available.
- Existing repository `.env` configured.
- pgx dependencies installed.

No new migration is required for Demo 09.

## 33. Start PostgreSQL

From the repository root:

```powershell
docker compose up -d
docker compose ps
```

If necessary:

```powershell
$env:DATABASE_URL = "postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable"
```

## 34. Run Individual Tests

### Transaction and Session Lock Behavior

```powershell
go test -v ./examples/09-advisory-locks -run "TestTransactionLock|TestSessionLock_Survives|TestSessionLock_Reentrant" -count=1
```

### Warehouse Coordination

```powershell
go test -v ./examples/09-advisory-locks -run "TestReconciliation|TestWarehouseLockKey" -count=1
```

### Lock Inspection and Session Termination

```powershell
go test -v ./examples/09-advisory-locks -run "TestAdvisoryLock_VisibleInPgLocks|TestSessionLock_ReleasedWhenBackendTerminates" -count=1
```

## 35. Run the Full Test Suite

```powershell
go test -v ./examples/09-advisory-locks -count=1
```

Run the Go race detector:

```powershell
go test -race ./examples/09-advisory-locks -count=1
```

Run all repository tests:

```powershell
go test ./... -count=1
```

The Go race detector identifies unsafe concurrent access to Go memory; it does not replace PostgreSQL lock-behavior tests.

## 36. Expected Test Coverage

| Test | Concept |
|---|---|
| `TestTransactionLock_ReleasedOnCommit` | Transaction lock lifecycle |
| `TestTransactionLock_ReleasedOnRollback` | Automatic rollback release |
| `TestSessionLock_SurvivesTransactionRollback` | Session ownership |
| `TestSessionLock_ReentrantAcquisitions` | Reentrant lock counts |
| `TestReconciliation_SameWarehouseOnlyOneExecutes` | Warehouse mutual exclusion |
| `TestReconciliation_DifferentWarehousesRunConcurrently` | Independent keys |
| `TestReconciliation_CallbackErrorReleasesLock` | Error cleanup |
| `TestWarehouseLockKey_Deterministic` | Stable key mapping |
| `TestAdvisoryLock_VisibleInPgLocks` | Lock-manager inspection |
| `TestSessionLock_ReleasedWhenBackendTerminates` | Connection-loss release |

---

# Part X — Engineering Takeaways

**1. Advisory locks coordinate application-defined logical resources.** They do not require a corresponding table row.

**2. Transaction-level advisory locks have a simple lifecycle.** Commit and rollback release them automatically.

**3. Session-level locks require explicit ownership management.** They remain held across transaction boundaries.

**4. Connection pooling is a major session-lock hazard.** Acquisition and release must use the same database session.

**5. Reentrant acquisitions must be balanced by corresponding releases.** A single unlock may not release every acquisition.

**6. Advisory locks enable warehouse-scoped mutual exclusion.** Different logical resources can execute concurrently when their keys differ.

**7. Hashing provides deterministic keys but not collision-free identity.** Key-generation conventions must be documented and consistent.

**8. `pg_locks` provides visibility into active advisory locks.** Backend PID and key information help debug ownership and contention.

**9. Database session termination releases session-level advisory locks.** This does not guarantee that the application process has stopped executing external work.

**10. Advisory locks are cooperative and not equivalent to durable distributed leases.** Long-running workflows need appropriate ownership recovery, fencing, and idempotency.

---

## What's Next?

### Demo 10 — Concurrency Control Under High Contention

The next demonstration compares several PostgreSQL concurrency-control strategies under the same workload.

We'll benchmark:

- Unsafe read-modify-write.
- Pessimistic locking with `SELECT FOR UPDATE`.
- Optimistic locking with version-based CAS.
- Atomic conditional updates.
- Transaction retries under contention.

We will measure:

- Successful operations.
- Rejected operations.
- Conflict and retry rates.
- Throughput.
- Average latency.
- p95 and p99 latency.
- Final balance correctness.

The objective is not to declare one strategy universally superior.

Instead, we'll examine how correctness, contention, transaction duration, and retry behavior affect performance.

---

Part of [Concurrency Control in Practice](../../README.md) — exploring PostgreSQL concurrency, transaction isolation, MVCC, locking, background processing, and distributed coordination using Go.