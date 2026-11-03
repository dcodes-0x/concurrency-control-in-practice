# Concurrency Control in Practice

### Understanding Database Concurrency, Transactions, and Distributed Coordination Through Go and PostgreSQL

A hands-on engineering repository exploring how PostgreSQL handles concurrent operations, how race conditions occur, and how different concurrency-control strategies affect correctness, reliability, and performance.

Rather than explaining concurrency through isolated theoretical examples, this repository uses **reproducible experiments against a real PostgreSQL database**.

Each demonstration focuses on a specific concurrency problem, explains its underlying database behavior, and provides Go implementations with integration tests.

The repository progresses from basic lost updates to transaction isolation, MVCC, deadlocks, concurrent job processing, distributed coordination, and high-contention performance benchmarking.

**Tech Stack:** Go · PostgreSQL 16 · pgx/v5 · Docker · SQL · Go Testing

---

## Why This Repository Exists

Concurrency bugs are among the most difficult problems to reproduce and debug in backend systems.

Many are not caused by incorrect SQL syntax or invalid application logic. They emerge when individually valid operations execute concurrently.

Consider a wallet containing ₹1,000.

Two requests arrive simultaneously, each attempting to withdraw ₹600.

Both requests read the same available balance and independently determine that sufficient funds exist.

If their updates are not coordinated correctly, the application may acknowledge both withdrawals even though the wallet cannot fund them.

Similar problems arise in:

- Inventory reservations and overselling.
- Wallets, payments, and financial transactions.
- Order processing and state transitions.
- Background job execution.
- Distributed workflow orchestration.
- Warehouse and inventory reconciliation.
- Concurrent updates across microservices.

Solving these problems requires understanding the interaction between **application-level concurrency, PostgreSQL transaction semantics, isolation levels, locking, and failure recovery**.

This repository was built to explore those interactions through executable code.

---

## What You'll Learn

By working through the demonstrations, you'll understand:

- Why lost updates occur even when individual SQL statements succeed.
- How PostgreSQL coordinates concurrent reads and writes.
- When to use pessimistic versus optimistic locking.
- How atomic SQL statements eliminate certain race conditions.
- What PostgreSQL isolation levels guarantee—and what they do not.
- How MVCC manages multiple row versions.
- Why deadlocks occur and how to prevent or recover from them.
- How `FOR UPDATE SKIP LOCKED` enables concurrent job processing.
- How leases, heartbeats, and claim tokens support worker recovery.
- How advisory locks coordinate logical resources across application instances.
- How contention and retry amplification affect throughput and latency.
- Why database correctness and exactly-once external side effects are different problems.

---

## Repository Structure

```text
concurrency-control-in-practice/
│
├── docker-compose.yml
├── go.mod
├── go.sum
├── .env.example
├── README.md
│
├── migrations/
│   ├── 000001_create_wallets.up.sql
│   ├── 000001_create_wallets.down.sql
│   ├── 000002_create_on_call_doctors.up.sql
│   ├── 000002_create_on_call_doctors.down.sql
│   ├── 000003_create_queue_jobs.up.sql
│   ├── 000003_create_queue_jobs.down.sql
│   ├── 000004_add_job_leases.up.sql
│   └── 000004_add_job_leases.down.sql
│
└── examples/
    ├── 01-lost-update/
    ├── 02-pessimistic-locking/
    ├── 03-optimistic-locking/
    ├── 04-atomic-updates/
    ├── 05-transaction-isolation/
    ├── 06-mvcc-internals/
    ├── 07-deadlocks/
    ├── 08-skip-locked/
    ├── 09-advisory-locks/
    └── 10-contention-benchmark/
```

Each demonstration is designed to be understood independently while building on concepts introduced earlier in the repository.

---

## Learning Roadmap

| # | Topic | Core Concept | Implementation |
|---|---|---|---|
| 01 | [Lost Updates](examples/01-lost-update/) | Race conditions in read-modify-write operations | Unsafe concurrent wallet withdrawals |
| 02 | [Pessimistic Locking](examples/02-pessimistic-locking/) | `SELECT FOR UPDATE` | Transactional wallet withdrawals |
| 03 | [Optimistic Locking](examples/03-optimistic-locking/) | Version-based compare-and-swap | Conflict detection and retries |
| 04 | [Atomic Updates](examples/04-atomic-updates/) | Conditional SQL updates | Atomic balance modifications |
| 05 | [Transaction Isolation](examples/05-transaction-isolation/) | Isolation levels and serialization anomalies | Non-repeatable reads, phantom reads, write skew |
| 06 | [MVCC Internals](examples/06-mvcc-internals/) | Tuple visibility and row versions | `xmin`, `xmax`, `ctid`, vacuum |
| 07 | [Deadlocks](examples/07-deadlocks/) | Circular lock dependencies | Deadlock reproduction, avoidance and retry |
| 08 | [SKIP LOCKED Job Queue](examples/08-skip-locked/) | Concurrent background processing | Job claims, leases, heartbeats, fencing |
| 09 | [Advisory Locks](examples/09-advisory-locks/) | Logical resource coordination | Warehouse-scoped reconciliation |
| 10 | [Contention Benchmarking](examples/10-contention-benchmark/) | Correctness versus performance | Multi-worker and multi-wallet experiments |

**Recommended order:** Follow the demonstrations sequentially, particularly if you're learning PostgreSQL concurrency from the fundamentals.

---

# Part I — Concurrency Control

## 01. Lost Updates

**Problem:** Two concurrent requests read the same data and overwrite one another's changes.

Suppose a wallet has a balance of 1,000.

```text
Initial balance = 1000

Transaction A                    Transaction B
-------------                    -------------
Read balance = 1000              Read balance = 1000

Calculate 1000 - 100             Calculate 1000 - 100

UPDATE balance = 900             UPDATE balance = 900

Commit                           Commit
```

Both transactions succeed, but the final balance is 900.

The correct balance should be 800.

This demonstrates why an application-level read-modify-write sequence is unsafe without additional concurrency control.

**Concepts covered:**

- Race conditions.
- Lost-update anomalies.
- Read-modify-write operations.
- Concurrent execution schedules.
- Deterministic concurrency testing.

[Explore Demo 01](examples/01-lost-update/)

## 02. Pessimistic Locking

**Solution:** Acquire a row lock before reading and modifying shared data.

```sql
BEGIN;

SELECT balance
FROM wallets
WHERE id = $1
FOR UPDATE;

UPDATE wallets
SET balance = balance - $2
WHERE id = $1;

COMMIT;
```

PostgreSQL ensures that conflicting transactions cannot simultaneously modify the locked row.

**Concepts covered:**

- Row-level locking.
- Lock contention.
- Transaction boundaries.
- Lock waiting.
- Insufficient-funds validation.
- Correctness under concurrent withdrawals.

**Trade-off:** Strong coordination comes at the cost of waiting when many requests target the same row.

[Explore Demo 02](examples/02-pessimistic-locking/)

## 03. Optimistic Locking

**Solution:** Detect concurrent modifications instead of locking the row before the initial read.

Each wallet has a version.

```sql
SELECT balance, version
FROM wallets
WHERE id = $1;
```

Update only if the observed version is still current:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1
WHERE id = $2
  AND version = $3;
```

If zero rows are updated, another operation modified the wallet.

The application can then reread the current state and retry.

**Concepts covered:**

- Compare-and-swap (CAS).
- Version-based concurrency control.
- Optimistic conflict detection.
- Retry budgets.
- Backoff.
- Retry amplification.

**Trade-off:** Optimistic locking works well when conflicts are infrequent, but repeated retries can increase load under high contention.

[Explore Demo 03](examples/03-optimistic-locking/)

## 04. Atomic SQL Updates

**Solution:** Combine validation and modification into one SQL statement.

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1
WHERE id = $2
  AND balance >= $1;
```

The database handles the conditional update atomically.

There is no separate application-level balance read that can become stale before the update.

**Concepts covered:**

- Atomic SQL operations.
- Conditional updates.
- PostgreSQL row-update semantics.
- Preventing overspending.
- Reducing application round trips.

**Trade-off:** Atomic updates are effective for operations expressible in a single statement, but more complex multi-step business workflows may require explicit transactions.

[Explore Demo 04](examples/04-atomic-updates/)

---

# Part II — PostgreSQL Transaction Internals

## 05. Transaction Isolation Levels

Database isolation determines which concurrent transaction effects are visible and which anomalies are permitted.

The demonstration explores PostgreSQL's isolation levels:

| Isolation level | PostgreSQL behavior |
|---|---|
| Read Uncommitted | Behaves like Read Committed |
| Read Committed | Each statement sees a fresh committed snapshot |
| Repeatable Read | Transaction uses a stable snapshot; prevents PostgreSQL phantom reads |
| Serializable | Provides serializable execution semantics, with possible serialization failures |

### Demonstrated anomalies

**Non-repeatable read**

The same row can return different committed values in separate statements of a Read Committed transaction.

**Phantom read**

At Read Committed, repeating a predicate query can return a different set of rows.

**Write skew**

Two transactions independently update different rows after reading the same invariant.

Write skew can occur under PostgreSQL Repeatable Read.

**Serializable execution**

PostgreSQL Serializable isolation can detect dangerous concurrent execution patterns and abort one transaction with SQLSTATE `40001`.

Applications must be prepared to retry the entire failed transaction.

[Explore Demo 05](examples/05-transaction-isolation/)

## 06. MVCC Internals

PostgreSQL uses **Multi-Version Concurrency Control (MVCC)** to manage visibility between concurrent transactions.

Instead of overwriting a row's physical representation in place for every update, PostgreSQL generally creates new tuple versions.

This allows readers and writers to operate with transactionally consistent visibility rules.

The demonstration inspects PostgreSQL tuple metadata:

| Column | Meaning |
|---|---|
| `xmin` | Transaction ID associated with tuple creation |
| `xmax` | Transaction ID associated with tuple deletion/update or locking |
| `ctid` | Physical tuple location within the table |

Example:

```sql
SELECT
    xmin,
    xmax,
    ctid,
    balance
FROM wallets;
```

### Concepts covered

- MVCC snapshots.
- Tuple versions.
- Transaction visibility.
- Updates and rollback.
- Dead tuples.
- Vacuum.
- Long-running transaction implications.

**Important:** MVCC visibility does not eliminate the need for explicit concurrency control when enforcing business invariants.

[Explore Demo 06](examples/06-mvcc-internals/)

## 07. Deadlocks

A deadlock occurs when transactions form a circular lock dependency.

```text
Transaction A                     Transaction B
-------------                     -------------
Locks Wallet 1                    Locks Wallet 2

Attempts Wallet 2                 Attempts Wallet 1

Waits for B                       Waits for A
```

Neither transaction can continue without the other releasing its lock.

PostgreSQL detects deadlocks and aborts a participating transaction.

Deadlock errors use SQLSTATE `40P01`.

### Prevention and recovery

The demonstration explores:

- Consistent row-lock acquisition order.
- Avoiding unnecessary transaction duration.
- Deadlock detection.
- Error classification.
- Retrying the complete transaction.

A common prevention technique is to acquire multiple resource locks in a consistent global order.

[Explore Demo 07](examples/07-deadlocks/)

---

# Part III — Background Processing and Distributed Coordination

## 08. PostgreSQL Job Queue with SKIP LOCKED

Multiple workers often need to process jobs stored in a shared database.

A naive implementation can create contention or accidentally assign the same job to multiple workers.

PostgreSQL provides:

```sql
SELECT id
FROM queue_jobs
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

Instead of waiting on locked rows, competing workers can skip those rows and consider other available jobs.

### Implemented capabilities

- Atomic job claiming.
- Concurrent worker processing.
- Nonblocking row selection.
- Lease-based job ownership.
- Heartbeat renewal.
- Expired-lease recovery.
- Claim-token validation.
- Stale-worker rejection.
- Maximum attempt budgets.
- Failed-job cleanup.
- Graceful worker cancellation.

### Job lifecycle

```text
PENDING
   |
   v
PROCESSING
   |
   +---- Success ----------------> COMPLETED
   |
   +---- Lease expires
              |
              +---- Attempts available ---> PROCESSING
              |
              +---- Attempts exhausted ---> FAILED
```

### Important delivery guarantee

The queue is designed for **at-least-once processing**.

Atomic claiming prevents competing workers from successfully claiming the same available job at the same instant.

However, a job can still be executed more than once after a crash or lease expiration.

External side effects must therefore be idempotent or protected by an appropriate downstream fencing mechanism.

[Explore Demo 08](examples/08-skip-locked/)

## 09. PostgreSQL Advisory Locks

Some coordination problems do not naturally correspond to locking a database row.

Examples include:

- Running one reconciliation per warehouse.
- Avoiding overlapping scheduled tasks.
- Coordinating logical resource operations.
- Serializing database maintenance activities.

PostgreSQL advisory locks allow applications to coordinate using numeric resource keys.

### Transaction-level locks

```sql
SELECT pg_try_advisory_xact_lock(42001);
```

Released automatically when the transaction commits or rolls back.

### Session-level locks

```sql
SELECT pg_try_advisory_lock(42001);
```

Remain held until explicitly released or until the owning database session ends.

### Warehouse reconciliation

```text
Instance A → Warehouse 104 → Lock K1
Instance B → Warehouse 104 → Lock K1

Only one acquires the lock.

Instance C → Warehouse 205 → Lock K2

Can execute independently.
```

### Implemented experiments

- Transaction-scoped lock acquisition.
- Automatic lock release on commit/rollback.
- Session-level lock ownership.
- Reentrant lock acquisition.
- Connection-pool ownership hazards.
- Warehouse-scoped mutual exclusion.
- Concurrent execution across warehouses.
- `pg_locks` inspection.
- Database-session termination.

**Important:** Advisory locks provide cooperative coordination, not durable leases or exactly-once external execution.

[Explore Demo 09](examples/09-advisory-locks/)

---

# Part IV — Performance Engineering

## 10. Concurrency Control Under High Contention

Correctness is only one part of concurrency-control design.

Different strategies also affect database round trips, lock waiting, retry rates, throughput, and tail latency.

Demo 10 compares:

1. Unsafe read-modify-write.
2. Pessimistic locking.
3. Optimistic locking.
4. Atomic SQL updates.

### Workload dimensions

- Number of concurrent workers.
- Operations per worker.
- Number of independently writable wallets.
- Withdrawal amount.
- Optimistic retry budget.
- Single hot-row contention.
- Distributed multi-wallet contention.

### Collected metrics

| Metric | Purpose |
|---|---|
| Total attempts | Requested operation volume |
| Successful operations | Completed business operations |
| Failures | Rejected or failed requests |
| CAS conflicts | Optimistic retry pressure |
| Successful operations/sec | Effective throughput |
| Average latency | Mean operation duration |
| p95 latency | Higher-percentile latency |
| p99 latency | Tail latency |
| Expected vs actual balance | Data correctness |
| Final version | Committed update count |

### Multi-wallet experiments

```text
High contention

20 workers ─────────> 1 wallet


Distributed contention

20 workers ─────────> 20 wallets
```

The runner supports repeated experiments, configurable warm-up runs, median summaries, and CSV export.

**Benchmark results are environment-dependent.** This repository provides the tools to measure performance; it does not assume one strategy will always be fastest.

[Explore Demo 10](examples/10-contention-benchmark/)

---

# Getting Started

## Prerequisites

Make sure the following are installed:

- Go.
- Docker Desktop or Docker Engine with Compose.
- PostgreSQL migration CLI (`golang-migrate`).
- Git.

A native PostgreSQL installation is not required when using Docker Compose.

## 1. Clone the Repository

```bash
git clone https://github.com/dcodes-0x/concurrency-control-in-practice.git

cd concurrency-control-in-practice
```

## 2. Start PostgreSQL

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

The local development configuration uses PostgreSQL 16.

Example connection settings:

| Setting | Value |
|---|---|
| Host | localhost |
| Port | 55433 |
| Database | concurrency_lab |
| Username | dev |
| Password | devpassword |

These credentials are for local development only.

## 3. Configure Environment Variables

Create a `.env` file in the repository root.

If the repository contains `.env.example`, copy it:

```powershell
Copy-Item .env.example .env
```

Set:

```dotenv
DATABASE_URL=postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable
```

Do not commit real credentials or production environment files.

## 4. Install Dependencies

```bash
go mod download
```

## 5. Apply Database Migrations

Using `golang-migrate`:

```powershell
migrate -path ./migrations -database "postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable" up
```

The existing migrations create the wallet, on-call doctor, and background job queue tables.

The latest migration in the current project is version `000004`.

## 6. Run Integration Tests

Run all tests:

```bash
go test ./... -count=1
```

Run with verbose output:

```bash
go test -v ./... -count=1
```

Run with the Go race detector:

```bash
go test -race ./... -count=1
```

### Run Specific Demonstrations

```bash
go test -v ./examples/01-lost-update -count=1

go test -v ./examples/02-pessimistic-locking -count=1

go test -v ./examples/03-optimistic-locking -count=1

go test -v ./examples/04-atomic-updates -count=1

go test -v ./examples/05-transaction-isolation -count=1

go test -v ./examples/06-mvcc-internals -count=1

go test -v ./examples/07-deadlocks -count=1

go test -v ./examples/08-skip-locked -count=1

go test -v ./examples/09-advisory-locks -count=1

go test -v ./examples/10-contention-benchmark -count=1
```

The tests use real PostgreSQL transactions rather than mocked database interactions.

Some examples deliberately reproduce concurrency anomalies, deadlocks, or lock contention. These are intentional demonstrations rather than unexpected application behavior.

---

# Running the Contention Benchmarks

The tenth demonstration includes a standalone Go benchmark runner.

## Single-Wallet Benchmark

```bash
go run ./examples/10-contention-benchmark -workers=20 -ops=100 -strategy=all
```

Run one strategy:

```bash
go run ./examples/10-contention-benchmark -strategy=atomic
```

## Comparative Multi-Wallet Benchmark

The following PowerShell command executes repeated experiments using 1, 5, and 20 wallets:

```powershell
go run ./examples/10-contention-benchmark `
  -experiment `
  -workers=20 `
  -ops=100 `
  -wallets=1,5,20 `
  -iterations=5 `
  -warmup=1 `
  -retries=50 `
  -csv=benchmark-results.csv
```

The runner produces a summary and exports per-iteration metrics to CSV.

The CSV can be used for further analysis and visualization.

Do not interpret the unsafe implementation's throughput as evidence of acceptable correctness.

---

# Key Engineering Principles

## 1. Database Transactions Do Not Automatically Prevent Every Race Condition

A transaction groups operations into a commit or rollback unit.

However, the isolation level and SQL statements determine which concurrent execution patterns are permitted.

An incorrect read-modify-write sequence can still lose updates.

## 2. Use the Simplest Correct Concurrency Primitive

For a simple balance decrement, an atomic conditional SQL statement may be sufficient.

For complex business logic requiring a consistent read before modification, a row lock or carefully designed optimistic concurrency protocol may be more appropriate.

Avoid introducing distributed locking when database constraints or atomic operations already enforce the required invariant.

## 3. Keep Transactions Short

Long-running transactions increase operational risk and can contribute to:

- Lock contention.
- Connection-pool exhaustion.
- MVCC cleanup interference.
- Deadlock exposure.
- Higher latency.

Avoid holding transactions open while waiting on slow external systems.

## 4. Retry Complete Transactional Units

When handling deadlocks or serialization failures, rerun the complete transaction after rollback.

Retrying only the final failed SQL statement may be insufficient because earlier reads and decisions can become invalid.

Retries should be bounded and consider backoff and request deadlines.

## 5. Idempotency Is Different From Mutual Exclusion

Mutual exclusion coordinates which operations may execute concurrently.

Idempotency ensures repeated execution does not duplicate the intended business effect.

A distributed system frequently needs both.

For example, a queue worker may obtain an exclusive database claim and still repeat an external side effect after crashing before acknowledgement.

## 6. Locks Are Not Always Durable Ownership Records

PostgreSQL row locks and advisory locks are tied to transactions or sessions.

They are released when their ownership scope ends.

Long-running asynchronous workflows may require durable job state, leases, ownership generations, heartbeats, and recovery logic.

## 7. Correctness Must Be Measured Alongside Performance

Throughput alone cannot establish whether a concurrent implementation is safe.

Every benchmark should identify the data invariant it expects to preserve.

For wallet withdrawals, that means reconciling the persisted balance against successful operations.

---

# Choosing a Concurrency-Control Strategy

| Scenario | Useful starting approach |
|---|---|
| Simple conditional counter update | Atomic SQL update |
| Wallet withdrawal with multiple dependent reads | Pessimistic locking or carefully designed CAS |
| Relatively infrequent concurrent updates | Optimistic locking |
| Multi-row invariant with possible write skew | Serializable transaction or explicit locking |
| Multiple workers processing database jobs | `FOR UPDATE SKIP LOCKED` |
| Short logical-resource coordination | Transaction-level advisory lock |
| Long-running background processing | Durable claim and lease protocol |
| Multiple dependent updates across services | Workflow orchestration with idempotency and compensation |

These are starting points, not universal prescriptions.

The appropriate design depends on invariants, contention patterns, failure modes, and operational constraints.

---

# Testing Philosophy

The repository emphasizes deterministic concurrency testing wherever possible.

Instead of relying only on random goroutine scheduling or arbitrary sleeps, demonstrations use:

- Explicit transaction boundaries.
- Channels to coordinate execution order.
- Independent PostgreSQL connections.
- Controlled lock acquisition.
- Database state verification.
- SQLSTATE error classification.
- Targeted lease expiration.
- Concurrent worker assertions.

The objective is to make important concurrency behavior reproducible and explainable.

Randomized testing and load testing remain useful complements, but they should not replace deterministic correctness tests.

---

# Limitations

This repository is intended for learning, experimentation, and demonstrating backend concurrency patterns.

It is not a production-ready wallet service, payment processor, or general-purpose job platform.

Important limitations include:

- Simplified business workflows.
- Local PostgreSQL configuration.
- Limited monitoring and operational tooling.
- No production authentication or authorization model.
- No complete distributed tracing implementation.
- Benchmark results dependent on local hardware.
- Simplified job retry and failure policies.
- No guarantee of exactly-once external side effects.

Production deployments require additional work around security, observability, fault injection, operational safeguards, deployment strategy, and incident recovery.

---

# References

Useful primary documentation:

- [PostgreSQL — Explicit Locking](https://www.postgresql.org/docs/16/explicit-locking.html)
- [PostgreSQL — Transaction Isolation](https://www.postgresql.org/docs/16/transaction-iso.html)
- [PostgreSQL — Concurrency Control](https://www.postgresql.org/docs/16/mvcc.html)
- [PostgreSQL — Monitoring Database Activity and Locks](https://www.postgresql.org/docs/16/monitoring-stats.html)
- [PostgreSQL — Advisory Lock Functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)
- [pgx — PostgreSQL Driver for Go](https://github.com/jackc/pgx)
- [golang-migrate](https://github.com/golang-migrate/migrate)

---

# Future Improvements

Potential extensions include:

- Automated CI integration against PostgreSQL.
- PostgreSQL lock-wait and transaction instrumentation.
- Benchmark result visualization.
- Connection-pool saturation experiments.
- Retry backoff comparisons.
- Randomized concurrent execution tests.
- Fault injection during transaction commit.
- More advanced queue recovery policies.
- Idempotency-key and transactional-outbox demonstrations.
- Distributed fencing-token experiments.

---

## Final Thought

Concurrency control is not about eliminating concurrency. It is about allowing operations to execute concurrently **without violating business invariants**.

The right solution is rarely the most complicated one.

Sometimes it is a single atomic `UPDATE`.

Sometimes it is `SELECT FOR UPDATE`.

Sometimes it is optimistic version checking, a serializable transaction, or a durable background-processing protocol.

The engineering challenge is understanding the failure mode, selecting an appropriate primitive, and verifying the behavior under real concurrent execution.

---

**Built with Go and PostgreSQL.**

If this repository helps you understand database concurrency or debug a distributed-system failure, consider starring it.

[GitHub — dcodes-0x/concurrency-control-in-practice](https://github.com/dcodes-0x/concurrency-control-in-practice)