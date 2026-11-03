# 10 — PostgreSQL Concurrency Control Under High Contention

**A reproducible Go benchmarking lab comparing unsafe read-modify-write, pessimistic locking, optimistic concurrency control, and atomic SQL updates using PostgreSQL.**

Concurrency-control strategies often look straightforward in isolation. Their trade-offs become clearer when many requests compete to modify the same database row.

This demonstration benchmarks four approaches under controlled concurrent withdrawal workloads and evaluates both **data correctness** and **performance**.

The objective is not to identify one universally fastest strategy. Instead, it is to understand how lock contention, application retries, database round trips, and workload distribution influence throughput and latency.

> A high-throughput implementation is not useful if it acknowledges successful withdrawals without actually applying them.

---

## 1. What We're Comparing

| Strategy | Approach | Concurrency protection |
|---|---|---|
| Unsafe read-modify-write | Read balance, calculate in Go, write absolute balance | None |
| Pessimistic locking | Read wallet using `SELECT FOR UPDATE` inside a transaction | Database row lock |
| Optimistic locking | Read balance/version; update only when version matches | Version-based compare-and-swap |
| Atomic update | Validate and decrement in one SQL statement | Atomic conditional row update |

All strategies operate on the same `wallets` schema and use PostgreSQL's `READ COMMITTED` isolation level.

For the safe strategies, every successfully committed withdrawal must be reflected in the final wallet balance.

## 2. Project Structure

```text
10-contention-benchmark/
├── main.go
├── strategies.go
├── experiments.go
├── benchmark_test.go
└── README.md
```

| File | Responsibility |
|---|---|
| `main.go` | CLI flags, single-wallet runner, metrics and reporting |
| `strategies.go` | Four withdrawal implementations |
| `experiments.go` | Multi-wallet workloads, repeated runs and CSV export |
| `benchmark_test.go` | Concurrency correctness and metric tests |
| `README.md` | Benchmark design and interpretation |

The implementation reuses the `wallets` table introduced in Demo 01.

No additional migration is required.

---

# Part I — The Workload

## 3. Wallet Schema

The benchmark uses a wallet with a balance and a monotonically incremented version.

The relevant columns are:

```sql
id UUID PRIMARY KEY,
balance BIGINT NOT NULL,
version BIGINT NOT NULL
```

The existing migration also includes timestamps and a nonnegative balance constraint.

Each benchmark iteration creates fresh wallets with sufficient starting funds for the entire workload.

## 4. Withdrawal Operation

Every worker repeatedly performs:

```text
Withdraw 1 unit from a specified wallet.
```

For example:

```text
Concurrent workers:       20
Operations per worker:    100
Total operations:       2,000
Withdrawal amount:          1
Initial balance:        3,000
```

If all 2,000 withdrawals are acknowledged and committed correctly:

```text
Expected final balance = 3,000 - 2,000 = 1,000
```

The wallet's version should also increase by 2,000.

## 5. Correctness Invariant

The benchmark verifies:

```text
Actual balance = Initial balance - Successful withdrawals × Amount
```

It also checks:

```text
Final version = Number of successful withdrawals
```

These checks are appropriate for the controlled workload because every withdrawal increments the version exactly once.

The benchmark verifies each wallet separately in the multi-wallet experiment.

An aggregate balance check alone would be insufficient because errors across separate wallets could theoretically offset one another.

---

# Part II — Strategy Implementations

## 6. Unsafe Read-Modify-Write

The unsafe implementation first reads the balance:

```sql
SELECT balance
FROM wallets
WHERE id = $1;
```

It calculates the new balance in Go and writes an absolute value:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1
WHERE id = $2;
```

The critical problem is that another transaction may update the row after the initial read.

### Lost-Update Example

```text
Initial balance = 1,000

Worker A reads 1,000
Worker B reads 1,000

Worker A calculates 900
Worker B calculates 900

Worker A writes 900
Worker B writes 900
```

Both operations report success, but the final balance is 900 instead of 800.

This is a lost update caused by using a stale application-side balance to construct an absolute assignment.

The unsafe strategy includes an optional artificial delay between reading and writing to make this race easier to observe.

**This implementation is intentionally incorrect and must not be used in production.**

## 7. Pessimistic Locking

The pessimistic implementation begins a database transaction and locks the wallet row:

```sql
BEGIN;

SELECT balance
FROM wallets
WHERE id = $1
FOR UPDATE;
```

It checks available funds and updates the balance:

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1
WHERE id = $2;

COMMIT;
```

A competing transaction attempting to acquire an incompatible lock on the same row must wait.

### Advantages

- Straightforward correctness model.
- Suitable for multiple related reads and writes.
- Prevents competing transactions from modifying the locked row simultaneously.

### Trade-offs

- Lock waiting increases latency under contention.
- Transactions must remain open while protected logic executes.
- Incorrect lock ordering across multiple rows can produce deadlocks.
- Slow operations inside transactions increase contention.

This strategy is particularly useful when business logic needs to examine data before updating it and the logic cannot be expressed safely in one SQL statement.

## 8. Optimistic Concurrency Control

The optimistic implementation reads:

```sql
SELECT balance, version
FROM wallets
WHERE id = $1;
```

It attempts a conditional update:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1
WHERE id = $2
  AND version = $3;
```

Here, `$3` is the version observed by the initial read.

If another request changed the wallet, the update affects zero rows.

The operation then rereads the current state and retries, up to the configured retry budget.

### Advantages

- No application-level row lock is held between the initial read and attempted update.
- Useful when conflicting updates are relatively rare.
- Explicitly detects concurrent modifications.

### Trade-offs

- Conflicts require repeated database queries.
- Under high contention, retries can amplify load.
- Retry exhaustion reduces the successful completion rate.
- Retry policies need bounded attempts and careful error handling.

The implementation's retry counter represents failed CAS update attempts, not necessarily a complete count of all SQL operations.

## 9. Atomic Conditional Update

The atomic strategy combines the balance check and decrement into one SQL statement:

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1
WHERE id = $2
  AND balance >= $1;
```

PostgreSQL coordinates conflicting updates to the same row.

At `READ COMMITTED`, the update condition is evaluated appropriately against the row version available after conflicting updates are resolved.

This avoids the separate read-modify-write window.

### Advantages

- One database statement for the operation.
- No application-managed version retry loop.
- Validation and modification happen atomically.
- Often a strong choice for simple counter or balance updates.

### Trade-offs

- The operation must be expressible within the SQL statement.
- Conflicting updates still contend for the same row.
- More complex workflows may require an explicit transaction.

The benchmark uses a conditional atomic decrement because it directly represents the withdrawal invariant.

---

# Part III — Measuring Performance

## 10. Metrics

The runner records:

| Metric | Meaning |
|---|---|
| Attempts | Total requested withdrawal operations |
| Successes | Operations reporting successful completion |
| Failures | Operations returning errors |
| CAS conflicts | Failed version-matching update attempts |
| Retry exhausted | Operations that exceeded the CAS retry budget |
| Duration | Wall-clock duration of the workload |
| Attempts/sec | Total requests divided by duration |
| Successful ops/sec | Successful operations divided by duration |
| Average latency | Mean operation duration |
| p95 latency | 95th-percentile operation duration |
| p99 latency | 99th-percentile operation duration |
| Correct | Whether persisted wallet state matches acknowledged operations |

### Throughput

\[
\text{Attempts/sec} =
\frac{\text{Total attempts}}{\text{Elapsed seconds}}
\]

\[
\text{Successful ops/sec} =
\frac{\text{Successful operations}}{\text{Elapsed seconds}}
\]

Both values matter.

An implementation that rejects many operations might have a high request-processing rate but a low rate of successful business operations.

## 11. Latency Percentiles

The benchmark records the elapsed time of each withdrawal call.

Samples include connection-pool acquisition, PostgreSQL queries, lock waiting, transaction operations, and optimistic CAS retries.

The runner sorts the samples and uses nearest-rank percentiles.

- Average shows the mean observed operation time.
- p95 captures a higher-latency portion of the workload.
- p99 emphasizes tail latency.

These values are application-observed latencies, not isolated PostgreSQL execution times.

## 12. Correctness Before Performance

The unsafe implementation may finish quickly while producing an incorrect final balance.

For example:

```text
Attempts:          2,000
Acknowledged:      2,000
Expected balance:  1,000
Actual balance:    2,700
Correctness:       FAIL
```

These numbers are hypothetical.

Such an implementation must not be ranked as the best production strategy based on throughput alone.

Conversely, an optimistic CAS implementation may return some retry-exhaustion errors while preserving the balance invariant.

That is a completion-rate limitation, not automatically a data-integrity violation.

---

# Part IV — Benchmarking Contention

## 13. Single Hot Wallet

In the original benchmark, every worker updates one wallet.

```text
Worker 1 ─┐
Worker 2 ─┤
Worker 3 ─┼──> Wallet A
Worker 4 ─┤
Worker 5 ─┘
```

This creates substantial contention because all operations target the same row.

The experiment is useful for exploring the cost of serialization and repeated CAS conflicts.

## 14. Multiple Wallets

The comparative experiment also distributes requests across several wallets.

```text
Worker 1 ──> Wallet A
Worker 2 ──> Wallet B
Worker 3 ──> Wallet C
Worker 4 ──> Wallet D
Worker 5 ──> Wallet E
```

Operations against independent rows may proceed concurrently.

Increasing the number of wallets does not eliminate database overhead, but it can reduce contention on an individual row.

## 15. Example Workload Matrix

| Parameter | Hot row | Moderate contention | Distributed |
|---|---:|---:|---:|
| Workers | 20 | 20 | 20 |
| Operations per worker | 100 | 100 | 100 |
| Total operations | 2,000 | 2,000 | 2,000 |
| Wallets | 1 | 5 | 20 |
| Warm-up runs | 1 | 1 | 1 |
| Measured runs | 5 | 5 | 5 |

All scenarios use the same withdrawal amount and total number of requested operations.

Each iteration uses fresh disposable wallets.

## 16. Warm-Up and Repeated Runs

The comparative runner performs a configurable number of unmeasured warm-up iterations.

It then performs the requested measured iterations and reports their median metrics.

Warm-up and measured iterations use separate wallet state.

This avoids reusing balances modified by earlier experiments.

### Limitations

The current runner uses a fixed strategy execution order.

Repeated runs help reveal variability, but they do not eliminate effects from system temperature, cache state, Docker activity, CPU scheduling, or background PostgreSQL maintenance.

For more rigorous comparison, strategy order should be randomized or balanced across repetitions, and results should include variability estimates.

---

# Part V — Running the Benchmark

## 17. Prerequisites

- Go installed.
- Docker Desktop running.
- PostgreSQL 16 available.
- Repository dependencies installed.
- Existing `wallets` migration applied.
- Valid `DATABASE_URL` in the repository `.env` file.

Start PostgreSQL:

```powershell
docker compose up -d
```

## 18. Run Correctness Tests

```powershell
go test -v ./examples/10-contention-benchmark -count=1
```

Run the race detector:

```powershell
go test -race ./examples/10-contention-benchmark -count=1
```

Run the complete repository suite:

```powershell
go test ./... -count=1
```

The Go race detector identifies Go memory races. It does not replace database-level concurrency tests.

## 19. Run a Single-Wallet Benchmark

```powershell
go run ./examples/10-contention-benchmark -workers=20 -ops=100 -strategy=all
```

Run individual strategies:

```powershell
go run ./examples/10-contention-benchmark -strategy=unsafe
go run ./examples/10-contention-benchmark -strategy=pessimistic
go run ./examples/10-contention-benchmark -strategy=optimistic
go run ./examples/10-contention-benchmark -strategy=atomic
```

Change the optimistic retry budget:

```powershell
go run ./examples/10-contention-benchmark -strategy=optimistic -retries=100
```

Change the unsafe delay:

```powershell
go run ./examples/10-contention-benchmark -strategy=unsafe -unsafe-delay=5ms
```

The unsafe delay is included specifically to demonstrate lost updates, not to establish a fair raw-latency comparison.

## 20. Run Repeatable Multi-Wallet Experiments

A small verification run:

```powershell
go run ./examples/10-contention-benchmark `
  -experiment `
  -workers=5 `
  -ops=20 `
  -wallets=1,5 `
  -iterations=2 `
  -warmup=1 `
  -csv=benchmark-results.csv
```

Full experiment:

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

This configuration runs:

- 4 strategies.
- 3 wallet-count configurations.
- 5 measured iterations for each combination.
- 1 warm-up iteration per combination.

That gives 60 measured iterations and 12 warm-up iterations.

## 21. CSV Results

The experiment runner exports one row per measured iteration.

Columns:

```text
strategy
wallets
iteration
attempts
successes
failures
retry_exhausted
cas_conflicts
duration_ms
attempts_per_sec
successes_per_sec
avg_latency_ms
p95_latency_ms
p99_latency_ms
correct
```

These results can be analyzed using spreadsheets, Python, or a separate plotting tool.

Avoid combining incorrect runs with correct implementations into one performance ranking.

---

# Part VI — Interpreting the Results

## 22. What Should We Expect?

### Unsafe Read-Modify-Write

Under sufficient contention, the unsafe implementation can acknowledge withdrawals that are not reflected in the final balance.

Its success counter is therefore not trustworthy evidence of correctly applied business operations.

### Pessimistic Locking

The database serializes incompatible operations on the hot row.

This provides a straightforward correctness model but can increase lock-wait latency.

### Optimistic Locking

Conflicting version updates produce failed CAS attempts.

Under low contention, many operations may succeed on the first attempt.

Under high contention, retry amplification and exhaustion may become significant.

### Atomic Update

The database performs the decrement in one conditional statement.

This reduces application-side round trips compared with a separate read and update, although row contention remains.

Actual performance depends on the environment and workload.

## 23. Retry Amplification

For optimistic CAS, one logical withdrawal may require multiple database attempts.

A rough conflict-per-success indicator can be calculated as:

\[
\text{Conflicts per successful operation}
=
\frac{\text{CAS conflicts}}{\text{Successful operations}}
\]

This is a useful comparison metric, but not a complete measure of SQL requests or total resource usage.

An increase in conflicts generally indicates that more attempted updates are being invalidated by concurrent changes.

Retry backoff and admission control can sometimes reduce wasted contention, but they introduce additional latency and tuning requirements.

## 24. Why a Single Hot Row Matters

Even if the application uses many goroutines and PostgreSQL connections, conflicting updates against the same database row cannot all modify that row simultaneously.

The database must coordinate those updates.

Increasing application concurrency beyond the useful capacity of the contended resource may increase waiting and retries without proportionately increasing successful throughput.

This is why measuring different wallet counts is valuable.

It distinguishes a system limited by one hot row from a system that has independent rows available for concurrent updates.

## 25. Benchmarking Caveats

This implementation is a learning and experimentation tool rather than a production load-testing platform.

Important limitations include:

- One local PostgreSQL environment.
- Fixed strategy execution order.
- Simplified wallet operations.
- No external service calls.
- No CPU or database I/O profiling.
- No lock-wait histograms.
- No connection-pool wait histograms.
- No statistical confidence intervals.
- No automated randomized workload distribution.
- Artificial delay in the unsafe strategy.

The experiment also treats an operation's returned error as a failure. In real distributed systems, connection or commit errors can sometimes leave the final commit outcome ambiguous. Production financial operations require durable operation identifiers and reconciliation of uncertain outcomes.

---

# Part VII — Correctness Tests

## 26. Safe Strategies Preserve Balance

Test:

```text
TestBenchmark_SafeStrategiesPreserveBalance
```

Runs pessimistic, optimistic, and atomic withdrawals against disposable wallets.

Checks that the final balance and version match successful acknowledgements.

## 27. Optimistic Retry Exhaustion

Test:

```text
TestOptimisticCAS_ZeroRetryBudgetPreservesConsistency
```

Runs concurrent CAS operations without additional retries.

Some operations may fail after version conflicts.

The test verifies that only successfully completed withdrawals affect the final balance.

The test does not require a particular number of conflicts, because that number depends on execution scheduling.

## 28. Deterministic Lost Update

Test:

```text
TestUnsafeReadModifyWrite_LosesUpdate
```

Two goroutines read the same initial balance before either is allowed to write.

Both then update the wallet using their previously calculated absolute balance.

Expected:

```text
Initial balance:    1,000
Withdrawal A:         100
Withdrawal B:         100

Correct balance:      800
Unsafe balance:       900
Final version:          2
```

The synchronization barrier makes the lost-update schedule deterministic.

This proves that two successfully committed updates do not necessarily imply two correctly applied business operations.

## 29. Percentile Calculations

Test:

```text
TestPercentileCalculation
```

Verifies average latency and the percentile helper using a fixed sample set.

This tests the reporting utility independently of PostgreSQL.

---

# Part VIII — Engineering Takeaways

**1. Correctness is a prerequisite for performance comparisons.** Fast incorrect implementations are not viable alternatives.

**2. High concurrency does not guarantee high throughput.** A single hot database row can become the serialization bottleneck.

**3. Pessimistic locking trades waiting for controlled ownership.** It is effective for transactional logic that requires locked reads.

**4. Optimistic concurrency trades lock ownership for conflict detection and retries.** Its effectiveness depends on contention and retry policy.

**5. Atomic conditional updates are strong candidates for simple state transitions.** They minimize the gap between validation and mutation.

**6. Retry exhaustion is different from data corruption.** Measure successful business throughput as well as attempted request throughput.

**7. Per-resource correctness verification is essential.** Aggregate-only checks can conceal localized errors.

**8. Tail latency is often more informative than average latency under contention.** Lock waits and retries influence p95 and p99 behavior.

**9. Multi-wallet workloads help isolate hot-row contention.** Distributing updates across independent rows changes the concurrency profile.

**10. Benchmark conclusions must be tied to the tested workload and environment.** There is no universally optimal concurrency-control strategy.

---

## What This Repository Demonstrates

This is Demo 10 of **Concurrency Control in Practice**.

Together, the demonstrations cover:

1. Lost updates.
2. Pessimistic locking.
3. Optimistic concurrency control.
4. Atomic updates.
5. Transaction isolation.
6. MVCC internals.
7. Deadlock detection and avoidance.
8. PostgreSQL `SKIP LOCKED` job queues.
9. PostgreSQL advisory locks.
10. Concurrency benchmarking under contention.

The repository focuses on reproducible examples that connect SQL behavior to real backend engineering decisions.

[Back to the repository overview](../../README.md)