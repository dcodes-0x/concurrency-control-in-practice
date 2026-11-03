# 04 — Atomic Conditional Updates: Concurrency Safety in a Single SQL Statement

**Preventing lost updates and concurrent overspending using PostgreSQL atomic updates and Go.**

In the previous demonstrations, we explored how concurrent requests can corrupt application-level state and how to prevent those anomalies using pessimistic and optimistic locking.

This example implements a simpler concurrency-control strategy: **perform the balance validation and deduction in a single conditional SQL statement.**

The implementation requires no application-side read-modify-write sequence, no explicit `SELECT FOR UPDATE`, and no version-conflict retry loop.

> **Core principle:** When an operation can be expressed as a single conditional database update, let the database perform the validation and modification atomically.

---

## 1. The Problem

Consider a wallet with an initial balance of 1000 units.

Two withdrawal requests arrive concurrently:

| Operation | Amount |
|---|---:|
| Initial balance | 1000 |
| Withdrawal A | 100 |
| Withdrawal B | 200 |
| Expected final balance | 700 |

A naive implementation first reads the balance:

```sql
SELECT balance
FROM wallets
WHERE id = $1;
```

It then calculates the new balance in application memory:

```go
newBalance := balance - amount
```

And writes the calculated value:

```sql
UPDATE wallets
SET balance = $1
WHERE id = $2;
```

Two concurrent requests can read the same original balance, independently calculate their updates, and overwrite each other's changes.

This is the **lost-update anomaly**, reproduced in [Demo 01](../01-lost-update/README.md).

## 2. The Atomic Update Solution

Instead of separating reading, validation, calculation, and updating, combine the operation into one SQL statement:

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2
  AND balance >= $1
RETURNING id, balance, version;
```

This statement:

1. Identifies the wallet.
2. Checks whether sufficient funds remain.
3. Deducts the requested amount from the current row balance.
4. Increments the version.
5. Returns the updated state.

All these operations execute as part of one SQL statement.

### Why This Matters

The difference is subtle but fundamental.

**Unsafe application-side calculation:**

```sql
UPDATE wallets
SET balance = 900
WHERE id = $1;
```

The replacement value might have been calculated from stale state.

**Atomic database-side calculation:**

```sql
UPDATE wallets
SET balance = balance - 100
WHERE id = $1
  AND balance >= 100;
```

The calculation uses the current row value as handled by PostgreSQL's update execution.

This prevents competing requests from silently overwriting one another's deductions under PostgreSQL's default `READ COMMITTED` isolation level.

## 3. Concurrent Execution

Assume the wallet begins with:

```text
balance = 1000
version = 0
```

Two requests arrive concurrently.

```mermaid
sequenceDiagram
    participant A as Withdrawal A
    participant DB as PostgreSQL
    participant B as Withdrawal B

    A->>DB: UPDATE balance = balance - 100
    B->>DB: UPDATE balance = balance - 200

    Note over DB: Concurrent updates target the same row

    DB-->>A: balance=900, version=1
    Note over B,DB: B uses the updated row after waiting
    DB-->>B: balance=700, version=2

    Note over DB: Final balance = 700
```

This shows one possible execution order. Request B could also execute first.

The important guarantee is that **both successful deductions are preserved**, regardless of which request executes first.

### Does PostgreSQL Still Use Locks?

Yes.

Atomic updates do not eliminate database-level locking.

When concurrent `UPDATE` statements modify the same row, PostgreSQL coordinates the writes using row-level locking.

If one transaction is already modifying the row, another conflicting update may wait until that transaction completes.

Under `READ COMMITTED`, PostgreSQL rechecks the update condition against the updated row before proceeding.

The difference is that the Go application does not explicitly acquire a locking read or maintain a transaction across separate read-and-write statements.

## 4. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| HTTP | `net/http` |
| Database | PostgreSQL 16 |
| Database driver | `pgx/v5` |
| Infrastructure | Docker Compose |
| Identifiers | UUID |
| Testing | Go `testing`, `httptest` |
| Concurrency mechanism | Atomic conditional SQL `UPDATE` |

## 5. Project Structure

```text
04-atomic-updates/
├── main.go
├── model.go
├── handler.go
├── service.go
├── repository.go
├── concurrency_test.go
└── README.md
```

The application follows:

```text
HTTP Client
    |
    v
Wallet Handler
    |
    v
Wallet Service
    |
    | Validate request amount
    v
Wallet Repository
    |
    | Atomic conditional UPDATE
    v
PostgreSQL
```

### Layer Responsibilities

| Layer | Responsibility |
|---|---|
| Handler | Parse requests and return HTTP responses |
| Service | Validate input and invoke the repository |
| Repository | Execute the atomic deduction |
| PostgreSQL | Coordinate concurrent writes and enforce constraints |

Compared with earlier demonstrations, the service has significantly less concurrency-specific logic.

## 6. Database Schema

This example reuses the `wallets` table created in Demo 01:

```sql
CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    balance BIGINT NOT NULL
        CHECK (balance >= 0),

    version BIGINT NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Why Use an Integer Balance?

Currency amounts should generally be stored using exact numeric representations.

For this demonstration, `BIGINT` represents amounts in the smallest currency unit, rather than using floating-point values.

### Why Keep the Version Column?

Although this approach does not use a version-based CAS condition, every successful withdrawal increments `version`.

This lets tests verify how many successful modifications occurred.

It also preserves compatibility with the earlier optimistic-locking demonstration.

### Why Keep the CHECK Constraint?

The constraint:

```sql
CHECK (balance >= 0)
```

provides an additional database-level invariant.

However, it does not replace the conditional withdrawal logic.

The condition:

```sql
AND balance >= $1
```

allows the operation to reject insufficient funds without attempting an invalid negative-balance update.

## 7. Repository Implementation

The repository exposes one withdrawal operation:

```go
func (r *PostgresWalletRepository) WithdrawAtomic(
    ctx context.Context,
    walletID uuid.UUID,
    amount int64,
) (WithdrawResult, error)
```

The successful path executes:

```go
err := r.db.QueryRow(ctx, `
    UPDATE wallets
    SET balance = balance - $1,
        version = version + 1,
        updated_at = NOW()
    WHERE id = $2
      AND balance >= $1
    RETURNING id, balance, version
`, amount, walletID).Scan(
    &result.WalletID,
    &result.Balance,
    &result.Version,
)
```

### Successful Update

If PostgreSQL returns an updated row, the withdrawal succeeded.

The repository returns the new balance and version.

### No Row Updated

If no row matches, two common explanations are possible:

- The wallet does not exist.
- The wallet exists but has insufficient funds.

The repository performs a secondary existence check on the failure path to select an appropriate application error.

Because this lookup is separate, the classification can reflect a later database state if a wallet is concurrently deleted or recreated.

### Unexpected Database Error

Unexpected SQL errors are returned rather than automatically retried.

An uncertain outcome following a connection failure must not be treated as proof that the deduction did not happen.

## 8. Service Implementation

The service is intentionally small:

```go
func (s *WalletService) Withdraw(
    ctx context.Context,
    input WithdrawInput,
) (WithdrawResult, error) {
    if input.Amount <= 0 {
        return WithdrawResult{}, ErrInvalidAmount
    }

    return s.repo.WithdrawAtomic(
        ctx,
        input.WalletID,
        input.Amount,
    )
}
```

The service validates the input and delegates the conditional state transition to the database repository.

There is no reason to duplicate the balance read and comparison in Go for this operation.

This demonstrates an important design principle:

**Concurrency control should be no more complex than the invariant being protected requires.**

## 9. Why No Explicit Transaction?

A standalone SQL statement runs atomically within PostgreSQL's transaction machinery.

For this single-row balance deduction, validation and modification are already contained in one statement.

An explicit transaction would add complexity without improving the concurrency guarantee for this particular operation.

However, consider a wallet transfer:

```text
1. Debit Wallet A
2. Credit Wallet B
3. Insert transfer ledger entry
```

Those are multiple changes that must commit or roll back together.

A single-wallet atomic update is not sufficient to guarantee the correctness of that entire transfer workflow.

An explicit transaction would be appropriate.

## 10. Running the Application

Execute these commands from the repository root.

### Prerequisites

- Go 1.22+
- Docker Desktop
- PostgreSQL container running
- Existing wallet migration applied

### Start PostgreSQL

```bash
docker compose up -d
docker compose ps
```

### Configure Environment Variables

Create or update the root `.env`:

```dotenv
APP_ENV=development
ATOMIC_APP_PORT=8085

DATABASE_URL=postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable
```

The connection details must match your local Docker Compose configuration.

### Start the API

```bash
go run ./examples/04-atomic-updates
```

Expected startup message:

```text
Atomic withdrawal API running on :8085
```

## 11. API Demonstration

Create a wallet:

```sql
INSERT INTO wallets (balance)
VALUES (1000)
RETURNING id, balance, version;
```

Send a withdrawal request:

```bash
curl --location \
'http://localhost:8085/wallets/YOUR_WALLET_UUID/withdraw' \
--header 'Content-Type: application/json' \
--data '{
    "amount": 100
}'
```

Expected response:

```json
{
    "wallet_id": "YOUR_WALLET_UUID",
    "balance": 900,
    "version": 1
}
```

Send another withdrawal of 200.

Expected resulting balance: 700.

### Insufficient Funds

Attempt to withdraw 1000 after the balance has reached 700.

The API should respond with:

```http
HTTP/1.1 409 Conflict
```

The wallet remains unchanged.

### API Status Codes

| Status | Meaning |
|---|---|
| 200 OK | Withdrawal completed |
| 400 Bad Request | Invalid wallet ID, request body, or amount |
| 404 Not Found | Wallet not found |
| 409 Conflict | Insufficient funds |
| 500 Internal Server Error | Unexpected failure |

## 12. Integration Testing

The integration tests exercise real SQL statements against PostgreSQL rather than using mocked database behavior.

### Test 1 — Concurrent Atomic Withdrawals

Two concurrent requests withdraw 100 and 200 from a balance of 1000.

Expected:

```text
Successful requests: 2
Final balance: 700
Final version: 2
```

Both deductions must be reflected.

### Test 2 — Concurrent Overspending Prevention

Two requests each attempt to withdraw 700 from a balance of 1000.

Only one withdrawal can succeed.

Expected:

```text
Successful requests: 1
Conflict responses: 1

Final balance: 300
Final version: 1
```

The balance condition is evaluated as part of the atomic update, preventing both requests from spending the same funds.

### Test 3 — High-Concurrency Atomic Updates

The test creates a wallet containing 1000 units.

It then sends **100 concurrent HTTP withdrawal requests**, each deducting one unit.

Expected:

```text
Initial balance: 1000
Concurrent requests: 100
Withdrawal amount: 1

Successful requests: 100
Final balance: 900
Final version: 100
```

No application-level mutex, explicit locking read, or version-based retry loop is used.

The test verifies that PostgreSQL correctly applies every successful deduction.

### Test 4 — Row-Lock Contention

Transaction A explicitly acquires a wallet row lock:

```sql
BEGIN;

SELECT balance
FROM wallets
WHERE id = $1
FOR UPDATE;
```

The test then submits a withdrawal against the same wallet.

While Transaction A holds the lock, the withdrawal should remain pending.

After Transaction A commits, the withdrawal can complete.

This demonstrates that even a single atomic `UPDATE` participates in PostgreSQL's normal row-level locking behavior.

The test uses a bounded observation period and does not instrument the precise instant the competing query reaches its lock wait.

### Test 5 — Insufficient-Funds Rejection

A wallet contains 1000 units.

A request attempts to withdraw 1500.

Expected:

```text
HTTP status: 409

Balance: 1000
Version: 0
```

No row satisfies the conditional update, so neither the balance nor the version changes.

Despite the test name `TestAtomicRollbackOnInsufficientFunds`, this does not involve an explicit rollback. The conditional statement simply updates no rows.

## 13. Running the Tests

Run the tests for this example:

```bash
go test -v ./examples/04-atomic-updates -count=1
```

Run all examples:

```bash
go test ./... -count=1
```

### Expected Test Summary

```text
=== RUN   TestConcurrentAtomicWithdrawals
--- PASS: TestConcurrentAtomicWithdrawals

=== RUN   TestAtomicOverspendingPrevention
--- PASS: TestAtomicOverspendingPrevention

=== RUN   TestHighConcurrencyAtomicUpdates
--- PASS: TestHighConcurrencyAtomicUpdates

=== RUN   TestAtomicUpdateUnderLockContention
--- PASS: TestAtomicUpdateUnderLockContention

=== RUN   TestAtomicRollbackOnInsufficientFunds
--- PASS: TestAtomicRollbackOnInsufficientFunds

PASS
```

These are expected results, not recorded output from a verified local execution.

## 14. Understanding PostgreSQL's Guarantees

### Atomicity

Each successful `UPDATE` modifies the wallet balance and version together.

A partial change to only one of those columns is not committed.

### Concurrency Control

Concurrent updates to the same wallet row are coordinated by PostgreSQL.

At `READ COMMITTED`, a conflicting update may wait for another transaction and then reevaluate its condition against the updated row.

### Conditional Business Validation

The database executes:

```sql
WHERE balance >= $1
```

as part of deciding whether the update is allowed.

The balance validation is therefore not separated from the deduction by an application-side race window.

### Row-Level Contention

Even though the application does not explicitly request `SELECT FOR UPDATE`, concurrent updates to the same row still contend.

A hot wallet receiving many updates may experience increased lock waiting and latency.

### Isolation-Level Considerations

The behavior described here assumes PostgreSQL's default `READ COMMITTED` isolation.

At `REPEATABLE READ` or `SERIALIZABLE`, conflicting concurrent updates can instead result in serialization failures that the application must handle appropriately.

A single atomic statement is not a guarantee that every transaction will succeed under every isolation level.

## 15. Comparing All Four Demonstrations

| Property | Naive | Pessimistic | Optimistic | Atomic Update |
|---|---|---|---|---|
| Initial balance read in Go | Yes | Yes | Yes | No |
| Explicit `SELECT FOR UPDATE` | No | Yes | No | No |
| Version CAS | No | No | Yes | No |
| Conditional balance check in SQL | No | No | Not in current CAS query | Yes |
| Application-managed transaction | No | Yes | No | No |
| Application conflict retries | No | Not normally for row contention | Yes | Not normally for row contention |
| Prevents the demonstrated lost update | No | Yes | Yes | Yes |
| Can reject insufficient funds | Yes, but subject to stale reads | Yes | Yes, with fresh-read validation | Yes |
| Successful-path SQL statements | 2 | Multiple | 2 per attempt | 1 |

The atomic approach has a particularly attractive property: **the entire single-row business invariant is enforced within one database operation.**

However, this does not mean it always outperforms the alternatives.

Performance depends on contention, connection-pool behavior, transaction duration, network latency, and the broader workload.

## 16. When to Use Each Approach

### Atomic Conditional Updates

Prefer when the business operation can be expressed as one SQL statement involving one row or a suitably scoped set of rows.

Examples:

- Deducting inventory quantity.
- Decrementing a wallet balance.
- Incrementing counters.
- Consuming a quota.
- Conditional state transitions.

### Pessimistic Locking

Consider when the operation requires multiple dependent reads and writes that must be coordinated while a consistent set of rows is protected.

Examples:

- Complex account transfers.
- Multi-step reservation workflows.
- Operations requiring coordinated changes across related records.

### Optimistic Locking

Consider when conflicts are relatively uncommon and the application needs to detect stale updates.

Examples:

- Concurrent document editing.
- Configuration updates.
- Versioned entity modifications.
- Business objects with low write contention.

### Stronger Transaction Isolation

Consider when correctness depends on relationships across multiple rows, including aggregate constraints or write-skew scenarios.

The appropriate choice depends on the invariant, not simply the number of requests.

## 17. Production Considerations

### Idempotency

Atomicity is not idempotency.

If a withdrawal succeeds but its HTTP response is lost, a client retry may perform another successful deduction.

Request-level idempotency is still necessary to avoid duplicate business effects.

See [Idempotency in Practice](https://github.com/dcodes-0x/idempotency-in-practice).

### High Contention

Updates to one frequently modified wallet remain serialized at the row level.

Track lock waits, query latency, and database connection utilization.

### Timeouts

Use bounded request deadlines and appropriate PostgreSQL statement or lock timeouts.

A single statement can still wait on locks or exceed a client latency budget.

### Multiple-Row Business Operations

A single wallet update does not guarantee the atomicity of a multi-statement business workflow.

Use explicit transactions when several database changes must commit together.

### Retry Safety

An unsuccessful conditional update due to insufficient funds does not mean a retry is useful.

A database timeout or connection failure may have an uncertain outcome, so blindly repeating the withdrawal can be unsafe without idempotency.

### Indexing

The wallet lookup uses its UUID primary key.

The existing primary-key index supports identifying the targeted wallet efficiently.

Adding unrelated indexes does not remove row contention.

### Observability

Useful metrics for a production implementation include:

```text
wallet_withdrawal_requests_total
wallet_withdrawal_failures_total
wallet_withdrawal_duration_seconds
wallet_insufficient_funds_total
database_lock_wait_duration_seconds
```

These are recommended instrumentation concepts, not metrics currently implemented in the example.

## 18. Engineering Takeaways

**1. Prefer database-side atomic operations when they fully express the business invariant.**

**2. Avoid unnecessary application-side read-modify-write sequences.**

**3. PostgreSQL still coordinates concurrent writes internally.** No explicit locking read does not mean no locking.

**4. Conditional updates can enforce business rules while modifying the row.**

**5. High contention remains a performance concern even when the operation is correct.**

**6. Atomicity, isolation, and idempotency solve different problems.** Real systems frequently need more than one of these guarantees.

**7. A simpler concurrency-control mechanism is often preferable when it provides the same required correctness guarantees.**

## 19. What's Next?

### Demo 05 — Transaction Isolation Levels

We'll explore PostgreSQL's isolation levels using explicitly controlled concurrent transactions.

Topics include:

- `READ COMMITTED`
- `REPEATABLE READ`
- `SERIALIZABLE`
- Non-repeatable reads
- Phantom reads and snapshot behavior
- Serialization failures
- Transaction retries
- Write skew and business invariants

We'll reproduce the relevant anomalies using real PostgreSQL transactions and compare the guarantees each isolation level provides.

---

Part of [Concurrency Control in Practice](../../README.md) — exploring concurrency anomalies, locking, MVCC, and transaction isolation through runnable Go and PostgreSQL examples.