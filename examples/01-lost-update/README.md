# 01 — Lost Update: When Concurrent Writes Go Wrong

**Understanding lost updates, race conditions, and stale writes using Go and PostgreSQL.**

This example demonstrates how two concurrent operations can independently succeed while producing an incorrect final database state.

We intentionally implement an unsafe wallet withdrawal operation using application-level read-modify-write logic, reproduce the lost-update anomaly with a deterministic concurrency test, and examine why PostgreSQL's normal row-level locking does not prevent this particular failure.

> **Learning objective:** Understand why concurrency-safe database statements do not automatically make a multi-step business operation concurrency-safe.

## 1. The Problem

Consider a digital wallet with an initial balance of ₹1,000.

Two legitimate withdrawal requests arrive concurrently:

- Request A withdraws ₹100.
- Request B withdraws ₹200.

Both operations have sufficient funds and should succeed.

The expected final balance is:

```text
Initial balance    = 1000
Withdrawal A       = -100
Withdrawal B       = -200
------------------------
Expected balance   =  700
```

However, a naive implementation can leave the balance at ₹800 or ₹900.

Both requests return success, but one withdrawal's effect is overwritten.

This is a **lost-update anomaly**.

### The Failure Timeline

```mermaid
sequenceDiagram
    participant A as Request A
    participant DB as PostgreSQL
    participant B as Request B

    A->>DB: SELECT balance
    DB-->>A: 1000

    B->>DB: SELECT balance
    DB-->>B: 1000

    Note over A,B: Both requests calculate from stale shared state

    A->>A: 1000 - 100 = 900
    B->>B: 1000 - 200 = 800

    A->>DB: UPDATE balance = 900
    DB-->>A: Success

    B->>DB: UPDATE balance = 800
    DB-->>B: Success

    Note over DB: Final balance = 800
    Note over DB: Expected balance = 700
```

The database has not corrupted a row. Both updates are valid SQL operations.

The business operation is incorrect because the second write replaces the result of the first.

## 2. Why This Happens

The naive withdrawal implementation follows four steps:

1. Read the current wallet balance.
2. Validate that sufficient funds are available.
3. Calculate the new balance in application memory.
4. Update the database with the calculated balance.

The SQL operations are:

```sql
SELECT balance
FROM wallets
WHERE id = $1;
```

Followed by:

```sql
UPDATE wallets
SET balance = $1,
    updated_at = NOW()
WHERE id = $2;
```

The critical problem is the gap between reading and writing.

Another request may modify the wallet during that interval.

The `UPDATE` statement does not verify whether the balance has changed since the original read.

### The Unsafe Pattern

```go
balance, err := repo.GetBalance(ctx, walletID)
if err != nil {
    return err
}

if balance < amount {
    return ErrInsufficientFunds
}

newBalance := balance - amount

return repo.SetBalance(ctx, walletID, newBalance)
```

This code appears logically correct when requests execute sequentially.

Under concurrency, however, multiple requests can read the same balance and independently calculate replacement values.

**The problem is not simultaneous access to Go memory. It is concurrent access to shared database state without sufficient coordination.**

## 3. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| HTTP server | `net/http` |
| Database | PostgreSQL 16 |
| Database driver | `pgx/v5` |
| Containerization | Docker Compose |
| Integration testing | Go `testing`, `httptest` |
| Identifiers | UUID |
| Configuration | Environment variables, `godotenv` |

## 4. Architecture

The application follows a layered architecture:

```text
HTTP Client
    |
    | POST /wallets/{id}/withdraw
    v
Wallet Handler
    |
    | Parse input and handle HTTP responses
    v
Wallet Service
    |
    | Validate balance
    | Calculate new balance
    v
Wallet Repository
    |
    | SELECT balance
    | UPDATE balance
    v
PostgreSQL
```

### Responsibilities

| Layer | Responsibility |
|---|---|
| Handler | Parse wallet ID, decode requests, map errors to HTTP responses |
| Service | Validate withdrawal amounts and enforce business rules |
| Repository | Execute PostgreSQL queries |
| Database | Persist wallet balances and enforce schema constraints |

The architecture separates concerns but deliberately does not protect the read-modify-write operation from concurrent updates.

**Clean architecture and concurrency correctness are separate concerns.**

## 5. Project Structure

```text
01-lost-update/
├── main.go
├── handler.go
├── service.go
├── repository.go
├── model.go
├── concurrency_test.go
└── README.md
```

All Go source files belong to `package main`, allowing the example to run independently within the repository's root Go module.

## 6. Database Design

The wallet table is defined in the repository's first migration.

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

### Design Decisions

**UUID primary key**

Each wallet has a unique identifier independent of database-generated numeric sequences.

**Integer balance**

Balances use the smallest currency unit, such as paise, instead of floating-point values.

**Nonnegative balance constraint**

The database rejects negative balances through `CHECK (balance >= 0)`.

However, this constraint cannot detect lost updates when the final balance remains nonnegative.

**Version column**

The `version` column is reserved for the optimistic-locking demonstration.

It is not used by the naive implementation.

## 7. Running the Example

Run the following commands from the repository root.

### Prerequisites

- Go 1.22 or newer
- Docker Desktop
- Docker Compose
- PostgreSQL container running
- Wallet database migration applied

### Step 1 — Start PostgreSQL

```bash
docker compose up -d
```

Verify:

```bash
docker compose ps
```

The PostgreSQL container should report a healthy status.

### Step 2 — Configure Environment Variables

Create a `.env` file in the repository root:

```dotenv
APP_ENV=development
APP_PORT=8082

DATABASE_URL=postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable
```

The port and credentials are for the local development environment and should match `docker-compose.yml`.

Do not commit `.env` to version control.

### Step 3 — Apply Migration

Using `golang-migrate` in PowerShell:

```powershell
$env:DATABASE_URL = "postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable"

migrate -path ./migrations -database $env:DATABASE_URL up
```

The required table is `wallets`.

### Step 4 — Run the Wallet API

```bash
go run ./examples/01-lost-update
```

Expected startup message:

```text
Wallet API running on :8082
```

## 8. Reproducing a Withdrawal

### Create a Wallet

Execute against PostgreSQL:

```sql
INSERT INTO wallets (balance)
VALUES (1000)
RETURNING id, balance;
```

Copy the generated UUID.

### Send a Withdrawal Request

Replace `YOUR_WALLET_UUID` with the generated identifier.

```bash
curl --location --request POST \
'http://localhost:8082/wallets/YOUR_WALLET_UUID/withdraw' \
--header 'Content-Type: application/json' \
--data '{
    "amount": 100
}'
```

Expected response:

```http
HTTP/1.1 204 No Content
```

### Verify the Updated Balance

```sql
SELECT id, balance
FROM wallets
WHERE id = 'YOUR_WALLET_UUID';
```

After one successful withdrawal of 100 units from a balance of 1000, the expected balance is 900.

Sequential execution alone does not necessarily reproduce the concurrency bug.

For that, we use a controlled integration test.

## 9. Deterministic Concurrency Testing

The integration test is the central demonstration in this example.

It uses two concurrent HTTP requests and a synchronization barrier to guarantee that both withdrawal operations read the same initial balance before either writes.

### Test Scenario

| Property | Value |
|---|---|
| Initial balance | 1000 |
| Concurrent withdrawal A | 100 |
| Concurrent withdrawal B | 200 |
| Expected correct balance | 700 |
| Possible unsafe balance | 800 or 900 |
| Expected HTTP responses | Two successful `204` responses |

### Why Not Simply Start Two Goroutines?

Starting two goroutines does not guarantee that their database operations overlap.

For example, Request A might finish completely before Request B reads the wallet balance.

In that case, the final balance would correctly be 700, and the test would fail to reproduce the intended anomaly.

To eliminate this scheduling uncertainty, we introduce a synchronization barrier.

### The Synchronization Barrier

The test wraps the real PostgreSQL repository with a test-specific implementation:

```go
type BarrierWalletRepository struct {
    repo    *PostgresWalletRepository
    barrier *ReadBarrier
}
```

The wrapper intercepts `GetBalance()`.

```go
func (r *BarrierWalletRepository) GetBalance(
    ctx context.Context,
    walletID uuid.UUID,
) (int64, error) {

    balance, err := r.repo.GetBalance(ctx, walletID)
    if err != nil {
        return 0, err
    }

    if err := r.barrier.Wait(ctx); err != nil {
        return 0, err
    }

    return balance, nil
}
```

Each request reads the real balance from PostgreSQL, then waits.

Both requests proceed only after both reads have completed.

### Controlled Execution Timeline

```mermaid
sequenceDiagram
    participant A as Withdrawal A
    participant Barrier as Test Barrier
    participant B as Withdrawal B
    participant DB as PostgreSQL

    par Concurrent reads
        A->>DB: SELECT balance
        DB-->>A: 1000
        A->>Barrier: Wait
    and
        B->>DB: SELECT balance
        DB-->>B: 1000
        B->>Barrier: Wait
    end

    Note over Barrier: Both reads completed
    Barrier-->>A: Release
    Barrier-->>B: Release

    A->>DB: UPDATE balance = 900
    B->>DB: UPDATE balance = 800

    Note over DB: Last absolute UPDATE wins
    Note over DB: Incorrect final balance
```

This makes the lost-update demonstration reproducible without adding artificial delays or relying on random timing.

The application still executes real SQL statements against PostgreSQL.

## 10. Running the Integration Test

From the repository root:

```bash
go test -v ./examples/01-lost-update -run TestLostUpdateWithConcurrentWithdrawals -count=1
```

### Expected Output

The final balance depends on the order in which PostgreSQL executes the two updates.

Example:

```text
=== RUN   TestLostUpdateWithConcurrentWithdrawals
    Withdrawal of 100 succeeded
    Withdrawal of 200 succeeded
    Initial balance: 1000
    Expected balance: 700
    Actual balance: 800
    Lost-update anomaly reproduced successfully
--- PASS: TestLostUpdateWithConcurrentWithdrawals
PASS
```

The final balance may instead be 900.

### Why Does the Test Pass?

This test is intentionally designed to reproduce an incorrect business outcome.

Its assertion verifies that:

- Both HTTP requests succeeded.
- Both requests read the original balance.
- The final balance is either 800 or 900.
- The correct balance of 700 was not produced.

A passing test therefore proves that the unsafe implementation exhibits the intended failure under the controlled schedule.

It does not mean that the business operation is correct.

## 11. Understanding PostgreSQL's Behavior

PostgreSQL protects database consistency using mechanisms such as row-level locks, MVCC, and transaction isolation.

However, these guarantees do not automatically prevent every application-level concurrency anomaly.

In this example, each request executes:

```sql
SELECT balance FROM wallets WHERE id = $1;
```

And later:

```sql
UPDATE wallets
SET balance = $1
WHERE id = $2;
```

These are separate statements without a transaction spanning the original read and final write.

Under PostgreSQL's default `READ COMMITTED` isolation level, each statement operates according to its own visibility rules.

### Why Row-Level Locks Don't Save Us

Each `UPDATE` acquires the necessary row-level lock before modifying the wallet.

If two updates target the same row, PostgreSQL coordinates them so that the writes do not physically corrupt the row.

But both application requests have already calculated their replacement balances from the original value.

The second update therefore remains free to overwrite the first update's result.

The problem is **stale application state**, not unsafe storage-engine writes.

### Why the CHECK Constraint Doesn't Save Us

The database contains:

```sql
CHECK (balance >= 0)
```

Both 800 and 900 satisfy this constraint.

The database cannot infer that two successful withdrawals should have produced 700.

A database constraint only enforces the invariant expressed by that constraint.

### Why UUIDs Don't Matter Here

UUIDs uniquely identify wallet records.

They do not provide concurrency control over the mutable balance stored in those records.

## 12. Production Implications

Lost updates can appear in systems that perform read-modify-write operations without sufficient synchronization.

Examples include:

| System | Potential failure |
|---|---|
| Wallet | One withdrawal overwrites another |
| Inventory | Concurrent stock decrements produce incorrect quantities |
| Order management | One status update overwrites another |
| Quota management | Concurrent usage updates lose increments |
| Account settings | Concurrent changes overwrite previously saved values |

The general failure pattern is the same:

```text
Read shared state
        |
Calculate a new value
        |
Write an absolute replacement
        |
Another operation's update is lost
```

For financial or inventory systems, these errors can create significant business inconsistencies.

## 13. Potential Solutions

There is no single concurrency-control strategy appropriate for every workload.

Future examples will explore several approaches.

### Pessimistic Locking

Use `SELECT ... FOR UPDATE` within a transaction to lock the wallet row before reading and modifying its balance.

Other transactions attempting conflicting operations must wait for the lock to be released.

### Optimistic Locking

Use a version column to detect whether another transaction modified the wallet after it was read.

If the version has changed, reject or retry the update.

### Atomic Conditional Updates

Move the balance calculation into a single SQL statement:

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2
  AND balance >= $1
RETURNING balance;
```

PostgreSQL performs the conditional update atomically for the targeted row, preventing this stale application-side overwrite.

### Serializable Transactions

For more complex multi-row invariants, PostgreSQL's `SERIALIZABLE` isolation level can detect certain unsafe concurrent executions and abort transactions that require retry.

Serializable isolation does not remove the need for correct transaction boundaries and retry handling.

## 14. Engineering Takeaways

**1. Concurrently correct SQL statements can produce incorrect business outcomes.**

Each database write may execute successfully while the overall operation violates the intended invariant.

**2. Read-modify-write is dangerous without concurrency coordination.**

Separating a read and an absolute update creates an opportunity for stale writes.

**3. Database constraints enforce only the invariants they express.**

A nonnegative balance constraint cannot detect a lost withdrawal.

**4. PostgreSQL row locking is not equivalent to application-level operation atomicity.**

The database protects individual updates, but an application's multi-step workflow may still be unsafe.

**5. Deterministic concurrency tests are more valuable than timing-dependent tests.**

Synchronization barriers make problematic interleavings reproducible and easier to debug.

**6. Concurrency control requires trade-offs.**

Pessimistic locks, optimistic versioning, atomic statements, and serializable transactions differ in contention behavior, throughput, complexity, and failure handling.

## 15. What's Next?

### 02 — Pessimistic Locking with SELECT FOR UPDATE

The next demonstration will implement a concurrency-safe withdrawal using PostgreSQL transactions and row-level locking.

We'll compare the same wallet workload:

```text
Demo 01 — Unsafe read-modify-write

Initial balance: 1000
Withdrawals: 100 + 200
Final balance: 800 or 900

Result: Lost update
```

Against:

```text
Demo 02 — SELECT FOR UPDATE

Initial balance: 1000
Withdrawals: 100 + 200
Final balance: 700

Result: Both withdrawals correctly reflected
```

We'll also examine lock duration, transaction boundaries, lock waiting, deadlocks, and failure handling.

---

Part of [Concurrency Control in Practice](../../README.md) — exploring race conditions, transaction isolation, MVCC, and database locking through runnable Go and PostgreSQL examples.