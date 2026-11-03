# 02 — Pessimistic Locking: Preventing Lost Updates

**Understanding PostgreSQL row-level locking, transaction boundaries, and concurrency-safe operations using Go.**

In [Demo 01 — Lost Update](../01-lost-update/README.md), we demonstrated how concurrent operations can successfully execute while producing an incorrect final database state.

This example solves the lost-update problem using **pessimistic locking with PostgreSQL's `SELECT FOR UPDATE`**.

Instead of allowing multiple requests to read and modify the same wallet independently, we lock the wallet row before calculating the updated balance.

> **Core principle:** When modifying shared state, acquire the appropriate lock, validate the business invariant, and commit the update within the same transaction.

---

## 1. The Problem

Consider a wallet containing 1000 units.

Two concurrent withdrawal requests arrive:

| Operation | Amount |
|---|---:|
| Initial balance | 1000 |
| Withdrawal A | 100 |
| Withdrawal B | 200 |
| Expected final balance | 700 |

In the naive implementation, both requests may read the original balance of 1000.

Each independently calculates its replacement value:

```text
Request A:
    Read balance = 1000
    Withdraw 100
    New balance = 900

Request B:
    Read balance = 1000
    Withdraw 200
    New balance = 800
```

Because both operations overwrite the database balance using stale values, the final balance may become 800 or 900 instead of 700.

This is the lost-update anomaly.

## 2. The Solution: Pessimistic Locking

Pessimistic concurrency control assumes that concurrent modifications may conflict.

Instead of detecting conflicts after they happen, the application acquires a lock before reading and updating shared state.

PostgreSQL provides row-level locking through:

```sql
SELECT balance
FROM wallets
WHERE id = $1
FOR UPDATE;
```

When executed inside a transaction, this statement locks the selected wallet row against conflicting updates and locking reads.

The lock is held until the transaction commits or rolls back.

### Execution Timeline

```mermaid
sequenceDiagram
    participant A as Withdrawal A
    participant DB as PostgreSQL
    participant B as Withdrawal B

    A->>DB: BEGIN
    A->>DB: SELECT balance FOR UPDATE
    DB-->>A: 1000 (row locked)

    B->>DB: BEGIN
    B->>DB: SELECT balance FOR UPDATE
    Note over B,DB: Waiting for A to release lock

    A->>DB: UPDATE balance = 900
    A->>DB: COMMIT

    Note over DB: Lock released

    DB-->>B: 900 (row locked)
    B->>DB: UPDATE balance = 700
    B->>DB: COMMIT

    Note over DB: Final balance = 700
```

The second transaction reads the updated balance after the first transaction commits.

Both successful withdrawals are correctly reflected.

## 3. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| HTTP server | `net/http` |
| Database | PostgreSQL 16 |
| Database driver | `pgx/v5` |
| Infrastructure | Docker Compose |
| Integration testing | Go `testing`, `httptest` |
| Identifiers | UUID |
| Configuration | Environment variables, `godotenv` |

## 4. Application Architecture

The implementation follows a layered design:

```text
HTTP Client
    |
    | POST /wallets/{id}/withdraw
    v
Wallet Handler
    |
    | Parse request, map HTTP errors
    v
Wallet Service
    |
    | Validate withdrawal amount
    v
Wallet Repository
    |
    | BEGIN
    | SELECT FOR UPDATE
    | Validate available balance
    | UPDATE wallet
    | COMMIT
    v
PostgreSQL
```

### Layer Responsibilities

| Layer | Responsibility |
|---|---|
| Handler | HTTP request and response handling |
| Service | Input validation and business-facing orchestration |
| Repository | Transaction management, locking, persistence |
| Database | Enforce constraints, transaction isolation, and locks |

The service depends on the `WalletStore` interface instead of directly depending on PostgreSQL.

In this example, the repository owns the transaction boundary because the entire wallet withdrawal is represented by a single atomic persistence operation.

## 5. Project Structure

```text
02-pessimistic-locking/
├── main.go
├── model.go
├── handler.go
├── service.go
├── repository.go
├── concurrency_test.go
└── README.md
```

All Go files use `package main`, allowing the demonstration to run independently while sharing the repository's root Go module.

## 6. Database Schema

This example reuses the wallet table created for Demo 01.

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

### Design Considerations

**UUID primary key:** Each wallet has a unique identifier.

**Integer balance:** Amounts are represented in the smallest currency unit to avoid floating-point precision issues.

**Nonnegative constraint:** PostgreSQL prevents negative balances, but this constraint alone does not prevent lost updates.

**Version column:** Successful withdrawals increment the version. This will also support our future optimistic-locking demonstration.

## 7. How the Implementation Works

The repository executes the entire operation inside one PostgreSQL transaction.

### Step 1 — Begin Transaction

```go
tx, err := r.db.Begin(ctx)
if err != nil {
    return 0, err
}
```

### Step 2 — Acquire the Row Lock

```go
err = tx.QueryRow(ctx, `
    SELECT balance
    FROM wallets
    WHERE id = $1
    FOR UPDATE
`, walletID).Scan(&balance)
```

If another transaction already holds a conflicting lock, PostgreSQL waits until that transaction releases it, unless a timeout, cancellation, or lock error interrupts the operation.

### Step 3 — Validate Available Funds

```go
if balance < amount {
    return 0, ErrInsufficientFunds
}
```

The validation occurs while the transaction holds the wallet lock.

This ensures the operation does not make its decision using a balance that another transaction can concurrently overwrite.

### Step 4 — Update the Balance

```go
newBalance := balance - amount
```

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2;
```

The update runs within the same transaction.

### Step 5 — Commit

```go
if err := tx.Commit(ctx); err != nil {
    return 0, err
}
```

Committing makes the change durable and releases transaction-held locks.

If the transaction fails before committing, rollback discards its uncommitted modifications and releases its locks.

### Transaction Boundary

```text
BEGIN
  |
  +-- SELECT balance FOR UPDATE
  |       |
  |       +-- Acquire wallet row lock
  |
  +-- Validate sufficient funds
  |
  +-- Calculate new balance
  |
  +-- UPDATE wallets
  |
COMMIT
  |
  +-- Release row lock
```

**The lock must remain active across both the read and the update.**

Executing `SELECT FOR UPDATE` in a separate autocommit statement would release its lock before the subsequent business update.

## 8. Running the Example

Execute the commands from the repository root.

### Prerequisites

- Go 1.22 or newer
- Docker Desktop
- PostgreSQL 16
- Wallet migration applied

### Start PostgreSQL

```bash
docker compose up -d
docker compose ps
```

### Configure Environment

Create a `.env` file in the repository root:

```dotenv
APP_ENV=development
APP_PORT=8082
PESSIMISTIC_APP_PORT=8083

DATABASE_URL=postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable
```

The connection details are for local development and must match your Docker configuration.

### Run the Application

```bash
go run ./examples/02-pessimistic-locking
```

Expected startup message:

```text
Pessimistic locking API running on :8083
```

## 9. API Demonstration

### Create a Wallet

Execute:

```sql
INSERT INTO wallets (balance)
VALUES (1000)
RETURNING id, balance;
```

Copy the generated wallet UUID.

### Withdraw Funds

Replace `YOUR_WALLET_UUID` with the actual identifier.

```bash
curl --location \
'http://localhost:8083/wallets/YOUR_WALLET_UUID/withdraw' \
--header 'Content-Type: application/json' \
--data '{
    "amount": 100
}'
```

Expected response: HTTP `200 OK`.

```json
{
    "wallet_id": "YOUR_WALLET_UUID",
    "balance": 900
}
```

A subsequent withdrawal of 200 should return a balance of 700.

Sequential requests validate the API's basic functionality. The concurrency guarantees are tested separately.

## 10. Concurrent Integration Testing

The repository includes tests using a real PostgreSQL database and HTTP test server.

### Test 1 — Concurrent Withdrawals

Two requests withdraw 100 and 200 from a wallet containing 1000.

Expected state:

| Property | Expected |
|---|---:|
| Successful withdrawals | 2 |
| Final balance | 700 |
| Wallet version | 2 |

Both successful withdrawals must be reflected in the final balance.

### Test 2 — Concurrent Insufficient Funds

Two requests each attempt to withdraw 700 from a balance of 1000.

Expected behavior:

| Property | Expected |
|---|---:|
| Successful withdrawals | 1 |
| Rejected withdrawals | 1 |
| Final balance | 300 |
| Wallet version | 1 |

The first successful transaction reduces the balance to 300.

The next transaction acquires the lock, observes the updated balance, and rejects the withdrawal for insufficient funds.

This prevents both requests from spending the same available funds.

### Test 3 — Row-Lock Contention

An integration test explicitly acquires a wallet row lock in Transaction A:

```sql
BEGIN;

SELECT balance
FROM wallets
WHERE id = $1
FOR UPDATE;
```

While Transaction A holds the lock, the test sends another withdrawal through the HTTP API.

The request should remain pending until the lock is released.

After Transaction A commits, the withdrawal completes and updates the wallet.

This test observes blocking over a bounded interval. A stricter test could instrument the precise point where the competing transaction attempts its locking query.

### Run All Tests

```bash
go test -v ./examples/02-pessimistic-locking -count=1
```

### Expected Test Summary

```text
=== RUN   TestConcurrentWithdrawals
    Final balance: 700
    Wallet version: 2
--- PASS: TestConcurrentWithdrawals

=== RUN   TestConcurrentInsufficientFunds
    Successful withdrawals: 1
    Rejected withdrawals: 1
    Final balance: 300
    Wallet version: 1
--- PASS: TestConcurrentInsufficientFunds

=== RUN   TestRowLockContention
    Transaction A acquired the wallet row lock
    Withdrawal still pending while row is locked
    Transaction A committed and released the lock
    Final balance: 900
    Wallet version: 1
--- PASS: TestRowLockContention

PASS
```

These are illustrative expected results; actual output should be captured from the test execution.

## 11. How PostgreSQL Handles Row Locks

PostgreSQL uses multiple mechanisms to manage concurrent access, including MVCC and row-level locks.

### Ordinary SELECT vs SELECT FOR UPDATE

An ordinary `SELECT` generally does not block on another transaction's row-level `FOR UPDATE` lock under PostgreSQL MVCC. It can read an appropriate visible version of the row.

However:

```sql
SELECT balance
FROM wallets
WHERE id = $1
FOR UPDATE;
```

requests a lock on the selected row.

Another transaction requesting a conflicting lock on that row must wait until the earlier transaction finishes.

### Lock Scope

For the wallet example:

- Transactions modifying the same wallet contend for the row.
- Transactions modifying different wallets generally do not conflict on that row lock.
- Ordinary MVCC reads can continue according to their transaction snapshots.
- The row lock remains held until commit or rollback.

This supports concurrency across independent wallets while coordinating modifications to the same wallet.

### Isolation Level

The example uses PostgreSQL's default `READ COMMITTED` isolation level.

After a transaction waits for a conflicting row modification to complete, its locking query can observe and lock the updated row.

This is sufficient for the demonstrated single-wallet withdrawal pattern.

More complex invariants involving several rows or aggregate conditions may require additional locking or stronger isolation.

## 12. Pessimistic Locking vs the Naive Approach

| Property | Naive Implementation | Pessimistic Locking |
|---|---|---|
| Read operation | Ordinary SELECT | SELECT FOR UPDATE |
| Transaction around read and write | No | Yes |
| Stale absolute updates | Possible | Prevented for this wallet |
| Concurrent withdrawal safety | Unsafe | Protected |
| Insufficient funds under contention | Can be incorrect | Validated under lock |
| Lock waiting | No explicit read lock | Conflicting requests wait |
| Operational complexity | Low | Requires transaction management |
| Throughput under high contention | Potentially higher, but incorrect | Limited by serialized operations |

A concurrency strategy should be evaluated for both correctness and performance.

The fastest implementation is not necessarily the correct one.

## 13. Production Trade-offs

### Lock Contention

When many requests target the same wallet, transactions must wait for the same row lock.

As contention increases, latency may rise and throughput for that wallet may decrease.

### Transaction Duration

Keep transactions short.

Avoid network calls, unnecessary processing, and long-running operations while holding a row lock.

### Connection Pool Pressure

Transactions waiting on locks occupy database connections.

High contention can exhaust an undersized connection pool and increase request latency.

### Deadlocks

When a transaction locks multiple rows, inconsistent lock acquisition order can cause deadlocks.

PostgreSQL detects database deadlocks and aborts a participating transaction.

Applications should use consistent lock ordering and carefully designed retries when appropriate.

### Timeouts

Production implementations should use appropriate request deadlines, statement timeouts, and lock timeouts.

A transaction waiting indefinitely is undesirable for latency-sensitive APIs.

### Commit Ambiguity

If a connection fails while committing, the application may not know whether the transaction succeeded.

Pessimistic locking does not resolve ambiguous commit outcomes by itself.

Request-level idempotency or another durable operation identity is needed when clients may retry operations with side effects.

## 14. Why Pessimistic Locking Is Not Idempotency

Pessimistic locking controls concurrent access to mutable shared state.

Idempotency controls repeated execution of a logical operation.

These are different guarantees.

For example:

```text
Initial balance: 1000

Request A: Withdraw 100
Request B: Withdraw 100

Both requests are accepted.

Final balance: 800
```

Pessimistic locking correctly serializes both withdrawals.

But if Request B was an unintended retry of Request A, the system has still processed the logical operation twice.

Preventing that requires idempotency protection, such as the mechanisms demonstrated in [Idempotency in Practice](https://github.com/dcodes-0x/idempotency-in-practice).

Real backend services often need both concurrency control and idempotency.

## 15. Engineering Takeaways

**1. Acquire the lock before reading shared mutable state.**

Locking after calculating an update from stale state is insufficient.

**2. Transaction boundaries define lock lifetime.**

The read, validation, and update must execute inside the same transaction.

**3. Validate business invariants while holding the lock.**

The available balance must be checked against the state protected by the lock.

**4. Pessimistic locking trades contention for coordination.**

Conflicting operations wait rather than independently overwriting shared state.

**5. Correctness and throughput are different dimensions.**

A concurrency strategy must satisfy business invariants before its performance is optimized.

**6. Idempotency and concurrency control solve different problems.**

One prevents duplicate business effects; the other coordinates competing state modifications.

## 16. What's Next?

### 03 — Optimistic Locking

The next example will solve the same wallet concurrency problem without acquiring a row lock during the initial read.

Instead, we'll use a version column and a conditional update:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2
  AND version = $3;
```

If another transaction modified the wallet, the version will no longer match.

The update affects zero rows, allowing the application to detect a concurrency conflict and retry or reject the operation.

We'll compare optimistic and pessimistic locking using the same wallet model and concurrent workloads.

---

Part of [Concurrency Control in Practice](../../README.md) — exploring database concurrency, transaction isolation, MVCC, and locking through runnable Go and PostgreSQL examples.