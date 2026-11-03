# 03 — Optimistic Locking: Version-Based Concurrency Control

**Preventing lost updates using PostgreSQL, version-based Compare-and-Set (CAS), and bounded retries in Go.**

This example demonstrates how optimistic concurrency control prevents concurrent requests from silently overwriting each other's database changes.

Unlike pessimistic locking, optimistic locking does not acquire an explicit row lock during the initial read. Instead, it assumes concurrent conflicts are relatively uncommon and detects them when attempting to update shared state.

We implement two approaches:

1. **Fail-fast optimistic locking:** Detect a version conflict and reject the operation.
2. **Optimistic locking with retries:** Re-read the latest state, revalidate business rules, and retry the conditional update.

> **Core principle:** Never overwrite shared mutable state based on a stale read. Verify that the state has not changed before committing the update.

---

## 1. The Problem: Lost Updates

Consider a wallet containing 1000 units.

Two concurrent requests attempt withdrawals:

| Operation | Amount |
|---|---:|
| Initial balance | 1000 |
| Withdrawal A | 100 |
| Withdrawal B | 200 |
| Expected balance if both succeed | 700 |

A naive read-modify-write implementation might execute:

```sql
SELECT balance
FROM wallets
WHERE id = $1;
```

Both requests can read 1000 and independently calculate their replacement values.

```text
Request A reads 1000
Request B reads 1000

Request A calculates 900
Request B calculates 800

Request A writes 900
Request B writes 800

Final balance: 800
Expected balance: 700
```

Both operations may report success even though one update has been lost.

We reproduced this anomaly in [Demo 01 — Lost Update](../01-lost-update/README.md).

In [Demo 02 — Pessimistic Locking](../02-pessimistic-locking/README.md), we solved it with `SELECT FOR UPDATE`.

This example solves the same problem using **optimistic concurrency control**.

## 2. How Optimistic Locking Works

Each wallet has a `version` column.

Whenever the wallet is successfully modified, its version increments.

The application reads both the balance and the version:

```sql
SELECT id, balance, version
FROM wallets
WHERE id = $1;
```

It then attempts an update conditioned on the version it observed:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2
  AND version = $3
RETURNING id, balance, version;
```

This operation is called **Compare-and-Set (CAS)**.

The update succeeds only if the wallet still has the expected version.

### Example

Initially:

```text
Wallet balance: 1000
Wallet version: 0
```

Two requests concurrently read that state.

```mermaid
sequenceDiagram
    participant A as Withdrawal A
    participant DB as PostgreSQL
    participant B as Withdrawal B

    A->>DB: SELECT balance, version
    DB-->>A: 1000, version 0

    B->>DB: SELECT balance, version
    DB-->>B: 1000, version 0

    Note over A,B: Both requests read the same version

    A->>DB: UPDATE balance=900 WHERE version=0
    DB-->>A: Success, version=1

    B->>DB: UPDATE balance=800 WHERE version=0
    DB-->>B: No rows returned

    Note over B: Version conflict detected
    Note over DB: Balance remains 900
```

The second update does not overwrite the first.

The database detects that its expected version is stale.

**Important:** In this fail-fast scenario, only one withdrawal succeeds. A final balance of 900 is correct because the withdrawal of 200 was rejected.

This is fundamentally different from the lost-update anomaly, where both requests reported success.

## 3. Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| HTTP server | `net/http` |
| Database | PostgreSQL 16 |
| Database driver | `pgx/v5` |
| Containerization | Docker Compose |
| Concurrency control | Version-based CAS |
| Retry strategy | Bounded exponential backoff with full jitter |
| Testing | Go `testing`, `httptest`, PostgreSQL integration tests |
| Identifiers | UUID |

## 4. Project Structure

```text
03-optimistic-locking/
├── main.go
├── model.go
├── handler.go
├── service.go
├── repository.go
├── concurrency_test.go
├── retry_test.go
└── README.md
```

The example follows the familiar layered architecture:

```text
HTTP Client
    |
    v
Wallet Handler
    |
    v
Wallet Service
    |
    | Read wallet
    | Validate balance
    | Calculate new balance
    | Attempt CAS
    | Retry on conflict
    v
Wallet Repository
    |
    | SELECT balance, version
    | UPDATE ... WHERE version = ?
    v
PostgreSQL
```

The repository owns SQL operations.

The service owns business validation and retry behavior.

The handler translates application results into HTTP responses.

## 5. Database Design

We reuse the `wallets` table from the earlier demonstrations.

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

### Why Use a Version Column?

The version acts as a concurrency-control token.

Suppose the application reads:

```text
balance = 1000
version = 4
```

Before the application updates the wallet, another transaction modifies it:

```text
balance = 900
version = 5
```

The original request attempts:

```sql
UPDATE wallets
SET balance = 800,
    version = version + 1
WHERE id = $1
  AND version = 4;
```

The update matches zero rows.

Because the expected version is no longer current, the stale update is rejected.

### Important Versioning Rule

Every operation that modifies concurrency-protected wallet state must participate in the versioning convention.

An unrelated operation that modifies the balance without incrementing `version` can undermine the optimistic-locking guarantee.

## 6. Repository Implementation

The repository exposes two operations.

### Read Wallet State

```go
func (r *PostgresWalletRepository) GetWallet(
    ctx context.Context,
    walletID uuid.UUID,
) (Wallet, error)
```

The SQL is a regular MVCC read:

```sql
SELECT id, balance, version
FROM wallets
WHERE id = $1;
```

No explicit `FOR UPDATE` lock is acquired.

### Compare-and-Set Balance

```go
func (r *PostgresWalletRepository) CompareAndSetBalance(
    ctx context.Context,
    walletID uuid.UUID,
    expectedVersion int64,
    newBalance int64,
) (Wallet, bool, error)
```

Its SQL:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2
  AND version = $3
RETURNING id, balance, version;
```

The method distinguishes:

| Result | Meaning |
|---|---|
| Updated row returned | CAS succeeded |
| No row returned | Expected version no longer matches, or wallet no longer exists |
| Database error | Query execution failed |

A zero-row update is not automatically proof of a version mismatch. The wallet might have been deleted. Our demonstration returns a version conflict on this path; a production implementation may distinguish the cases.

## 7. Fail-Fast Optimistic Locking

Our first service constructor is:

```go
service := NewWalletService(repo)
```

This configuration uses zero retries.

The withdrawal flow is:

```text
1. Validate amount
2. Read wallet balance and version
3. Check sufficient funds
4. Calculate updated balance
5. Execute CAS
6. Return success or version conflict
```

If another request changes the wallet before the CAS operation, the service returns:

```text
ErrVersionConflict
```

The HTTP handler maps this to:

```http
HTTP/1.1 409 Conflict
```

This approach is useful when the caller should explicitly handle concurrency conflicts or when automatically repeating a business operation would be inappropriate.

## 8. Optimistic Locking with Automatic Retries

Fail-fast behavior protects correctness but may unnecessarily reject requests that would remain valid after refreshing the wallet state.

For example, suppose two requests withdraw 100 and 200 from 1000.

After the first withdrawal succeeds, the second can still safely withdraw its amount.

Instead of immediately returning a conflict, the application can retry.

### Retry Flow

```mermaid
flowchart TD
    A[Receive withdrawal] --> B[Read wallet and version]
    B --> C{Sufficient funds?}
    C -->|No| D[Reject withdrawal]
    C -->|Yes| E[Attempt version-based CAS]
    E --> F{CAS succeeded?}
    F -->|Yes| G[Return success]
    F -->|No| H{Retry budget available?}
    H -->|No| I[Return version conflict]
    H -->|Yes| J[Backoff with jitter]
    J --> B
```

The retry-enabled constructor is:

```go
service := NewRetryingWalletService(repo)
```

### Retry Configuration

```go
RetryPolicy{
    MaxRetries: 3,
    BaseDelay:  10 * time.Millisecond,
    MaxDelay:   80 * time.Millisecond,
}
```

This means:

- One initial CAS attempt.
- Up to three additional attempts after conflicts.
- Four total attempts at most.
- Fresh wallet reads on every retry.
- Exponential backoff with a maximum delay.
- Jitter to reduce synchronized retries.

### Why Re-read the Wallet?

A failed CAS means the original wallet snapshot may no longer be valid.

Retrying with the old version and balance would likely fail again.

Instead:

```text
Read balance=1000, version=0
       |
       v
CAS fails
       |
       v
Re-read balance=900, version=1
       |
       v
Recalculate withdrawal
       |
       v
CAS with version=1
       |
       v
Success
```

The application must recalculate the new balance from the latest observed state.

### Why Revalidate Available Funds?

Consider two withdrawals of 700 from an initial balance of 1000.

Both initially observe sufficient funds.

The first withdrawal succeeds:

```text
Balance = 300
Version = 1
```

The second request detects a version conflict.

On retry, it reads 300 and discovers insufficient funds.

It must reject the operation rather than perform another deduction.

**Business validation is required on every retry, not just the first attempt.**

## 9. Exponential Backoff and Jitter

When multiple requests conflict, immediate retries can create additional contention.

To reduce repeated simultaneous retries, the service uses capped exponential backoff with full jitter.

A simplified delay progression before jitter is:

| Retry | Backoff cap |
|---|---:|
| 1 | 10 ms |
| 2 | 20 ms |
| 3 | 40 ms |

Each actual delay is randomly selected from zero up to the corresponding cap.

The configured maximum delay is 80 ms, which would matter if additional retries were allowed.

### Why Jitter?

Without jitter, requests that fail at approximately the same time may retry together.

That can create another wave of conflicts.

Randomized delays distribute retry attempts over time and reduce synchronization.

### Context Cancellation

The service checks:

```go
if err := ctx.Err(); err != nil {
    return WithdrawResult{}, err
}
```

And uses a context-aware wait:

```go
select {
case <-ctx.Done():
    return ctx.Err()
case <-timer.C:
    return nil
}
```

This prevents the retry loop from continuing after its request context has been cancelled.

## 10. Running the Application

Run commands from the repository root.

### Prerequisites

- Go 1.22 or later
- Docker Desktop
- PostgreSQL container running
- Wallet migration applied

Start PostgreSQL:

```bash
docker compose up -d
```

Configure your local `.env`:

```dotenv
APP_ENV=development
OPTIMISTIC_APP_PORT=8084

DATABASE_URL=postgres://dev:devpassword@localhost:55433/concurrency_lab?sslmode=disable
```

The database port must match your local Docker Compose configuration.

Run:

```bash
go run ./examples/03-optimistic-locking
```

Expected startup message:

```text
Optimistic locking API running on :8084
```

The running application uses the retry-enabled service when `main.go` is configured with:

```go
service := NewRetryingWalletService(repo)
```

## 11. API Demonstration

Create a wallet:

```sql
INSERT INTO wallets (balance)
VALUES (1000)
RETURNING id, balance, version;
```

A new wallet begins at version 0.

Send a withdrawal:

```bash
curl --location \
'http://localhost:8084/wallets/YOUR_WALLET_UUID/withdraw' \
--header 'Content-Type: application/json' \
--data '{
    "amount": 100
}'
```

Expected successful response:

```json
{
    "wallet_id": "YOUR_WALLET_UUID",
    "balance": 900,
    "version": 1
}
```

A second successful withdrawal of 200 should produce:

```json
{
    "wallet_id": "YOUR_WALLET_UUID",
    "balance": 700,
    "version": 2
}
```

Sequential requests verify the basic update behavior. The concurrency guarantees are verified separately through integration tests.

## 12. Integration Testing

The tests use a real PostgreSQL instance and HTTP test server.

### Test 1 — Single Withdrawal

Verifies that a successful withdrawal modifies both the balance and version.

```text
Initial balance = 1000
Initial version = 0

Withdraw = 100

Final balance = 900
Final version = 1
```

### Test 2 — Stale Version Rejection

The test reads version 0 and successfully performs one CAS.

It then attempts another CAS using the same stale version.

Expected outcome:

```text
First CAS:  Success
Second CAS: Rejected

Balance: 900
Version: 1
```

This verifies the fundamental optimistic-locking guarantee.

### Test 3 — Deterministic Concurrent Conflict

Two concurrent HTTP requests both read version 0.

A test-only synchronization barrier ensures neither request attempts its CAS until both reads have completed.

Expected result:

```text
One request succeeds.
One request returns HTTP 409.

Final version = 1.
Final balance reflects only the successful withdrawal.
```

This test uses the fail-fast constructor.

### Test 4 — Concurrent Insufficient Funds

Two requests each attempt to withdraw 700 from a balance of 1000.

Both begin with the same version.

Exactly one CAS succeeds.

The other request receives a concurrency conflict rather than overwriting the wallet.

Final state:

```text
Balance = 300
Version = 1
```

### Test 5 — Concurrent Withdrawals with Retry

Both requests initially read version 0.

One CAS succeeds.

The second encounters a version conflict, performs a fresh read, recalculates the balance, and retries.

Expected result:

```text
Initial balance = 1000

Withdraw A = 100
Withdraw B = 200

Both requests succeed.

Final balance = 700
Final version = 2
```

### Test 6 — Insufficient Funds After Retry

Both requests initially attempt to withdraw 700.

One succeeds.

The other retries and observes that only 300 remains.

Expected result:

```text
Successful withdrawals = 1
Insufficient-funds responses = 1

Final balance = 300
Final version = 1
```

### Test 7 — Retry Limit Exhaustion

A fake repository deliberately rejects every CAS.

With two configured retries:

```text
Initial attempt = 1
Additional retries = 2

Total CAS attempts = 3
```

The service must return `ErrVersionConflict` after exhausting its retry budget.

### Test 8 — Context Cancellation

The test verifies that an already-cancelled request stops instead of performing unnecessary retry work.

This ensures retry behavior respects cancellation.

## 13. Run the Tests

Run all tests for Demo 03:

```bash
go test -v ./examples/03-optimistic-locking -count=1
```

Run the entire repository test suite:

```bash
go test ./... -count=1
```

The original fail-fast tests use:

```go
NewWalletService(repo)
```

Retry-enabled tests use:

```go
NewRetryingWalletService(repo)
```

This distinction allows both concurrency-control behaviors to be tested independently.

## 14. PostgreSQL Concurrency Guarantees

A frequent misconception is that optimistic locking means the database does not acquire locks.

That is incorrect.

PostgreSQL still uses row-level locking internally when executing an `UPDATE`.

Optimistic locking avoids holding an explicit `SELECT FOR UPDATE` lock across the application's initial read and computation.

Under PostgreSQL's default `READ COMMITTED` isolation level, concurrent updates to the same row are coordinated.

When an update waits for a competing transaction, PostgreSQL re-evaluates the relevant update condition against the updated row.

Therefore:

```sql
UPDATE wallets
SET balance = $1,
    version = version + 1
WHERE id = $2
  AND version = $3;
```

does not blindly overwrite a newer version.

If the expected version no longer matches, the update affects zero rows.

This is the foundation of our CAS implementation.

## 15. Pessimistic vs Optimistic Locking

| Property | Pessimistic Locking | Optimistic Locking |
|---|---|---|
| Initial read | `SELECT FOR UPDATE` | Ordinary `SELECT` |
| Conflict strategy | Prevent competing access through lock waiting | Detect competing updates through CAS |
| Application transaction spans read and write | Yes | Not required for this single-row CAS |
| Concurrent reads | Supported | Supported |
| Explicit lock during initial read | Yes | No |
| Handling conflicts | Wait, then read current state | Reject or retry |
| High contention | More waiting | More CAS failures and retries |
| Low contention | Lock overhead still applies | Few version conflicts |
| Implementation concerns | Lock duration, deadlocks, connection usage | Retry limits, conflict rates, stale reads |
| Typical fit | Frequently conflicting operations | Relatively infrequent conflicts |

Neither approach is universally superior.

The choice depends on workload characteristics, business requirements, and contention patterns.

## 16. Why Optimistic Locking Is Not Idempotency

Optimistic locking prevents stale database updates.

It does not prevent the same logical operation from being executed repeatedly.

Suppose a client successfully withdraws 100 but does not receive the HTTP response.

It retries the request.

The second request performs a fresh read, observes the latest wallet version, and can successfully withdraw another 100.

The version check does not recognize the request as a duplicate.

Therefore, real payment or wallet systems may need both:

- **Optimistic concurrency control** to prevent conflicting state updates.
- **Idempotency keys** to prevent duplicate business operations caused by retries.

See [Idempotency in Practice](https://github.com/dcodes-0x/idempotency-in-practice).

## 17. Production Considerations

### Conflict Rate

Monitor the percentage of CAS attempts rejected because of version mismatches.

A high conflict rate may indicate significant contention on particular records.

### Retry Amplification

A retrying request performs additional reads and conditional writes.

Under heavy contention, retries can increase database load and latency.

Bounded retries and backoff help control this amplification.

### Retry Exhaustion

If the retry budget is exhausted, return an explicit conflict rather than retry indefinitely.

### Lost Responses and Idempotency

A database operation may succeed even when its HTTP response is lost.

Retrying an entire business operation is not automatically safe. Request-level idempotency is necessary when duplicate execution would be harmful.

### Multi-Row Invariants

Version checking one wallet row protects that row from stale writes.

It does not automatically protect invariants involving multiple records.

For example, a rule involving an aggregate balance across multiple wallets may require broader transactional coordination or stronger isolation.

### Missing or Deleted Records

A CAS that affects zero rows can represent either a version mismatch or a deleted record.

Production error handling should distinguish these cases where the API contract requires it.

### Version Discipline

Every writer modifying the protected wallet state must increment the version.

Bypassing the version convention weakens the correctness guarantee.

### Observability

Useful metrics include:

```text
wallet_cas_conflicts_total
wallet_cas_retries_total
wallet_cas_retry_exhausted_total
wallet_withdrawal_duration_seconds
```

These are suggested instrumentation names, not metrics implemented by this example.

## 18. Engineering Takeaways

**1. Optimistic locking detects concurrent state changes instead of locking the row before reading it.**

**2. Version checks must be part of the database update condition.** A separate application-side version comparison is insufficient.

**3. Zero-row CAS results must be handled explicitly.** Never treat them as successful writes.

**4. Retries require fresh reads.** Reusing a stale balance or version defeats the purpose of retrying.

**5. Business invariants must be revalidated after every conflict.** A competing transaction may have changed available funds.

**6. Retries must be bounded and cancellation-aware.** Infinite retries can cause excessive load and unpredictable latency.

**7. Optimistic concurrency control and idempotency solve different problems.** Both may be required in reliable financial workflows.

## 19. What's Next?

### Demo 04 — Atomic Conditional Updates

Our next demonstration will examine whether the wallet withdrawal can be safely implemented using a single SQL statement.

Instead of reading the balance into Go and then writing a replacement value, we'll let PostgreSQL perform the calculation:

```sql
UPDATE wallets
SET balance = balance - $1,
    version = version + 1,
    updated_at = NOW()
WHERE id = $2
  AND balance >= $1
RETURNING balance, version;
```

We'll examine:

- Atomic updates.
- Concurrent arithmetic on shared rows.
- Conditional balance validation.
- PostgreSQL row locking during updates.
- Throughput and contention trade-offs.
- When a single SQL statement is preferable to an application-level concurrency-control workflow.

---

Part of [Concurrency Control in Practice](../../README.md) — exploring PostgreSQL transactions, MVCC, locking, and concurrency anomalies through runnable Go implementations and integration tests.