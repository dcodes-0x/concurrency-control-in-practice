package isolation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type writeSkewResult struct {
	commits               int
	serializationFailures int
	finalOnCall           int
}

func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == "40001"
}

func countOnCall(
	ctx context.Context,
	tx pgx.Tx,
	shiftID uuid.UUID,
) (int, error) {
	var count int

	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM on_call_doctors
		WHERE shift_id = $1
		  AND is_on_call = TRUE
	`, shiftID).Scan(&count)

	return count, err
}

func setDoctorOffCall(
	ctx context.Context,
	tx pgx.Tx,
	doctorID uuid.UUID,
) error {
	tag, err := tx.Exec(ctx, `
		UPDATE on_call_doctors
		SET is_on_call = FALSE
		WHERE id = $1
	`, doctorID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf(
			"expected one updated doctor, got %d",
			tag.RowsAffected(),
		)
	}

	return nil
}

func setupWriteSkewDoctors(
	t *testing.T,
	db *pgxpool.Pool,
) (uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()

	shiftID := uuid.New()
	aliceID := uuid.New()
	bobID := uuid.New()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_, err := db.Exec(ctx, `
		INSERT INTO on_call_doctors
			(id, shift_id, doctor_name, is_on_call)
		VALUES
			($1, $3, 'Alice', TRUE),
			($2, $3, 'Bob', TRUE)
	`, aliceID, bobID, shiftID)
	if err != nil {
		t.Fatalf("insert doctors: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := db.Exec(cleanupCtx, `
			DELETE FROM on_call_doctors
			WHERE shift_id = $1
		`, shiftID)
		if err != nil {
			t.Errorf("cleanup doctors: %v", err)
		}
	})

	return shiftID, aliceID, bobID
}

func runWriteSkewScenario(
	t *testing.T,
	level pgx.TxIsoLevel,
) writeSkewResult {
	t.Helper()

	db := openTestDB(t)

	shiftID, aliceID, bobID := setupWriteSkewDoctors(
		t,
		db,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	// Two independent transactions with the same
	// isolation level.
	txA, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: level,
	})
	if err != nil {
		t.Fatalf("begin transaction A: %v", err)
	}

	txB, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: level,
	})
	if err != nil {
		_ = txA.Rollback(context.Background())
		t.Fatalf("begin transaction B: %v", err)
	}

	// Safety rollback for all non-committed paths.
	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer rollbackCancel()

		_ = txA.Rollback(rollbackCtx)
		_ = txB.Rollback(rollbackCtx)
	}()

	// Both transactions establish their snapshots
	// before either transaction modifies any row.
	countA, err := countOnCall(ctx, txA, shiftID)
	if err != nil {
		t.Fatalf("transaction A count: %v", err)
	}

	countB, err := countOnCall(ctx, txB, shiftID)
	if err != nil {
		t.Fatalf("transaction B count: %v", err)
	}

	t.Logf("Transaction A observes %d doctors on call", countA)
	t.Logf("Transaction B observes %d doctors on call", countB)

	if countA != 2 || countB != 2 {
		t.Fatalf(
			"expected both transactions to observe 2; got A=%d B=%d",
			countA,
			countB,
		)
	}

	// Both transactions independently decide that
	// one doctor can leave because the other remains.
	//
	// They update different rows. The transaction
	// overlap is controlled without sleeps or races.
	if err := setDoctorOffCall(ctx, txA, aliceID); err != nil {
		t.Fatalf("transaction A update: %v", err)
	}

	t.Log("Transaction A set Alice off call")

	result := writeSkewResult{}

	err = setDoctorOffCall(ctx, txB, bobID)

	if err != nil {
		if isSerializationFailure(err) {
			result.serializationFailures++
			t.Logf("Transaction B update aborted: %v", err)
		} else {
			t.Fatalf("transaction B update: %v", err)
		}
	} else {
		t.Log("Transaction B set Bob off call")
	}

	// Commit A first. Under SERIALIZABLE, B must not
	// also commit an outcome that leaves zero doctors.
	if err := txA.Commit(ctx); err != nil {
		if isSerializationFailure(err) {
			result.serializationFailures++
			t.Logf("Transaction A aborted: %v", err)
		} else {
			t.Fatalf("transaction A commit: %v", err)
		}
	} else {
		result.commits++
		t.Log("Transaction A committed")
	}

	// If B's UPDATE failed, B's transaction is already
	// unusable and must be rolled back. Otherwise,
	// attempt its commit.
	if err == nil {
		if commitErr := txB.Commit(ctx); commitErr != nil {
			if isSerializationFailure(commitErr) {
				result.serializationFailures++
				t.Logf(
					"Transaction B serialization failure: %v",
					commitErr,
				)
			} else {
				t.Fatalf(
					"transaction B commit: %v",
					commitErr,
				)
			}
		} else {
			result.commits++
			t.Log("Transaction B committed")
		}
	}

	// Observe the actual committed state using a
	// fresh query outside both transactions.
	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM on_call_doctors
		WHERE shift_id = $1
		  AND is_on_call = TRUE
	`, shiftID).Scan(&result.finalOnCall)

	if err != nil {
		t.Fatalf("final on-call count: %v", err)
	}

	t.Logf("Successful commits: %d", result.commits)
	t.Logf(
		"Serialization failures: %d",
		result.serializationFailures,
	)
	t.Logf("Final on-call doctors: %d", result.finalOnCall)

	return result
}

// REPEATABLE READ permits write skew across
// different rows, even with stable snapshots.
func TestRepeatableRead_WriteSkew(t *testing.T) {
	result := runWriteSkewScenario(
		t,
		pgx.RepeatableRead,
	)

	if result.commits != 2 {
		t.Errorf(
			"expected 2 commits, got %d",
			result.commits,
		)
	}

	if result.serializationFailures != 0 {
		t.Errorf(
			"expected 0 serialization failures, got %d",
			result.serializationFailures,
		)
	}

	if result.finalOnCall != 0 {
		t.Errorf(
			"expected invariant violation (0 on call), got %d",
			result.finalOnCall,
		)
	}

	t.Log("Write skew reproduced: business invariant violated")
}

// SERIALIZABLE prevents this nonserializable result
// by aborting one of the conflicting transactions.
func TestSerializable_PreventsWriteSkew(t *testing.T) {
	result := runWriteSkewScenario(
		t,
		pgx.Serializable,
	)

	if result.commits != 1 {
		t.Errorf(
			"expected 1 successful commit, got %d",
			result.commits,
		)
	}

	if result.serializationFailures != 1 {
		t.Errorf(
			"expected 1 serialization failure, got %d",
			result.serializationFailures,
		)
	}

	if result.finalOnCall != 1 {
		t.Errorf(
			"expected 1 doctor remaining, got %d",
			result.finalOnCall,
		)
	}

	t.Log("Serializable prevented write skew")
}
