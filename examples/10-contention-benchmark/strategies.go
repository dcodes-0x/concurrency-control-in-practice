package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrRetriesExhausted  = errors.New("CAS retry budget exhausted")
)

type StrategyFunc func(
	ctx context.Context,
	db *pgxpool.Pool,
	walletID uuid.UUID,
	amount int64,
) (conflicts int, err error)

func unsafeWithdraw(delay time.Duration) StrategyFunc {
	return func(
		ctx context.Context,
		db *pgxpool.Pool,
		walletID uuid.UUID,
		amount int64,
	) (int, error) {
		var balance int64

		err := db.QueryRow(ctx, `
			SELECT balance
			FROM wallets
			WHERE id = $1
		`, walletID).Scan(&balance)
		if err != nil {
			return 0, err
		}

		if balance < amount {
			return 0, ErrInsufficientFunds
		}

		// Deliberately widen the race window.
		// Never use this strategy in production.
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-timer.C:
			}
		}

		// BUG: Updates from another request may have
		// committed since we read balance.
		//
		// This absolute assignment can overwrite
		// another withdrawal's result.
		tag, err := db.Exec(ctx, `
			UPDATE wallets
			SET balance = $1,
			    version = version + 1,
			    updated_at = NOW()
			WHERE id = $2
		`, balance-amount, walletID)
		if err != nil {
			return 0, err
		}

		if tag.RowsAffected() != 1 {
			return 0, pgx.ErrNoRows
		}

		return 0, nil
	}
}

func pessimisticWithdraw(
	ctx context.Context,
	db *pgxpool.Pool,
	walletID uuid.UUID,
	amount int64,
) (int, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return 0, err
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	var balance int64

	err = tx.QueryRow(ctx, `
		SELECT balance
		FROM wallets
		WHERE id = $1
		FOR UPDATE
	`, walletID).Scan(&balance)
	if err != nil {
		return 0, err
	}

	if balance < amount {
		return 0, ErrInsufficientFunds
	}

	tag, err := tx.Exec(ctx, `
		UPDATE wallets
		SET balance = balance - $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
	`, amount, walletID)
	if err != nil {
		return 0, err
	}

	if tag.RowsAffected() != 1 {
		return 0, pgx.ErrNoRows
	}

	return 0, tx.Commit(ctx)
}

func optimisticWithdraw(maxRetries int) StrategyFunc {
	return func(
		ctx context.Context,
		db *pgxpool.Pool,
		walletID uuid.UUID,
		amount int64,
	) (int, error) {
		conflicts := 0

		for attempt := 0; attempt <= maxRetries; attempt++ {
			if err := ctx.Err(); err != nil {
				return conflicts, err
			}

			var balance, version int64

			err := db.QueryRow(ctx, `
				SELECT balance, version
				FROM wallets
				WHERE id = $1
			`, walletID).Scan(&balance, &version)
			if err != nil {
				return conflicts, err
			}

			if balance < amount {
				return conflicts, ErrInsufficientFunds
			}

			tag, err := db.Exec(ctx, `
				UPDATE wallets
				SET balance = $1,
				    version = version + 1,
				    updated_at = NOW()
				WHERE id = $2
				  AND version = $3
			`, balance-amount, walletID, version)
			if err != nil {
				return conflicts, err
			}

			if tag.RowsAffected() == 1 {
				return conflicts, nil
			}

			// Another request changed the version
			// after our read.
			conflicts++
		}

		return conflicts, fmt.Errorf(
			"%w: %d conflicts",
			ErrRetriesExhausted,
			conflicts,
		)
	}
}

func atomicWithdraw(
	ctx context.Context,
	db *pgxpool.Pool,
	walletID uuid.UUID,
	amount int64,
) (int, error) {
	tag, err := db.Exec(ctx, `
		UPDATE wallets
		SET balance = balance - $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
		  AND balance >= $1
	`, amount, walletID)
	if err != nil {
		return 0, err
	}

	if tag.RowsAffected() != 1 {
		return 0, ErrInsufficientFunds
	}

	return 0, nil
}
