package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

type PostgresWalletRepository struct {
	db *pgxpool.Pool
}

func NewPostgresWalletRepository(
	db *pgxpool.Pool,
) *PostgresWalletRepository {
	return &PostgresWalletRepository{
		db: db,
	}
}

// WithdrawAtomic validates available funds and deducts
// the balance using a single atomic SQL statement.
func (r *PostgresWalletRepository) WithdrawAtomic(
	ctx context.Context,
	walletID uuid.UUID,
	amount int64,
) (WithdrawResult, error) {
	var result WithdrawResult

	err := r.db.QueryRow(ctx, `
		UPDATE wallets
		SET balance = balance - $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
		  AND balance >= $1
		RETURNING id, balance, version
	`,
		amount,
		walletID,
	).Scan(
		&result.WalletID,
		&result.Balance,
		&result.Version,
	)

	if err == nil {
		return result, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return WithdrawResult{}, fmt.Errorf(
			"atomic withdrawal failed: %w",
			err,
		)
	}

	// No row was updated.
	//
	// Possible reasons:
	// 1. Wallet does not exist.
	// 2. Wallet has insufficient funds.
	//
	// Perform a separate lookup only on the failure path.
	var exists bool

	err = r.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM wallets
			WHERE id = $1
		)
	`, walletID).Scan(&exists)

	if err != nil {
		return WithdrawResult{}, fmt.Errorf(
			"wallet existence check failed: %w",
			err,
		)
	}

	if !exists {
		return WithdrawResult{}, ErrWalletNotFound
	}

	return WithdrawResult{}, ErrInsufficientFunds
}
