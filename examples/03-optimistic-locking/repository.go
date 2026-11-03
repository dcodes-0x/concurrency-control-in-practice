package main

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrWalletNotFound = errors.New("wallet not found")

type PostgresWalletRepository struct {
	db *pgxpool.Pool
}

func NewPostgresWalletRepository(
	db *pgxpool.Pool,
) *PostgresWalletRepository {
	return &PostgresWalletRepository{db: db}
}

func (r *PostgresWalletRepository) GetWallet(
	ctx context.Context,
	walletID uuid.UUID,
) (Wallet, error) {
	var wallet Wallet

	err := r.db.QueryRow(ctx, `
		SELECT id, balance, version
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(
		&wallet.ID,
		&wallet.Balance,
		&wallet.Version,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Wallet{}, ErrWalletNotFound
	}

	if err != nil {
		return Wallet{}, err
	}

	return wallet, nil
}

// CompareAndSetBalance performs an optimistic update.
//
// Returns:
//   - updated wallet when the expected version matches
//   - updated=false when the version no longer matches
//   - an error when the database operation fails
func (r *PostgresWalletRepository) CompareAndSetBalance(
	ctx context.Context,
	walletID uuid.UUID,
	expectedVersion int64,
	newBalance int64,
) (Wallet, bool, error) {
	var updated Wallet

	err := r.db.QueryRow(ctx, `
		UPDATE wallets
		SET balance = $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
		  AND version = $3
		RETURNING id, balance, version
	`,
		newBalance,
		walletID,
		expectedVersion,
	).Scan(
		&updated.ID,
		&updated.Balance,
		&updated.Version,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Wallet{}, false, nil
	}

	if err != nil {
		return Wallet{}, false, err
	}

	return updated, true, nil
}
