package main

import (
	"context"
	"errors"

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
	return &PostgresWalletRepository{db: db}
}

func (r *PostgresWalletRepository) Withdraw(
	ctx context.Context,
	walletID uuid.UUID,
	amount int64,
) (int64, error) {

	// Begin a database transaction.
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Acquire a row lock before reading the balance.
	var balance int64

	err = tx.QueryRow(ctx, `
		SELECT balance
		FROM wallets
		WHERE id = $1
		FOR UPDATE
	`, walletID).Scan(&balance)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrWalletNotFound
	}
	if err != nil {
		return 0, err
	}

	// Validate while holding the row lock.
	if balance < amount {
		return 0, ErrInsufficientFunds
	}

	newBalance := balance - amount

	// Update within the same transaction.
	tag, err := tx.Exec(ctx, `
		UPDATE wallets
		SET balance = $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
	`, newBalance, walletID)

	if err != nil {
		return 0, err
	}

	if tag.RowsAffected() != 1 {
		return 0, ErrWalletNotFound
	}

	// Commit releases the lock.
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return newBalance, nil
}
