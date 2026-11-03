package main

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresWalletRepository struct {
	db *pgxpool.Pool
}

func NewPostgresWalletRepository(
	db *pgxpool.Pool,
) *PostgresWalletRepository {
	return &PostgresWalletRepository{db: db}
}

func (r *PostgresWalletRepository) GetBalance(
	ctx context.Context,
	walletID uuid.UUID,
) (int64, error) {
	var balance int64

	err := r.db.QueryRow(ctx, `
		SELECT balance
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(&balance)

	return balance, err
}

func (r *PostgresWalletRepository) SetBalance(
	ctx context.Context,
	walletID uuid.UUID,
	balance int64,
) error {
	_, err := r.db.Exec(ctx, `
		UPDATE wallets
		SET balance = $1,
		    updated_at = NOW()
		WHERE id = $2
	`, balance, walletID)

	return err
}
