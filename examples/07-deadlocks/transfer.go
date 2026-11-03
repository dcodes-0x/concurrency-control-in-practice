package deadlocks

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidTransfer   = errors.New("invalid transfer")
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

type Transfer struct {
	From   uuid.UUID
	To     uuid.UUID
	Amount int64
}

type WalletState struct {
	ID      uuid.UUID
	Balance int64
}

// lockWallet acquires a row-level lock for the
// duration of the caller's transaction.
func lockWallet(
	ctx context.Context,
	tx pgx.Tx,
	walletID uuid.UUID,
) (WalletState, error) {
	var wallet WalletState

	err := tx.QueryRow(ctx, `
		SELECT id, balance
		FROM wallets
		WHERE id = $1
		FOR UPDATE
	`, walletID).Scan(
		&wallet.ID,
		&wallet.Balance,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return WalletState{}, ErrWalletNotFound
	}
	if err != nil {
		return WalletState{}, err
	}

	return wallet, nil
}

// applyTransfer expects both wallet rows to
// already be locked by the current transaction.
//
// No external side effects are performed.
func applyTransfer(
	ctx context.Context,
	tx pgx.Tx,
	transfer Transfer,
	fromBalance int64,
) error {
	if transfer.Amount <= 0 || transfer.From == transfer.To {
		return ErrInvalidTransfer
	}

	if fromBalance < transfer.Amount {
		return ErrInsufficientFunds
	}

	// Debit the source wallet.
	tag, err := tx.Exec(ctx, `
		UPDATE wallets
		SET balance = balance - $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
	`, transfer.Amount, transfer.From)

	if err != nil {
		return fmt.Errorf("debit wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrWalletNotFound
	}

	// Credit the destination wallet.
	tag, err = tx.Exec(ctx, `
		UPDATE wallets
		SET balance = balance + $1,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $2
	`, transfer.Amount, transfer.To)

	if err != nil {
		return fmt.Errorf("credit wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrWalletNotFound
	}

	return nil
}
