package main

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var (
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrInsufficientFunds = errors.New("insufficient balance")
)

type WalletStore interface {
	GetBalance(
		ctx context.Context,
		walletID uuid.UUID,
	) (int64, error)

	SetBalance(
		ctx context.Context,
		walletID uuid.UUID,
		balance int64,
	) error
}

type WalletService struct {
	repo WalletStore
}

func NewWalletService(repo WalletStore) *WalletService {
	return &WalletService{repo: repo}
}

func (s *WalletService) Withdraw(
	ctx context.Context,
	input WithdrawInput,
) error {
	if input.Amount <= 0 {
		return ErrInvalidAmount
	}

	// Step 1: Read current balance.
	balance, err := s.repo.GetBalance(
		ctx,
		input.WalletID,
	)
	if err != nil {
		return err
	}

	// Step 2: Validate funds.
	if balance < input.Amount {
		return ErrInsufficientFunds
	}

	// Step 3: Calculate balance in application memory.
	newBalance := balance - input.Amount

	// Step 4: Write the calculated value.
	// WARNING: Intentionally unsafe under concurrency.
	return s.repo.SetBalance(
		ctx,
		input.WalletID,
		newBalance,
	)
}
