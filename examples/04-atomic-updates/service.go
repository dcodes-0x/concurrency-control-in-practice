package main

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var ErrInvalidAmount = errors.New("amount must be positive")

type WalletStore interface {
	WithdrawAtomic(
		ctx context.Context,
		walletID uuid.UUID,
		amount int64,
	) (WithdrawResult, error)
}

type WalletService struct {
	repo WalletStore
}

func NewWalletService(repo WalletStore) *WalletService {
	return &WalletService{
		repo: repo,
	}
}

func (s *WalletService) Withdraw(
	ctx context.Context,
	input WithdrawInput,
) (WithdrawResult, error) {
	if input.Amount <= 0 {
		return WithdrawResult{}, ErrInvalidAmount
	}

	return s.repo.WithdrawAtomic(
		ctx,
		input.WalletID,
		input.Amount,
	)
}
