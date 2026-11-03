package main

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrVersionConflict   = errors.New("wallet was modified concurrently")
)

type WalletStore interface {
	GetWallet(
		ctx context.Context,
		walletID uuid.UUID,
	) (Wallet, error)

	CompareAndSetBalance(
		ctx context.Context,
		walletID uuid.UUID,
		expectedVersion int64,
		newBalance int64,
	) (Wallet, bool, error)
}

type RetryPolicy struct {
	// Number of additional attempts after the initial CAS.
	MaxRetries int

	// Initial retry backoff.
	BaseDelay time.Duration

	// Maximum retry backoff.
	MaxDelay time.Duration
}

type WalletService struct {
	repo        WalletStore
	retryPolicy RetryPolicy
}

// NewWalletService creates a fail-fast implementation.
//
// A version mismatch immediately returns ErrVersionConflict.
// Existing concurrency tests can use this constructor.
func NewWalletService(repo WalletStore) *WalletService {
	return &WalletService{
		repo: repo,
		retryPolicy: RetryPolicy{
			MaxRetries: 0,
		},
	}
}

// NewRetryingWalletService enables bounded retries
// with exponential backoff and jitter.
func NewRetryingWalletService(repo WalletStore) *WalletService {
	return &WalletService{
		repo: repo,
		retryPolicy: RetryPolicy{
			MaxRetries: 3,
			BaseDelay:  10 * time.Millisecond,
			MaxDelay:   80 * time.Millisecond,
		},
	}
}

func (s *WalletService) Withdraw(
	ctx context.Context,
	input WithdrawInput,
) (WithdrawResult, error) {
	if input.Amount <= 0 {
		return WithdrawResult{}, ErrInvalidAmount
	}

	for attempt := 0; attempt <= s.retryPolicy.MaxRetries; attempt++ {
		// Stop immediately if the request has been cancelled.
		if err := ctx.Err(); err != nil {
			return WithdrawResult{}, err
		}

		// Step 1: Read the latest visible wallet state.
		// This is an ordinary SELECT, without FOR UPDATE.
		wallet, err := s.repo.GetWallet(
			ctx,
			input.WalletID,
		)
		if err != nil {
			return WithdrawResult{}, err
		}

		// Step 2: Validate available funds on every attempt.
		// A competing withdrawal may have changed the balance.
		if wallet.Balance < input.Amount {
			return WithdrawResult{}, ErrInsufficientFunds
		}

		// Step 3: Calculate the replacement balance.
		newBalance := wallet.Balance - input.Amount

		// Step 4: Conditionally update the wallet only if
		// its version still matches what we previously read.
		updatedWallet, updated, err :=
			s.repo.CompareAndSetBalance(
				ctx,
				input.WalletID,
				wallet.Version,
				newBalance,
			)

		if err != nil {
			return WithdrawResult{}, err
		}

		// Step 5: Successful CAS.
		if updated {
			return WithdrawResult{
				WalletID: updatedWallet.ID,
				Balance:  updatedWallet.Balance,
				Version:  updatedWallet.Version,
			}, nil
		}

		// Step 6: CAS failed due to a possible concurrent
		// modification (or a wallet deleted after the read).
		// Do not retry after the configured limit.
		if attempt == s.retryPolicy.MaxRetries {
			return WithdrawResult{}, ErrVersionConflict
		}

		// Step 7: Wait before retrying with a fresh read.
		if err := s.waitBeforeRetry(ctx, attempt); err != nil {
			return WithdrawResult{}, err
		}
	}

	return WithdrawResult{}, ErrVersionConflict
}

// waitBeforeRetry implements capped exponential backoff
// with full jitter, respecting request cancellation.
func (s *WalletService) waitBeforeRetry(
	ctx context.Context,
	attempt int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	delay := s.retryPolicy.BaseDelay
	maxDelay := s.retryPolicy.MaxDelay

	// Double the delay for each successive retry,
	// without exceeding MaxDelay.
	for i := 0; i < attempt; i++ {
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}

		delay *= 2
	}

	if delay > maxDelay {
		delay = maxDelay
	}

	// Full jitter: choose a random duration between
	// zero and the calculated exponential backoff.
	if delay > 0 {
		delay = time.Duration(
			rand.Int63n(int64(delay) + 1),
		)
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return nil
	}
}
