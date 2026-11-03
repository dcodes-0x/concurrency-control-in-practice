package main

import "github.com/google/uuid"

type Wallet struct {
	ID      uuid.UUID `json:"id"`
	Balance int64     `json:"balance"`
	Version int64     `json:"version"`
}

type WithdrawInput struct {
	WalletID uuid.UUID
	Amount   int64
}
