package main

import "github.com/google/uuid"

type WithdrawInput struct {
	WalletID uuid.UUID
	Amount   int64
}

type WithdrawResult struct {
	WalletID uuid.UUID `json:"wallet_id"`
	Balance  int64     `json:"balance"`
}
