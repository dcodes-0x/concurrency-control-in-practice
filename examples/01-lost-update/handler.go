package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type WithdrawRequest struct {
	Amount int64 `json:"amount"`
}

type WalletHandler struct {
	service *WalletService
}

func NewWalletHandler(
	service *WalletService,
) *WalletHandler {
	return &WalletHandler{service: service}
}

func (h *WalletHandler) Withdraw(
	w http.ResponseWriter,
	r *http.Request,
) {
	walletID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "Invalid wallet ID", http.StatusBadRequest)
		return
	}

	var req WithdrawRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	err = h.service.Withdraw(
		r.Context(),
		WithdrawInput{
			WalletID: walletID,
			Amount:   req.Amount,
		},
	)

	switch {
	case errors.Is(err, ErrInvalidAmount):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return

	case errors.Is(err, ErrInsufficientFunds):
		http.Error(w, err.Error(), http.StatusConflict)
		return

	case errors.Is(err, pgx.ErrNoRows):
		http.Error(w, "Wallet not found", http.StatusNotFound)
		return

	case err != nil:
		log.Printf("Withdrawal failed: %v", err)
		http.Error(
			w,
			"Internal server error",
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
