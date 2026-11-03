package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/google/uuid"
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
	return &WalletHandler{
		service: service,
	}
}

func (h *WalletHandler) Withdraw(
	w http.ResponseWriter,
	r *http.Request,
) {
	walletID, err := uuid.Parse(
		r.PathValue("id"),
	)
	if err != nil {
		http.Error(
			w,
			"invalid wallet ID",
			http.StatusBadRequest,
		)
		return
	}

	var req WithdrawRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	result, err := h.service.Withdraw(
		r.Context(),
		WithdrawInput{
			WalletID: walletID,
			Amount:   req.Amount,
		},
	)

	switch {
	case errors.Is(err, ErrInvalidAmount):
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return

	case errors.Is(err, ErrInsufficientFunds):
		http.Error(
			w,
			err.Error(),
			http.StatusConflict,
		)
		return

	case errors.Is(err, ErrWalletNotFound):
		http.Error(
			w,
			err.Error(),
			http.StatusNotFound,
		)
		return

	case err != nil:
		log.Printf("withdrawal failed: %v", err)
		http.Error(
			w,
			"internal server error",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("encode response: %v", err)
	}
}
