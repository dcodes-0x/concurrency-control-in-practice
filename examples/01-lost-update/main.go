package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "8082"
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	repo := NewPostgresWalletRepository(db)
	service := NewWalletService(repo)
	handler := NewWalletHandler(service)

	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /wallets/{id}/withdraw",
		handler.Withdraw,
	)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Wallet API running on :%s", port)
	log.Fatal(server.ListenAndServe())
}
