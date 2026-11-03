package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Config struct {
	Workers      int
	OpsPerWorker int
	Amount       int64
	MaxRetries   int
	UnsafeDelay  time.Duration
	RequestLimit time.Duration
}

type sample struct {
	latency   time.Duration
	conflicts int
	err       error
}

type Report struct {
	Strategy        string
	Attempts        int
	Successes       int
	Failures        int
	Conflicts       int
	Duration        time.Duration
	AverageLatency  time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	InitialBalance  int64
	ExpectedBalance int64
	ActualBalance   int64
	FinalVersion    int64
	Correct         bool
}

func main() {
	// ------------------------------------------
	// Common benchmark flags
	// ------------------------------------------

	workers := flag.Int(
		"workers",
		20,
		"number of concurrent workers",
	)

	ops := flag.Int(
		"ops",
		100,
		"withdrawals per worker",
	)

	amount := flag.Int64(
		"amount",
		1,
		"amount per withdrawal",
	)

	strategyName := flag.String(
		"strategy",
		"all",
		"all, unsafe, pessimistic, optimistic, or atomic",
	)

	maxRetries := flag.Int(
		"retries",
		50,
		"maximum optimistic CAS retries",
	)

	unsafeDelay := flag.Duration(
		"unsafe-delay",
		2*time.Millisecond,
		"artificial delay for unsafe strategy",
	)

	requestLimit := flag.Duration(
		"request-timeout",
		30*time.Second,
		"timeout per withdrawal",
	)

	// ------------------------------------------
	// Comparative experiment flags
	// ------------------------------------------

	experiment := flag.Bool(
		"experiment",
		false,
		"run repeatable multi-wallet experiments",
	)

	walletsFlag := flag.String(
		"wallets",
		"1,20",
		"comma-separated wallet counts",
	)

	iterations := flag.Int(
		"iterations",
		5,
		"measured runs per strategy and wallet count",
	)

	warmup := flag.Int(
		"warmup",
		1,
		"unmeasured warm-up runs",
	)

	csvPath := flag.String(
		"csv",
		"benchmark-results.csv",
		"output CSV path",
	)

	flag.Parse()

	// ------------------------------------------
	// Validate common configuration
	// ------------------------------------------

	cfg := Config{
		Workers:      *workers,
		OpsPerWorker: *ops,
		Amount:       *amount,
		MaxRetries:   *maxRetries,
		UnsafeDelay:  *unsafeDelay,
		RequestLimit: *requestLimit,
	}

	if err := validateConfig(cfg); err != nil {
		log.Fatal(err)
	}

	// ------------------------------------------
	// Mode 1: Comparative experiments
	// ------------------------------------------

	if *experiment {
		walletCounts, err := parseWalletCounts(
			*walletsFlag,
		)
		if err != nil {
			log.Fatal(err)
		}

		experimentConfig := ExperimentConfig{
			Workers:      cfg.Workers,
			OpsPerWorker: cfg.OpsPerWorker,
			Amount:       cfg.Amount,
			WalletCounts: walletCounts,
			Iterations:   *iterations,
			WarmupRuns:   *warmup,
			MaxRetries:   cfg.MaxRetries,
			UnsafeDelay:  cfg.UnsafeDelay,
			Timeout:      cfg.RequestLimit,
			CSVPath:      *csvPath,
		}

		if err := runComparativeExperiments(
			experimentConfig,
		); err != nil {
			log.Fatal(err)
		}

		return
	}

	// ------------------------------------------
	// Mode 2: Original single-wallet benchmark
	// ------------------------------------------

	if err := runSingleWalletBenchmark(
		cfg,
		*strategyName,
	); err != nil {
		log.Fatal(err)
	}
}

func validateConfig(cfg Config) error {
	if cfg.Workers <= 0 {
		return fmt.Errorf("workers must be positive")
	}

	if cfg.OpsPerWorker <= 0 {
		return fmt.Errorf(
			"operations per worker must be positive",
		)
	}

	if cfg.Amount <= 0 {
		return fmt.Errorf(
			"withdrawal amount must be positive",
		)
	}

	if cfg.MaxRetries < 0 {
		return fmt.Errorf(
			"maximum retries cannot be negative",
		)
	}

	if cfg.UnsafeDelay < 0 {
		return fmt.Errorf(
			"unsafe delay cannot be negative",
		)
	}

	if cfg.RequestLimit <= 0 {
		return fmt.Errorf(
			"request timeout must be positive",
		)
	}

	// Prevent overflow when multiplying the
	// number of workers and operations.
	if int64(cfg.Workers) >
		math.MaxInt64/int64(cfg.OpsPerWorker) {
		return fmt.Errorf("operation count overflow")
	}

	totalOperations := int64(cfg.Workers) *
		int64(cfg.OpsPerWorker)

	// The benchmark allocates one result per
	// operation, so count must fit into int.
	maxInt := int64(^uint(0) >> 1)

	if totalOperations > maxInt {
		return fmt.Errorf(
			"operation count exceeds platform int",
		)
	}

	// The wallet starts with enough funds for all
	// withdrawals, plus an additional 1000 units.
	if totalOperations >
		(math.MaxInt64-1000)/cfg.Amount {
		return fmt.Errorf(
			"initial balance would overflow int64",
		)
	}

	return nil
}

func openBenchmarkPool(
	cfg Config,
) (*pgxpool.Pool, error) {
	_ = godotenv.Load(".env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf(
			"DATABASE_URL is required",
		)
	}

	poolConfig, err := pgxpool.ParseConfig(
		databaseURL,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"parse database config: %w",
			err,
		)
	}

	poolConfig.MaxConns = int32(cfg.Workers + 2)

	if poolConfig.MaxConns < 4 {
		poolConfig.MaxConns = 4
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	db, err := pgxpool.NewWithConfig(
		ctx,
		poolConfig,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create database pool: %w",
			err,
		)
	}

	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf(
			"database ping: %w",
			err,
		)
	}

	return db, nil
}

func runSingleWalletBenchmark(
	cfg Config,
	strategyName string,
) error {
	db, err := openBenchmarkPool(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	strategies := map[string]StrategyFunc{
		"unsafe": unsafeWithdraw(
			cfg.UnsafeDelay,
		),
		"pessimistic": pessimisticWithdraw,
		"optimistic": optimisticWithdraw(
			cfg.MaxRetries,
		),
		"atomic": atomicWithdraw,
	}

	order := []string{
		"unsafe",
		"pessimistic",
		"optimistic",
		"atomic",
	}

	if strategyName != "all" {
		if _, exists := strategies[strategyName]; !exists {
			return fmt.Errorf(
				"unknown strategy: %s",
				strategyName,
			)
		}

		order = []string{strategyName}
	}

	totalOperations := cfg.Workers *
		cfg.OpsPerWorker

	fmt.Printf(
		"\nWorkers: %d | Ops/worker: %d | Total: %d | Amount: %d\n",
		cfg.Workers,
		cfg.OpsPerWorker,
		totalOperations,
		cfg.Amount,
	)

	fmt.Printf(
		"CAS retry budget: %d | Unsafe delay: %s\n\n",
		cfg.MaxRetries,
		cfg.UnsafeDelay,
	)

	for _, name := range order {
		report, err := runBenchmark(
			db,
			name,
			strategies[name],
			cfg,
		)
		if err != nil {
			return fmt.Errorf(
				"%s benchmark: %w",
				name,
				err,
			)
		}

		printReport(report)
	}

	fmt.Println(
		"\nNote: compare correctness before comparing throughput.",
	)

	return nil
}

func runBenchmark(
	db *pgxpool.Pool,
	name string,
	strategy StrategyFunc,
	cfg Config,
) (Report, error) {
	total := cfg.Workers * cfg.OpsPerWorker

	initial := int64(total)*cfg.Amount + 1000

	walletID := uuid.New()

	setupCtx, setupCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer setupCancel()

	_, err := db.Exec(setupCtx, `
		INSERT INTO wallets (id, balance)
		VALUES ($1, $2)
	`, walletID, initial)
	if err != nil {
		return Report{}, fmt.Errorf(
			"create wallet: %w",
			err,
		)
	}

	// Delete only the wallet created for
	// this particular benchmark.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		_, _ = db.Exec(
			cleanupCtx,
			"DELETE FROM wallets WHERE id = $1",
			walletID,
		)
	}()

	results := make(chan sample, total)
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Start workers together to create
			// contention on the same wallet.
			<-start

			for j := 0; j < cfg.OpsPerWorker; j++ {
				requestCtx, cancel := context.WithTimeout(
					context.Background(),
					cfg.RequestLimit,
				)

				begin := time.Now()

				conflicts, err := strategy(
					requestCtx,
					db,
					walletID,
					cfg.Amount,
				)

				elapsed := time.Since(begin)

				cancel()

				results <- sample{
					latency:   elapsed,
					conflicts: conflicts,
					err:       err,
				}
			}
		}()
	}

	begin := time.Now()

	close(start)

	wg.Wait()

	elapsed := time.Since(begin)

	close(results)

	report := Report{
		Strategy:       name,
		Attempts:       total,
		InitialBalance: initial,
		Duration:       elapsed,
	}

	latencies := make(
		[]time.Duration,
		0,
		total,
	)

	for result := range results {
		latencies = append(
			latencies,
			result.latency,
		)

		report.Conflicts += result.conflicts

		if result.err != nil {
			report.Failures++
		} else {
			report.Successes++
		}
	}

	sort.Slice(
		latencies,
		func(i, j int) bool {
			return latencies[i] < latencies[j]
		},
	)

	report.AverageLatency = averageLatency(
		latencies,
	)

	report.P95Latency = percentile(
		latencies,
		95,
	)

	report.P99Latency = percentile(
		latencies,
		99,
	)

	// Only acknowledged successful withdrawals
	// contribute to the expected balance.
	report.ExpectedBalance =
		initial - int64(report.Successes)*cfg.Amount

	verifyCtx, verifyCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer verifyCancel()

	err = db.QueryRow(verifyCtx, `
		SELECT balance, version
		FROM wallets
		WHERE id = $1
	`, walletID).Scan(
		&report.ActualBalance,
		&report.FinalVersion,
	)
	if err != nil {
		return Report{}, fmt.Errorf(
			"verify wallet: %w",
			err,
		)
	}

	report.Correct =
		report.ActualBalance == report.ExpectedBalance &&
			report.FinalVersion == int64(report.Successes)

	return report, nil
}

func averageLatency(
	values []time.Duration,
) time.Duration {
	if len(values) == 0 {
		return 0
	}

	var total float64

	for _, value := range values {
		total += float64(value)
	}

	return time.Duration(
		total / float64(len(values)),
	)
}

func percentile(
	values []time.Duration,
	p int,
) time.Duration {
	if len(values) == 0 {
		return 0
	}

	index := int(math.Ceil(
		float64(p)*
			float64(len(values))/
			100,
	)) - 1

	if index < 0 {
		index = 0
	}

	if index >= len(values) {
		index = len(values) - 1
	}

	return values[index]
}

func printReport(r Report) {
	opsPerSecond := float64(0)
	successesPerSecond := float64(0)

	if r.Duration > 0 {
		opsPerSecond = float64(r.Attempts) /
			r.Duration.Seconds()

		successesPerSecond = float64(r.Successes) /
			r.Duration.Seconds()
	}

	verdict := "PASS"

	if !r.Correct {
		verdict = "FAIL - consistency violation"
	}

	fmt.Println(strings.Repeat("=", 64))
	fmt.Printf("Strategy: %s\n", r.Strategy)
	fmt.Printf("Attempts: %d\n", r.Attempts)
	fmt.Printf("Successes: %d\n", r.Successes)
	fmt.Printf("Failures: %d\n", r.Failures)
	fmt.Printf("CAS conflicts: %d\n", r.Conflicts)

	fmt.Printf(
		"Duration: %s\n",
		r.Duration.Round(time.Millisecond),
	)

	fmt.Printf(
		"Throughput: %.2f attempts/sec\n",
		opsPerSecond,
	)

	fmt.Printf(
		"Successful throughput: %.2f ops/sec\n",
		successesPerSecond,
	)

	fmt.Printf(
		"Avg latency: %s\n",
		r.AverageLatency.Round(time.Microsecond),
	)

	fmt.Printf(
		"p95 latency: %s\n",
		r.P95Latency.Round(time.Microsecond),
	)

	fmt.Printf(
		"p99 latency: %s\n",
		r.P99Latency.Round(time.Microsecond),
	)

	fmt.Printf(
		"Initial balance: %d\n",
		r.InitialBalance,
	)

	fmt.Printf(
		"Expected balance: %d\n",
		r.ExpectedBalance,
	)

	fmt.Printf(
		"Actual balance: %d\n",
		r.ActualBalance,
	)

	fmt.Printf(
		"Final version: %d\n",
		r.FinalVersion,
	)

	fmt.Printf(
		"Correctness: %s\n\n",
		verdict,
	)
}
