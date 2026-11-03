package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type ExperimentConfig struct {
	Workers      int
	OpsPerWorker int
	Amount       int64
	WalletCounts []int
	Iterations   int
	WarmupRuns   int
	MaxRetries   int
	UnsafeDelay  time.Duration
	Timeout      time.Duration
	CSVPath      string
}

type ExperimentResult struct {
	Strategy       string
	WalletCount    int
	Iteration      int
	Attempts       int
	Successes      int
	Failures       int
	RetryExhausted int
	CASConflicts   int
	Duration       time.Duration
	AvgLatency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	Correct        bool
}

type operationSample struct {
	WalletIndex int
	Latency     time.Duration
	Conflicts   int
	Err         error
}

func parseWalletCounts(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	counts := make([]int, 0, len(parts))
	seen := make(map[int]bool)

	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf(
				"invalid wallet count %q", part,
			)
		}
		if !seen[n] {
			counts = append(counts, n)
			seen[n] = true
		}
	}
	return counts, nil
}

func runComparativeExperiments(
	cfg ExperimentConfig,
) error {
	if cfg.Workers < 1 ||
		cfg.OpsPerWorker < 1 ||
		cfg.Amount < 1 ||
		cfg.Iterations < 1 ||
		cfg.WarmupRuns < 0 ||
		cfg.MaxRetries < 0 ||
		cfg.UnsafeDelay < 0 ||
		cfg.Timeout <= 0 ||
		len(cfg.WalletCounts) == 0 {
		return errors.New("invalid experiment configuration")
	}

	total64 := int64(cfg.Workers) *
		int64(cfg.OpsPerWorker)

	maxInt := int64(^uint(0) >> 1)
	if total64 <= 0 || total64 > maxInt ||
		total64 > (math.MaxInt64-1000)/cfg.Amount {
		return errors.New("operation count or balance overflow")
	}

	for _, n := range cfg.WalletCounts {
		if n < 1 {
			return errors.New("wallet counts must be positive")
		}
	}

	_ = godotenv.Load(".env")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return err
	}

	poolConfig.MaxConns = int32(cfg.Workers + 2)
	if poolConfig.MaxConns < 4 {
		poolConfig.MaxConns = 4
	}

	setupCtx, cancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer cancel()

	db, err := pgxpool.NewWithConfig(
		setupCtx, poolConfig,
	)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(setupCtx); err != nil {
		return err
	}

	strategies := []struct {
		Name string
		Fn   StrategyFunc
	}{
		{"unsafe", unsafeWithdraw(cfg.UnsafeDelay)},
		{"pessimistic", pessimisticWithdraw},
		{"optimistic", optimisticWithdraw(cfg.MaxRetries)},
		{"atomic", atomicWithdraw},
	}

	var all []ExperimentResult

	for _, walletCount := range cfg.WalletCounts {
		for _, strategy := range strategies {
			fmt.Printf(
				"\n%s | wallets=%d\n",
				strategy.Name,
				walletCount,
			)

			for i := 0; i < cfg.WarmupRuns; i++ {
				_, err := runExperimentIteration(
					db, strategy.Name, strategy.Fn,
					cfg, walletCount, 0,
				)
				if err != nil {
					return fmt.Errorf("warm-up: %w", err)
				}
			}

			for i := 1; i <= cfg.Iterations; i++ {
				result, err := runExperimentIteration(
					db, strategy.Name, strategy.Fn,
					cfg, walletCount, i,
				)
				if err != nil {
					return err
				}

				all = append(all, result)

				fmt.Printf(
					"run=%d success=%d/%d "+
						"conflicts=%d exhausted=%d "+
						"throughput=%.1f/s "+
						"p95=%s correct=%v\n",
					i,
					result.Successes,
					result.Attempts,
					result.CASConflicts,
					result.RetryExhausted,
					successThroughput(result),
					result.P95Latency.Round(time.Microsecond),
					result.Correct,
				)
			}
		}
	}

	if cfg.CSVPath != "" {
		if err := writeExperimentCSV(
			cfg.CSVPath, all,
		); err != nil {
			return err
		}
		fmt.Printf("\nCSV written: %s\n", cfg.CSVPath)
	}

	printExperimentSummary(all, cfg.WalletCounts)
	return nil
}

func runExperimentIteration(
	db *pgxpool.Pool,
	name string,
	strategy StrategyFunc,
	cfg ExperimentConfig,
	walletCount int,
	iteration int,
) (result ExperimentResult, err error) {
	total := cfg.Workers * cfg.OpsPerWorker

	// Every wallet has enough funds to cover ALL
	// withdrawals, even if workload distribution
	// is uneven.
	initialBalance := int64(total)*cfg.Amount + 1000

	walletIDs := make([]uuid.UUID, walletCount)

	setupCtx, cancelSetup := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancelSetup()

	for i := range walletIDs {
		walletIDs[i] = uuid.New()

		_, err := db.Exec(setupCtx, `
			INSERT INTO wallets (id, balance)
			VALUES ($1, $2)
		`, walletIDs[i], initialBalance)
		if err != nil {
			return result, fmt.Errorf(
				"insert experiment wallet: %w", err,
			)
		}
	}

	// Clean up disposable wallets after reporting
	// is complete, including on errors.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(), 30*time.Second,
		)
		defer cancel()

		for _, id := range walletIDs {
			_, cleanupErr := db.Exec(
				cleanupCtx,
				"DELETE FROM wallets WHERE id = $1",
				id,
			)
			if cleanupErr != nil && err == nil {
				err = fmt.Errorf(
					"cleanup wallet: %w", cleanupErr,
				)
			}
		}
	}()

	results := make(chan operationSample, total)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < cfg.Workers; worker++ {
		workerIndex := worker

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			for op := 0; op < cfg.OpsPerWorker; op++ {
				// Deterministic, evenly distributed
				// wallet selection.
				index := (workerIndex*cfg.OpsPerWorker +
					op) % walletCount

				requestCtx, cancel := context.WithTimeout(
					context.Background(), cfg.Timeout,
				)

				begin := time.Now()
				conflicts, operationErr := strategy(
					requestCtx,
					db,
					walletIDs[index],
					cfg.Amount,
				)
				elapsed := time.Since(begin)
				cancel()

				results <- operationSample{
					WalletIndex: index,
					Latency:     elapsed,
					Conflicts:   conflicts,
					Err:         operationErr,
				}
			}
		}()
	}

	begin := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begin)
	close(results)

	result = ExperimentResult{
		Strategy:    name,
		WalletCount: walletCount,
		Iteration:   iteration,
		Attempts:    total,
		Duration:    elapsed,
		Correct:     true,
	}

	successPerWallet := make([]int64, walletCount)
	latencies := make([]time.Duration, 0, total)
	var sumLatency float64

	for sample := range results {
		latencies = append(latencies, sample.Latency)
		sumLatency += float64(sample.Latency)
		result.CASConflicts += sample.Conflicts

		if sample.Err == nil {
			result.Successes++
			successPerWallet[sample.WalletIndex]++
		} else {
			result.Failures++

			if errors.Is(
				sample.Err,
				ErrRetriesExhausted,
			) {
				result.RetryExhausted++
			} else if !errors.Is(
				sample.Err,
				ErrInsufficientFunds,
			) {
				return result, fmt.Errorf(
					"%s unexpected operation error: %w",
					name, sample.Err,
				)
			}
		}
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	result.AvgLatency = time.Duration(
		sumLatency / float64(total),
	)
	result.P95Latency = percentile(latencies, 95)
	result.P99Latency = percentile(latencies, 99)

	verifyCtx, cancelVerify := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancelVerify()

	// Validate correctness PER WALLET.
	// An aggregate-only check could hide errors
	// that cancel one another across wallets.
	for index, id := range walletIDs {
		var actualBalance, actualVersion int64

		err := db.QueryRow(verifyCtx, `
			SELECT balance, version
			FROM wallets
			WHERE id = $1
		`, id).Scan(
			&actualBalance,
			&actualVersion,
		)
		if err != nil {
			return result, fmt.Errorf(
				"verify wallet %d: %w", index, err,
			)
		}

		expectedBalance := initialBalance -
			successPerWallet[index]*cfg.Amount

		if actualBalance != expectedBalance ||
			actualVersion != successPerWallet[index] {
			result.Correct = false
		}
	}

	return result, nil
}

func successThroughput(r ExperimentResult) float64 {
	if r.Duration <= 0 {
		return 0
	}
	return float64(r.Successes) / r.Duration.Seconds()
}

func writeExperimentCSV(
	path string,
	results []ExperimentResult,
) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create CSV: %w", err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)

	header := []string{
		"strategy",
		"wallets",
		"iteration",
		"attempts",
		"successes",
		"failures",
		"retry_exhausted",
		"cas_conflicts",
		"duration_ms",
		"attempts_per_sec",
		"successes_per_sec",
		"avg_latency_ms",
		"p95_latency_ms",
		"p99_latency_ms",
		"correct",
	}

	if err := writer.Write(header); err != nil {
		return err
	}

	formatFloat := func(n float64) string {
		return strconv.FormatFloat(n, 'f', 3, 64)
	}

	for _, r := range results {
		attemptRate := float64(0)
		if r.Duration > 0 {
			attemptRate = float64(r.Attempts) /
				r.Duration.Seconds()
		}

		row := []string{
			r.Strategy,
			strconv.Itoa(r.WalletCount),
			strconv.Itoa(r.Iteration),
			strconv.Itoa(r.Attempts),
			strconv.Itoa(r.Successes),
			strconv.Itoa(r.Failures),
			strconv.Itoa(r.RetryExhausted),
			strconv.Itoa(r.CASConflicts),
			formatFloat(float64(r.Duration.Milliseconds())),
			formatFloat(attemptRate),
			formatFloat(successThroughput(r)),
			formatFloat(float64(r.AvgLatency) / 1e6),
			formatFloat(float64(r.P95Latency) / 1e6),
			formatFloat(float64(r.P99Latency) / 1e6),
			strconv.FormatBool(r.Correct),
		}

		if err := writer.Write(row); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

func medianFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}

	return (sorted[mid-1] + sorted[mid]) / 2
}

func printExperimentSummary(
	results []ExperimentResult,
	walletCounts []int,
) {
	fmt.Println("\nMEDIAN OF MEASURED RUNS")
	fmt.Println(strings.Repeat("-", 94))
	fmt.Printf(
		"%-15s %8s %14s %13s %13s %12s %9s\n",
		"Strategy", "Wallets", "Success/s",
		"p95 (ms)", "p99 (ms)", "Conflicts", "Correct",
	)

	for _, walletCount := range walletCounts {
		for _, strategy := range []string{
			"unsafe",
			"pessimistic",
			"optimistic",
			"atomic",
		} {
			var rates, p95s, p99s, conflicts []float64
			allCorrect := true

			for _, r := range results {
				if r.Strategy != strategy ||
					r.WalletCount != walletCount {
					continue
				}

				rates = append(
					rates, successThroughput(r),
				)
				p95s = append(
					p95s, float64(r.P95Latency)/1e6,
				)
				p99s = append(
					p99s, float64(r.P99Latency)/1e6,
				)
				conflicts = append(
					conflicts, float64(r.CASConflicts),
				)

				if !r.Correct {
					allCorrect = false
				}
			}

			if len(rates) == 0 {
				continue
			}

			fmt.Printf(
				"%-15s %8d %14.1f %13.2f %13.2f %12.0f %9v\n",
				strategy,
				walletCount,
				medianFloat(rates),
				medianFloat(p95s),
				medianFloat(p99s),
				medianFloat(conflicts),
				allCorrect,
			)
		}
	}
}
