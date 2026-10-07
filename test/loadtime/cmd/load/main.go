package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/cometbft/cometbft/test/loadtime/payload"
	"github.com/cometbft/cometbft/types"
)

// progressInterval is how often the progress monitor prints a Monitor line.
const progressInterval = time.Second

// loadConfig holds the load parameters the harness sets.
type loadConfig struct {
	endpoints   []string
	connections int
	rate        int
	sendPeriod  time.Duration
	duration    time.Duration
	size        int
	startUnixMs int64
}

// TxGenerator holds the set of information that will be used to generate
// each transaction.
type TxGenerator struct {
	id    []byte
	conns uint64
	rate  uint64
	size  uint64
}

func main() {
	u := [16]byte(uuid.New()) // generate run ID on startup

	cfg, err := parseFlags()
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	waitUntilUnixMillis(cfg.startUnixMs)

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		runProgressMonitor(monitorCtx, cfg.endpoints[0], u[:], progressInterval)
	}()

	err = sendLoad(cfg, u[:])

	monitorCancel()
	<-monitorDone

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func (c *TxGenerator) GenerateTx() ([]byte, error) {
	return payload.NewBytes(&payload.Payload{
		Connections: c.conns,
		Rate:        c.rate,
		Size:        c.size,
		Id:          c.id,
	})
}

func parseFlags() (loadConfig, error) {
	var (
		cfg          loadConfig
		endpointsCSV string
		sendPeriod   string
		durationSecs int
	)

	fs := flag.NewFlagSet("loadtime", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.StringVar(&endpointsCSV, "endpoints", "", "Comma-separated CometBFT WebSocket RPC endpoints, e.g. ws://host:26657/websocket")
	fs.IntVar(&cfg.connections, "connections", 1, "Connections to open to each endpoint")
	fs.IntVar(&cfg.rate, "rate", 1000, "Transactions sent per send period on each connection")
	fs.StringVar(&sendPeriod, "send-period", "1s", "Period between transaction batches, as a Go duration (e.g. 100ms)")
	fs.IntVar(&durationSecs, "time", 60, "Duration of the load in seconds")
	fs.IntVar(&cfg.size, "size", 250, "Size of each transaction in bytes")
	fs.Int64Var(&cfg.startUnixMs, "start-unix-ms", 0, "Unix time in ms at which to start sending (0 = immediately)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stdout, "load generates timestamped transaction load for measuring end-to-end latency.")
		fmt.Fprintln(os.Stdout, "")
		fmt.Fprintln(os.Stdout, "Usage:")
		fmt.Fprintln(os.Stdout, "  load [flags]")
		fmt.Fprintln(os.Stdout, "")
		fmt.Fprintln(os.Stdout, "Flags:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		return loadConfig{}, err
	}
	if fs.NArg() > 0 {
		return loadConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	for _, p := range strings.Split(endpointsCSV, ",") {
		if p = strings.TrimSpace(p); p != "" {
			cfg.endpoints = append(cfg.endpoints, p)
		}
	}
	if len(cfg.endpoints) == 0 {
		return loadConfig{}, fmt.Errorf("--endpoints is required")
	}
	if cfg.connections < 1 {
		return loadConfig{}, fmt.Errorf("--connections must be >= 1")
	}
	if cfg.rate < 1 {
		return loadConfig{}, fmt.Errorf("--rate must be >= 1")
	}
	period, err := time.ParseDuration(strings.TrimSpace(sendPeriod))
	if err != nil {
		return loadConfig{}, fmt.Errorf("invalid --send-period %q: %w", sendPeriod, err)
	}
	if period <= 0 {
		return loadConfig{}, fmt.Errorf("--send-period must be > 0")
	}
	cfg.sendPeriod = period
	if durationSecs < 1 {
		return loadConfig{}, fmt.Errorf("--time must be >= 1")
	}
	cfg.duration = time.Duration(durationSecs) * time.Second
	minSize, err := payload.MaxUnpaddedSize()
	if err != nil {
		return loadConfig{}, err
	}
	if cfg.size < minSize {
		return loadConfig{}, fmt.Errorf("--size must be >= %d bytes, the unpadded payload size", minSize)
	}
	if cfg.startUnixMs < 0 {
		return loadConfig{}, fmt.Errorf("--start-unix-ms must be >= 0")
	}
	return cfg, nil
}

// sendLoad runs cfg.connections workers per endpoint, each sending cfg.rate
// transactions every cfg.sendPeriod until cfg.duration has passed.
func sendLoad(cfg loadConfig, runID []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	gen := &TxGenerator{
		id:    runID,
		conns: uint64(cfg.connections),
		rate:  uint64(cfg.rate),
		size:  uint64(cfg.size),
	}
	var wg sync.WaitGroup
	var startedWorkers int64

	for _, endpoint := range cfg.endpoints {
		rpcAddr, err := endpointToRPCAddr(endpoint)
		if err != nil {
			return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
		}
		for i := 0; i < cfg.connections; i++ {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()

				client, err := rpchttp.New(addr, "/websocket")
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to create RPC client for %s: %v\n", addr, err)
					return
				}
				atomic.AddInt64(&startedWorkers, 1)

				sendBatch := func() bool {
					for j := 0; j < cfg.rate; j++ {
						if ctx.Err() != nil {
							return false
						}
						tx, err := gen.GenerateTx()
						if err != nil {
							continue
						}
						// Broadcast errors are not retried: the load keeps its pressure,
						// and lost transactions show up as missing in the report.
						_ = broadcastAsync(ctx, client, tx)
					}
					return true
				}

				// Send the first batch immediately once the worker starts.
				if !sendBatch() {
					return
				}

				ticker := time.NewTicker(cfg.sendPeriod)
				defer ticker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if !sendBatch() {
							return
						}
					}
				}
			}(rpcAddr)
		}
	}

	wg.Wait()
	if atomic.LoadInt64(&startedWorkers) == 0 {
		return fmt.Errorf("failed to start any load workers")
	}
	return nil
}

func broadcastAsync(ctx context.Context, client *rpchttp.HTTP, tx []byte) error {
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := client.BroadcastTxAsync(reqCtx, types.Tx(tx))
	return err
}

func waitUntilUnixMillis(startUnixMs int64) {
	if startUnixMs <= 0 {
		return
	}
	target := time.Unix(0, startUnixMs*int64(time.Millisecond))
	fmt.Printf("Waiting until benchmark start unix ms %d (%s)\n", startUnixMs, target.UTC().Format(time.RFC3339Nano))
	wait := time.Until(target)
	if wait > 0 {
		time.Sleep(wait)
	}
}
