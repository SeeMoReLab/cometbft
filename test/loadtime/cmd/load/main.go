package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/informalsystems/tm-load-test/pkg/loadtest"
	"github.com/sirupsen/logrus"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/cometbft/cometbft/test/loadtime/payload"
	"github.com/cometbft/cometbft/types"
)

// Ensure all of the interfaces are correctly satisfied.
var (
	_ loadtest.ClientFactory = (*ClientFactory)(nil)
	_ loadtest.Client        = (*TxGenerator)(nil)
)

// ClientFactory implements the loadtest.ClientFactory interface.
type ClientFactory struct {
	ID []byte
}

// TxGenerator is responsible for generating transactions.
// TxGenerator holds the set of information that will be used to generate
// each transaction.
type TxGenerator struct {
	id    []byte
	conns uint64
	rate  uint64
	size  uint64
}

func main() {
	logrus.SetLevel(logrus.WarnLevel)

	u := [16]byte(uuid.New()) // generate run ID on startup
	if err := loadtest.RegisterClientFactory("loadtime-client", &ClientFactory{ID: u[:]}); err != nil {
		panic(err)
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "coordinator", "worker", "version", "completion":
			loadtest.Run(&loadtest.CLIConfig{
				AppName:              "loadtime",
				AppShortDesc:         "Generate timestamped transaction load.",
				AppLongDesc:          "loadtime generates transaction load for the purpose of measuring the end-to-end latency of a transaction from submission to execution in a CometBFT network.",
				DefaultClientFactory: "loadtime-client",
			})
			return
		}
	}

	cfg, sendPeriod, progressInterval, progressEnabled, verbose, startUnixMs, err := parseFlags()
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	if verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	waitUntilUnixMillis(startUnixMs)

	var monitorCancel context.CancelFunc
	monitorDone := make(chan struct{})
	if progressEnabled {
		var monitorCtx context.Context
		monitorCtx, monitorCancel = context.WithCancel(context.Background())
		go func() {
			defer close(monitorDone)
			runProgressMonitor(monitorCtx, cfg.Endpoints[0], u[:], progressInterval)
		}()
	} else {
		close(monitorDone)
	}

	if sendPeriod%time.Second == 0 {
		err = loadtest.ExecuteStandalone(cfg)
	} else {
		if cfg.ExpectPeers > 0 || cfg.EndpointSelectMethod != loadtest.SelectSuppliedEndpoints || cfg.MaxEndpoints > 0 || cfg.MinConnectivity > 0 {
			fmt.Fprintln(os.Stderr, "error: sub-second --send-period is currently supported only for directly supplied endpoints (no peer discovery options)")
			os.Exit(2)
		}
		err = executeStandaloneWithDuration(cfg, sendPeriod, u[:], verbose)
	}

	if monitorCancel != nil {
		monitorCancel()
	}
	<-monitorDone

	if err != nil {
		os.Exit(1)
	}
}

func (f *ClientFactory) ValidateConfig(cfg loadtest.Config) error {
	psb, err := payload.MaxUnpaddedSize()
	if err != nil {
		return err
	}
	if psb > cfg.Size {
		return fmt.Errorf("payload size exceeds configured size")
	}
	return nil
}

func (f *ClientFactory) NewClient(cfg loadtest.Config) (loadtest.Client, error) {
	return &TxGenerator{
		id:    f.ID,
		conns: uint64(cfg.Connections),
		rate:  uint64(cfg.Rate),
		size:  uint64(cfg.Size),
	}, nil
}

func (c *TxGenerator) GenerateTx() ([]byte, error) {
	return payload.NewBytes(&payload.Payload{
		Connections: c.conns,
		Rate:        c.rate,
		Size:        c.size,
		Id:          c.id,
	})
}

func parseFlags() (loadtest.Config, time.Duration, time.Duration, bool, bool, int64, error) {
	var cfg loadtest.Config
	var endpointsCSV string
	var sendPeriodArg string
	var progressEvery string
	var noProgress bool
	var verbose bool
	var startUnixMs int64

	fs := flag.NewFlagSet("loadtime", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	fs.StringVar(&cfg.ClientFactory, "client-factory", "loadtime-client", "The identifier of the client factory to use for generating load testing transactions")
	fs.IntVar(&cfg.Connections, "connections", 1, "The number of connections to open to each endpoint simultaneously")
	fs.IntVar(&cfg.Connections, "c", 1, "The number of connections to open to each endpoint simultaneously")
	fs.IntVar(&cfg.Time, "time", 60, "The duration (in seconds) for which to handle the load test")
	fs.IntVar(&cfg.Time, "T", 60, "The duration (in seconds) for which to handle the load test")
	fs.StringVar(&sendPeriodArg, "send-period", "1s", "The period at which to send transaction batches (supports sub-second values like 200ms; plain integers are treated as seconds)")
	fs.StringVar(&sendPeriodArg, "p", "1s", "The period at which to send transaction batches (supports sub-second values like 200ms; plain integers are treated as seconds)")
	fs.IntVar(&cfg.Rate, "rate", 1000, "The number of transactions to generate each send period on each connection, to each endpoint")
	fs.IntVar(&cfg.Rate, "r", 1000, "The number of transactions to generate each send period on each connection, to each endpoint")
	fs.IntVar(&cfg.Size, "size", 250, "The size of each transaction, in bytes - must be greater than 40")
	fs.IntVar(&cfg.Size, "s", 250, "The size of each transaction, in bytes - must be greater than 40")
	fs.IntVar(&cfg.Count, "count", -1, "The maximum number of transactions to send - set to -1 to turn off this limit")
	fs.IntVar(&cfg.Count, "N", -1, "The maximum number of transactions to send - set to -1 to turn off this limit")
	fs.StringVar(&cfg.BroadcastTxMethod, "broadcast-tx-method", "async", "The broadcast_tx method to use when submitting transactions - can be async, sync or commit")
	fs.StringVar(&endpointsCSV, "endpoints", "", "A comma-separated list of URLs indicating CometBFT WebSockets RPC endpoints to which to connect")
	fs.StringVar(&cfg.EndpointSelectMethod, "endpoint-select-method", loadtest.SelectSuppliedEndpoints, "The method by which to select endpoints")
	fs.IntVar(&cfg.ExpectPeers, "expect-peers", 0, "The minimum number of peers to expect when crawling the P2P network from the specified endpoint(s) prior to waiting for workers to connect")
	fs.IntVar(&cfg.MaxEndpoints, "max-endpoints", 0, "The maximum number of endpoints to use for testing, where 0 means unlimited")
	fs.IntVar(&cfg.PeerConnectTimeout, "peer-connect-timeout", 600, "The number of seconds to wait for all required peers to connect if expect-peers > 0")
	fs.IntVar(&cfg.MinConnectivity, "min-peer-connectivity", 0, "The minimum number of peers to which each peer must be connected before starting the load test")
	fs.StringVar(&cfg.StatsOutputFile, "stats-output", "", "Where to store aggregate statistics (in CSV format) for the load test")
	fs.BoolVar(&verbose, "verbose", false, "Increase output logging verbosity to DEBUG level")
	fs.BoolVar(&verbose, "v", false, "Increase output logging verbosity to DEBUG level")
	fs.StringVar(&progressEvery, "progress-interval", "1s", "How often to emit '-- Monitor' lines (e.g. 1s, 2s)")
	fs.BoolVar(&noProgress, "no-progress", false, "Disable live progress monitor output")
	fs.Int64Var(&startUnixMs, "start-unix-ms", 0, "Absolute Unix timestamp in milliseconds at which to begin sending transactions (0 = start immediately)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stdout, "loadtime generates transaction load for measuring end-to-end transaction latency.")
		fmt.Fprintln(os.Stdout, "")
		fmt.Fprintln(os.Stdout, "Usage:")
		fmt.Fprintln(os.Stdout, "  load [flags]")
		fmt.Fprintln(os.Stdout, "")
		fmt.Fprintln(os.Stdout, "Flags:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		return loadtest.Config{}, 0, 0, false, false, 0, err
	}
	if fs.NArg() > 0 {
		return loadtest.Config{}, 0, 0, false, false, 0, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	parts := strings.Split(endpointsCSV, ",")
	cfg.Endpoints = make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cfg.Endpoints = append(cfg.Endpoints, p)
		}
	}

	sendPeriod, err := parseSendPeriod(sendPeriodArg)
	if err != nil {
		return loadtest.Config{}, 0, 0, false, false, 0, err
	}
	// tm-load-test config only supports integer-second send periods.
	// We keep cfg.SendPeriod valid and handle sub-second pacing in standalone mode.
	cfg.SendPeriod = int(math.Ceil(sendPeriod.Seconds()))
	if cfg.SendPeriod < 1 {
		cfg.SendPeriod = 1
	}

	interval, err := time.ParseDuration(progressEvery)
	if err != nil {
		return loadtest.Config{}, 0, 0, false, false, 0, fmt.Errorf("invalid --progress-interval %q: %w", progressEvery, err)
	}
	if interval <= 0 {
		return loadtest.Config{}, 0, 0, false, false, 0, fmt.Errorf("--progress-interval must be > 0")
	}
	if startUnixMs < 0 {
		return loadtest.Config{}, 0, 0, false, false, 0, fmt.Errorf("--start-unix-ms must be >= 0")
	}

	return cfg, sendPeriod, interval, !noProgress, verbose, startUnixMs, nil
}

func parseSendPeriod(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, fmt.Errorf("--send-period must not be empty")
	}

	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0, fmt.Errorf("--send-period must be > 0")
		}
		return time.Duration(secs) * time.Second, nil
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid --send-period %q: %w", v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("--send-period must be > 0")
	}
	return d, nil
}

type txSendLimiter struct {
	max      int64
	reserved int64
}

func newTxSendLimiter(max int) *txSendLimiter {
	return &txSendLimiter{max: int64(max)}
}

func (l *txSendLimiter) reserve() bool {
	if l.max < 0 {
		return true
	}
	for {
		cur := atomic.LoadInt64(&l.reserved)
		if cur >= l.max {
			return false
		}
		if atomic.CompareAndSwapInt64(&l.reserved, cur, cur+1) {
			return true
		}
	}
}

func executeStandaloneWithDuration(cfg loadtest.Config, sendPeriod time.Duration, runID []byte, verbose bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Time)*time.Second)
	defer cancel()

	gen := &TxGenerator{
		id:    runID,
		conns: uint64(cfg.Connections),
		rate:  uint64(cfg.Rate),
		size:  uint64(cfg.Size),
	}
	limiter := newTxSendLimiter(cfg.Count)
	var wg sync.WaitGroup
	var startedWorkers int64

	for _, endpoint := range cfg.Endpoints {
		rpcAddr, err := endpointToRPCAddr(endpoint)
		if err != nil {
			return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
		}
		for i := 0; i < cfg.Connections; i++ {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()

				client, err := rpchttp.New(addr, "/websocket")
				if err != nil {
					if verbose {
						logrus.WithError(err).Warnf("failed to create RPC client for %s", addr)
					}
					return
				}
				atomic.AddInt64(&startedWorkers, 1)

				sendBatch := func() bool {
					for j := 0; j < cfg.Rate; j++ {
						if ctx.Err() != nil {
							return false
						}
						if !limiter.reserve() {
							cancel()
							return false
						}

						tx, err := gen.GenerateTx()
						if err != nil {
							if verbose {
								logrus.WithError(err).Warn("failed to generate transaction")
							}
							continue
						}
						if err := broadcastByMethod(ctx, client, cfg.BroadcastTxMethod, tx); err != nil {
							// Keep sending despite RPC errors to preserve load pressure.
							if verbose {
								logrus.WithError(err).Warnf("broadcast (%s) failed", cfg.BroadcastTxMethod)
							}
						}
					}
					return true
				}

				// Send the first batch immediately once the worker starts.
				if !sendBatch() {
					return
				}

				ticker := time.NewTicker(sendPeriod)
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

func broadcastByMethod(ctx context.Context, client *rpchttp.HTTP, method string, tx []byte) error {
	t := types.Tx(tx)
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	switch method {
	case "async":
		_, err := client.BroadcastTxAsync(reqCtx, t)
		return err
	case "sync":
		_, err := client.BroadcastTxSync(reqCtx, t)
		return err
	case "commit":
		_, err := client.BroadcastTxCommit(reqCtx, t)
		return err
	default:
		return fmt.Errorf("unsupported broadcast method %q", method)
	}
}

func waitUntilUnixMillis(startUnixMs int64) {
	if startUnixMs <= 0 {
		return
	}
	target := time.Unix(0, startUnixMs*int64(time.Millisecond))
	wait := time.Until(target)
	if wait > 0 {
		time.Sleep(wait)
	}
}
