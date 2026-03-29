package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/informalsystems/tm-load-test/pkg/loadtest"
	"github.com/sirupsen/logrus"

	"github.com/cometbft/cometbft/test/loadtime/payload"
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

	cfg, progressInterval, progressEnabled, verbose, err := parseFlags()
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

	err = loadtest.ExecuteStandalone(cfg)

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

func parseFlags() (loadtest.Config, time.Duration, bool, bool, error) {
	var cfg loadtest.Config
	var endpointsCSV string
	var progressEvery string
	var noProgress bool
	var verbose bool

	fs := flag.NewFlagSet("loadtime", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	fs.StringVar(&cfg.ClientFactory, "client-factory", "loadtime-client", "The identifier of the client factory to use for generating load testing transactions")
	fs.IntVar(&cfg.Connections, "connections", 1, "The number of connections to open to each endpoint simultaneously")
	fs.IntVar(&cfg.Connections, "c", 1, "The number of connections to open to each endpoint simultaneously")
	fs.IntVar(&cfg.Time, "time", 60, "The duration (in seconds) for which to handle the load test")
	fs.IntVar(&cfg.Time, "T", 60, "The duration (in seconds) for which to handle the load test")
	fs.IntVar(&cfg.SendPeriod, "send-period", 1, "The period (in seconds) at which to send batches of transactions")
	fs.IntVar(&cfg.SendPeriod, "p", 1, "The period (in seconds) at which to send batches of transactions")
	fs.IntVar(&cfg.Rate, "rate", 1000, "The number of transactions to generate each second on each connection, to each endpoint")
	fs.IntVar(&cfg.Rate, "r", 1000, "The number of transactions to generate each second on each connection, to each endpoint")
	fs.IntVar(&cfg.Size, "size", 250, "The size of each transaction, in bytes - must be greater than 40")
	fs.IntVar(&cfg.Size, "s", 250, "The size of each transaction, in bytes - must be greater than 40")
	fs.IntVar(&cfg.Count, "count", -1, "The maximum number of transactions to send - set to -1 to turn off this limit")
	fs.IntVar(&cfg.Count, "N", -1, "The maximum number of transactions to send - set to -1 to turn off this limit")
	fs.StringVar(&cfg.BroadcastTxMethod, "broadcast-tx-method", "async", "The broadcast_tx method to use when submitting transactions - can be async, sync or commit")
	fs.StringVar(&endpointsCSV, "endpoints", "", "A comma-separated list of URLs indicating Tendermint WebSockets RPC endpoints to which to connect")
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
		return loadtest.Config{}, 0, false, false, err
	}
	if fs.NArg() > 0 {
		return loadtest.Config{}, 0, false, false, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	parts := strings.Split(endpointsCSV, ",")
	cfg.Endpoints = make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cfg.Endpoints = append(cfg.Endpoints, p)
		}
	}

	interval, err := time.ParseDuration(progressEvery)
	if err != nil {
		return loadtest.Config{}, 0, false, false, fmt.Errorf("invalid --progress-interval %q: %w", progressEvery, err)
	}
	if interval <= 0 {
		return loadtest.Config{}, 0, false, false, fmt.Errorf("--progress-interval must be > 0")
	}

	return cfg, interval, !noProgress, verbose, nil
}
