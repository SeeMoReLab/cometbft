package main

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/cometbft/cometbft/test/loadtime/payload"
	"github.com/cometbft/cometbft/types"
)

type progressState struct {
	initialized bool
	prevHeight  int64
	prevBlock   *types.Block

	totalFinished int64
	totalLatency  time.Duration
}

func runProgressMonitor(ctx context.Context, wsEndpoint string, runID []byte, interval time.Duration) {
	rpcAddr, err := endpointToRPCAddr(wsEndpoint)
	if err != nil {
		fmt.Printf("-- Monitor error: invalid endpoint %q: %v\n", wsEndpoint, err)
		return
	}

	client, err := rpchttp.New(rpcAddr, "/websocket")
	if err != nil {
		fmt.Printf("-- Monitor error: failed to create RPC client for %q: %v\n", rpcAddr, err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	st := &progressState{}

	for {
		select {
		case <-ctx.Done():
			// Best-effort final flush before exit.
			sampleCount, sampleLatencies := collectFinishedTxs(context.Background(), client, runID, st)
			printProgressLine(start, st, sampleCount, sampleLatencies)
			return
		case <-ticker.C:
			sampleCount, sampleLatencies := collectFinishedTxs(ctx, client, runID, st)
			printProgressLine(start, st, sampleCount, sampleLatencies)
		}
	}
}

func collectFinishedTxs(ctx context.Context, client *rpchttp.HTTP, runID []byte, st *progressState) (int64, []time.Duration) {
	status, err := client.Status(ctx)
	if err != nil {
		fmt.Printf("-- Monitor error: status query failed: %v\n", err)
		return 0, nil
	}

	latest := status.SyncInfo.LatestBlockHeight
	if latest <= 0 {
		return 0, nil
	}

	if !st.initialized {
		b, err := client.Block(ctx, &latest)
		if err != nil {
			fmt.Printf("-- Monitor error: initial block query failed at height %d: %v\n", latest, err)
			return 0, nil
		}
		st.initialized = true
		st.prevHeight = latest
		st.prevBlock = b.Block
		return 0, nil
	}

	var sampleCount int64
	var sampleLatencies []time.Duration

	for st.prevHeight+1 <= latest {
		curHeight := st.prevHeight + 1
		cur, err := client.Block(ctx, &curHeight)
		if err != nil {
			fmt.Printf("-- Monitor error: block query failed at height %d: %v\n", curHeight, err)
			break
		}

		for _, tx := range st.prevBlock.Data.Txs {
			p, err := payload.FromBytes(tx)
			if err != nil {
				continue
			}
			if !bytes.Equal(p.Id, runID) {
				continue
			}
			if p.Time == nil {
				continue
			}

			latency := cur.Block.Time.Sub(p.Time.AsTime())
			sampleCount++
			sampleLatencies = append(sampleLatencies, latency)
			st.totalFinished++
			st.totalLatency += latency
		}

		st.prevBlock = cur.Block
		st.prevHeight = curHeight
	}

	return sampleCount, sampleLatencies
}

func printProgressLine(start time.Time, st *progressState, sampleCount int64, sampleLatencies []time.Duration) {
	elapsed := int(time.Since(start).Seconds())

	if sampleCount == 0 {
		fmt.Printf("-- Monitor t=%ds finished_total=%d finished_delta=0 avg_latency_ms=NA p95_latency_ms=NA\n",
			elapsed, st.totalFinished)
		return
	}

	sampleAvg := durationAvg(sampleLatencies)
	p95 := durationPercentile(sampleLatencies, 95)
	totalAvg := time.Duration(0)
	if st.totalFinished > 0 {
		totalAvg = time.Duration(int64(st.totalLatency) / st.totalFinished)
	}

	fmt.Printf("-- Monitor t=%ds finished_total=%d finished_delta=%d avg_latency_ms=%.2f p95_latency_ms=%.2f total_avg_latency_ms=%.2f\n",
		elapsed,
		st.totalFinished,
		sampleCount,
		durationMs(sampleAvg),
		durationMs(p95),
		durationMs(totalAvg),
	)
}

func durationAvg(v []time.Duration) time.Duration {
	if len(v) == 0 {
		return 0
	}
	var sum int64
	for _, d := range v {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(v)))
}

func durationPercentile(v []time.Duration, p float64) time.Duration {
	if len(v) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(v))
	copy(sorted, v)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := int((p / 100.0) * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func durationMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func endpointToRPCAddr(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	case "http", "https":
		// ok
	default:
		return "", fmt.Errorf("unsupported endpoint scheme %q", u.Scheme)
	}

	if strings.HasSuffix(u.Path, "/websocket") {
		u.Path = strings.TrimSuffix(u.Path, "/websocket")
	}
	u.RawQuery = ""
	u.Fragment = ""

	if u.Path == "" {
		return u.String(), nil
	}
	// Keep any explicit non-websocket prefix path, e.g. /rpc.
	return strings.TrimSuffix(u.String(), "/"), nil
}
