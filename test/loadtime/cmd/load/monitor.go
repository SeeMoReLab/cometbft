package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
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
	// prevSample is when the previous Monitor line was printed.
	prevSample time.Time
}

// clientTag is the "[client <ISO-8601 UTC>]" prefix the other protocols' clients print.
func clientTag() string {
	return fmt.Sprintf("[client %s]", time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"))
}

func runProgressMonitor(ctx context.Context, wsEndpoint string, runID []byte, interval time.Duration) {
	rpcAddr, err := endpointToRPCAddr(wsEndpoint)
	if err != nil {
		fmt.Printf("%s Monitor error: invalid endpoint %q: %v\n", clientTag(), wsEndpoint, err)
		return
	}

	client, err := rpchttp.New(rpcAddr, "/websocket")
	if err != nil {
		fmt.Printf("%s Monitor error: failed to create RPC client for %q: %v\n", clientTag(), rpcAddr, err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	st := &progressState{prevSample: time.Now()}

	for {
		select {
		case <-ctx.Done():
			// No final flush: the shutdown's partial interval would print a sample of a few
			// ms, whose tps is meaningless.
			return
		case <-ticker.C:
			sampleCount, sampleLatencies := collectFinishedTxs(ctx, client, runID, st)
			printProgressLine(st, sampleCount, sampleLatencies)
		}
	}
}

func collectFinishedTxs(ctx context.Context, client *rpchttp.HTTP, runID []byte, st *progressState) (int64, []time.Duration) {
	status, err := client.Status(ctx)
	if err != nil {
		fmt.Printf("%s Monitor error: status query failed: %v\n", clientTag(), err)
		return 0, nil
	}

	latest := status.SyncInfo.LatestBlockHeight
	if latest <= 0 {
		return 0, nil
	}

	if !st.initialized {
		b, err := client.Block(ctx, &latest)
		if err != nil {
			fmt.Printf("%s Monitor error: initial block query failed at height %d: %v\n", clientTag(), latest, err)
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
			fmt.Printf("%s Monitor error: block query failed at height %d: %v\n", clientTag(), curHeight, err)
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
		}

		st.prevBlock = cur.Block
		st.prevHeight = curHeight
	}

	return sampleCount, sampleLatencies
}

// printProgressLine prints the Monitor line in the other protocols' client format:
// whole-ms nearest-rank percentiles, all 0 when nothing finished. Latency here is the
// committing block's time minus the transaction's creation time, not a client reply.
// There is no err field: the load client never learns of rejected transactions.
func printProgressLine(st *progressState, sampleCount int64, sampleLatencies []time.Duration) {
	now := time.Now()
	seconds := now.Sub(st.prevSample).Seconds()
	st.prevSample = now
	tps := 0.0
	if seconds > 0 {
		tps = float64(sampleCount) / seconds
	}

	fmt.Printf("%s Monitor duration=%.3fs succ=%d tps=%.3f avg_ms=%.3f p50=%d p95=%d p99=%d max=%d\n",
		clientTag(),
		seconds,
		sampleCount,
		tps,
		durationMs(durationAvg(sampleLatencies)),
		percentileMs(sampleLatencies, 0.50),
		percentileMs(sampleLatencies, 0.95),
		percentileMs(sampleLatencies, 0.99),
		durationMax(sampleLatencies).Milliseconds(),
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

func durationMax(v []time.Duration) time.Duration {
	var maximum time.Duration
	for _, duration := range v {
		if duration > maximum {
			maximum = duration
		}
	}
	return maximum
}

// percentileMs returns the nearest-rank percentile (the ceil(n*p)-th smallest) in whole ms.
func percentileMs(v []time.Duration, p float64) int64 {
	if len(v) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(v))
	copy(sorted, v)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rank := int(math.Ceil(p * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1].Milliseconds()
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
