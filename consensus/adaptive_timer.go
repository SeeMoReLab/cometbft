package consensus

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"google.golang.org/grpc"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	adaptivetimers "github.com/cometbft/cometbft/proto/adaptive_timers"
	types "github.com/gogo/protobuf/types"
)

// windowData accumulates per-block statistics for one half-epoch window.
type windowData struct {
	startHeight int64
	endHeight   int64
	totalTxs    uint32
	heightCount uint32
	latenciesMs []float64 // propose(round=0) → finalizeCommit per height
	batchSizes  []float64 // txs per committed block
	roundsGt0   uint32    // heights where commitRound > 0 (timeout violations / leader changes)
	windowStart time.Time
}

type pendingReward struct {
	episode     uint32
	report      *adaptivetimers.TendermintReport
	timeoutUsed *adaptivetimers.TendermintTimeout
}

// EpochTracker coordinates the adaptive-timer feedback loop.
// It is owned exclusively by the consensus receiveRoutine goroutine,
// except for timeoutResultCh which is written by a background polling goroutine.
type EpochTracker struct {
	consensusCfg *cfg.ConsensusConfig
	logger       log.Logger
	client       adaptivetimers.LearningAgentClient // nil = disabled
	conn         *grpc.ClientConn

	nodeID    uint32
	epochSize int64
	episode   uint32
	resetOnce bool // Reset has been called

	// windowA collects stats for tx 0..n/2 (used in SendReport).
	windowA windowData
	// windowB collects stats for tx 0.8n..n (used as reward).
	windowB windowData

	// txCounterA counts txs committed in phase A (before n/2 trigger fires).
	txCounterA int64
	// txCounterB counts txs committed in phase B (after n/2 trigger fires).
	txCounterB int64

	// proposeTime maps height → time.Now() at enterPropose(round=0).
	proposeTime map[int64]time.Time

	pendingReward  *pendingReward
	currentTimeout *adaptivetimers.TendermintTimeout

	// timeoutResultCh receives the recommended timeout from the polling goroutine.
	// Buffered(1): at most one result per episode.
	timeoutResultCh chan *adaptivetimers.TendermintTimeout
	pollingCancel   context.CancelFunc
}

// NewEpochTracker creates an EpochTracker. If AdaptiveTimerAddr is empty the
// tracker is a no-op (client == nil).
func NewEpochTracker(config *cfg.ConsensusConfig, logger log.Logger) (*EpochTracker, error) {
	now := time.Now()
	et := &EpochTracker{
		consensusCfg:    config,
		logger:          logger,
		nodeID:          config.AdaptiveTimerNodeIndex,
		epochSize:       config.AdaptiveTimerEpochSize,
		proposeTime:     make(map[int64]time.Time),
		windowA:         windowData{windowStart: now},
		windowB:         windowData{windowStart: now},
		timeoutResultCh: make(chan *adaptivetimers.TendermintTimeout, 1),
		currentTimeout: &adaptivetimers.TendermintTimeout{
			ProposeTimeoutMilliseconds:   uint32(config.TimeoutPropose.Milliseconds()),
			PrevoteTimeoutMilliseconds:   uint32(config.TimeoutPrevote.Milliseconds()),
			PrecommitTimeoutMilliseconds: uint32(config.TimeoutPrecommit.Milliseconds()),
		},
	}

	if config.AdaptiveTimerAddr == "" {
		return et, nil
	}

	//nolint:staticcheck
	conn, err := grpc.Dial(config.AdaptiveTimerAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("adaptive_timer: failed to dial %s: %w", config.AdaptiveTimerAddr, err)
	}
	et.conn = conn
	et.client = adaptivetimers.NewLearningAgentClient(conn)

	return et, nil
}

// SetLogger updates the logger used by the tracker.
// On the first call, it also resets the remote agent so each node starts
// from a clean episode-0 state with a visible log line.
func (et *EpochTracker) SetLogger(l log.Logger) {
	et.logger = l
	if et.client == nil || et.resetOnce {
		return
	}
	et.resetOnce = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := et.client.Reset(ctx, &types.Empty{}); err != nil {
		et.logger.Error("adaptive_timer: Reset failed", "err", err)
	} else {
		et.logger.Info("adaptive_timer: agent reset ok")
	}
}

func (et *EpochTracker) Close() {
	if et.pollingCancel != nil {
		et.pollingCancel()
	}
	if et.conn != nil {
		et.conn.Close()
	}
}

// RecordProposeStart records the wall-clock time of enterPropose for round 0.
// Must be called only for round == 0.
func (et *EpochTracker) RecordProposeStart(height int64) {
	if et.client == nil {
		return
	}
	et.proposeTime[height] = time.Now()
}

// OnBlockCommitted is called from finalizeCommit after recordMetrics.
// height is the committed height, txCount is len(block.Data.Txs),
// commitRound is cs.CommitRound.
func (et *EpochTracker) OnBlockCommitted(height int64, txCount int, commitRound int32) {
	if et.client == nil {
		return
	}

	// Compute consensus latency for this height.
	var latencyMs float64
	if t, ok := et.proposeTime[height]; ok {
		latencyMs = float64(time.Since(t).Milliseconds())
		delete(et.proposeTime, height)
	}

	wasViolation := commitRound > 0
	txInt := int64(txCount)

	// Phase A: accumulate until n/2 transactions.
	if et.txCounterA < et.epochSize/2 {
		accumulateHeight(&et.windowA, height, float64(txCount), latencyMs, wasViolation)
		et.txCounterA += txInt
		if et.txCounterA >= et.epochSize/2 {
			et.sendReportAndStartPolling()
		}
		return
	}

	// Phase B: accumulate from n/2 toward n.
	accumulateHeight(&et.windowB, height, float64(txCount), latencyMs, wasViolation)
	et.txCounterB += txInt

	// At 0.8n total (= 0.3n into phase B): stop polling and apply timeout.
	stopAt := int64(float64(et.epochSize) * 0.3)
	if et.txCounterB >= stopAt && et.txCounterB-txInt < stopAt {
		et.stopPollingAndApply()
	}

	// At n total (= 0.5n into phase B): snapshot B as pending reward, reset.
	if et.txCounterB >= et.epochSize/2 {
		et.snapshotBAndReset()
	}
}

// ApplyPendingTimeout writes the current adaptive timeout values into the live config.
// Must be called only from the consensus receiveRoutine goroutine.
func (et *EpochTracker) ApplyPendingTimeout(config *cfg.ConsensusConfig) {
	if et.client == nil || et.currentTimeout == nil {
		return
	}
	propose := time.Duration(et.currentTimeout.ProposeTimeoutMilliseconds) * time.Millisecond
	prevote := time.Duration(et.currentTimeout.PrevoteTimeoutMilliseconds) * time.Millisecond
	precommit := time.Duration(et.currentTimeout.PrecommitTimeoutMilliseconds) * time.Millisecond
	// Guard against zero/negative values from the agent.
	if propose <= 0 || prevote <= 0 || precommit <= 0 {
		et.logger.Error("adaptive_timer: ignoring invalid timeout from agent",
			"propose_ms", et.currentTimeout.ProposeTimeoutMilliseconds,
			"prevote_ms", et.currentTimeout.PrevoteTimeoutMilliseconds,
			"precommit_ms", et.currentTimeout.PrecommitTimeoutMilliseconds,
		)
		return
	}
	et.logger.Info("adaptive_timer: applying timeouts",
		"propose_before_ms", config.TimeoutPropose.Milliseconds(),
		"prevote_before_ms", config.TimeoutPrevote.Milliseconds(),
		"precommit_before_ms", config.TimeoutPrecommit.Milliseconds(),
		"propose_after_ms", propose.Milliseconds(),
		"prevote_after_ms", prevote.Milliseconds(),
		"precommit_after_ms", precommit.Milliseconds(),
		"propose_changed", config.TimeoutPropose != propose,
		"prevote_changed", config.TimeoutPrevote != prevote,
		"precommit_changed", config.TimeoutPrecommit != precommit,
	)
	config.TimeoutPropose = propose
	config.TimeoutPrevote = prevote
	config.TimeoutPrecommit = precommit
}

func (et *EpochTracker) sendReportAndStartPolling() {
	report := buildReport(&et.windowA)

	et.logger.Info("adaptive_timer: sending report",
		"episode", et.episode,
		"start_height", et.windowA.startHeight,
		"end_height", et.windowA.endHeight,
		"total_txs", report.TotalTransactions,
		"total_consensus_instances", report.TotalConsensusInstances,
		"avg_latency_ms", report.AvgConsensusLatencyMs,
		"p95_latency_ms", report.P95ConsensusLatencyMs,
		"p99_latency_ms", report.P99ConsensusLatencyMs,
		"throughput_tps", report.ThroughputTps,
		"timeout_violation_rate", report.TimeoutViolationRate,
		"avg_batch_size", report.AvgBatchSize,
		"leader_change_count", report.LeaderChangeCount,
	)

	// Build reward for the prior episode, if any.
	var reward *adaptivetimers.Reward
	if et.pendingReward != nil {
		pr := et.pendingReward
		reward = &adaptivetimers.Reward{
			Value: &adaptivetimers.Reward_Tendermint{
				Tendermint: &adaptivetimers.TendermintReward{
					Episode:     pr.episode,
					Report:      pr.report,
					TimeoutUsed: pr.timeoutUsed,
				},
			},
		}
		et.logger.Info("adaptive_timer: sending reward",
			"reward_for_episode", pr.episode,
			"attached_to_episode", et.episode,
			"reward_total_txs", pr.report.TotalTransactions,
			"reward_avg_latency_ms", pr.report.AvgConsensusLatencyMs,
			"reward_throughput_tps", pr.report.ThroughputTps,
			"reward_timeout_violation_rate", pr.report.TimeoutViolationRate,
			"timeout_used_propose_ms", pr.timeoutUsed.ProposeTimeoutMilliseconds,
			"timeout_used_prevote_ms", pr.timeoutUsed.PrevoteTimeoutMilliseconds,
			"timeout_used_precommit_ms", pr.timeoutUsed.PrecommitTimeoutMilliseconds,
		)
	} else {
		et.logger.Info("adaptive_timer: no reward to send", "episode", et.episode)
	}

	msg := &adaptivetimers.ReportLocal{
		NodeId:    et.nodeID,
		Episode:   et.episode,
		Protocol:  adaptivetimers.Protocol_PROTOCOL_TENDERMINT,
		StartTick: uint32(et.windowA.startHeight),
		ReportSeq: uint32(et.windowA.endHeight + 1),
		State: &adaptivetimers.ReportLocal_TendermintReport{
			TendermintReport: report,
		},
		Reward: reward,
	}

	// Send asynchronously.
	episode := et.episode
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := et.client.SendReport(ctx, msg); err != nil {
			et.logger.Error("adaptive_timer: SendReport failed", "episode", episode, "err", err)
		} else {
			et.logger.Info("adaptive_timer: SendReport ok", "episode", episode)
		}
	}()

	// Cancel any prior polling goroutine before starting a new one.
	if et.pollingCancel != nil {
		et.pollingCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	et.pollingCancel = cancel
	go et.pollForTimeout(ctx, episode)
}

func (et *EpochTracker) pollForTimeout(ctx context.Context, episode uint32) {
	req := &adaptivetimers.TimeoutRequest{
		Episode:  episode,
		Protocol: adaptivetimers.Protocol_PROTOCOL_TENDERMINT,
	}

	backoff := 200 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := et.client.GetTimeout(pollCtx, req)
		cancel()

		if err != nil {
			et.logger.Error("adaptive_timer: GetTimeout failed", "episode", episode, "err", err)
		} else {
			switch resp.Status {
			case adaptivetimers.TimeoutStatus_READY:
				if resp.Timeout != nil {
					if tm := resp.Timeout.GetTendermint(); tm != nil {
						select {
						case et.timeoutResultCh <- tm:
						default:
						}
					}
				}
				return
			case adaptivetimers.TimeoutStatus_NOT_RECEIVED, adaptivetimers.TimeoutStatus_PENDING:
				// Back off and retry.
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}
}

func (et *EpochTracker) stopPollingAndApply() {
	if et.pollingCancel != nil {
		et.pollingCancel()
		et.pollingCancel = nil
	}

	// Non-blocking, apply if a timeout recommendation arrived.
	select {
	case tm := <-et.timeoutResultCh:
		et.currentTimeout = tm
		et.logger.Info("adaptive_timer: applying new timeout",
			"episode", et.episode,
			"propose_ms", tm.ProposeTimeoutMilliseconds,
			"prevote_ms", tm.PrevoteTimeoutMilliseconds,
			"precommit_ms", tm.PrecommitTimeoutMilliseconds,
		)
	default:
		et.logger.Info("adaptive_timer: no timeout received by 0.8n, keeping previous",
			"episode", et.episode,
		)
	}

	// Reset window B data for the reward accumulation window.
	et.windowB = windowData{windowStart: time.Now()}
}

func (et *EpochTracker) snapshotBAndReset() {
	bReport := buildReport(&et.windowB)
	et.pendingReward = &pendingReward{
		episode:     et.episode,
		report:      bReport,
		timeoutUsed: et.currentTimeout,
	}

	et.episode++
	now := time.Now()
	et.windowA = windowData{windowStart: now}
	et.windowB = windowData{windowStart: now}
	et.txCounterA = 0
	et.txCounterB = 0

	// Drain any stale timeout written by the previous episode's polling goroutine
	select {
	case <-et.timeoutResultCh:
	default:
	}
}

func accumulateHeight(w *windowData, height int64, txCount, latencyMs float64, violation bool) {
	if w.heightCount == 0 {
		w.startHeight = height
	}
	w.endHeight = height
	w.totalTxs += uint32(txCount)
	w.heightCount++
	if latencyMs > 0 {
		w.latenciesMs = append(w.latenciesMs, latencyMs)
	}
	if txCount > 0 {
		w.batchSizes = append(w.batchSizes, txCount)
	}
	if violation {
		w.roundsGt0++
	}
}

func buildReport(w *windowData) *adaptivetimers.TendermintReport {
	elapsed := time.Since(w.windowStart).Seconds()
	var tps float64
	if elapsed > 0 {
		tps = float64(w.totalTxs) / elapsed
	}

	var violationRate float64
	if w.heightCount > 0 {
		violationRate = float64(w.roundsGt0) / float64(w.heightCount)
	}

	return &adaptivetimers.TendermintReport{
		TotalTransactions:       w.totalTxs,
		TotalConsensusInstances: w.heightCount,
		AvgConsensusLatencyMs:   windowAvg(w.latenciesMs),
		P95ConsensusLatencyMs:   windowPercentile(w.latenciesMs, 95),
		P99ConsensusLatencyMs:   windowPercentile(w.latenciesMs, 99),
		ThroughputTps:           float32(tps),
		TimeoutViolationRate:    float32(violationRate),
		AvgBatchSize:            windowAvg(w.batchSizes),
		P95BatchSize:            windowPercentile(w.batchSizes, 95),
		LeaderChangeCount:       w.roundsGt0,
		RegencyChangeCount:      w.roundsGt0,
	}
}

func windowAvg(vals []float64) float32 {
	if len(vals) == 0 {
		return 0
	}
	var s float64
	for _, v := range vals {
		s += v
	}
	return float32(s / float64(len(vals)))
}

func windowPercentile(vals []float64, p float64) float32 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float32(sorted[idx])
}
