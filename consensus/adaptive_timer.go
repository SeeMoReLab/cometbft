package consensus

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	adaptivetimers "github.com/cometbft/cometbft/proto/adaptive_timers"
	types "github.com/gogo/protobuf/types"
)

// windowData accumulates per-block statistics for one learning window.
type windowData struct {
	startHeight          int64
	endHeight            int64
	totalTxs             uint32
	heightCount          uint32
	latenciesMs          []float64 // propose(round=0) → finalizeCommit per height
	proposeLatenciesMs   []float64 // enterPropose(round=0) → enterPrevote(round=0)
	prevoteLatenciesMs   []float64 // enterPrevote(round=0) → enterPrecommit(round=0)
	precommitLatenciesMs []float64 // enterPrecommit(round=0) → finalizeCommit
	batchSizes           []float64 // txs per committed block
	roundsGt0            uint32    // heights where commitRound > 0 (timeout violations / leader changes)
	windowStart          time.Time
}

// heightSample is one committed height's contribution to a learning window.
type heightSample struct {
	height             int64
	txCount            float64
	latencyMs          float64
	proposeLatencyMs   float64
	prevoteLatencyMs   float64
	precommitLatencyMs float64
	violation          bool
}

type pendingReward struct {
	episode     uint32
	report      *adaptivetimers.TendermintReport
	timeoutUsed *adaptivetimers.TendermintTimeout
}

type learningEpochBoundaries struct {
	reportAt      int64
	applyAt       int64
	rewardStartAt int64
	endAt         int64
}

func newLearningEpochBoundaries(epochSize int64) learningEpochBoundaries {
	gap := epochSize / 10
	return learningEpochBoundaries{
		reportAt:      epochSize,
		applyAt:       epochSize + gap,
		rewardStartAt: epochSize + 2*gap,
		endAt:         2*epochSize + 2*gap,
	}
}

// wallClockStage is the position within one wall-clock learning episode.
type wallClockStage uint8

const (
	// wallClockIdle is the state before the first block commits. The episode clock
	// only starts once the node is actually making progress, so that an idle
	// startup period is not measured as if it were a feature window.
	wallClockIdle wallClockStage = iota
	wallClockFeature
	wallClockReplyWait
	wallClockWarmup
	wallClockReward
)

func (s wallClockStage) String() string {
	switch s {
	case wallClockIdle:
		return "idle"
	case wallClockFeature:
		return "feature"
	case wallClockReplyWait:
		return "reply_wait"
	case wallClockWarmup:
		return "warmup"
	case wallClockReward:
		return "reward"
	default:
		return "unknown"
	}
}

// EpochTracker coordinates the adaptive-timer feedback loop.
//
// Episode boundaries are drawn one of two ways, selected by the window mode:
//
//   - consensus: every boundary is a count of committed heights, so all state is
//     mutated from the consensus receiveRoutine.
//   - wall-clock: every boundary is an instant, driven by a background goroutine
//     so that episodes keep advancing even while consensus is stalled.
//
// mu therefore guards all mutable state, since in wall-clock mode the driver and
// receiveRoutine both touch it.
type EpochTracker struct {
	consensusCfg *cfg.ConsensusConfig
	logger       log.Logger
	client       adaptivetimers.LearningAgentClient // nil = disabled
	conn         *grpc.ClientConn

	nodeID uint32

	// windowMode is one of cfg.AdaptiveTimerWindowMode*. Immutable after construction.
	windowMode string
	// epochSize is the consensus-mode window length in committed heights.
	epochSize int64
	// Wall-clock mode stage lengths.
	featureDuration time.Duration
	replyWait       time.Duration
	warmupDuration  time.Duration
	rewardDuration  time.Duration

	mu        sync.Mutex
	episode   uint32
	resetOnce bool // Reset has been called

	// windowA collects the feature window used in SendReport.
	windowA windowData
	// windowB collects the reward window, after reply waiting and warm-up.
	windowB windowData

	// consensusCounterA counts committed heights in phase A (consensus mode only).
	consensusCounterA int64
	// consensusCounterB counts committed heights in phase B (consensus mode only).
	consensusCounterB int64

	// stage is the current wall-clock stage (wall-clock mode only).
	stage wallClockStage
	// stageDeadline is the instant at which the current wall-clock stage ends.
	stageDeadline time.Time
	// wallClockCancel stops the episode driver goroutine.
	wallClockCancel context.CancelFunc

	// proposeTime maps height -> time.Now() at enterPropose(round=0).
	proposeTime map[int64]time.Time
	// prevoteTime maps height -> time.Now() at enterPrevote(round=0).
	prevoteTime map[int64]time.Time
	// precommitTime maps height -> time.Now() at enterPrecommit(round=0).
	precommitTime map[int64]time.Time

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
	windowMode := config.AdaptiveTimerWindowMode
	if windowMode != cfg.AdaptiveTimerWindowModeConsensus && windowMode != cfg.AdaptiveTimerWindowModeWallClock {
		return nil, fmt.Errorf(
			"adaptive_timer: unknown window mode %q; expected %q or %q",
			windowMode, cfg.AdaptiveTimerWindowModeConsensus, cfg.AdaptiveTimerWindowModeWallClock,
		)
	}

	now := time.Now()
	et := &EpochTracker{
		consensusCfg:    config,
		logger:          logger,
		nodeID:          config.AdaptiveTimerNodeIndex,
		windowMode:      windowMode,
		epochSize:       config.AdaptiveTimerEpochSize,
		featureDuration: config.AdaptiveTimerFeatureDuration,
		replyWait:       config.AdaptiveTimerReplyWait,
		warmupDuration:  config.AdaptiveTimerWarmupDuration,
		rewardDuration:  config.AdaptiveTimerRewardDuration,
		proposeTime:     make(map[int64]time.Time),
		prevoteTime:     make(map[int64]time.Time),
		precommitTime:   make(map[int64]time.Time),
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

	if windowMode == cfg.AdaptiveTimerWindowModeWallClock {
		if config.AdaptiveTimerFeatureDuration <= 0 || config.AdaptiveTimerReplyWait <= 0 ||
			config.AdaptiveTimerWarmupDuration <= 0 || config.AdaptiveTimerRewardDuration <= 0 {
			return nil, fmt.Errorf(
				"adaptive_timer: wall-clock mode requires positive durations, got feature=%s reply_wait=%s warmup=%s reward=%s",
				config.AdaptiveTimerFeatureDuration, config.AdaptiveTimerReplyWait,
				config.AdaptiveTimerWarmupDuration, config.AdaptiveTimerRewardDuration,
			)
		}
	} else if config.AdaptiveTimerEpochSize <= 0 {
		return nil, fmt.Errorf(
			"adaptive_timer: consensus mode requires a positive epoch size, got %d",
			config.AdaptiveTimerEpochSize,
		)
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
	et.mu.Lock()
	et.logger = l
	alreadyReset := et.resetOnce
	et.resetOnce = true
	et.mu.Unlock()

	if et.client == nil || alreadyReset {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := et.client.Reset(ctx, &types.Empty{}); err != nil {
		l.Error("adaptive_timer: Reset failed", "err", err)
		return
	}
	l.Info("adaptive_timer: agent reset ok", "window_mode", et.windowMode)
}

func (et *EpochTracker) Close() {
	et.mu.Lock()
	pollingCancel := et.pollingCancel
	wallClockCancel := et.wallClockCancel
	et.pollingCancel = nil
	et.wallClockCancel = nil
	et.mu.Unlock()

	if wallClockCancel != nil {
		wallClockCancel()
	}
	if pollingCancel != nil {
		pollingCancel()
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
	et.mu.Lock()
	defer et.mu.Unlock()
	et.proposeTime[height] = time.Now()
}

// RecordPrevoteStart records the wall-clock time of enterPrevote for round 0.
func (et *EpochTracker) RecordPrevoteStart(height int64) {
	if et.client == nil {
		return
	}
	et.mu.Lock()
	defer et.mu.Unlock()
	et.prevoteTime[height] = time.Now()
}

// RecordPrecommitStart records the wall-clock time of enterPrecommit for round 0.
func (et *EpochTracker) RecordPrecommitStart(height int64) {
	if et.client == nil {
		return
	}
	et.mu.Lock()
	defer et.mu.Unlock()
	et.precommitTime[height] = time.Now()
}

// OnBlockCommitted is called from finalizeCommit after recordMetrics.
// height is the committed height, txCount is len(block.Data.Txs),
// commitRound is cs.CommitRound.
func (et *EpochTracker) OnBlockCommitted(height int64, txCount int, commitRound int32) {
	if et.client == nil {
		return
	}

	et.mu.Lock()
	defer et.mu.Unlock()

	now := time.Now()
	sample := et.buildHeightSampleLocked(now, height, txCount, commitRound)

	if et.windowMode == cfg.AdaptiveTimerWindowModeWallClock {
		et.accumulateWallClockLocked(now, sample)
		return
	}
	et.accumulateConsensusLocked(sample)
}

// buildHeightSampleLocked computes the end-to-end and phase latencies for a
// committed height and releases its recorded phase-entry times.
func (et *EpochTracker) buildHeightSampleLocked(now time.Time, height int64, txCount int, commitRound int32) heightSample {
	sample := heightSample{
		height:    height,
		txCount:   float64(txCount),
		violation: commitRound > 0,
	}

	proposeT, hasProposeT := et.proposeTime[height]
	prevoteT, hasPrevoteT := et.prevoteTime[height]
	precommitT, hasPrecommitT := et.precommitTime[height]
	if hasProposeT {
		sample.latencyMs = float64(now.Sub(proposeT).Milliseconds())
		delete(et.proposeTime, height)
	}
	if hasProposeT && hasPrevoteT {
		sample.proposeLatencyMs = float64(prevoteT.Sub(proposeT).Milliseconds())
	}
	if hasPrevoteT && hasPrecommitT {
		sample.prevoteLatencyMs = float64(precommitT.Sub(prevoteT).Milliseconds())
	}
	if hasPrecommitT {
		sample.precommitLatencyMs = float64(now.Sub(precommitT).Milliseconds())
	}
	if hasPrevoteT {
		delete(et.prevoteTime, height)
	}
	if hasPrecommitT {
		delete(et.precommitTime, height)
	}
	return sample
}

// accumulateConsensusLocked advances the episode by counting committed heights.
func (et *EpochTracker) accumulateConsensusLocked(sample heightSample) {
	boundaries := newLearningEpochBoundaries(et.epochSize)

	// Feature collection: accumulate n consensus instances.
	if et.consensusCounterA < boundaries.reportAt {
		accumulateHeight(&et.windowA, sample)
		et.consensusCounterA++
		if et.consensusCounterA >= boundaries.reportAt {
			et.sendReportAndStartPollingLocked(time.Now())
		}
		return
	}

	// The remainder of the learning cycle contains reply waiting, warm-up, and reward.
	accumulateHeight(&et.windowB, sample)
	et.consensusCounterB++
	epochPosition := et.consensusCounterA + et.consensusCounterB

	// After a 0.1n reply window, stop polling and apply the recommendation.
	if epochPosition >= boundaries.applyAt && epochPosition-1 < boundaries.applyAt {
		et.stopPollingAndApplyLocked()
	}

	// Discard the next 0.1n as post-apply warm-up, then start reward collection.
	if epochPosition >= boundaries.rewardStartAt && epochPosition-1 < boundaries.rewardStartAt {
		et.startRewardWindowLocked(time.Now())
	}

	// After n reward instances, snapshot the reward window and begin the next episode.
	if epochPosition >= boundaries.endAt && et.windowB.heightCount > 0 {
		et.snapshotBAndResetLocked()
	}
}

// accumulateWallClockLocked files a committed height into whichever wall-clock
// window is currently open. Heights committed during reply-wait or warm-up are
// deliberately dropped: they belong to neither the feature nor the reward window.
func (et *EpochTracker) accumulateWallClockLocked(now time.Time, sample heightSample) {
	if et.stage == wallClockIdle {
		et.startWallClockLocked(now)
	}

	// A stage is sealed at an exact instant. A commit that lands after that instant
	// but acquires the lock before the driver does belongs to the next stage.
	if !et.stageDeadline.IsZero() && now.After(et.stageDeadline) {
		return
	}

	switch et.stage {
	case wallClockFeature:
		accumulateHeight(&et.windowA, sample)
	case wallClockReward:
		accumulateHeight(&et.windowB, sample)
	case wallClockIdle, wallClockReplyWait, wallClockWarmup:
	}
}

// startWallClockLocked anchors the episode clock at the first committed height
// and launches the driver goroutine.
func (et *EpochTracker) startWallClockLocked(start time.Time) {
	et.windowA = windowData{windowStart: start}
	et.windowB = windowData{windowStart: start}
	et.stage = wallClockFeature
	et.stageDeadline = start.Add(et.featureDuration)

	ctx, cancel := context.WithCancel(context.Background())
	et.wallClockCancel = cancel

	et.logger.Info("adaptive_timer: wall-clock learning started",
		"episode", et.episode,
		"feature_duration", et.featureDuration,
		"reply_wait", et.replyWait,
		"warmup_duration", et.warmupDuration,
		"reward_duration", et.rewardDuration,
	)

	go et.runWallClockEpisodes(ctx, et.episode, start, false)
}

// runWallClockEpisodes drives episode boundaries off the clock. Each handler
// returns false once the tracker has moved on from the stage it was waiting for,
// which is how the goroutine retires.
//
// With fromWarmup set the loop resumes mid-episode at the warm-up stage, which is
// how a fast-forwarded node rejoins after applying a recommendation out of band.
func (et *EpochTracker) runWallClockEpisodes(ctx context.Context, episode uint32, episodeStart time.Time, fromWarmup bool) {
	for {
		applyAt := episodeStart
		if fromWarmup {
			fromWarmup = false
		} else {
			featureEnd := episodeStart.Add(et.featureDuration)
			if !waitUntil(ctx, featureEnd) || !et.onWallClockFeatureDeadline(episode, featureEnd) {
				return
			}

			applyAt = featureEnd.Add(et.replyWait)
			if !waitUntil(ctx, applyAt) || !et.onWallClockApplyDeadline(episode, applyAt) {
				return
			}
		}

		rewardStart := applyAt.Add(et.warmupDuration)
		if !waitUntil(ctx, rewardStart) || !et.onWallClockRewardStart(episode, rewardStart) {
			return
		}

		rewardEnd := rewardStart.Add(et.rewardDuration)
		if !waitUntil(ctx, rewardEnd) || !et.onWallClockRewardDeadline(episode, rewardEnd) {
			return
		}

		episode++
		episodeStart = rewardEnd
	}
}

// waitUntil blocks until deadline, reporting false if ctx was cancelled first.
func waitUntil(ctx context.Context, deadline time.Time) bool {
	delay := time.Until(deadline)
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (et *EpochTracker) atWallClockStageLocked(episode uint32, stage wallClockStage) bool {
	return et.windowMode == cfg.AdaptiveTimerWindowModeWallClock &&
		et.episode == episode &&
		et.stage == stage
}

func (et *EpochTracker) onWallClockFeatureDeadline(episode uint32, deadline time.Time) bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	if !et.atWallClockStageLocked(episode, wallClockFeature) {
		return false
	}

	et.sendReportAndStartPollingLocked(deadline)
	et.stage = wallClockReplyWait
	et.stageDeadline = deadline.Add(et.replyWait)
	return true
}

func (et *EpochTracker) onWallClockApplyDeadline(episode uint32, deadline time.Time) bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	if !et.atWallClockStageLocked(episode, wallClockReplyWait) {
		return false
	}

	et.stopPollingAndApplyLocked()
	et.stage = wallClockWarmup
	et.stageDeadline = deadline.Add(et.warmupDuration)
	return true
}

func (et *EpochTracker) onWallClockRewardStart(episode uint32, start time.Time) bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	if !et.atWallClockStageLocked(episode, wallClockWarmup) {
		return false
	}

	et.startRewardWindowLocked(start)
	et.stage = wallClockReward
	et.stageDeadline = start.Add(et.rewardDuration)
	return true
}

func (et *EpochTracker) onWallClockRewardDeadline(episode uint32, end time.Time) bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	if !et.atWallClockStageLocked(episode, wallClockReward) {
		return false
	}

	et.snapshotBLocked(end)
	et.logger.Info("adaptive_timer: wall-clock reward captured",
		"episode", et.episode,
		"reward_duration", et.rewardDuration,
		"total_consensus_instances", et.windowB.heightCount,
		"total_transactions", et.windowB.totalTxs,
	)

	et.episode++
	et.windowA = windowData{windowStart: end}
	et.windowB = windowData{windowStart: end}
	et.stage = wallClockFeature
	et.stageDeadline = end.Add(et.featureDuration)
	et.drainTimeoutResultLocked()
	return true
}

// ApplyPendingTimeout writes the current adaptive timeout values into the live config.
// Must be called only from the consensus receiveRoutine goroutine.
func (et *EpochTracker) ApplyPendingTimeout(config *cfg.ConsensusConfig) {
	if et.client == nil {
		return
	}

	et.mu.Lock()
	defer et.mu.Unlock()
	if et.currentTimeout == nil {
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

// sendReportAndStartPollingLocked seals the feature window at reportEnd, ships it
// with the previous episode's reward, and starts polling for a recommendation.
func (et *EpochTracker) sendReportAndStartPollingLocked(reportEnd time.Time) {
	if et.client == nil {
		return
	}
	report := buildReport(&et.windowA, reportEnd)

	et.logger.Info("adaptive_timer: sending report",
		"episode", et.episode,
		"window_mode", et.windowMode,
		"start_height", et.windowA.startHeight,
		"end_height", et.windowA.endHeight,
		"total_txs", report.TotalTransactions,
		"total_consensus_instances", report.TotalConsensusInstances,
		"avg_latency_ms", report.AvgConsensusLatencyMs,
		"p50_latency_ms", report.P50ConsensusLatencyMs,
		"p90_latency_ms", report.P90ConsensusLatencyMs,
		"throughput_tps", report.ThroughputTps,
		"timeout_violation_rate", report.TimeoutViolationRate,
		"avg_batch_size", report.AvgBatchSize,
		"p50_batch_size", report.P50BatchSize,
		"p90_batch_size", report.P90BatchSize,
		"leader_change_count", report.LeaderChangeCount,
		"propose_latency_ms", report.ProposeLatencyMs,
		"prevote_latency_ms", report.PrevoteLatencyMs,
		"precommit_latency_ms", report.PrecommitLatencyMs,
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
		State: &adaptivetimers.ReportLocal_TendermintState{
			TendermintState: report,
		},
		Reward: reward,
	}

	// Cancel any prior polling goroutine before starting the report/reply cycle.
	if et.pollingCancel != nil {
		et.pollingCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	et.pollingCancel = cancel

	// Send asynchronously, then poll only after the agent has accepted the report.
	// This avoids spending a short reply window backing off from a NOT_RECEIVED
	// response caused by racing SendReport.
	//
	// episode and logger are captured here rather than read from et, since the
	// goroutine outlives the lock and both fields move on without it.
	episode := et.episode
	logger := et.logger
	go func() {
		sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := et.client.SendReport(sendCtx, msg)
		sendCancel()
		if err != nil {
			logger.Error("adaptive_timer: SendReport failed", "episode", episode, "err", err)
			return
		}
		logger.Info("adaptive_timer: SendReport ok", "episode", episode)
		et.pollForTimeout(ctx, logger, episode)
	}()
}

func (et *EpochTracker) pollForTimeout(ctx context.Context, logger log.Logger, episode uint32) {
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
			logger.Error("adaptive_timer: GetTimeout failed", "episode", episode, "err", err)
		} else {
			switch resp.Status {
			case adaptivetimers.TimeoutStatus_READY:
				if resp.Timeout != nil {
					if tm := resp.Timeout.GetTendermint(); tm != nil {
						// In sharing mode the agent answers with the newest episode it
						// has sealed, which may be past the one polled. Queueing that
						// for the local apply point would be wrong: the apply point it
						// belongs to has already gone by.
						if resp.Episode > episode {
							et.fastForward(resp.Episode, episode, tm)
							return
						}
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

func (et *EpochTracker) stopPollingAndApplyLocked() {
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
			"window_mode", et.windowMode,
			"propose_ms", tm.ProposeTimeoutMilliseconds,
			"prevote_ms", tm.PrevoteTimeoutMilliseconds,
			"precommit_ms", tm.PrecommitTimeoutMilliseconds,
		)
	default:
		et.logger.Info("adaptive_timer: reply deadline reached without a recommendation, keeping previous",
			"episode", et.episode,
			"window_mode", et.windowMode,
		)
	}
}

// fastForward jumps to an episode that the agent has already decided while this
// node was still working on an older one. Reaching this point means the node
// missed at least one episode outright: in consensus mode because heights caught
// up via blocksync never reach OnBlockCommitted, or after a restart; in wall-clock
// mode only if the node was down, since episodes there advance on the clock and
// so keep pace regardless of commit rate.
//
// The recommendation is applied immediately rather than at the usual apply point,
// since that point has passed, and the remainder of the episode keeps its normal
// shape so the resulting reward is still measured over a full-length window.
func (et *EpochTracker) fastForward(target, polled uint32, tm *adaptivetimers.TendermintTimeout) {
	et.mu.Lock()
	defer et.mu.Unlock()

	// The local episode may have advanced on its own between the poll and here.
	if target <= et.episode {
		return
	}

	now := time.Now()
	previous := et.episode
	et.episode = target
	et.currentTimeout = tm

	if et.pollingCancel != nil {
		et.pollingCancel()
		et.pollingCancel = nil
	}
	et.drainTimeoutResultLocked()

	// The abandoned episode's in-flight windows are discarded rather than
	// re-attributed: they were measured against an episode that no longer applies.
	// pendingReward is deliberately left alone, matching SmartBFT. It cannot leak
	// into the new episode's report either way, because both branches below resume
	// past the report point, so it is overwritten at the next reward deadline.
	et.windowA = windowData{windowStart: now}
	et.windowB = windowData{windowStart: now}

	if et.windowMode == cfg.AdaptiveTimerWindowModeWallClock {
		if et.wallClockCancel != nil {
			et.wallClockCancel()
		}
		et.stage = wallClockWarmup
		et.stageDeadline = now.Add(et.warmupDuration)

		ctx, cancel := context.WithCancel(context.Background())
		et.wallClockCancel = cancel
		// The driver is bound to a single episode number and its handlers bail as
		// soon as the episode changes, so it has to be restarted here or learning
		// stops for the rest of the run.
		go et.runWallClockEpisodes(ctx, target, now, true)
	} else {
		// Resume at the instant the apply point would have been, so the remaining
		// warm-up and the reward window are both full length.
		boundaries := newLearningEpochBoundaries(et.epochSize)
		et.consensusCounterA = boundaries.reportAt
		et.consensusCounterB = boundaries.applyAt - boundaries.reportAt
	}

	et.logger.Info("adaptive_timer: fast-forward",
		"from_episode", previous,
		"to_episode", target,
		"polled_episode", polled,
		"window_mode", et.windowMode,
		"propose_ms", tm.ProposeTimeoutMilliseconds,
		"prevote_ms", tm.PrevoteTimeoutMilliseconds,
		"precommit_ms", tm.PrecommitTimeoutMilliseconds,
	)
}

// startRewardWindowLocked opens the reward window at start, discarding whatever
// was collected during warm-up.
func (et *EpochTracker) startRewardWindowLocked(start time.Time) {
	et.windowB = windowData{windowStart: start}
	et.logger.Info("adaptive_timer: reward window started after warm-up",
		"episode", et.episode,
		"window_mode", et.windowMode,
		"epoch_position", et.consensusCounterA+et.consensusCounterB,
		"timeout_used_propose_ms", et.currentTimeout.ProposeTimeoutMilliseconds,
		"timeout_used_prevote_ms", et.currentTimeout.PrevoteTimeoutMilliseconds,
		"timeout_used_precommit_ms", et.currentTimeout.PrecommitTimeoutMilliseconds,
	)
}

// snapshotBLocked freezes the reward window at end so it can ride along with the
// next episode's report.
func (et *EpochTracker) snapshotBLocked(end time.Time) {
	et.pendingReward = &pendingReward{
		episode:     et.episode,
		report:      buildReport(&et.windowB, end),
		timeoutUsed: et.currentTimeout,
	}
}

func (et *EpochTracker) snapshotBAndResetLocked() {
	now := time.Now()
	et.snapshotBLocked(now)

	et.episode++
	et.windowA = windowData{windowStart: now}
	et.windowB = windowData{windowStart: now}
	et.consensusCounterA = 0
	et.consensusCounterB = 0
	et.drainTimeoutResultLocked()
}

// drainTimeoutResultLocked discards a timeout written by the previous episode's
// polling goroutine after that episode's apply point had already passed.
func (et *EpochTracker) drainTimeoutResultLocked() {
	select {
	case <-et.timeoutResultCh:
	default:
	}
}

func accumulateHeight(w *windowData, s heightSample) {
	if w.heightCount == 0 {
		w.startHeight = s.height
	}
	w.endHeight = s.height
	w.totalTxs += uint32(s.txCount)
	w.heightCount++
	if s.latencyMs > 0 {
		w.latenciesMs = append(w.latenciesMs, s.latencyMs)
	}
	if s.proposeLatencyMs > 0 {
		w.proposeLatenciesMs = append(w.proposeLatenciesMs, s.proposeLatencyMs)
	}
	if s.prevoteLatencyMs > 0 {
		w.prevoteLatenciesMs = append(w.prevoteLatenciesMs, s.prevoteLatencyMs)
	}
	if s.precommitLatencyMs > 0 {
		w.precommitLatenciesMs = append(w.precommitLatenciesMs, s.precommitLatencyMs)
	}
	if s.txCount > 0 {
		w.batchSizes = append(w.batchSizes, s.txCount)
	}
	if s.violation {
		w.roundsGt0++
	}
}

// buildReport summarises a window that closed at end. Passing the closing instant
// explicitly matters in wall-clock mode, where throughput must be divided by the
// nominal window length rather than by however long it took to get here.
func buildReport(w *windowData, end time.Time) *adaptivetimers.TendermintReport {
	elapsed := end.Sub(w.windowStart).Seconds()
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
		P50ConsensusLatencyMs:   windowPercentile(w.latenciesMs, 50),
		P90ConsensusLatencyMs:   windowPercentile(w.latenciesMs, 90),
		ThroughputTps:           float32(tps),
		TimeoutViolationRate:    float32(violationRate),
		AvgBatchSize:            windowAvg(w.batchSizes),
		P50BatchSize:            windowPercentile(w.batchSizes, 50),
		P90BatchSize:            windowPercentile(w.batchSizes, 90),
		LeaderChangeCount:       w.roundsGt0,
		RegencyChangeCount:      w.roundsGt0,
		ProposeLatencyMs:        windowAvg(w.proposeLatenciesMs),
		PrevoteLatencyMs:        windowAvg(w.prevoteLatenciesMs),
		PrecommitLatencyMs:      windowAvg(w.precommitLatenciesMs),
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
