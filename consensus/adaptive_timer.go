package consensus

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	adaptivetimers "github.com/cometbft/cometbft/proto/adaptive_timers"
	types "github.com/gogo/protobuf/types"
)

// envLearningAnchorUnixMS is the apply instant of episode 1 in unix ms (benchmark
// start plus the failure-spec warm-up). The harness sets it on every replica, and
// learning refuses to start without it.
const envLearningAnchorUnixMS = "COMETBFT_LEARNING_ANCHOR_UNIX_MS"

// wallClockJoinSlack is how late a driver joining from idle may open its feature
// window. Commits before the window opens are dropped while the report still
// divides by the full window length, so a later join skips to the next episode.
const wallClockJoinSlack = 100 * time.Millisecond

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
	interCommitGapsMs    []float64 // between consecutive commits in this window: n commits give n-1 gaps
	lastCommitAt         time.Time // commit instant of the window's latest height
	batchSizes           []float64 // txs per committed block
	roundsGt0            uint32    // heights where commitRound > 0: the height's leader was replaced
	roundChanges         uint32    // sum of commitRound: every round change installs a new proposer
	windowStart          time.Time
}

// heightSample is one committed height's contribution to a learning window.
// Phase latencies are float ms and count only when their has* flag is set, so a
// sub-millisecond phase is kept as a sample rather than dropped as missing.
type heightSample struct {
	height             int64
	committedAt        time.Time
	txCount            float64
	latencyMs          float64
	proposeLatencyMs   float64
	prevoteLatencyMs   float64
	precommitLatencyMs float64
	hasPropose         bool
	hasPrevote         bool
	hasPrecommit       bool
	commitRound        int32
}

type pendingReward struct {
	episode     uint32
	report      *adaptivetimers.TendermintReport
	timeoutUsed *adaptivetimers.TendermintTimeout
}

// wallClockStage is the position within one wall-clock learning episode.
type wallClockStage uint8

const (
	// wallClockIdle is the state before the episode the driver joined has started
	// (at startup, or after skipping overrun episodes). Commits are dropped.
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
// Episode boundaries are wall-clock instants, driven by a background goroutine
// so that episodes keep advancing even while consensus is stalled. mu guards all
// mutable state, since the driver and the consensus receiveRoutine both touch it.
type EpochTracker struct {
	logger log.Logger
	client adaptivetimers.LearningAgentClient // nil = disabled
	conn   *grpc.ClientConn

	nodeID uint32

	// Wall-clock stage lengths.
	featureDuration time.Duration
	replyWait       time.Duration
	warmupDuration  time.Duration
	rewardDuration  time.Duration

	// anchor is the apply instant of episode 1, read once from the wall clock and
	// carried on the monotonic clock. Episode e applies at anchor + (e-1)*period and
	// starts feature + replyWait before that, on every replica.
	anchor       time.Time
	anchorUnixMS int64

	mu        sync.Mutex
	episode   uint32
	resetOnce bool // Reset has been called

	// windowA collects the feature window used in SendReport.
	windowA windowData
	// windowB collects the reward window, after reply waiting and warm-up.
	windowB windowData

	// stage is the current wall-clock stage.
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
	// proposeInForceMs is the propose timeout last written to the live config,
	// reported as timeout_ms. It lags currentTimeout until the next round 0.
	proposeInForceMs uint32
	// appliedLogged is set once the initial "applied timeouts:" line is logged.
	appliedLogged bool

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
		logger:          logger,
		nodeID:          config.AdaptiveTimerNodeIndex,
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
		proposeInForceMs: uint32(config.TimeoutPropose.Milliseconds()),
	}

	if config.AdaptiveTimerAddr == "" {
		return et, nil
	}

	anchorText := strings.TrimSpace(os.Getenv(envLearningAnchorUnixMS))
	if anchorText == "" {
		return nil, fmt.Errorf("adaptive_timer: learning is enabled but %s is not set", envLearningAnchorUnixMS)
	}
	anchorUnixMS, err := strconv.ParseInt(anchorText, 10, 64)
	if err != nil || anchorUnixMS <= 0 {
		return nil, fmt.Errorf("adaptive_timer: invalid %s=%q", envLearningAnchorUnixMS, anchorText)
	}
	et.anchorUnixMS = anchorUnixMS
	et.anchor = now.Add(time.UnixMilli(anchorUnixMS).Sub(now))

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
// from a clean episode-0 state with a visible log line, and then joins the
// episode grid.
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
	} else {
		l.Info("adaptive_timer: agent reset ok")
	}

	et.mu.Lock()
	defer et.mu.Unlock()
	et.startWallClockLocked(time.Now())
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
	et.accumulateWallClockLocked(now, sample)
}

// buildHeightSampleLocked computes the end-to-end and phase latencies for a
// committed height and releases its recorded phase-entry times.
func (et *EpochTracker) buildHeightSampleLocked(now time.Time, height int64, txCount int, commitRound int32) heightSample {
	sample := heightSample{
		height:      height,
		committedAt: now,
		txCount:     float64(txCount),
		commitRound: commitRound,
	}

	proposeT, hasProposeT := et.proposeTime[height]
	prevoteT, hasPrevoteT := et.prevoteTime[height]
	precommitT, hasPrecommitT := et.precommitTime[height]
	if hasProposeT {
		sample.latencyMs = float64(now.Sub(proposeT).Milliseconds())
		delete(et.proposeTime, height)
	}
	if hasProposeT && hasPrevoteT {
		sample.proposeLatencyMs = durationMs(prevoteT.Sub(proposeT))
		sample.hasPropose = true
	}
	if hasPrevoteT && hasPrecommitT {
		sample.prevoteLatencyMs = durationMs(precommitT.Sub(prevoteT))
		sample.hasPrevote = true
	}
	if hasPrecommitT {
		sample.precommitLatencyMs = durationMs(now.Sub(precommitT))
		sample.hasPrecommit = true
	}
	if hasPrevoteT {
		delete(et.prevoteTime, height)
	}
	if hasPrecommitT {
		delete(et.precommitTime, height)
	}
	return sample
}

// accumulateWallClockLocked files a committed height into whichever wall-clock
// window is currently open. Heights committed while idle, during reply-wait or
// during warm-up are deliberately dropped: they belong to neither the feature nor
// the reward window.
func (et *EpochTracker) accumulateWallClockLocked(now time.Time, sample heightSample) {
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

// period is the length of one episode.
func (et *EpochTracker) period() time.Duration {
	return et.featureDuration + et.replyWait + et.warmupDuration + et.rewardDuration
}

// applyOffset is the time from an episode's start to its apply instant.
func (et *EpochTracker) applyOffset() time.Duration {
	return et.featureDuration + et.replyWait
}

// gridApplyAt is the apply instant of episode e (e >= 1) on the anchored grid.
func (et *EpochTracker) gridApplyAt(episode uint32) time.Time {
	return et.anchor.Add(time.Duration(int64(episode)-1) * et.period())
}

// gridEpisodeStart is the start of episode e's feature window on the anchored grid.
func (et *EpochTracker) gridEpisodeStart(episode uint32) time.Time {
	return et.gridApplyAt(episode).Add(-et.applyOffset())
}

// firstEpisodeFrom is the smallest episode e >= 1 that starts at or after now.
func (et *EpochTracker) firstEpisodeFrom(now time.Time) uint32 {
	late := now.Sub(et.gridEpisodeStart(1))
	if late <= 0 {
		return 1
	}
	period := et.period()
	return uint32(1 + (late+period-1)/period)
}

// startWallClockLocked joins the anchored episode grid at the first episode that
// has not started yet and launches the driver goroutine.
func (et *EpochTracker) startWallClockLocked(now time.Time) {
	first := et.firstEpisodeFrom(now)
	et.episode = first
	et.stage = wallClockIdle
	et.stageDeadline = time.Time{}

	ctx, cancel := context.WithCancel(context.Background())
	et.wallClockCancel = cancel

	et.logger.Info("adaptive_timer: wall-clock learning started",
		"anchor_unix_ms", et.anchorUnixMS,
		"period_ms", et.period().Milliseconds(),
		"apply_offset_ms", et.applyOffset().Milliseconds(),
		"first_episode", first,
		"first_episode_start_in_ms", et.gridEpisodeStart(first).Sub(now).Milliseconds(),
		"feature_duration", et.featureDuration,
		"reply_wait", et.replyWait,
		"warmup_duration", et.warmupDuration,
		"reward_duration", et.rewardDuration,
	)

	go et.runWallClockEpisodes(ctx, first, wallClockIdle)
}

// runWallClockEpisodes drives episode boundaries off the anchored grid: every
// deadline of episode e is derived from e, never from when the previous one ran.
// Each handler returns false once the tracker has moved on from the stage it was
// waiting for, which is how the goroutine retires.
//
// resume is the stage the tracker was left in for episode: idle starts the episode
// from its feature window, while reply_wait and warmup resume mid-episode, which is
// how a fast-forwarded node rejoins the target's grid slot.
func (et *EpochTracker) runWallClockEpisodes(ctx context.Context, episode uint32, resume wallClockStage) {
	// joining is set while the driver itself has to open the next feature window,
	// rather than the previous reward deadline having opened it on time.
	joining := resume == wallClockIdle
	for {
		episodeStart := et.gridEpisodeStart(episode)
		featureEnd := episodeStart.Add(et.featureDuration)
		applyAt := featureEnd.Add(et.replyWait)
		switch resume {
		case wallClockWarmup:
		case wallClockReplyWait:
			if !waitUntil(ctx, applyAt) || !et.onWallClockApplyDeadline(episode, applyAt) {
				return
			}
		default:
			if !waitUntil(ctx, episodeStart) {
				return
			}
			// An episode is overrun once its start has passed by more than the join
			// slack, or, if it was opened on time, once its feature window is over.
			// Either way it cannot be reported in full: skip to the next one ahead.
			lateLimit := et.featureDuration
			if joining {
				lateLimit = wallClockJoinSlack
			}
			if now := time.Now(); now.Sub(episodeStart) > lateLimit {
				next := et.firstEpisodeFrom(now)
				if !et.skipOverrunEpisodes(episode, next, now) {
					return
				}
				episode = next
				joining = true
				continue
			}
			if !et.onWallClockEpisodeStart(episode, episodeStart) {
				return
			}
			joining = false

			if !waitUntil(ctx, featureEnd) || !et.onWallClockFeatureDeadline(episode, featureEnd) {
				return
			}

			if !waitUntil(ctx, applyAt) || !et.onWallClockApplyDeadline(episode, applyAt) {
				return
			}
		}
		resume = wallClockIdle

		rewardStart := applyAt.Add(et.warmupDuration)
		if !waitUntil(ctx, rewardStart) || !et.onWallClockRewardStart(episode, rewardStart) {
			return
		}

		rewardEnd := rewardStart.Add(et.rewardDuration)
		if !waitUntil(ctx, rewardEnd) || !et.onWallClockRewardDeadline(episode, rewardEnd) {
			return
		}

		episode++
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
	return et.episode == episode && et.stage == stage
}

// onWallClockEpisodeStart opens the feature window of an episode the driver joined
// from idle. A consecutive episode is already open, since the previous reward
// deadline hands over to the next feature window at the same instant.
func (et *EpochTracker) onWallClockEpisodeStart(episode uint32, start time.Time) bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	if et.episode != episode {
		return false
	}
	if et.stage == wallClockIdle {
		et.windowA = windowData{windowStart: start}
		et.windowB = windowData{windowStart: start}
		et.stage = wallClockFeature
		et.stageDeadline = start.Add(et.featureDuration)
	}
	return et.stage == wallClockFeature
}

// skipOverrunEpisodes moves the driver from an overrun episode to next, idling
// until next starts.
func (et *EpochTracker) skipOverrunEpisodes(episode, next uint32, now time.Time) bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	if et.episode != episode {
		return false
	}
	et.logger.Error("adaptive_timer: episode overrun, skipping ahead on the grid",
		"episode", episode,
		"next_episode", next,
		"late_ms", now.Sub(et.gridEpisodeStart(episode)).Milliseconds(),
	)
	et.episode = next
	et.stage = wallClockIdle
	et.stageDeadline = time.Time{}
	et.drainTimeoutResultLocked()
	return true
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
	// The harness plot reads this line: once with the initial values, then on every change.
	if !et.appliedLogged || config.TimeoutPropose != propose || config.TimeoutPrevote != prevote ||
		config.TimeoutPrecommit != precommit {
		et.logger.Info("applied timeouts:",
			"propose_ms", propose.Milliseconds(),
			"prevote_ms", prevote.Milliseconds(),
			"precommit_ms", precommit.Milliseconds(),
		)
		et.appliedLogged = true
	}
	config.TimeoutPropose = propose
	config.TimeoutPrevote = prevote
	config.TimeoutPrecommit = precommit
	et.proposeInForceMs = et.currentTimeout.ProposeTimeoutMilliseconds
}

// sendReportAndStartPollingLocked seals the feature window at reportEnd, ships it
// with the previous episode's reward, and starts polling for a recommendation.
func (et *EpochTracker) sendReportAndStartPollingLocked(reportEnd time.Time) {
	if et.client == nil {
		return
	}
	report := buildReport(&et.windowA, reportEnd, et.proposeInForceMs)

	et.logger.Info("adaptive_timer: sending report",
		"episode", et.episode,
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

	gridApplyUnixMS := et.anchorUnixMS + (int64(et.episode)-1)*et.period().Milliseconds()
	lagMS := time.Since(et.gridApplyAt(et.episode)).Milliseconds()

	// Non-blocking, apply if a timeout recommendation arrived.
	select {
	case tm := <-et.timeoutResultCh:
		et.currentTimeout = tm
		et.logger.Info("adaptive_timer: applying new timeout",
			"episode", et.episode,
			"propose_ms", tm.ProposeTimeoutMilliseconds,
			"prevote_ms", tm.PrevoteTimeoutMilliseconds,
			"precommit_ms", tm.PrecommitTimeoutMilliseconds,
			"grid_apply_unix_ms", gridApplyUnixMS,
			"lag_ms", lagMS,
		)
	default:
		et.logger.Info("adaptive_timer: reply deadline reached without a recommendation, keeping previous",
			"episode", et.episode,
			"grid_apply_unix_ms", gridApplyUnixMS,
			"lag_ms", lagMS,
		)
	}
}

// fastForward jumps to an episode that the agent has already decided while this
// node was still working on an older one. Reaching this point means the node
// missed at least one episode outright, which happens only if the node was down,
// since episodes advance on the clock and so keep pace regardless of commit rate.
//
// The node resumes on the target's grid slot. Before the target's apply instant it
// waits in reply-wait and applies the recommendation on the grid, as if it had
// polled it itself. Past that instant the recommendation is applied immediately,
// and the node resumes in the target's warm-up if its reward window has not
// started yet, so the resulting reward is still measured over a full-length
// window, and otherwise idles until the next episode that has not started.
func (et *EpochTracker) fastForward(target, polled uint32, tm *adaptivetimers.TendermintTimeout) {
	et.mu.Lock()
	defer et.mu.Unlock()

	// The local episode may have advanced on its own between the poll and here.
	if target <= et.episode {
		return
	}

	now := time.Now()
	previous := et.episode

	if et.pollingCancel != nil {
		et.pollingCancel()
		et.pollingCancel = nil
	}
	et.drainTimeoutResultLocked()

	// The abandoned episode's in-flight windows are discarded rather than
	// re-attributed: they were measured against an episode that no longer applies.
	// pendingReward is kept, as on an overrun skip: the agent files a reward under
	// the episode it carries, whichever report it rides on.
	et.windowA = windowData{windowStart: now}
	et.windowB = windowData{windowStart: now}

	if et.wallClockCancel != nil {
		et.wallClockCancel()
	}
	applyAt := et.gridApplyAt(target)
	rewardStart := applyAt.Add(et.warmupDuration)
	gridFields := []interface{}{
		"grid_apply_unix_ms", et.anchorUnixMS + (int64(target)-1)*et.period().Milliseconds(),
	}
	switch {
	case now.Before(applyAt):
		et.episode = target
		et.stage = wallClockReplyWait
		et.stageDeadline = applyAt
		// A cancelled polling goroutine may still slip a stale result in, without
		// the lock, so make room until this one is queued instead of blocking.
	queue:
		for {
			select {
			case et.timeoutResultCh <- tm:
				break queue
			default:
				et.drainTimeoutResultLocked()
			}
		}
	case !now.After(rewardStart):
		et.currentTimeout = tm
		et.episode = target
		et.stage = wallClockWarmup
		et.stageDeadline = rewardStart
		gridFields = append(gridFields, "lag_ms", now.Sub(applyAt).Milliseconds())
	default:
		et.currentTimeout = tm
		et.episode = et.firstEpisodeFrom(now)
		et.stage = wallClockIdle
		et.stageDeadline = time.Time{}
		gridFields = append(gridFields, "lag_ms", now.Sub(applyAt).Milliseconds())
	}

	ctx, cancel := context.WithCancel(context.Background())
	et.wallClockCancel = cancel
	// The driver is bound to a single episode number and its handlers bail as
	// soon as the episode changes, so it has to be restarted here or learning
	// stops for the rest of the run.
	go et.runWallClockEpisodes(ctx, et.episode, et.stage)

	// Without lag_ms the recommendation is queued, and the apply-deadline line
	// carries the lag once it is applied on the grid.
	et.logger.Info("adaptive_timer: fast-forward", append([]interface{}{
		"from_episode", previous,
		"to_episode", target,
		"resumed_episode", et.episode,
		"resumed_stage", et.stage,
		"polled_episode", polled,
		"propose_ms", tm.ProposeTimeoutMilliseconds,
		"prevote_ms", tm.PrevoteTimeoutMilliseconds,
		"precommit_ms", tm.PrecommitTimeoutMilliseconds,
	}, gridFields...)...)
}

// startRewardWindowLocked opens the reward window at start, discarding whatever
// was collected during warm-up.
func (et *EpochTracker) startRewardWindowLocked(start time.Time) {
	et.windowB = windowData{windowStart: start}
	et.logger.Info("adaptive_timer: reward window started after warm-up",
		"episode", et.episode,
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
		report:      buildReport(&et.windowB, end, et.proposeInForceMs),
		timeoutUsed: et.currentTimeout,
	}
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
	} else {
		w.interCommitGapsMs = append(w.interCommitGapsMs, durationMs(s.committedAt.Sub(w.lastCommitAt)))
	}
	w.lastCommitAt = s.committedAt
	w.endHeight = s.height
	w.totalTxs += uint32(s.txCount)
	w.heightCount++
	if s.latencyMs > 0 {
		w.latenciesMs = append(w.latenciesMs, s.latencyMs)
	}
	if s.hasPropose && s.proposeLatencyMs >= 0 {
		w.proposeLatenciesMs = append(w.proposeLatenciesMs, s.proposeLatencyMs)
	}
	if s.hasPrevote && s.prevoteLatencyMs >= 0 {
		w.prevoteLatenciesMs = append(w.prevoteLatenciesMs, s.prevoteLatencyMs)
	}
	if s.hasPrecommit && s.precommitLatencyMs >= 0 {
		w.precommitLatenciesMs = append(w.precommitLatenciesMs, s.precommitLatencyMs)
	}
	if s.txCount > 0 {
		w.batchSizes = append(w.batchSizes, s.txCount)
	}
	if s.commitRound > 0 {
		w.roundsGt0++
		w.roundChanges += uint32(s.commitRound)
	}
}

// buildReport summarises a window that closed at end. Passing the closing instant
// explicitly matters because throughput must be divided by the nominal window
// length rather than by however long it took to get here. proposeTimeoutMs is the
// propose timeout in force, reported as timeout_ms.
func buildReport(w *windowData, end time.Time, proposeTimeoutMs uint32) *adaptivetimers.TendermintReport {
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
		P95ConsensusLatencyMs:   windowPercentile(w.latenciesMs, 95),
		ThroughputTps:           float32(tps),
		TimeoutViolationRate:    float32(violationRate),
		AvgInterCommitGapMs:     windowAvg(w.interCommitGapsMs),
		P50InterCommitGapMs:     windowPercentile(w.interCommitGapsMs, 50),
		P90InterCommitGapMs:     windowPercentile(w.interCommitGapsMs, 90),
		P95InterCommitGapMs:     windowPercentile(w.interCommitGapsMs, 95),
		AvgBatchSize:            windowAvg(w.batchSizes),
		P50BatchSize:            windowPercentile(w.batchSizes, 50),
		P90BatchSize:            windowPercentile(w.batchSizes, 90),
		P95BatchSize:            windowPercentile(w.batchSizes, 95),
		// Tendermint rotates the proposer every height, so a "leader change" is a
		// height whose round-0 proposer was replaced, and a "regency change" is
		// each round change, the analogue of a view change.
		LeaderChangeCount:     w.roundsGt0,
		RegencyChangeCount:    w.roundChanges,
		ProposeLatencyMs:      windowAvg(w.proposeLatenciesMs),
		P50ProposeLatencyMs:   windowPercentile(w.proposeLatenciesMs, 50),
		P90ProposeLatencyMs:   windowPercentile(w.proposeLatenciesMs, 90),
		P95ProposeLatencyMs:   windowPercentile(w.proposeLatenciesMs, 95),
		PrevoteLatencyMs:      windowAvg(w.prevoteLatenciesMs),
		P50PrevoteLatencyMs:   windowPercentile(w.prevoteLatenciesMs, 50),
		P90PrevoteLatencyMs:   windowPercentile(w.prevoteLatenciesMs, 90),
		P95PrevoteLatencyMs:   windowPercentile(w.prevoteLatenciesMs, 95),
		PrecommitLatencyMs:    windowAvg(w.precommitLatenciesMs),
		P50PrecommitLatencyMs: windowPercentile(w.precommitLatenciesMs, 50),
		P90PrecommitLatencyMs: windowPercentile(w.precommitLatenciesMs, 90),
		P95PrecommitLatencyMs: windowPercentile(w.precommitLatenciesMs, 95),
		TimeoutMs:             proposeTimeoutMs,
	}
}

// durationMs converts d to float milliseconds without truncating to whole ms.
func durationMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
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
