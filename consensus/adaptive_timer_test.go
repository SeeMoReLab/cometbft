package consensus

import (
	"testing"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	adaptivetimers "github.com/cometbft/cometbft/proto/adaptive_timers"
)

func TestLearningEpochBoundaries(t *testing.T) {
	boundaries := newLearningEpochBoundaries(50)
	if boundaries.reportAt != 50 {
		t.Fatalf("reportAt = %d, want 50", boundaries.reportAt)
	}
	if boundaries.applyAt != 55 {
		t.Fatalf("applyAt = %d, want 55", boundaries.applyAt)
	}
	if boundaries.rewardStartAt != 60 {
		t.Fatalf("rewardStartAt = %d, want 60", boundaries.rewardStartAt)
	}
	if boundaries.endAt != 110 {
		t.Fatalf("endAt = %d, want 110", boundaries.endAt)
	}
	if featureLength := boundaries.reportAt; featureLength != 50 {
		t.Fatalf("feature length = %d, want 50", featureLength)
	}
	if rewardLength := boundaries.endAt - boundaries.rewardStartAt; rewardLength != 50 {
		t.Fatalf("reward length = %d, want 50", rewardLength)
	}
}

func TestApplyDoesNotStartRewardWindow(t *testing.T) {
	previousStart := time.Unix(100, 0)
	tracker := &EpochTracker{
		logger:          log.NewNopLogger(),
		windowMode:      cfg.AdaptiveTimerWindowModeConsensus,
		episode:         1,
		windowB:         windowData{heightCount: 10, totalTxs: 20, windowStart: previousStart},
		timeoutResultCh: make(chan *adaptivetimers.TendermintTimeout, 1),
		currentTimeout: &adaptivetimers.TendermintTimeout{
			ProposeTimeoutMilliseconds:   150,
			PrevoteTimeoutMilliseconds:   50,
			PrecommitTimeoutMilliseconds: 50,
		},
	}
	tracker.timeoutResultCh <- &adaptivetimers.TendermintTimeout{
		ProposeTimeoutMilliseconds:   300,
		PrevoteTimeoutMilliseconds:   100,
		PrecommitTimeoutMilliseconds: 100,
	}

	tracker.stopPollingAndApplyLocked()

	if tracker.currentTimeout.ProposeTimeoutMilliseconds != 300 {
		t.Fatalf("applied propose timeout = %d, want 300", tracker.currentTimeout.ProposeTimeoutMilliseconds)
	}
	if tracker.windowB.heightCount != 10 || tracker.windowB.totalTxs != 20 {
		t.Fatalf("apply reset reward metrics: heights=%d txs=%d", tracker.windowB.heightCount, tracker.windowB.totalTxs)
	}
	if !tracker.windowB.windowStart.Equal(previousStart) {
		t.Fatalf("apply changed reward window start: got %s want %s", tracker.windowB.windowStart, previousStart)
	}
}

func TestRewardWindowStartsAfterWarmup(t *testing.T) {
	tracker := &EpochTracker{
		logger:            log.NewNopLogger(),
		windowMode:        cfg.AdaptiveTimerWindowModeConsensus,
		episode:           1,
		consensusCounterA: 50,
		consensusCounterB: 10,
		windowB:           windowData{heightCount: 20, totalTxs: 40, windowStart: time.Unix(100, 0)},
		currentTimeout: &adaptivetimers.TendermintTimeout{
			ProposeTimeoutMilliseconds:   300,
			PrevoteTimeoutMilliseconds:   100,
			PrecommitTimeoutMilliseconds: 100,
		},
	}

	start := time.Unix(500, 0)
	tracker.startRewardWindowLocked(start)

	if tracker.windowB.heightCount != 0 || tracker.windowB.totalTxs != 0 {
		t.Fatalf("reward window was not reset: heights=%d txs=%d", tracker.windowB.heightCount, tracker.windowB.totalTxs)
	}
	if !tracker.windowB.windowStart.Equal(start) {
		t.Fatalf("reward window start = %s, want %s", tracker.windowB.windowStart, start)
	}
}

// newWallClockTestTracker builds a tracker in wall-clock mode with no gRPC client,
// so the stage machine can be stepped by hand without a live agent. sendReport is
// skipped because it is only reached with a non-nil client.
func newWallClockTestTracker() *EpochTracker {
	return &EpochTracker{
		logger:          log.NewNopLogger(),
		windowMode:      cfg.AdaptiveTimerWindowModeWallClock,
		featureDuration: 8 * time.Second,
		replyWait:       2 * time.Second,
		warmupDuration:  2 * time.Second,
		rewardDuration:  8 * time.Second,
		proposeTime:     make(map[int64]time.Time),
		prevoteTime:     make(map[int64]time.Time),
		precommitTime:   make(map[int64]time.Time),
		timeoutResultCh: make(chan *adaptivetimers.TendermintTimeout, 1),
		currentTimeout: &adaptivetimers.TendermintTimeout{
			ProposeTimeoutMilliseconds:   150,
			PrevoteTimeoutMilliseconds:   50,
			PrecommitTimeoutMilliseconds: 50,
		},
	}
}

func TestWallClockStagesAdvanceOnDeadlines(t *testing.T) {
	tracker := newWallClockTestTracker()
	start := time.Unix(1000, 0)
	tracker.stage = wallClockFeature
	tracker.stageDeadline = start.Add(tracker.featureDuration)
	tracker.windowA = windowData{windowStart: start}

	featureEnd := start.Add(tracker.featureDuration)
	// With a nil client the report send is skipped, but the stage transition and
	// window bookkeeping are the same.
	if ok := tracker.onWallClockFeatureDeadline(0, featureEnd); !ok {
		t.Fatal("feature deadline handler returned false")
	}
	if tracker.stage != wallClockReplyWait {
		t.Fatalf("stage after feature deadline = %s, want reply_wait", tracker.stage)
	}

	applyAt := featureEnd.Add(tracker.replyWait)
	if ok := tracker.onWallClockApplyDeadline(0, applyAt); !ok {
		t.Fatal("apply deadline handler returned false")
	}
	if tracker.stage != wallClockWarmup {
		t.Fatalf("stage after apply deadline = %s, want warmup", tracker.stage)
	}

	rewardStart := applyAt.Add(tracker.warmupDuration)
	if ok := tracker.onWallClockRewardStart(0, rewardStart); !ok {
		t.Fatal("reward start handler returned false")
	}
	if tracker.stage != wallClockReward {
		t.Fatalf("stage after reward start = %s, want reward", tracker.stage)
	}
	if !tracker.windowB.windowStart.Equal(rewardStart) {
		t.Fatalf("reward window start = %s, want %s", tracker.windowB.windowStart, rewardStart)
	}
	if !tracker.stageDeadline.Equal(rewardStart.Add(tracker.rewardDuration)) {
		t.Fatalf("reward deadline = %s, want %s", tracker.stageDeadline, rewardStart.Add(tracker.rewardDuration))
	}

	rewardEnd := rewardStart.Add(tracker.rewardDuration)
	if ok := tracker.onWallClockRewardDeadline(0, rewardEnd); !ok {
		t.Fatal("reward deadline handler returned false")
	}
	if tracker.stage != wallClockFeature {
		t.Fatalf("stage after reward deadline = %s, want feature", tracker.stage)
	}
	if tracker.episode != 1 {
		t.Fatalf("episode after reward deadline = %d, want 1", tracker.episode)
	}
	if tracker.pendingReward == nil {
		t.Fatal("reward deadline did not capture a pending reward")
	}
	if !tracker.windowA.windowStart.Equal(rewardEnd) {
		t.Fatalf("next feature window start = %s, want %s", tracker.windowA.windowStart, rewardEnd)
	}
	if !tracker.stageDeadline.Equal(rewardEnd.Add(tracker.featureDuration)) {
		t.Fatalf("next feature deadline = %s, want %s", tracker.stageDeadline, rewardEnd.Add(tracker.featureDuration))
	}
}

func TestWallClockHandlerStopsOnStaleEpisode(t *testing.T) {
	tracker := newWallClockTestTracker()
	tracker.episode = 3
	tracker.stage = wallClockFeature

	if tracker.onWallClockFeatureDeadline(2, time.Unix(1000, 0)) {
		t.Fatal("feature deadline handler ran for a stale episode")
	}
	if tracker.stage != wallClockFeature {
		t.Fatalf("stale handler mutated stage to %s", tracker.stage)
	}
}

func TestWallClockAccumulatesOnlyInFeatureAndReward(t *testing.T) {
	start := time.Unix(1000, 0)
	sample := heightSample{height: 7, txCount: 5}

	cases := []struct {
		stage   wallClockStage
		wantA   uint32
		wantB   uint32
		comment string
	}{
		{wallClockFeature, 1, 0, "feature heights land in the report window"},
		{wallClockReplyWait, 0, 0, "reply-wait heights are dropped"},
		{wallClockWarmup, 0, 0, "warm-up heights are dropped"},
		{wallClockReward, 0, 1, "reward heights land in the reward window"},
	}

	for _, tc := range cases {
		tracker := newWallClockTestTracker()
		tracker.stage = tc.stage
		tracker.stageDeadline = start.Add(time.Second)
		tracker.accumulateWallClockLocked(start, sample)

		if tracker.windowA.heightCount != tc.wantA || tracker.windowB.heightCount != tc.wantB {
			t.Fatalf("%s: stage %s gave windowA=%d windowB=%d, want %d and %d",
				tc.comment, tc.stage, tracker.windowA.heightCount, tracker.windowB.heightCount, tc.wantA, tc.wantB)
		}
	}
}

func TestWallClockDropsHeightsPastTheStageDeadline(t *testing.T) {
	start := time.Unix(1000, 0)
	tracker := newWallClockTestTracker()
	tracker.stage = wallClockFeature
	tracker.stageDeadline = start

	tracker.accumulateWallClockLocked(start.Add(time.Millisecond), heightSample{height: 7, txCount: 5})

	if tracker.windowA.heightCount != 0 {
		t.Fatalf("height committed past the deadline was counted: heights=%d", tracker.windowA.heightCount)
	}
}

func newConsensusTestTracker(epochSize int64) *EpochTracker {
	return &EpochTracker{
		logger:          log.NewNopLogger(),
		windowMode:      cfg.AdaptiveTimerWindowModeConsensus,
		epochSize:       epochSize,
		proposeTime:     make(map[int64]time.Time),
		prevoteTime:     make(map[int64]time.Time),
		precommitTime:   make(map[int64]time.Time),
		timeoutResultCh: make(chan *adaptivetimers.TendermintTimeout, 1),
		currentTimeout: &adaptivetimers.TendermintTimeout{
			ProposeTimeoutMilliseconds:   150,
			PrevoteTimeoutMilliseconds:   50,
			PrecommitTimeoutMilliseconds: 50,
		},
	}
}

func recommendation(proposeMs uint32) *adaptivetimers.TendermintTimeout {
	return &adaptivetimers.TendermintTimeout{
		ProposeTimeoutMilliseconds:   proposeMs,
		PrevoteTimeoutMilliseconds:   100,
		PrecommitTimeoutMilliseconds: 100,
	}
}

// A fast-forwarded consensus-mode episode must still produce a full-length reward
// window, otherwise the reward the agent trains on is shorter than every other one.
func TestFastForwardConsensusKeepsFullRewardWindow(t *testing.T) {
	const epochSize = 20
	tracker := newConsensusTestTracker(epochSize)
	tracker.episode = 3

	tracker.fastForward(7, 3, recommendation(999))

	if tracker.episode != 7 {
		t.Fatalf("episode after fast-forward = %d, want 7", tracker.episode)
	}
	if tracker.currentTimeout.ProposeTimeoutMilliseconds != 999 {
		t.Fatalf("timeout was not applied: propose_ms = %d", tracker.currentTimeout.ProposeTimeoutMilliseconds)
	}

	boundaries := newLearningEpochBoundaries(epochSize)
	if pos := tracker.consensusCounterA + tracker.consensusCounterB; pos != boundaries.applyAt {
		t.Fatalf("epoch position after fast-forward = %d, want applyAt %d", pos, boundaries.applyAt)
	}

	// Warm-up runs to rewardStartAt, then a full epochSize of reward heights.
	remaining := boundaries.endAt - boundaries.applyAt
	for h := int64(1); h <= remaining; h++ {
		tracker.accumulateConsensusLocked(heightSample{height: h, txCount: 1})
	}

	if tracker.episode != 8 {
		t.Fatalf("episode after completing the fast-forwarded cycle = %d, want 8", tracker.episode)
	}
	if tracker.pendingReward == nil {
		t.Fatal("fast-forwarded episode produced no reward")
	}
	if tracker.pendingReward.episode != 7 {
		t.Fatalf("reward episode = %d, want 7", tracker.pendingReward.episode)
	}
	if got := tracker.pendingReward.report.TotalConsensusInstances; got != epochSize {
		t.Fatalf("reward window = %d heights, want a full %d", got, epochSize)
	}
	if tracker.pendingReward.timeoutUsed.ProposeTimeoutMilliseconds != 999 {
		t.Fatalf("reward attributed to propose_ms = %d, want the fast-forwarded 999",
			tracker.pendingReward.timeoutUsed.ProposeTimeoutMilliseconds)
	}
}

func TestFastForwardWallClockResumesAtWarmup(t *testing.T) {
	tracker := newWallClockTestTracker()
	tracker.episode = 3
	tracker.stage = wallClockFeature
	tracker.stageDeadline = time.Now().Add(tracker.featureDuration)
	defer tracker.Close()

	before := time.Now()
	tracker.fastForward(7, 3, recommendation(999))

	if tracker.episode != 7 {
		t.Fatalf("episode after fast-forward = %d, want 7", tracker.episode)
	}
	if tracker.stage != wallClockWarmup {
		t.Fatalf("stage after fast-forward = %s, want warmup", tracker.stage)
	}
	if tracker.stageDeadline.Before(before.Add(tracker.warmupDuration)) {
		t.Fatalf("warm-up deadline = %s, want at or after %s",
			tracker.stageDeadline, before.Add(tracker.warmupDuration))
	}
	if tracker.wallClockCancel == nil {
		t.Fatal("fast-forward did not restart the wall-clock driver")
	}
	if tracker.currentTimeout.ProposeTimeoutMilliseconds != 999 {
		t.Fatalf("timeout was not applied: propose_ms = %d", tracker.currentTimeout.ProposeTimeoutMilliseconds)
	}
}

// The driver is bound to one episode number, so the goroutine abandoned by a
// fast-forward has to retire instead of racing the one that replaced it.
func TestFastForwardRetiresTheAbandonedWallClockDriver(t *testing.T) {
	tracker := newWallClockTestTracker()
	tracker.episode = 3
	tracker.stage = wallClockReward
	defer tracker.Close()

	tracker.fastForward(7, 3, recommendation(999))

	// A handler from the abandoned episode 3 driver, arriving late.
	if tracker.onWallClockRewardDeadline(3, time.Now()) {
		t.Fatal("abandoned driver handler ran after fast-forward")
	}
	if tracker.episode != 7 {
		t.Fatalf("abandoned driver mutated episode to %d", tracker.episode)
	}
	if tracker.stage != wallClockWarmup {
		t.Fatalf("abandoned driver mutated stage to %s", tracker.stage)
	}
}

func TestFastForwardIgnoresStaleTarget(t *testing.T) {
	tracker := newConsensusTestTracker(20)
	tracker.episode = 9
	tracker.consensusCounterA = 4

	tracker.fastForward(7, 3, recommendation(999))

	if tracker.episode != 9 {
		t.Fatalf("stale fast-forward moved episode to %d, want 9", tracker.episode)
	}
	if tracker.consensusCounterA != 4 {
		t.Fatalf("stale fast-forward repositioned counters to %d, want 4", tracker.consensusCounterA)
	}
	if tracker.currentTimeout.ProposeTimeoutMilliseconds != 150 {
		t.Fatalf("stale fast-forward applied a timeout: propose_ms = %d",
			tracker.currentTimeout.ProposeTimeoutMilliseconds)
	}
}

func TestBuildReportUsesNominalWindowLength(t *testing.T) {
	start := time.Unix(1000, 0)
	w := &windowData{
		totalTxs:    800,
		heightCount: 4,
		roundsGt0:   1,
		windowStart: start,
	}

	report := buildReport(w, start.Add(8*time.Second))

	if report.ThroughputTps != 100 {
		t.Fatalf("throughput = %f, want 100", report.ThroughputTps)
	}
	if report.TimeoutViolationRate != 0.25 {
		t.Fatalf("violation rate = %f, want 0.25", report.TimeoutViolationRate)
	}
}
