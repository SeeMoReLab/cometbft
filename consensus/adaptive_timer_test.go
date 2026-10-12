package consensus

import (
	"context"
	"testing"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	adaptivetimers "github.com/cometbft/cometbft/proto/adaptive_timers"
)

func TestApplyDoesNotStartRewardWindow(t *testing.T) {
	previousStart := time.Unix(100, 0)
	tracker := &EpochTracker{
		logger:          log.NewNopLogger(),
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
		logger:  log.NewNopLogger(),
		episode: 1,
		windowB: windowData{heightCount: 20, totalTxs: 40, windowStart: time.Unix(100, 0)},
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
// skipped because it is only reached with a non-nil client. Episode 1 applies now.
func newWallClockTestTracker() *EpochTracker {
	now := time.Now()
	return &EpochTracker{
		logger:          log.NewNopLogger(),
		anchor:          now,
		anchorUnixMS:    now.UnixMilli(),
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
		{wallClockIdle, 0, 0, "heights before the joined episode starts are dropped"},
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

func recommendation(proposeMs uint32) *adaptivetimers.TendermintTimeout {
	return &adaptivetimers.TendermintTimeout{
		ProposeTimeoutMilliseconds:   proposeMs,
		PrevoteTimeoutMilliseconds:   100,
		PrecommitTimeoutMilliseconds: 100,
	}
}

func TestFirstEpisodeJoinsTheGrid(t *testing.T) {
	tracker := newWallClockTestTracker()
	start1 := tracker.anchor.Add(-tracker.applyOffset())
	period := tracker.period()

	cases := []struct {
		now  time.Time
		want uint32
	}{
		{start1.Add(-time.Minute), 1},
		{start1, 1},
		{start1.Add(time.Nanosecond), 2},
		{start1.Add(2 * period), 3},
		{start1.Add(2*period + time.Millisecond), 4},
	}
	for _, tc := range cases {
		if got := tracker.firstEpisodeFrom(tc.now); got != tc.want {
			t.Fatalf("first episode at start1%+v = %d, want %d", tc.now.Sub(start1), got, tc.want)
		}
	}
	if !tracker.gridApplyAt(3).Equal(tracker.anchor.Add(2 * period)) {
		t.Fatalf("episode 3 applies at %s, want anchor + 2 periods", tracker.gridApplyAt(3))
	}
}

func TestOverrunSkipIdlesUntilTheNextEpisodeOpens(t *testing.T) {
	tracker := newWallClockTestTracker()
	tracker.episode = 2
	tracker.stage = wallClockFeature

	if !tracker.skipOverrunEpisodes(2, 5, time.Now()) {
		t.Fatal("skip refused the current episode")
	}
	if tracker.episode != 5 || tracker.stage != wallClockIdle {
		t.Fatalf("after skip: episode %d stage %s, want 5 idle", tracker.episode, tracker.stage)
	}

	start := tracker.gridEpisodeStart(5)
	if !tracker.onWallClockEpisodeStart(5, start) {
		t.Fatal("episode start handler refused the joined episode")
	}
	if tracker.stage != wallClockFeature || !tracker.windowA.windowStart.Equal(start) {
		t.Fatalf("episode 5 opened at stage %s window %s, want feature at %s",
			tracker.stage, tracker.windowA.windowStart, start)
	}
	if !tracker.stageDeadline.Equal(start.Add(tracker.featureDuration)) {
		t.Fatalf("feature deadline = %s, want %s", tracker.stageDeadline, start.Add(tracker.featureDuration))
	}
}

// A target whose apply instant is still ahead is applied on the grid, not early:
// the node waits in reply-wait with the recommendation queued for the apply deadline.
func TestFastForwardBeforeApplyWaitsForTheGrid(t *testing.T) {
	tracker := newWallClockTestTracker()
	tracker.episode = 3
	tracker.stage = wallClockFeature
	tracker.stageDeadline = time.Now().Add(tracker.featureDuration)
	defer tracker.Close()

	tracker.fastForward(7, 3, recommendation(999))

	tracker.mu.Lock()
	episode, stage, deadline := tracker.episode, tracker.stage, tracker.stageDeadline
	propose := tracker.currentTimeout.ProposeTimeoutMilliseconds
	tracker.mu.Unlock()
	if episode != 7 || stage != wallClockReplyWait {
		t.Fatalf("after fast-forward: episode %d stage %s, want 7 reply_wait", episode, stage)
	}
	applyAt := tracker.gridApplyAt(7)
	if !deadline.Equal(applyAt) {
		t.Fatalf("reply-wait deadline = %s, want the grid's %s", deadline, applyAt)
	}
	if tracker.wallClockCancel == nil {
		t.Fatal("fast-forward did not restart the wall-clock driver")
	}
	if propose != 150 {
		t.Fatalf("timeout was applied before the grid apply instant: propose_ms = %d", propose)
	}

	if !tracker.onWallClockApplyDeadline(7, applyAt) {
		t.Fatal("apply deadline handler refused the fast-forwarded episode")
	}
	if tracker.currentTimeout.ProposeTimeoutMilliseconds != 999 {
		t.Fatalf("queued timeout was not applied: propose_ms = %d", tracker.currentTimeout.ProposeTimeoutMilliseconds)
	}
	if tracker.stage != wallClockWarmup || !tracker.stageDeadline.Equal(applyAt.Add(tracker.warmupDuration)) {
		t.Fatalf("after apply: stage %s deadline %s, want warmup until %s",
			tracker.stage, tracker.stageDeadline, applyAt.Add(tracker.warmupDuration))
	}
}

// A target that has applied but whose reward window has not started is applied
// at once, and the node resumes in the target's warm-up on the grid.
func TestFastForwardDuringWarmupResumesAtWarmup(t *testing.T) {
	tracker := newWallClockTestTracker()
	// Episode 7 applied 1 s ago, so its reward window opens in 1 s.
	tracker.anchor = time.Now().Add(-6*tracker.period() - time.Second)
	tracker.episode = 3
	tracker.stage = wallClockFeature
	defer tracker.Close()

	tracker.fastForward(7, 3, recommendation(999))

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.episode != 7 || tracker.stage != wallClockWarmup {
		t.Fatalf("after fast-forward: episode %d stage %s, want 7 warmup", tracker.episode, tracker.stage)
	}
	wantDeadline := tracker.gridApplyAt(7).Add(tracker.warmupDuration)
	if !tracker.stageDeadline.Equal(wantDeadline) {
		t.Fatalf("warm-up deadline = %s, want the grid's %s", tracker.stageDeadline, wantDeadline)
	}
	if tracker.currentTimeout.ProposeTimeoutMilliseconds != 999 {
		t.Fatalf("timeout was not applied: propose_ms = %d", tracker.currentTimeout.ProposeTimeoutMilliseconds)
	}
}

// A target whose reward window has already started is skipped: the node idles
// until the next episode that has not started, on the same grid.
func TestFastForwardPastRewardStartJoinsNextEpisode(t *testing.T) {
	tracker := newWallClockTestTracker()
	// Episode 7 applied 5 s ago, so its reward window opened 3 s ago and episode 8
	// starts in 5 s.
	tracker.anchor = time.Now().Add(-6*tracker.period() - 5*time.Second)
	tracker.episode = 3
	tracker.stage = wallClockFeature
	tracker.pendingReward = &pendingReward{episode: 2}
	defer tracker.Close()

	tracker.fastForward(7, 3, recommendation(999))

	if tracker.episode != 8 {
		t.Fatalf("episode after fast-forward = %d, want 8", tracker.episode)
	}
	if tracker.stage != wallClockIdle {
		t.Fatalf("stage after fast-forward = %s, want idle", tracker.stage)
	}
	if tracker.pendingReward == nil || tracker.pendingReward.episode != 2 {
		t.Fatal("the completed episode's reward was dropped")
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
	if tracker.stage != wallClockReplyWait {
		t.Fatalf("abandoned driver mutated stage to %s", tracker.stage)
	}
}

// A driver that joins from idle more than the join slack after the episode's start
// skips it, since the commits it dropped while idle belong to the feature window.
func TestLateJoinSkipsToTheNextEpisode(t *testing.T) {
	tracker := newWallClockTestTracker()
	// Episode 1 started 1 s ago.
	tracker.anchor = time.Now().Add(tracker.applyOffset() - time.Second)
	tracker.episode = 1
	tracker.stage = wallClockIdle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tracker.runWallClockEpisodes(ctx, 1, wallClockIdle)

	deadline := time.Now().Add(2 * time.Second)
	for {
		tracker.mu.Lock()
		episode, stage := tracker.episode, tracker.stage
		tracker.mu.Unlock()
		if episode == 2 && stage == wallClockIdle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("late join left episode %d stage %s, want 2 idle", episode, stage)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFastForwardIgnoresStaleTarget(t *testing.T) {
	tracker := newWallClockTestTracker()
	tracker.episode = 9
	tracker.stage = wallClockFeature

	tracker.fastForward(7, 3, recommendation(999))

	if tracker.episode != 9 {
		t.Fatalf("stale fast-forward moved episode to %d, want 9", tracker.episode)
	}
	if tracker.stage != wallClockFeature {
		t.Fatalf("stale fast-forward moved stage to %s, want feature", tracker.stage)
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

	report := buildReport(w, start.Add(8*time.Second), 300)

	if report.ThroughputTps != 100 {
		t.Fatalf("throughput = %f, want 100", report.ThroughputTps)
	}
	if report.TimeoutViolationRate != 0.25 {
		t.Fatalf("violation rate = %f, want 0.25", report.TimeoutViolationRate)
	}
}

func TestReportSeparatesLeaderAndRegencyChanges(t *testing.T) {
	start := time.Unix(100, 0)
	w := windowData{windowStart: start}
	for i, round := range []int32{0, 1, 0, 3} {
		accumulateHeight(&w, heightSample{height: int64(i + 1), txCount: 1, commitRound: round})
	}

	report := buildReport(&w, start.Add(time.Second), 300)
	if report.LeaderChangeCount != 2 {
		t.Fatalf("leader changes = %d, want 2 heights with a replaced proposer", report.LeaderChangeCount)
	}
	if report.RegencyChangeCount != 4 {
		t.Fatalf("regency changes = %d, want 4 round changes", report.RegencyChangeCount)
	}
}

func TestReportGapsPhasesAndTimeout(t *testing.T) {
	start := time.Unix(100, 0)
	w := windowData{windowStart: start}
	// Commits 0, 10, 30 and 60 ms into the window give the three gaps 10, 20 and 30.
	// The first height has a sub-millisecond propose phase and a zero-length prevote
	// phase, both of which are kept; the last has no phase stamps at all.
	samples := []heightSample{
		{committedAt: start, txCount: 5, latencyMs: 40, proposeLatencyMs: 0.25, prevoteLatencyMs: 0,
			precommitLatencyMs: 2, hasPropose: true, hasPrevote: true, hasPrecommit: true},
		{committedAt: start.Add(10 * time.Millisecond), txCount: 0, latencyMs: 50, proposeLatencyMs: 4,
			prevoteLatencyMs: 1, precommitLatencyMs: 3, hasPropose: true, hasPrevote: true, hasPrecommit: true},
		{committedAt: start.Add(30 * time.Millisecond), txCount: 7, latencyMs: 60, proposeLatencyMs: 8,
			prevoteLatencyMs: 2, precommitLatencyMs: 4, hasPropose: true, hasPrevote: true, hasPrecommit: true},
		{committedAt: start.Add(60 * time.Millisecond), txCount: 9, latencyMs: 70},
	}
	for i, s := range samples {
		s.height = int64(i + 1)
		accumulateHeight(&w, s)
	}

	report := buildReport(&w, start.Add(time.Second), 300)
	checks := []struct {
		name      string
		got, want float32
	}{
		{"p95 latency", report.P95ConsensusLatencyMs, 70},
		{"avg gap", report.AvgInterCommitGapMs, 20},
		{"p50 gap", report.P50InterCommitGapMs, 20},
		{"p90 gap", report.P90InterCommitGapMs, 30},
		{"p95 gap", report.P95InterCommitGapMs, 30},
		{"p50 batch", report.P50BatchSize, 7},
		{"p95 batch", report.P95BatchSize, 9},
		{"p50 propose", report.P50ProposeLatencyMs, 4},
		{"p95 propose", report.P95ProposeLatencyMs, 8},
		{"avg prevote", report.PrevoteLatencyMs, 1},
		{"p50 prevote", report.P50PrevoteLatencyMs, 1},
		{"p90 precommit", report.P90PrecommitLatencyMs, 4},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Fatalf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if got := report.ProposeLatencyMs; got < 4.083 || got > 4.084 {
		t.Fatalf("avg propose = %v, want 12.25/3 with the sub-millisecond sample kept", got)
	}
	if report.TimeoutMs != 300 {
		t.Fatalf("timeout_ms = %d, want 300", report.TimeoutMs)
	}

	// A new window starts its gaps afresh: its first commit has no predecessor.
	next := windowData{windowStart: start.Add(time.Second)}
	accumulateHeight(&next, heightSample{height: 5, committedAt: start.Add(2 * time.Second), txCount: 1})
	if len(next.interCommitGapsMs) != 0 {
		t.Fatalf("first commit of a window produced gaps %v", next.interCommitGapsMs)
	}
}

func TestApplyRecordsTheProposeTimeoutInForce(t *testing.T) {
	tracker := &EpochTracker{
		logger:           log.NewNopLogger(),
		client:           adaptivetimers.NewLearningAgentClient(nil),
		currentTimeout:   recommendation(450),
		proposeInForceMs: 300,
	}
	config := cfg.DefaultConsensusConfig()

	tracker.ApplyPendingTimeout(config)

	if tracker.proposeInForceMs != 450 {
		t.Fatalf("propose in force = %d, want 450", tracker.proposeInForceMs)
	}
}
