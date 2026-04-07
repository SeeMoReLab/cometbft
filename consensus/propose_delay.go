package consensus

import (
	"encoding/xml"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/libs/log"
)

const (
	envProposeDelayFailureSpec = "COMETBFT_PROPOSE_DELAY_FAILURE_SPEC"
	envNodeIndex               = "COMETBFT_NODE_INDEX"
	envProposeDelayFaultyNodes = "COMETBFT_PROPOSE_DELAY_FAULTY_NODES"
	envProposeDelayStartUnixMS = "COMETBFT_PROPOSE_DELAY_START_UNIX_MS"
)

type failureSpec struct {
	WarmUpTime string         `xml:"warmUpTime"`
	Phases     []failurePhase `xml:"phases>phase"`
}

type failurePhase struct {
	AtTime     string            `xml:"atTime"`
	Time       string            `xml:"time"` // legacy fallback
	Tendermint failureTendermint `xml:"tendermint"`
}

type failureTendermint struct {
	ProposalDelay *failureProposalDelay `xml:"proposalDelay"`
}

type failureProposalDelay struct {
	DelayMs string `xml:"delayMs"`
}

type nodeDelayUpdate struct {
	at    time.Duration
	delay time.Duration
}

type proposeDelayController struct {
	delayNS atomic.Int64
	logger  log.Logger
	mtx     sync.RWMutex
}

func newProposeDelayControllerFromEnv(logger log.Logger) *proposeDelayController {
	specFile := strings.TrimSpace(os.Getenv(envProposeDelayFailureSpec))
	if specFile == "" {
		return nil
	}

	nodeIndex, ok := parseRequiredNonNegativeIntEnv(envNodeIndex, logger)
	if !ok {
		return nil
	}

	faultyNodes, ok := parseRequiredNonNegativeIntEnv(envProposeDelayFaultyNodes, logger)
	if !ok {
		return nil
	}
	if faultyNodes == 0 {
		if logger != nil {
			logger.Info("propose-delay: disabled (faulty nodes = 0)")
		}
		return nil
	}
	if nodeIndex >= faultyNodes {
		if logger != nil {
			logger.Info("propose-delay: disabled for non-faulty node", "node", nodeIndex, "faulty_nodes", faultyNodes)
		}
		return nil
	}

	startUnixMS := int64(0)
	if v := strings.TrimSpace(os.Getenv(envProposeDelayStartUnixMS)); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed < 0 {
			if logger != nil {
				logger.Error("propose-delay: invalid start unix ms", "env", envProposeDelayStartUnixMS, "value", v, "err", err)
			}
			return nil
		}
		startUnixMS = parsed
	}

	updates, err := loadNodeDelayScheduleFromFailureSpec(specFile)
	if err != nil {
		if logger != nil {
			logger.Error("propose-delay: failed to load failure spec", "file", specFile, "err", err)
		}
		return nil
	}
	if len(updates) == 0 {
		if logger != nil {
			logger.Info("propose-delay: no phase schedule entries found", "file", specFile)
		}
		return nil
	}

	c := &proposeDelayController{logger: logger}
	c.delayNS.Store(0)

	start := time.Now()
	if startUnixMS > 0 {
		start = time.Unix(0, startUnixMS*int64(time.Millisecond))
	}

	for _, update := range updates {
		u := update
		go func() {
			wait := time.Until(start.Add(u.at))
			if wait > 0 {
				time.Sleep(wait)
			}
			c.delayNS.Store(u.delay.Nanoseconds())
			c.logInfo("propose-delay: updated delay", "at", u.at.String(), "delay", u.delay.String())
		}()
	}

	c.logInfo(
		"propose-delay: enabled",
		"node", nodeIndex,
		"faulty_nodes", faultyNodes,
		"entries", len(updates),
		"file", specFile,
		"start_unix_ms", startUnixMS,
	)
	return c
}

func parseRequiredNonNegativeIntEnv(name string, logger log.Logger) (int, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		if logger != nil {
			logger.Error("propose-delay: missing required env", "env", name)
		}
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		if logger != nil {
			logger.Error("propose-delay: invalid env", "env", name, "value", value, "err", err)
		}
		return 0, false
	}
	return parsed, true
}

func loadNodeDelayScheduleFromFailureSpec(specFile string) ([]nodeDelayUpdate, error) {
	data, err := os.ReadFile(specFile)
	if err != nil {
		return nil, fmt.Errorf("reading failure spec: %w", err)
	}

	var spec failureSpec
	if err := xml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parsing failure spec xml: %w", err)
	}

	warmUp, err := parseSecondsDuration(spec.WarmUpTime)
	if err != nil {
		return nil, fmt.Errorf("invalid warmUpTime %q: %w", spec.WarmUpTime, err)
	}

	updates := make([]nodeDelayUpdate, 0, len(spec.Phases))
	for _, phase := range spec.Phases {
		atText := strings.TrimSpace(phase.AtTime)
		if atText == "" {
			atText = strings.TrimSpace(phase.Time)
		}
		at, err := parseSecondsDuration(atText)
		if err != nil {
			return nil, fmt.Errorf("invalid phase atTime/time %q: %w", atText, err)
		}

		delay := time.Duration(0)
		if phase.Tendermint.ProposalDelay != nil {
			delayText := strings.TrimSpace(phase.Tendermint.ProposalDelay.DelayMs)
			if delayText != "" {
				delay, err = parseMillisecondsDuration(delayText)
				if err != nil {
					return nil, fmt.Errorf("invalid tendermint.proposalDelay.delayMs %q: %w", delayText, err)
				}
			}
		}

		fireAfter := warmUp + at
		if fireAfter < 0 {
			fireAfter = 0
		}
		updates = append(updates, nodeDelayUpdate{at: fireAfter, delay: delay})
	}

	sort.Slice(updates, func(i, j int) bool { return updates[i].at < updates[j].at })
	return dedupeByAt(updates), nil
}

func dedupeByAt(in []nodeDelayUpdate) []nodeDelayUpdate {
	if len(in) <= 1 {
		return in
	}
	out := make([]nodeDelayUpdate, 0, len(in))
	for _, u := range in {
		if len(out) > 0 && out[len(out)-1].at == u.at {
			out[len(out)-1] = u
			continue
		}
		out = append(out, u)
	}
	return out
}

func parseSecondsDuration(text string) (time.Duration, error) {
	value := strings.TrimSpace(text)
	if value == "" {
		return 0, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	if seconds < 0 {
		seconds = 0
	}
	return time.Duration(math.Round(seconds * float64(time.Second))), nil
}

func parseMillisecondsDuration(text string) (time.Duration, error) {
	value := strings.TrimSpace(text)
	if value == "" {
		return 0, nil
	}
	milliseconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	if milliseconds < 0 {
		milliseconds = 0
	}
	return time.Duration(math.Round(milliseconds * float64(time.Millisecond))), nil
}

func (c *proposeDelayController) currentDelay() time.Duration {
	if c == nil {
		return 0
	}
	ns := c.delayNS.Load()
	if ns <= 0 {
		return 0
	}
	return time.Duration(ns)
}

func (c *proposeDelayController) setLogger(logger log.Logger) {
	if c == nil {
		return
	}
	c.mtx.Lock()
	c.logger = logger
	c.mtx.Unlock()
}

func (c *proposeDelayController) logInfo(msg string, keyvals ...interface{}) {
	if c == nil {
		return
	}
	c.mtx.RLock()
	l := c.logger
	c.mtx.RUnlock()
	if l != nil {
		l.Info(msg, keyvals...)
	}
}
