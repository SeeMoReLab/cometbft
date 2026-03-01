package consensus

import (
	"encoding/json"
	"fmt"
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
	envProposeDelaySchedule = "COMETBFT_PROPOSE_DELAY_SCHEDULE"
	envNodeIndex            = "COMETBFT_NODE_INDEX"
)

// DelayScheduleEntry defines when proposal delay is changed for specific nodes.
// Schedule file format:
//
//	[
//	  {"at":"10s","nodes":{"0":"4s","1":"2s"}},
//	  {"at":"40s","nodes":{"0":"0s"}}
//	]
type DelayScheduleEntry struct {
	At    string            `json:"at"`
	Nodes map[string]string `json:"nodes"`
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
	scheduleFile := strings.TrimSpace(os.Getenv(envProposeDelaySchedule))
	if scheduleFile == "" {
		return nil
	}

	nodeIndexStr := strings.TrimSpace(os.Getenv(envNodeIndex))
	if nodeIndexStr == "" {
		if logger != nil {
			logger.Error("propose-delay: schedule provided but node index env is missing", "env", envNodeIndex)
		}
		return nil
	}
	nodeIndex, err := strconv.Atoi(nodeIndexStr)
	if err != nil {
		if logger != nil {
			logger.Error("propose-delay: invalid node index", "env", envNodeIndex, "value", nodeIndexStr, "err", err)
		}
		return nil
	}

	updates, err := loadNodeDelaySchedule(scheduleFile, nodeIndex)
	if err != nil {
		if logger != nil {
			logger.Error("propose-delay: failed to load schedule", "file", scheduleFile, "err", err)
		}
		return nil
	}
	if len(updates) == 0 {
		if logger != nil {
			logger.Info("propose-delay: no schedule entries for this node", "node", nodeIndex, "file", scheduleFile)
		}
		return nil
	}

	c := &proposeDelayController{logger: logger}
	c.delayNS.Store(0)
	start := time.Now()
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
	c.logInfo("propose-delay: enabled", "node", nodeIndex, "entries", len(updates), "file", scheduleFile)
	return c
}

func loadNodeDelaySchedule(scheduleFile string, nodeIndex int) ([]nodeDelayUpdate, error) {
	data, err := os.ReadFile(scheduleFile)
	if err != nil {
		return nil, fmt.Errorf("reading schedule file: %w", err)
	}

	var schedule []DelayScheduleEntry
	if err := json.Unmarshal(data, &schedule); err != nil {
		return nil, fmt.Errorf("parsing schedule file: %w", err)
	}

	nodeKey := strconv.Itoa(nodeIndex)
	updates := make([]nodeDelayUpdate, 0)
	for _, entry := range schedule {
		delayStr, ok := entry.Nodes[nodeKey]
		if !ok {
			continue
		}

		fireAfter, err := time.ParseDuration(entry.At)
		if err != nil {
			return nil, fmt.Errorf("parsing at=%q: %w", entry.At, err)
		}
		if fireAfter < 0 {
			return nil, fmt.Errorf("schedule at must be >= 0, got %q", entry.At)
		}

		delay, err := time.ParseDuration(delayStr)
		if err != nil {
			return nil, fmt.Errorf("parsing delay=%q for node %d: %w", delayStr, nodeIndex, err)
		}
		if delay < 0 {
			return nil, fmt.Errorf("delay must be >= 0, got %q", delayStr)
		}

		updates = append(updates, nodeDelayUpdate{at: fireAfter, delay: delay})
	}

	sort.Slice(updates, func(i, j int) bool { return updates[i].at < updates[j].at })
	return updates, nil
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
