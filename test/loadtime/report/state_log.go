package report

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var heightPattern = regexp.MustCompile(`\bheight=(\d+)\b`)

// LoadCommitTimesFromStateLog parses CometBFT node logs and returns commit wall-clock
// timestamps by block height for lines that include "committed state".
func LoadCommitTimesFromStateLog(path string) (map[int64]time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening state log %q: %w", path, err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 1<<20)
	res := make(map[int64]time.Time)

	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			if ts, h, ok := parseCommittedStateLine(line); ok {
				res[h] = ts
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("reading state log %q: %w", path, readErr)
		}
	}

	return res, nil
}

func parseCommittedStateLine(line string) (time.Time, int64, bool) {
	if !strings.Contains(line, "committed state") {
		return time.Time{}, 0, false
	}

	heightMatch := heightPattern.FindStringSubmatch(line)
	if len(heightMatch) != 2 {
		return time.Time{}, 0, false
	}
	height, err := strconv.ParseInt(heightMatch[1], 10, 64)
	if err != nil {
		return time.Time{}, 0, false
	}

	start := strings.IndexByte(line, '[')
	end := strings.IndexByte(line, ']')
	if start < 0 || end <= start+1 {
		return time.Time{}, 0, false
	}
	tsText := strings.TrimSpace(line[start+1 : end])

	ts, ok := parseLogTimestamp(tsText)
	if !ok {
		return time.Time{}, 0, false
	}
	return ts, height, true
}

func parseLogTimestamp(text string) (time.Time, bool) {
	layouts := []string{
		"2006-01-02|15:04:05.000000",
		"2006-01-02|15:04:05.000",
		"2006-01-02|15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, text, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
