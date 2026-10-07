package report_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cometbft/cometbft/test/loadtime/report"
)

func writeStateLog(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node_0.log")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing state log: %v", err)
	}
	return path
}

func TestLoadCommitTimesFromStateLogISO(t *testing.T) {
	path := writeStateLog(t, ""+
		"I[2026-10-06T22:13:53.645Z] committed state                              module=state height=1 block_app_hash=0000000000000000\n"+
		"I[2026-10-06T22:13:53.700Z] finalizing commit of block                   module=consensus height=2\n"+
		"I[2026-10-06T22:13:53.703Z] committed state                              module=state height=2 block_app_hash=0000000000000000\n")

	got, err := report.LoadCommitTimesFromStateLog(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[int64]time.Time{
		1: time.Date(2026, 10, 6, 22, 13, 53, 645_000_000, time.UTC),
		2: time.Date(2026, 10, 6, 22, 13, 53, 703_000_000, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d heights, want %d: %v", len(got), len(want), got)
	}
	for h, w := range want {
		if !got[h].Equal(w) {
			t.Errorf("height %d: got %v, want %v", h, got[h], w)
		}
	}
}

func TestLoadCommitTimesFromStateLogNoMatchesIsError(t *testing.T) {
	path := writeStateLog(t, "I[not-a-timestamp] committed state module=state height=1\n")

	if _, err := report.LoadCommitTimesFromStateLog(path); err == nil {
		t.Fatal("expected an error when no committed state line parses")
	}
}
