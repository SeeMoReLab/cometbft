package consensus

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cometbft/cometbft/libs/log"
)

func TestProposeDelayControllerFromEnv(t *testing.T) {
	spec := filepath.Join(t.TempDir(), "failure_spec.xml")
	if err := os.WriteFile(spec, []byte(`<failureSpec><warmUpTime>0</warmUpTime><phases>
<phase><atTime>0</atTime><tendermint><proposalDelay><delayMs>100</delayMs></proposalDelay></tendermint></phase>
</phases></failureSpec>`), 0o600); err != nil {
		t.Fatal(err)
	}
	logger := log.NewNopLogger()

	t.Run("unconfigured", func(t *testing.T) {
		t.Setenv(envProposeDelayFailureSpec, "")
		c, err := newProposeDelayControllerFromEnv(logger)
		if c != nil || err != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", c, err)
		}
	})

	t.Run("missing start time is an error", func(t *testing.T) {
		t.Setenv(envProposeDelayFailureSpec, spec)
		t.Setenv(envNodeIndex, "0")
		t.Setenv(envProposeDelayFaultyNodes, "1")
		t.Setenv(envProposeDelayStartUnixMS, "")
		if _, err := newProposeDelayControllerFromEnv(logger); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("missing node index is an error", func(t *testing.T) {
		t.Setenv(envProposeDelayFailureSpec, spec)
		t.Setenv(envNodeIndex, "")
		t.Setenv(envProposeDelayFaultyNodes, "1")
		if _, err := newProposeDelayControllerFromEnv(logger); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("unreadable spec is an error", func(t *testing.T) {
		t.Setenv(envProposeDelayFailureSpec, filepath.Join(t.TempDir(), "missing.xml"))
		t.Setenv(envNodeIndex, "0")
		t.Setenv(envProposeDelayFaultyNodes, "1")
		if _, err := newProposeDelayControllerFromEnv(logger); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("non-faulty node is disabled", func(t *testing.T) {
		t.Setenv(envProposeDelayFailureSpec, spec)
		t.Setenv(envNodeIndex, "1")
		t.Setenv(envProposeDelayFaultyNodes, "1")
		c, err := newProposeDelayControllerFromEnv(logger)
		if c != nil || err != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", c, err)
		}
	})

	t.Run("faulty node is enabled", func(t *testing.T) {
		t.Setenv(envProposeDelayFailureSpec, spec)
		t.Setenv(envNodeIndex, "0")
		t.Setenv(envProposeDelayFaultyNodes, "1")
		t.Setenv(envProposeDelayStartUnixMS, "1")
		c, err := newProposeDelayControllerFromEnv(logger)
		if err != nil || c == nil {
			t.Fatalf("got (%v, %v), want a controller", c, err)
		}
	})
}

func TestFailureSpecWarmUpTimeMsTakesPrecedence(t *testing.T) {
	spec := filepath.Join(t.TempDir(), "failure_spec.xml")
	if err := os.WriteFile(spec, []byte(`<failureSpec><warmUpTime>10</warmUpTime><warmUpTimeMs>1500</warmUpTimeMs><phases>
<phase><atTime>2</atTime><tendermint><proposalDelay><delayMs>100</delayMs></proposalDelay></tendermint></phase>
</phases></failureSpec>`), 0o600); err != nil {
		t.Fatal(err)
	}
	updates, err := loadNodeDelayScheduleFromFailureSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].at != 3500*time.Millisecond {
		t.Fatalf("updates = %+v, want one update at 3.5s (1.5s warm-up + 2s)", updates)
	}
}
