package engine

import (
	"fmt"
	"testing"
)

// TestClosedWindowStateReleased simulates a long run over many windows
// and verifies that state does not grow with the number of closed
// windows.
func TestClosedWindowStateReleased(t *testing.T) {
	s := mustState(t, `SELECT user, COUNT(*) AS c FROM d GROUP BY user, TUMBLE(ts, '1m')`, 0)
	const windows = 1000
	for w := 0; w < windows; w++ {
		s.processEvent(ev(fmt.Sprintf("e%d", w), w, map[string]any{"user": "a"}))
		us := s.advanceWatermark(wm(w + 1))
		if len(us) != 1 || !us[0].Final {
			t.Fatalf("window %d did not close: %v", w, us)
		}
		if len(s.windows) > 1 {
			t.Fatalf("state grows with closed windows: %d open at window %d", len(s.windows), w)
		}
		if len(s.seen) > 1 {
			t.Fatalf("dedup set grows unboundedly: %d entries", len(s.seen))
		}
	}
	if len(s.windows) != 0 || len(s.seen) != 0 {
		t.Fatalf("residual state: %d windows, %d seen", len(s.windows), len(s.seen))
	}
}

// TestHopWindowStateReleased verifies hop windows are also released.
func TestHopWindowStateReleased(t *testing.T) {
	s := mustState(t, `SELECT COUNT(*) AS c FROM d GROUP BY HOP(ts, '4m', '1m')`, 0)
	for w := 0; w < 100; w++ {
		s.processEvent(ev(fmt.Sprintf("e%d", w), w, nil))
		s.advanceWatermark(wm(w + 1))
		// At most 4 hop windows (size/slide) can be open at once.
		if len(s.windows) > 4 {
			t.Fatalf("hop windows accumulate: %d open at minute %d", len(s.windows), w)
		}
	}
	s.advanceWatermark(wm(1000))
	if len(s.windows) != 0 {
		t.Fatalf("residual hop windows: %d", len(s.windows))
	}
}
