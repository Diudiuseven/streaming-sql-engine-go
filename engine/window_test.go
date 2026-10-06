package engine

import (
	"testing"
	"time"
)

func TestTumbleAssign(t *testing.T) {
	ts := time.Unix(0, 7*int64(time.Minute)).UTC()
	wins := assignWindows(kindTumble, 5*time.Minute, 0, ts)
	if len(wins) != 1 {
		t.Fatalf("got %d windows", len(wins))
	}
	w := wins[0]
	if !w.Start.Equal(time.Unix(0, 5*int64(time.Minute)).UTC()) ||
		!w.End.Equal(time.Unix(0, 10*int64(time.Minute)).UTC()) {
		t.Errorf("window = [%v, %v)", w.Start, w.End)
	}
	// Boundary: exactly at window edge belongs to the later window.
	wins = assignWindows(kindTumble, 5*time.Minute, 0, time.Unix(0, 10*int64(time.Minute)).UTC())
	if !wins[0].Start.Equal(time.Unix(0, 10*int64(time.Minute)).UTC()) {
		t.Errorf("boundary window start = %v", wins[0].Start)
	}
}

func TestHopAssign(t *testing.T) {
	// size 10m, slide 5m: t=7m is in windows [0,10) and [5,15).
	ts := time.Unix(0, 7*int64(time.Minute)).UTC()
	wins := assignWindows(kindHop, 10*time.Minute, 5*time.Minute, ts)
	if len(wins) != 2 {
		t.Fatalf("got %d windows: %v", len(wins), wins)
	}
	if !wins[0].Start.Equal(time.Unix(0, 5*int64(time.Minute)).UTC()) ||
		!wins[1].Start.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("windows = %v", wins)
	}
	// t=12m is in [5,15) and [10,20).
	ts = time.Unix(0, 12*int64(time.Minute)).UTC()
	wins = assignWindows(kindHop, 10*time.Minute, 5*time.Minute, ts)
	if len(wins) != 2 || !wins[0].Start.Equal(time.Unix(0, 10*int64(time.Minute)).UTC()) {
		t.Errorf("windows = %v", wins)
	}
}
