package engine

import (
	"testing"
	"time"

	"github.com/Diudiuseven/streaming-sql-engine-go/checkpoint"
)

func TestCheckpointRestore(t *testing.T) {
	dir := t.TempDir()
	store, err := checkpoint.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	stmt := `SELECT user, COUNT(*) AS c, SUM(v) AS s FROM d GROUP BY user, TUMBLE(ts, '5m')`

	// Phase 1: run engine, process some events, snapshot.
	e1 := New(Config{AllowedLateness: time.Minute})
	sub1, err := e1.Subscribe(stmt)
	if err != nil {
		t.Fatal(err)
	}
	pubEv(t, e1, "e1", 1, map[string]any{"user": "a", "v": int64(10)})
	pubEv(t, e1, "e2", 2, map[string]any{"user": "b", "v": int64(20)})
	pubEv(t, e1, "e1", 1, map[string]any{"user": "a", "v": int64(10)}) // duplicate
	pubWm(t, e1, 3)
	drain(sub1, 4, time.Second) // 2 rows + 1 dup verdict + nothing for wm
	if err := e1.Snapshot(store); err != nil {
		t.Fatal(err)
	}
	lastSeq := e1.LastSeq()
	e1.Close()

	// Phase 2: new engine restores; no committed results re-emitted,
	// dedup survives, window closes exactly once.
	e2 := New(Config{AllowedLateness: time.Minute})
	infos, restoredSeq, err := e2.Restore(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Stmt != stmt {
		t.Fatalf("infos = %+v", infos)
	}
	if restoredSeq != lastSeq {
		t.Fatalf("restored seq %d != %d", restoredSeq, lastSeq)
	}
	sub2, err := e2.Resume(infos[0].SubID)
	if err != nil {
		t.Fatal(err)
	}
	// The duplicate of e2 (still within lateness: horizon = wm 3 - 1m)
	// must still be detected after restore.
	pubEv(t, e2, "e2", 2, map[string]any{"user": "b", "v": int64(20)})
	us := drain(sub2, 1, time.Second)
	if len(us) != 1 || us[0].Kind != UpdateDuplicateEvent {
		t.Fatalf("expected duplicate verdict after restore, got %v", us)
	}
	// New event counts; window closes once with correct totals.
	pubEv(t, e2, "e3", 3, map[string]any{"user": "a", "v": int64(5)})
	pubWm(t, e2, 10)
	us = drain(sub2, 3, 2*time.Second) // 1 incremental + 2 finals
	var finals []Update
	for _, u := range us {
		if u.Final {
			finals = append(finals, u)
		}
	}
	if len(finals) != 2 {
		t.Fatalf("finals = %v", us)
	}
	byUser := map[string]Update{}
	for _, f := range finals {
		byUser[f.Row.Cols["user"].(string)] = f
	}
	if byUser["a"].Row.Cols["c"] != int64(2) || byUser["a"].Row.Cols["s"] != int64(15) {
		t.Errorf("user a row = %v", byUser["a"].Row.Cols)
	}
	if byUser["b"].Row.Cols["c"] != int64(1) {
		t.Errorf("user b row = %v", byUser["b"].Row.Cols)
	}
	e2.Close()
}

func TestRestoreContinuityMatchesNoRestart(t *testing.T) {
	dir := t.TempDir()
	store, _ := checkpoint.NewStore(dir)
	stmt := `SELECT user, SUM(v) AS s FROM d GROUP BY user, TUMBLE(ts, '5m')`

	// Engine A: processes events 0..3, snapshots, "crashes".
	ea := New(Config{})
	suba, _ := ea.Subscribe(stmt)
	for i, id := range []string{"a", "b", "c", "d"} {
		pubEv(t, ea, id, i, map[string]any{"user": "u", "v": int64(1)})
	}
	drain(suba, 4, time.Second)
	if err := ea.Snapshot(store); err != nil {
		t.Fatal(err)
	}
	ea.Close()

	// Engine B: restores, processes events 4..7, closes window.
	eb := New(Config{})
	infos, _, err := eb.Restore(store)
	if err != nil {
		t.Fatal(err)
	}
	subb, _ := eb.Resume(infos[0].SubID)
	for i, id := range []string{"e", "f", "g", "h"} {
		pubEv(t, eb, id, i+4, map[string]any{"user": "u", "v": int64(1)})
	}
	pubWm(t, eb, 10)
	usB := drain(subb, 5, 2*time.Second) // 4 incremental + 1 final
	eb.Close()

	// Reference: single engine processes all 8 events.
	ec := New(Config{})
	subc, _ := ec.Subscribe(stmt)
	for i, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		pubEv(t, ec, id, i, map[string]any{"user": "u", "v": int64(1)})
	}
	pubWm(t, ec, 10)
	usC := drain(subc, 9, 2*time.Second)
	ec.Close()

	finalOf := func(us []Update) Update {
		for _, u := range us {
			if u.Final {
				return u
			}
		}
		return Update{}
	}
	fb, fc := finalOf(usB), finalOf(usC)
	// Events a..e fall in window [0,5): sum is 5 in both runs.
	if !rowsEqual(fb.Row, fc.Row) || fb.Row.Cols["s"] != int64(5) {
		t.Fatalf("restored final = %v, reference = %v", fb.Row.Cols, fc.Row.Cols)
	}
}
