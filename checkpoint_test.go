package streamsql

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpointRecoveryWindowAware(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{AllowedLateness: 5 * time.Second, OutputBufferSize: 1 << 20}
	sql := `SELECT window_start, user, COUNT(*) AS c, SUM(amount) AS s FROM events GROUP BY TUMBLE(ts, '10s'), user`

	mk := func(i int, ts int64, user string, amt int64) Event {
		return mkEvent(fmt.Sprintf("e%d", i), ts, map[string]any{"user": user, "amount": amt})
	}
	users := []string{"a", "b", "c"}
	seed := int64(123)
	rnd := func(n int64) int64 {
		seed = (seed*6364136223846793005 + 1442695040888963407) & (1<<63 - 1)
		return seed % n
	}

	var all []Event
	eng := NewEngine(cfg)
	q, _ := eng.Register("q", sql)
	col := newCollector(q)
	ts := int64(0)
	for i := 0; i < 100; i++ {
		ts += rnd(400)
		ev := mk(i, ts, users[rnd(3)], rnd(50))
		all = append(all, ev)
		eng.Submit(ev)
	}
	if err := eng.Checkpoint(dir); err != nil {
		t.Fatal(err)
	}
	col.drain()
	eng.Close()

	eng2, err := RecoverEngine(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	q2 := eng2.queries["q"]
	col2 := newCollector(q2)
	for i := 100; i < 200; i++ {
		ts += rnd(400)
		ev := mk(i, ts, users[rnd(3)], rnd(50))
		all = append(all, ev)
		eng2.Submit(ev)
	}
	// the closer only advances the watermark; its own window stays open
	// (never fired before shutdown), so it is excluded from the reference.
	ts += 100000
	closer := mk(200, ts, "z", 0)
	eng2.Submit(closer)
	eng2.Sync()
	col2.drain()
	eng2.Close()

	want := batchReference(all, 10000)
	got := map[string][]any{}
	for _, r := range append(col.finalRows(), col2.finalRows()...) {
		k := fmt.Sprintf("%v|%v", r.Values[0], r.Values[1])
		if _, dup := got[k]; dup {
			t.Fatalf("duplicate final row for group %s after recovery", k)
		}
		got[k] = []any{r.Values[2], r.Values[3]}
	}
	if len(got) != len(want) {
		t.Fatalf("groups: got %d want %d", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing group %s after recovery", k)
		}
		if g[0] != w[0] || g[1] != w[1] {
			t.Fatalf("group %s: got %v want %v", k, g, w)
		}
	}
}

func TestCheckpointDedupStateSurvives(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{AllowedLateness: 5 * time.Second, OutputBufferSize: 1 << 20}
	sql := `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user`

	eng := NewEngine(cfg)
	eng.Register("q", sql)
	eng.Submit(mkEvent("e1", 1000, map[string]any{"user": "a"}))
	if err := eng.Checkpoint(dir); err != nil {
		t.Fatal(err)
	}
	eng.Close()

	eng2, err := RecoverEngine(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	q2 := eng2.queries["q"]
	col2 := newCollector(q2)
	// replay of an already-committed event must be detected as duplicate
	eng2.Submit(mkEvent("e1", 1000, map[string]any{"user": "a"}))
	eng2.Submit(mkEvent("e2", 20000, map[string]any{"user": "a"}))
	eng2.Sync()
	col2.drain()
	st := q2.Stats()
	if st.Duplicates != 1 {
		t.Fatalf("duplicate not detected after recovery: %+v", st)
	}
	finals := col2.finalRows()
	if len(finals) != 1 || finals[0].Values[1] != int64(1) {
		t.Fatalf("finals: %+v", finals)
	}
	eng2.Close()
}

func TestCheckpointWriteInterrupted(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{AllowedLateness: 5 * time.Second}
	eng := NewEngine(cfg)
	eng.Register("q", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user`)
	eng.Submit(mkEvent("e1", 1000, map[string]any{"user": "a"}))
	if err := eng.Checkpoint(dir); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(filepath.Join(dir, checkpointFile))
	if err != nil {
		t.Fatal(err)
	}
	// second checkpoint
	eng.Submit(mkEvent("e2", 2000, map[string]any{"user": "a"}))
	if err := eng.Checkpoint(dir); err != nil {
		t.Fatal(err)
	}
	eng.Close()

	// simulate a crash mid-write: a leftover temp file must not affect
	// recovery, and a truncated main file must be rejected, not half-read.
	if err := os.WriteFile(filepath.Join(dir, checkpointTmp), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	eng2, err := RecoverEngine(dir, cfg)
	if err != nil {
		t.Fatalf("leftover temp file broke recovery: %v", err)
	}
	eng2.Close()

	// corrupt the main file: recovery must fail with a checkpoint error
	if err := os.WriteFile(filepath.Join(dir, checkpointFile), good[:len(good)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverEngine(dir, cfg); !IsKind(err, ErrKindCheckpoint) {
		t.Fatalf("corrupt checkpoint: want ErrKindCheckpoint, got %v", err)
	}
}

func TestRecoverWithoutCheckpoint(t *testing.T) {
	if HasCheckpoint(t.TempDir()) {
		t.Fatal("HasCheckpoint on empty dir")
	}
	if _, err := RecoverEngine(t.TempDir(), Config{}); !IsKind(err, ErrKindCheckpoint) {
		t.Fatalf("want ErrKindCheckpoint, got %v", err)
	}
}
