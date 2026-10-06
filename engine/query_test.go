package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/Diudiuseven/streaming-sql-engine-go/event"
	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

func mustState(t *testing.T, stmt string, lateness time.Duration) *queryState {
	t.Helper()
	q, err := sqlparse.Parse(stmt)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s, err := newQueryState(q, lateness)
	if err != nil {
		t.Fatalf("newQueryState: %v", err)
	}
	return s
}

func ev(id string, min int, fields map[string]any) *event.Event {
	return &event.Event{
		ID:        id,
		EventTime: time.Unix(0, int64(min)*int64(time.Minute)).UTC(),
		Fields:    fields,
	}
}

func wm(min int) event.Watermark {
	return event.Watermark{T: time.Unix(0, int64(min)*int64(time.Minute)).UTC()}
}

func rowUpdates(us []Update) []Update {
	var out []Update
	for _, u := range us {
		if u.Kind == UpdateRow {
			out = append(out, u)
		}
	}
	return out
}

func TestWindowTriggerByEventTime(t *testing.T) {
	s := mustState(t,
		`SELECT user, COUNT(*) AS c FROM orders GROUP BY user, TUMBLE(ts, '5m')`, 0)
	if us := s.processEvent(ev("e1", 1, map[string]any{"user": "a"})); len(rowUpdates(us)) != 1 {
		t.Fatalf("expected 1 update, got %v", us)
	}
	// Watermark before window end [0,5): no close.
	if us := s.advanceWatermark(wm(4)); len(us) != 0 {
		t.Fatalf("window closed too early: %v", us)
	}
	// Watermark at 5 closes [0,5).
	us := s.advanceWatermark(wm(5))
	final := rowUpdates(us)
	if len(final) != 1 || !final[0].Final {
		t.Fatalf("expected 1 final update, got %v", us)
	}
	if final[0].Row.Cols["c"] != int64(1) || final[0].Row.Cols["user"] != "a" {
		t.Errorf("row = %v", final[0].Row.Cols)
	}
	// State released.
	if len(s.windows) != 0 {
		t.Errorf("window state not released: %d windows", len(s.windows))
	}
}

func TestOutOfOrderAndLate(t *testing.T) {
	s := mustState(t,
		`SELECT COUNT(*) AS c FROM s GROUP BY TUMBLE(ts, '5m')`, 2*time.Minute)
	s.processEvent(ev("e1", 4, nil))
	s.advanceWatermark(wm(6)) // horizon = 4
	// Event at 4 is at the horizon (not before it): counted.
	us := s.processEvent(ev("e2", 4, nil))
	if len(rowUpdates(us)) != 1 {
		t.Fatalf("in-lateness event not counted: %v", us)
	}
	// Event at 1 is beyond horizon 4: late verdict.
	us = s.processEvent(ev("e3", 1, nil))
	if len(us) != 1 || us[0].Kind != UpdateLateEvent || us[0].EventID != "e3" {
		t.Fatalf("expected late verdict, got %v", us)
	}
	// Window [0,5) closes at watermark 7 (horizon 5): count = 2.
	us = s.advanceWatermark(wm(7))
	final := rowUpdates(us)
	if len(final) != 1 || final[0].Row.Cols["c"] != int64(2) {
		t.Fatalf("final = %v", final)
	}
}

func TestDuplicateDetection(t *testing.T) {
	s := mustState(t, `SELECT COUNT(*) AS c FROM s GROUP BY TUMBLE(ts, '5m')`, 0)
	s.processEvent(ev("e1", 1, nil))
	us := s.processEvent(ev("e1", 1, nil))
	if len(us) != 1 || us[0].Kind != UpdateDuplicateEvent {
		t.Fatalf("expected duplicate verdict, got %v", us)
	}
	us = s.advanceWatermark(wm(5))
	if final := rowUpdates(us); len(final) != 1 || final[0].Row.Cols["c"] != int64(1) {
		t.Fatalf("duplicate was counted: %v", final)
	}
}

func TestIncrementalOnlyOnChange(t *testing.T) {
	s := mustState(t, `SELECT MIN(v) AS m FROM s GROUP BY TUMBLE(ts, '5m')`, 0)
	if us := s.processEvent(ev("e1", 1, map[string]any{"v": int64(5)})); len(us) != 1 {
		t.Fatalf("first event must emit, got %v", us)
	}
	// MIN unchanged by larger value: no update.
	if us := s.processEvent(ev("e2", 1, map[string]any{"v": int64(9)})); len(us) != 0 {
		t.Fatalf("unchanged group emitted: %v", us)
	}
	// Smaller value changes MIN: update.
	if us := s.processEvent(ev("e3", 1, map[string]any{"v": int64(2)})); len(us) != 1 {
		t.Fatalf("changed group did not emit")
	}
}

func TestHopWindows(t *testing.T) {
	s := mustState(t, `SELECT COUNT(*) AS c FROM s GROUP BY HOP(ts, '10m', '5m')`, 0)
	us := s.processEvent(ev("e1", 7, nil))
	if len(rowUpdates(us)) != 2 {
		t.Fatalf("event must land in 2 hop windows, got %v", us)
	}
	s.advanceWatermark(wm(10)) // closes [0,10)
	s.advanceWatermark(wm(15)) // closes [5,15)
	if len(s.windows) != 0 {
		t.Fatalf("windows not released: %d", len(s.windows))
	}
}

// TestBatchEquivalence feeds the same shuffled event sequence to a
// streaming query and compares final per-group results with a batch
// computation over the same events.
func TestBatchEquivalence(t *testing.T) {
	stmt := `SELECT user, COUNT(*) AS c, SUM(v) AS s FROM d GROUP BY user, TUMBLE(ts, '5m')`
	const lateness = 3 * time.Minute
	// Build events across windows [0,5),[5,10),[10,15) for users a,b.
	var events []*event.Event
	id := 0
	for min := 0; min < 15; min++ {
		for _, u := range []string{"a", "b"} {
			events = append(events, ev(fmt.Sprintf("e%d", id), min,
				map[string]any{"user": u, "v": int64(min + 1)}))
			id++
		}
	}
	// Streaming: feed events window by window, shuffled within each
	// window (out-of-order arrival), advancing the watermark after each
	// window. Also inject an allowed-late event for a previous window.
	stream := mustState(t, stmt, lateness)
	finalRows := map[string]Row{} // key: windowStart|user
	collect := func(us []Update) {
		for _, u := range us {
			if u.Kind == UpdateRow {
				finalRows[fmt.Sprintf("%d|%v", u.WindowStart.Unix(), u.Row.Cols["user"])] = u.Row
			}
		}
	}
	for w := 0; w < 3; w++ {
		// Events of window [5w, 5w+5) in reverse arrival order.
		for i := 4; i >= 0; i-- {
			collect(stream.processEvent(events[(w*5+i)*2]))
			collect(stream.processEvent(events[(w*5+i)*2+1]))
		}
		if w == 1 {
			// Late event for window 0, within allowed lateness
			// (current watermark 5, horizon 2; event at 3 is counted).
			collect(stream.processEvent(ev("late0", 3, map[string]any{"user": "a", "v": int64(100)})))
		}
		collect(stream.advanceWatermark(wm(5 * (w + 1))))
	}
	collect(stream.advanceWatermark(wm(1000)))
	// Batch: one shot, in order, then close everything.
	batch := mustState(t, stmt, lateness)
	batchRows := map[string]Row{}
	for _, e := range events {
		batch.processEvent(e)
	}
	batch.processEvent(ev("late0", 3, map[string]any{"user": "a", "v": int64(100)}))
	for _, u := range batch.advanceWatermark(wm(100)) {
		if u.Kind == UpdateRow {
			batchRows[fmt.Sprintf("%d|%v", u.WindowStart.Unix(), u.Row.Cols["user"])] = u.Row
		}
	}
	if len(finalRows) != len(batchRows) {
		t.Fatalf("streaming %d rows vs batch %d rows", len(finalRows), len(batchRows))
	}
	for k, br := range batchRows {
		sr, ok := finalRows[k]
		if !ok {
			t.Fatalf("missing streaming row %s", k)
		}
		if !rowsEqual(sr, br) {
			t.Errorf("row %s: stream=%v batch=%v", k, sr.Cols, br.Cols)
		}
	}
}

func TestOrderByDeterministic(t *testing.T) {
	stmt := `SELECT user, COUNT(*) AS c FROM s GROUP BY user, TUMBLE(ts, '5m') ORDER BY c DESC, user`
	run := func() []Update {
		s := mustState(t, stmt, 0)
		// a:1, b:2, c:2 — b and c tie on count.
		s.processEvent(ev("1", 1, map[string]any{"user": "c"}))
		s.processEvent(ev("2", 1, map[string]any{"user": "b"}))
		s.processEvent(ev("3", 1, map[string]any{"user": "a"}))
		s.processEvent(ev("4", 2, map[string]any{"user": "c"}))
		s.processEvent(ev("5", 2, map[string]any{"user": "b"}))
		return s.advanceWatermark(wm(5))
	}
	r1, r2 := run(), run()
	if len(r1) != 3 {
		t.Fatalf("got %d rows", len(r1))
	}
	for i := range r1 {
		if !rowsEqual(r1[i].Row, r2[i].Row) {
			t.Fatalf("non-deterministic order at %d", i)
		}
	}
	// c=2 (b,c tie broken by user asc), then a=1.
	got := []string{r1[0].Row.Cols["user"].(string), r1[1].Row.Cols["user"].(string), r1[2].Row.Cols["user"].(string)}
	want := []string{"b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestProjectionAndFilter(t *testing.T) {
	s := mustState(t, `SELECT user, v FROM s WHERE v >= 10`, 0)
	if us := s.processEvent(ev("1", 1, map[string]any{"user": "a", "v": int64(5)})); len(us) != 0 {
		t.Fatalf("filtered event emitted: %v", us)
	}
	us := s.processEvent(ev("2", 1, map[string]any{"user": "a", "v": int64(15)}))
	if len(us) != 1 || us[0].Row.Cols["v"] != int64(15) {
		t.Fatalf("projection = %v", us)
	}
}

func TestSemanticValidation(t *testing.T) {
	cases := []string{
		`SELECT COUNT(*) FROM s`,                                // aggregate without group by
		`SELECT user, COUNT(*) FROM s GROUP BY TUMBLE(ts,'5m')`, // user not in GROUP BY
		`SELECT WINDOW_START() FROM s`,                          // window func without window
		`SELECT * FROM s GROUP BY TUMBLE(ts,'5m')`,              // star with window
		`SELECT user FROM s ORDER BY user`,                      // order by without window
	}
	for _, c := range cases {
		q, err := sqlparse.Parse(c)
		if err != nil {
			t.Errorf("%q: parse error: %v", c, err)
			continue
		}
		if _, err := newQueryState(q, 0); err == nil {
			t.Errorf("%q: expected validation error", c)
		}
	}
}
