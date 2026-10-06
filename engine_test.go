package streamsql

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func mkEvent(id string, ts int64, fields map[string]any) Event {
	f := map[string]any{"ts": ts}
	for k, v := range fields {
		f[k] = v
	}
	return Event{ID: id, Timestamp: ts, Fields: f}
}

// collector accumulates a query's output. drain is called after
// engine.Sync, so all emitted batches are already in the buffered channel
// and no goroutine is needed.
type collector struct {
	q       *Query
	batches []Batch
}

func newCollector(q *Query) *collector { return &collector{q: q} }

func (c *collector) drain() {
	for {
		select {
		case b := <-c.q.Outputs():
			c.batches = append(c.batches, b)
		default:
			return
		}
	}
}

func (c *collector) finalRows() []Row {
	var out []Row
	for _, b := range c.batches {
		for _, r := range b.Rows {
			if r.Final {
				out = append(out, r)
			}
		}
	}
	return out
}

func (c *collector) updateRows() []Row {
	var out []Row
	for _, b := range c.batches {
		for _, r := range b.Rows {
			if !r.Final {
				out = append(out, r)
			}
		}
	}
	return out
}

func TestWindowTriggerDrivenByEventTime(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: time.Second})
	defer eng.Close()
	q, err := eng.Register("q", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user`)
	if err != nil {
		t.Fatal(err)
	}
	col := newCollector(q)

	eng.Submit(mkEvent("e1", 1000, map[string]any{"user": "a"}))
	eng.Submit(mkEvent("e2", 5000, map[string]any{"user": "a"}))
	eng.Sync()
	col.drain()
	if n := len(col.finalRows()); n != 0 {
		t.Fatalf("window fired too early: %d final rows", n)
	}
	// wall-clock waiting must NOT fire the window: only event-time
	// advancement does.
	time.Sleep(50 * time.Millisecond)
	eng.Sync()
	col.drain()
	if n := len(col.finalRows()); n != 0 {
		t.Fatalf("window fired by wall clock: %d final rows", n)
	}
	// event at 21000 -> watermark 20000 >= window end 10000 -> fire
	eng.Submit(mkEvent("e3", 21000, map[string]any{"user": "a"}))
	eng.Sync()
	col.drain()
	finals := col.finalRows()
	if len(finals) != 1 || finals[0].Values[1] != int64(2) {
		t.Fatalf("finals: %+v", finals)
	}
}

func TestOutOfOrderWithinLateness(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: 5 * time.Second})
	defer eng.Close()
	q, _ := eng.Register("q", `SELECT user, COUNT(*) AS c, SUM(amount) AS s FROM events GROUP BY TUMBLE(ts, '10s'), user ORDER BY user`)
	col := newCollector(q)

	// out of order but within 5s lateness
	eng.Submit(mkEvent("e1", 9000, map[string]any{"user": "a", "amount": int64(1)}))
	eng.Submit(mkEvent("e2", 5000, map[string]any{"user": "a", "amount": int64(2)}))
	eng.Submit(mkEvent("e3", 6000, map[string]any{"user": "b", "amount": int64(4)}))
	eng.Submit(mkEvent("e4", 20000, map[string]any{"user": "z", "amount": int64(0)})) // watermark -> 15000
	eng.Sync()
	col.drain()

	finals := col.finalRows()
	if len(finals) != 2 {
		t.Fatalf("finals: %+v", finals)
	}
	// ORDER BY user ASC
	if finals[0].Values[0] != "a" || finals[0].Values[1] != int64(2) || finals[0].Values[2] != int64(3) {
		t.Fatalf("row a: %+v", finals[0])
	}
	if finals[1].Values[0] != "b" || finals[1].Values[1] != int64(1) || finals[1].Values[2] != int64(4) {
		t.Fatalf("row b: %+v", finals[1])
	}
}

func TestLateEventRejectedWithConclusion(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: 5 * time.Second})
	defer eng.Close()
	q, _ := eng.Register("q", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user`)
	col := newCollector(q)

	eng.Submit(mkEvent("e1", 20000, map[string]any{"user": "a"})) // watermark 15000
	eng.Submit(mkEvent("e2", 14999, map[string]any{"user": "a"})) // too late
	eng.Submit(mkEvent("e3", 15001, map[string]any{"user": "a"})) // allowed
	eng.Sync()
	col.drain()

	st := q.Stats()
	if st.LateDropped != 1 || st.Processed != 2 {
		t.Fatalf("stats: %+v", st)
	}
	var lateErr *Error
	for {
		select {
		case e := <-q.Errors():
			if IsKind(e, ErrKindLateEvent) {
				lateErr = e
			}
		default:
			goto done
		}
	}
done:
	if lateErr == nil {
		t.Fatal("expected a distinguishable late-event conclusion")
	}
	// the late event must not be counted anywhere
	eng.Submit(mkEvent("e4", 40000, map[string]any{"user": "a"})) // close [10000,20000) and [20000,30000)
	eng.Sync()
	col.drain()
	var total int64
	for _, r := range col.finalRows() {
		total += r.Values[1].(int64)
	}
	if total != 2 {
		t.Fatalf("late event was counted: total=%d", total)
	}
}

func TestDuplicateEventNotCountedTwice(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: 5 * time.Second})
	defer eng.Close()
	q, _ := eng.Register("q", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user`)
	col := newCollector(q)

	eng.Submit(mkEvent("e1", 1000, map[string]any{"user": "a"}))
	eng.Submit(mkEvent("e1", 1000, map[string]any{"user": "a"})) // exact duplicate
	eng.Submit(mkEvent("e2", 2000, map[string]any{"user": "a"}))
	eng.Submit(mkEvent("e3", 30000, map[string]any{"user": "a"})) // close window
	eng.Sync()
	col.drain()

	st := q.Stats()
	if st.Duplicates != 1 {
		t.Fatalf("stats: %+v", st)
	}
	var sawDup bool
	for {
		select {
		case e := <-q.Errors():
			if IsKind(e, ErrKindDuplicate) {
				sawDup = true
			}
		default:
			goto done
		}
	}
done:
	if !sawDup {
		t.Fatal("duplicate was silently dropped")
	}
	finals := col.finalRows()
	if len(finals) != 1 || finals[0].Values[1] != int64(2) {
		t.Fatalf("finals: %+v", finals)
	}
}

// batchReference computes the expected final result of the query
// SELECT key, COUNT(*), SUM(amount) GROUP BY TUMBLE(ts, size), key
// over all events in one shot.
func batchReference(events []Event, size int64) map[string][]any {
	type acc struct {
		n   int64
		sum int64
	}
	m := map[[2]any]*acc{}
	w := &WindowSpec{Kind: Tumbling, Size: size, Slide: size}
	for _, ev := range events {
		ts := ev.Fields["ts"].(int64)
		key := ev.Fields["user"]
		win := w.Assign(ts)[0]
		k := [2]any{win.Start, key}
		a := m[k]
		if a == nil {
			a = &acc{}
			m[k] = a
		}
		a.n++
		a.sum += ev.Fields["amount"].(int64)
	}
	out := map[string][]any{}
	for k, a := range m {
		out[fmt.Sprintf("%v|%v", k[0], k[1])] = []any{a.n, a.sum}
	}
	return out
}

func TestIncrementalMatchesBatch(t *testing.T) {
	const size = int64(10000)
	eng := NewEngine(Config{AllowedLateness: 10 * time.Second, OutputBufferSize: 1 << 20})
	defer eng.Close()
	q, err := eng.Register("q", `SELECT window_start, user, COUNT(*) AS c, SUM(amount) AS s FROM events GROUP BY TUMBLE(ts, '10s'), user`)
	if err != nil {
		t.Fatal(err)
	}
	col := newCollector(q)

	// deterministic pseudo-random sequence with out-of-order events
	var events []Event
	seed := int64(42)
	rnd := func(n int64) int64 {
		seed = (seed*6364136223846793005 + 1442695040888963407) & (1<<63 - 1)
		return seed % n
	}
	users := []string{"a", "b", "c", "d"}
	maxTS := int64(0)
	for i := 0; i < 500; i++ {
		maxTS += rnd(500)
		ts := maxTS - rnd(8000) // out of order within 8s < 10s lateness
		if ts < 0 {
			ts = 0
		}
		u := users[rnd(int64(len(users)))]
		ev := mkEvent(fmt.Sprintf("e%d", i), ts, map[string]any{"user": u, "amount": rnd(100)})
		events = append(events, ev)
		eng.Submit(ev)
	}
	// close all windows
	eng.Submit(mkEvent("closer", maxTS+100000, map[string]any{"user": "z", "amount": int64(0)}))
	eng.Sync()
	col.drain()

	want := batchReference(events, size)
	got := map[string][]any{}
	for _, r := range col.finalRows() {
		ws := r.Values[0]
		u := r.Values[1]
		got[fmt.Sprintf("%v|%v", ws, u)] = []any{r.Values[2], r.Values[3]}
	}
	if len(got) != len(want) {
		t.Fatalf("group count: got %d want %d", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing group %s", k)
		}
		if !reflect.DeepEqual(g, w) {
			t.Fatalf("group %s: got %v want %v", k, g, w)
		}
	}
	// every final group must have had its last incremental update equal to
	// the final value (incremental correctness, not just final)
	lastUpdate := map[string][]any{}
	for _, r := range col.updateRows() {
		k := fmt.Sprintf("%v|%v", r.Values[0], r.Values[1])
		lastUpdate[k] = []any{r.Values[2], r.Values[3]}
	}
	for k, w := range want {
		if lu, ok := lastUpdate[k]; ok && !reflect.DeepEqual(lu, w) {
			t.Fatalf("last incremental update for %s: got %v want %v", k, lu, w)
		}
	}
}

func TestDeterministicReplay(t *testing.T) {
	run := func() []Batch {
		eng := NewEngine(Config{AllowedLateness: 3 * time.Second, OutputBufferSize: 1 << 20})
		defer eng.Close()
		q, _ := eng.Register("q", `SELECT user, COUNT(*) AS c, SUM(amount) AS s FROM events GROUP BY TUMBLE(ts, '5s'), user ORDER BY user DESC, c ASC`)
		col := newCollector(q)
		users := []string{"x", "y", "z"}
		seed := int64(7)
		rnd := func(n int64) int64 {
			seed = (seed*1103515245 + 12345) & (1<<31 - 1)
			return seed % n
		}
		maxTS := int64(0)
		for i := 0; i < 200; i++ {
			maxTS += rnd(300)
			ts := maxTS - rnd(2000)
			if ts < 0 {
				ts = 0
			}
			eng.Submit(mkEvent(fmt.Sprintf("e%d", i), ts, map[string]any{
				"user": users[rnd(3)], "amount": rnd(50)}))
		}
		eng.Submit(mkEvent("closer", maxTS+60000, map[string]any{"user": "x", "amount": int64(1)}))
		eng.Sync()
		col.drain()
		return col.batches
	}
	first := run()
	second := run()
	if !reflect.DeepEqual(first, second) {
		t.Fatal("identical inputs produced different outputs")
	}
	if len(first) == 0 {
		t.Fatal("no output produced")
	}
}

func TestWindowStateReleased(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: time.Second, OutputBufferSize: 1 << 20})
	defer eng.Close()
	q, _ := eng.Register("q", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '1s'), user`)
	_ = q

	// 1000 windows, each closed by the advancing watermark
	for i := int64(0); i < 1000; i++ {
		eng.Submit(mkEvent(fmt.Sprintf("e%d", i), i*1000, map[string]any{"user": "a"}))
	}
	eng.Sync()
	if n := q.OpenWindows(); n > 3 {
		t.Fatalf("window state not released: %d windows still open", n)
	}
	q.mu.Lock()
	groups := len(q.groups)
	seen := len(q.seenIDs)
	q.mu.Unlock()
	if groups > 3 {
		t.Fatalf("group state not released: %d groups", groups)
	}
	if seen > 3 {
		t.Fatalf("dedup state not released: %d entries", seen)
	}
}

func TestHoppingWindowAggregation(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: time.Second})
	defer eng.Close()
	q, _ := eng.Register("q", `SELECT window_start, window_end, COUNT(*) AS c FROM events GROUP BY HOP(ts, '10s', '30s'), user ORDER BY window_start`)
	col := newCollector(q)

	eng.Submit(mkEvent("e1", 25000, map[string]any{"user": "a"}))
	eng.Submit(mkEvent("e2", 100000, map[string]any{"user": "a"})) // close everything
	eng.Sync()
	col.drain()

	finals := col.finalRows()
	// event at 25000 belongs to windows [0,30k), [10k,40k), [20k,50k)
	if len(finals) != 3 {
		t.Fatalf("finals: %+v", finals)
	}
	for _, r := range finals {
		if r.Values[2] != int64(1) {
			t.Fatalf("count: %+v", r)
		}
	}
	starts := []int64{finals[0].Values[0].(int64), finals[1].Values[0].(int64), finals[2].Values[0].(int64)}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	if !reflect.DeepEqual(starts, []int64{0, 10000, 20000}) {
		t.Fatalf("window starts: %v", starts)
	}
}

func TestStatelessProjectionFilter(t *testing.T) {
	eng := NewEngine(Config{})
	defer eng.Close()
	q, err := eng.Register("q", `SELECT user, amount * 2 AS doubled FROM events WHERE amount >= 10`)
	if err != nil {
		t.Fatal(err)
	}
	col := newCollector(q)
	eng.Submit(mkEvent("e1", 1000, map[string]any{"user": "a", "amount": int64(5)}))
	eng.Submit(mkEvent("e2", 1001, map[string]any{"user": "b", "amount": int64(15)}))
	eng.Sync()
	col.drain()
	rows := col.updateRows()
	if len(rows) != 1 || rows[0].Values[0] != "b" || rows[0].Values[1] != int64(30) {
		t.Fatalf("rows: %+v", rows)
	}
}
