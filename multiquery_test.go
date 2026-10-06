package streamsql

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestIndependentProgress: two queries on the same stream keep their own
// watermarks and window sizes, and both produce correct results.
func TestIndependentProgress(t *testing.T) {
	eng := NewEngine(Config{AllowedLateness: 2 * time.Second, OutputBufferSize: 1 << 20})
	defer eng.Close()
	q1, err := eng.Register("fast-tumbling", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user ORDER BY user`)
	if err != nil {
		t.Fatal(err)
	}
	q2, err := eng.Register("wide-tumbling", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '30s'), user ORDER BY user`)
	if err != nil {
		t.Fatal(err)
	}
	c1, c2 := newCollector(q1), newCollector(q2)

	for i := int64(0); i < 6; i++ {
		eng.Submit(mkEvent(fmt.Sprintf("e%d", i), 1000+i*1000, map[string]any{"user": "a"}))
	}
	eng.Submit(mkEvent("closer", 100000, map[string]any{"user": "a"}))
	eng.Sync()
	c1.drain()
	c2.drain()

	f1 := c1.finalRows()
	if len(f1) != 1 || f1[0].Values[1] != int64(6) {
		t.Fatalf("q1 finals: %+v", f1)
	}
	f2 := c2.finalRows()
	if len(f2) != 1 || f2[0].Values[1] != int64(6) {
		t.Fatalf("q2 finals: %+v", f2)
	}
	// watermarks advance independently per query
	if q1.Watermark() != q2.Watermark() {
		t.Fatalf("same stream should give same watermark here: %d vs %d", q1.Watermark(), q2.Watermark())
	}
}

// TestSlowQueryDoesNotBlockFastQuery: a stalled query (output not consumed)
// must neither block Submit nor starve other queries; it must produce a
// distinguishable backpressure conclusion for itself.
func TestSlowQueryDoesNotBlockFastQuery(t *testing.T) {
	eng := NewEngine(Config{
		AllowedLateness:  time.Second,
		BufferSize:       8,
		OutputBufferSize: 1,
	})
	defer eng.Close()

	// slow query: windowed aggregation, output never drained -> its
	// goroutine blocks on emit, its input buffer fills, events get dropped
	// for THIS query only.
	slow, err := eng.Register("slow", `SELECT user, COUNT(*) AS c FROM events GROUP BY TUMBLE(ts, '10s'), user`)
	if err != nil {
		t.Fatal(err)
	}
	// fast query: stateless projection, drained concurrently.
	fast, err := eng.Register("fast", `SELECT user, amount FROM events`)
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	var mu sync.Mutex
	got := make([]Row, 0, n)
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for func() int { mu.Lock(); defer mu.Unlock(); return len(got) }() < n {
			select {
			case b := <-fast.Outputs():
				mu.Lock()
				got = append(got, b.Rows...)
				mu.Unlock()
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		if err := eng.Submit(mkEvent(fmt.Sprintf("e%d", i), int64(1000+i), map[string]any{"user": "a", "amount": int64(i)})); err != nil {
			t.Fatalf("submit blocked/failed at %d: %v", i, err)
		}
		// pace submissions so the fast query's own buffer never fills;
		// the slow query stalls regardless, so any drop for the fast
		// query would prove cross-query interference
		time.Sleep(time.Millisecond)
	}
	// Note: eng.Sync() is not called here on purpose: the slow query is
	// permanently stalled, so a barrier would never complete. The drainer
	// finishing is the synchronization point for the fast query.
	<-drainDone

	if len(got) != n {
		t.Fatalf("fast query lost events to slow query: got %d of %d", len(got), n)
	}
	fst := fast.Stats()
	if fst.Backpressured != 0 || fst.Processed != n {
		t.Fatalf("fast query affected: %+v", fst)
	}
	sst := slow.Stats()
	if sst.Backpressured == 0 {
		t.Fatalf("slow query should report backpressure, stats: %+v", sst)
	}
	// distinguishable conclusion on the slow query's error channel
	var sawBP bool
	for {
		select {
		case e := <-slow.Errors():
			if IsKind(e, ErrKindBackpressure) {
				sawBP = true
			}
		default:
			goto done
		}
	}
done:
	if !sawBP {
		t.Fatal("no distinguishable backpressure conclusion reported")
	}
}
