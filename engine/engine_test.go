package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Diudiuseven/streaming-sql-engine-go/event"
)

func pubEv(t *testing.T, e *Engine, id string, min int, fields map[string]any) {
	t.Helper()
	if err := e.Publish(event.Item{Event: &event.Event{
		Seq:       time.Now().UnixNano(),
		ID:        id,
		EventTime: time.Unix(0, int64(min)*int64(time.Minute)).UTC(),
		Fields:    fields,
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func pubWm(t *testing.T, e *Engine, min int) {
	t.Helper()
	if err := e.Publish(event.Item{Watermark: &event.Watermark{
		T: time.Unix(0, int64(min)*int64(time.Minute)).UTC(),
	}}); err != nil {
		t.Fatalf("publish watermark: %v", err)
	}
}

func drain(sub *Subscription, n int, timeout time.Duration) []Update {
	var out []Update
	timer := time.After(timeout)
	for len(out) < n {
		select {
		case u, ok := <-sub.Updates:
			if !ok {
				return out
			}
			out = append(out, u)
		case <-timer:
			return out
		}
	}
	return out
}

func TestMultiQuerySharedStream(t *testing.T) {
	e := New(Config{})
	defer e.Close()
	fast, err := e.Subscribe(`SELECT user, v FROM s WHERE v > 0`)
	if err != nil {
		t.Fatal(err)
	}
	agg, err := e.Subscribe(`SELECT COUNT(*) AS c FROM s GROUP BY TUMBLE(ts, '5m')`)
	if err != nil {
		t.Fatal(err)
	}
	pubEv(t, e, "e1", 1, map[string]any{"user": "a", "v": int64(5)})
	pubEv(t, e, "e2", 2, map[string]any{"user": "b", "v": int64(7)})
	pubWm(t, e, 5)

	fu := drain(fast, 2, time.Second)
	if len(fu) != 2 {
		t.Fatalf("fast query got %d updates", len(fu))
	}
	au := drain(agg, 3, time.Second) // 2 incremental + 1 final
	if len(au) != 3 || !au[2].Final || au[2].Row.Cols["c"] != int64(2) {
		t.Fatalf("agg updates = %v", au)
	}
}

func TestSlowQueryDoesNotBlockFast(t *testing.T) {
	e := New(Config{SubBuffer: 8})
	defer e.Close()
	slow, err := e.Subscribe(`SELECT user FROM s`)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := e.Subscribe(`SELECT user FROM s WHERE v > 100`)
	if err != nil {
		t.Fatal(err)
	}
	// Drain fast concurrently; never drain slow.
	fastCh := make(chan Update, 256)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case u, ok := <-fast.Updates:
				if !ok {
					return
				}
				fastCh <- u
			case <-stop:
				return
			}
		}
	}()
	// Overflow the slow subscription (never drained). Yield briefly so
	// the concurrent fast consumer can keep up with its small buffer.
	for i := 0; i < 100; i++ {
		pubEv(t, e, fmt.Sprintf("e%d", i), 1, map[string]any{"user": "a", "v": int64(200)})
		if i%4 == 3 {
			time.Sleep(time.Millisecond)
		}
	}
	// Slow subscription must have been terminated with ErrBackpressure.
	if !errors.Is(slow.Err(), ErrBackpressure) {
		t.Fatalf("slow.Err = %v, want ErrBackpressure", slow.Err())
	}
	// Fast subscription still receives everything.
	for i := 0; i < 100; i++ {
		select {
		case <-fastCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("fast received only %d updates, want 100", i)
		}
	}
	// Engine still accepts publishes.
	pubEv(t, e, "e100", 1, map[string]any{"user": "z", "v": int64(300)})
	select {
	case <-fastCh:
	case <-time.After(2 * time.Second):
		t.Fatal("fast did not receive post-overflow event")
	}
}

func TestDeterministicReplay(t *testing.T) {
	run := func() []Update {
		e := New(Config{})
		defer e.Close()
		sub, err := e.Subscribe(
			`SELECT user, SUM(v) AS s FROM d GROUP BY user, TUMBLE(ts, '5m') ORDER BY s DESC`)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			pubEv(t, e, fmt.Sprintf("e%d", i), i%5, map[string]any{
				"user": fmt.Sprintf("u%d", i%3), "v": int64(i)})
		}
		pubWm(t, e, 100)
		var all []Update
		timer := time.After(2 * time.Second)
		for {
			select {
			case u := <-sub.Updates:
				all = append(all, u)
				if u.Final {
					// collect until 3 final rows
					n := 0
					for _, x := range all {
						if x.Final {
							n++
						}
					}
					if n == 3 {
						return all
					}
				}
			case <-timer:
				t.Fatal("timeout waiting for finals")
			}
		}
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("replay length differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || !rowsEqual(a[i].Row, b[i].Row) {
			t.Fatalf("replay differs at %d: %v vs %v", i, a[i], b[i])
		}
	}
}
