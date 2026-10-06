package streamsql

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// aggMarkerPrefix prefixes synthetic identifiers that reference the i-th
// aggregate accumulator of a group. The \x00 byte cannot appear in lexer
// identifiers, so collisions with user columns are impossible.
const aggMarkerPrefix = "\x00agg"

// groupState holds the aggregate state of one (window, group-key) pair.
type groupState struct {
	Window  Window
	KeyVals []any
	Accs    []accumulator
	dirty   bool
}

// windowEntry indexes the groups belonging to one open window.
type windowEntry struct {
	End    int64
	Groups map[string]bool
}

// Stats reports per-query runtime counters.
type Stats struct {
	Processed     int64
	Emitted       int64
	LateDropped   int64
	Duplicates    int64
	Backpressured int64
	BadEvents     int64
	ErrorsDropped int64 // error notifications lost due to full error channel
}

// plan is the compiled, validated form of a Statement.
type plan struct {
	stmt      *Statement
	stateless bool
	groupKeys []string
	aggCalls  []Call
	sel       []SelectItem // rewritten: aggregates -> marker idents
	schema    []string
	orderIdx  []int
	orderDesc []bool
	limit     int64 // 0 = no limit
	window    *WindowSpec
}

// qmsg is the internal message sent to a query goroutine. A non-nil
// barrier makes the query acknowledge once all prior events are processed
// (used by Checkpoint and Sync).
type qmsg struct {
	ev      *Event
	barrier chan struct{}
}

// Query is a running continuous query.
type Query struct {
	name string
	plan *plan
	eng  *Engine

	in   chan qmsg
	out  chan Batch
	errs chan *Error
	done chan struct{}
	stop chan struct{}

	mu        sync.Mutex
	watermark int64
	maxSeen   int64
	groups    map[string]*groupState
	windows   map[int64]*windowEntry
	seenIDs   map[string]int64
	stats     Stats
	closed    bool
}

// Name returns the registration name.
func (q *Query) Name() string { return q.name }

// Schema returns the output column names aligned with Row.Values.
func (q *Query) Schema() []string { return append([]string(nil), q.plan.schema...) }

// Outputs delivers result batches.
func (q *Query) Outputs() <-chan Batch { return q.out }

// Errors delivers distinguishable runtime conclusions (late events,
// duplicates, backpressure, bad events).
func (q *Query) Errors() <-chan *Error { return q.errs }

// Stats returns a snapshot of runtime counters.
func (q *Query) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.stats
}

// Watermark returns the query's current event-time watermark (millis).
func (q *Query) Watermark() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.watermark
}

// OpenWindows returns the number of windows currently holding state.
func (q *Query) OpenWindows() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.windows)
}

// Close stops the query goroutine and releases its state. It is safe even
// if the query is stalled on a full output channel: pending emissions are
// abandoned. Events still queued in the input buffer are discarded.
func (q *Query) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	close(q.stop)
	q.mu.Unlock()
	<-q.done
}

func (q *Query) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// run is the query goroutine: events and barriers are handled strictly in
// order, which keeps per-query output deterministic.
func (q *Query) run() {
	defer close(q.done)
	for {
		select {
		case m := <-q.in:
			if m.barrier != nil {
				close(m.barrier)
				continue
			}
			q.process(m.ev)
		case <-q.stop:
			return
		}
	}
}

func (q *Query) reportErr(err error) {
	e, ok := err.(*Error)
	if !ok {
		e = &Error{Kind: ErrKindInternal, Msg: err.Error()}
	}
	select {
	case q.errs <- e:
	default:
		q.mu.Lock()
		q.stats.ErrorsDropped++
		q.mu.Unlock()
	}
}

// process handles one event: dedup, lateness check, watermark advance,
// filter, state update, incremental emission, window firing and state
// eviction.
func (q *Query) process(ev *Event) {
	p := q.plan
	var updates []Row
	var finals []Row

	q.mu.Lock()
	// 1. dedup: same event ID is never counted twice, and the duplicate is
	// reported rather than silently dropped.
	if _, dup := q.seenIDs[ev.ID]; dup {
		q.stats.Duplicates++
		q.mu.Unlock()
		q.reportErr(errorf(ErrKindDuplicate, "query %q: duplicate event ID %q ignored", q.name, ev.ID))
		return
	}
	// 2. event time for this query
	ts, err := q.eventTime(ev)
	if err != nil {
		q.stats.BadEvents++
		q.mu.Unlock()
		q.reportErr(err)
		return
	}
	// 3. lateness: events at or behind the watermark exceed the allowed
	// lateness and are rejected with a distinguishable conclusion.
	if ts <= q.watermark {
		q.stats.LateDropped++
		q.mu.Unlock()
		q.reportErr(errorf(ErrKindLateEvent, "query %q: event %q at %d is too late (watermark %d)", q.name, ev.ID, ts, q.watermark))
		return
	}
	// 4. advance watermark, driven purely by event time
	if ts > q.maxSeen {
		q.maxSeen = ts
		lat := q.eng.cfg.AllowedLateness.Milliseconds()
		if wm := q.maxSeen - lat; wm > q.watermark {
			q.watermark = wm
		}
	}
	q.seenIDs[ev.ID] = ts
	q.stats.Processed++

	// 5. filter
	pass := true
	if p.stmt.Where != nil {
		v, err := evalExpr(p.stmt.Where, fieldLookup(ev.Fields))
		if err != nil {
			q.stats.BadEvents++
			q.mu.Unlock()
			q.reportErr(errorf(ErrKindInternal, "query %q: WHERE evaluation failed for event %q: %v", q.name, ev.ID, err))
			return
		}
		b, ok := v.(bool)
		if !ok {
			q.stats.BadEvents++
			q.mu.Unlock()
			q.reportErr(errorf(ErrKindInternal, "query %q: WHERE did not evaluate to a boolean for event %q", q.name, ev.ID))
			return
		}
		pass = b
	}

	if pass {
		if p.stateless {
			row, err := q.projectStateless(ev)
			if err != nil {
				q.stats.BadEvents++
				q.mu.Unlock()
				q.reportErr(err)
				return
			}
			updates = append(updates, row)
		} else {
			updates = q.updateGroups(ev, ts)
		}
	}
	// 6. fire windows whose end is at or behind the watermark, then evict
	// dedup entries that can only belong to late events from now on.
	finals = q.fireWindowsLocked()
	q.evictDedupLocked()
	q.mu.Unlock()

	// emission happens outside the lock; a slow consumer stalls only this
	// query's goroutine.
	if len(updates) > 0 {
		q.emit(updates, false)
	}
	if len(finals) > 0 {
		q.emit(finals, true)
	}
}

// eventTime resolves the event time used by this query: the window time
// column for grouped queries, Event.Timestamp for stateless ones.
func (q *Query) eventTime(ev *Event) (int64, error) {
	if q.plan.stateless {
		return ev.Timestamp, nil
	}
	v, ok := fieldLookup(ev.Fields)(q.plan.window.TimeCol)
	if !ok {
		return 0, errorf(ErrKindInternal, "query %q: event %q misses time column %q", q.name, ev.ID, q.plan.window.TimeCol)
	}
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	}
	return 0, errorf(ErrKindInternal, "query %q: time column %q of event %q is not an integer (%T)", q.name, q.plan.window.TimeCol, ev.ID, v)
}

// updateGroups adds the event to every window/group it belongs to and
// returns update rows for exactly the groups that changed.
func (q *Query) updateGroups(ev *Event, ts int64) []Row {
	p := q.plan
	keyVals := make([]any, len(p.groupKeys))
	for i, k := range p.groupKeys {
		v, _ := fieldLookup(ev.Fields)(k)
		keyVals[i] = v
	}
	var rows []Row
	for _, w := range p.window.Assign(ts) {
		gk := groupKeyString(w, keyVals)
		gs, ok := q.groups[gk]
		if !ok {
			gs = &groupState{Window: w, KeyVals: keyVals}
			for _, c := range p.aggCalls {
				acc, err := newAccumulator(c.Name)
				if err != nil { // validated at compile time
					continue
				}
				gs.Accs = append(gs.Accs, acc)
			}
			q.groups[gk] = gs
			we := q.windows[w.Start]
			if we == nil {
				we = &windowEntry{End: w.End, Groups: map[string]bool{}}
				q.windows[w.Start] = we
			}
			we.Groups[gk] = true
		}
		for i, c := range p.aggCalls {
			var v any = struct{}{} // COUNT(*): non-nil sentinel
			if !c.Star {
				v, _ = evalExpr(c.Args[0], fieldLookup(ev.Fields))
			}
			gs.Accs[i].Add(v)
		}
		gs.dirty = true
		rows = append(rows, q.projectGroup(gs, false))
	}
	return rows
}

// fireWindowsLocked emits final rows for windows at or behind the
// watermark and releases their state. Caller holds q.mu.
func (q *Query) fireWindowsLocked() []Row {
	var starts []int64
	for s, we := range q.windows {
		if we.End <= q.watermark {
			starts = append(starts, s)
		}
	}
	if len(starts) == 0 {
		return nil
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	var rows []Row
	for _, s := range starts {
		we := q.windows[s]
		keys := make([]string, 0, len(we.Groups))
		for gk := range we.Groups {
			keys = append(keys, gk)
		}
		sort.Strings(keys)
		for _, gk := range keys {
			gs := q.groups[gk]
			if gs == nil {
				continue
			}
			rows = append(rows, q.projectGroup(gs, true))
			delete(q.groups, gk)
		}
		delete(q.windows, s)
	}
	return rows
}

// evictDedupLocked drops dedup entries at or behind the watermark; a
// duplicate arriving later would be rejected as late anyway.
func (q *Query) evictDedupLocked() {
	for id, ts := range q.seenIDs {
		if ts <= q.watermark {
			delete(q.seenIDs, id)
		}
	}
}

// projectStateless evaluates the SELECT list against one event.
func (q *Query) projectStateless(ev *Event) (Row, error) {
	vals := make([]any, len(q.plan.sel))
	for i, item := range q.plan.sel {
		v, err := evalExpr(item.Expr, fieldLookup(ev.Fields))
		if err != nil {
			return Row{}, errorf(ErrKindInternal, "query %q: projection failed for event %q: %v", q.name, ev.ID, err)
		}
		vals[i] = v
	}
	return Row{Values: vals}, nil
}

// projectGroup evaluates the SELECT list against a group state.
func (q *Query) projectGroup(gs *groupState, final bool) Row {
	lk := func(name string) (any, bool) {
		if strings.HasPrefix(name, aggMarkerPrefix) {
			var idx int
			fmt.Sscanf(name[len(aggMarkerPrefix):], "%d", &idx)
			if idx >= 0 && idx < len(gs.Accs) {
				return gs.Accs[idx].Value(), true
			}
			return nil, false
		}
		switch strings.ToLower(name) {
		case "window_start":
			return gs.Window.Start, true
		case "window_end":
			return gs.Window.End, true
		}
		for i, k := range q.plan.groupKeys {
			if k == name || stripQualifier(k) == name || k == stripQualifier(name) {
				return gs.KeyVals[i], true
			}
		}
		return nil, false
	}
	vals := make([]any, len(q.plan.sel))
	for i, item := range q.plan.sel {
		v, _ := evalExpr(item.Expr, lk)
		vals[i] = v
	}
	return Row{Values: vals, Final: final}
}

// emit sorts rows deterministically, applies LIMIT, and sends the batch.
func (q *Query) emit(rows []Row, final bool) {
	q.sortRows(rows)
	if q.plan.limit > 0 && int64(len(rows)) > q.plan.limit {
		rows = rows[:q.plan.limit]
	}
	q.mu.Lock()
	q.stats.Emitted += int64(len(rows))
	q.mu.Unlock()
	select {
	case q.out <- Batch{Query: q.name, Rows: rows}:
	case <-q.stop: // closing: abandon emission
	}
}

// sortRows orders rows by ORDER BY terms, then by the canonical encoding
// of the full row as a stable tie-break (documented determinism rule).
func (q *Query) sortRows(rows []Row) {
	p := q.plan
	sort.SliceStable(rows, func(a, b int) bool {
		ra, rb := rows[a], rows[b]
		for i, idx := range p.orderIdx {
			c := compareRowValues(ra.Values[idx], rb.Values[idx])
			if c != 0 {
				if p.orderDesc[i] {
					return c > 0
				}
				return c < 0
			}
		}
		return canonicalRow(ra.Values) < canonicalRow(rb.Values)
	})
}

// compareRowValues compares possibly-nil values; nil sorts last in
// ascending order (and therefore first in descending order).
func compareRowValues(l, r any) int {
	if l == nil && r == nil {
		return 0
	}
	if l == nil {
		return 1
	}
	if r == nil {
		return -1
	}
	if c, ok := compareValues(l, r); ok {
		return c
	}
	return strings.Compare(fmt.Sprintf("%v", l), fmt.Sprintf("%v", r))
}

func canonicalRow(vals []any) string {
	var sb strings.Builder
	for _, v := range vals {
		fmt.Fprintf(&sb, "%#v|", v)
	}
	return sb.String()
}

func groupKeyString(w Window, keyVals []any) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d|", w.Start)
	for _, v := range keyVals {
		fmt.Fprintf(&sb, "%#v|", v)
	}
	return sb.String()
}

func stripQualifier(name string) string {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[i+1:]
	}
	return name
}

// mathMinInt64 is the initial watermark/maxSeen: before any event arrives
// every timestamp is ahead of the watermark.
func mathMinInt64() int64 { return math.MinInt64 }
