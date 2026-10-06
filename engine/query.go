package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Diudiuseven/streaming-sql-engine-go/event"
	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

// groupState holds the aggregate accumulators of one group within one
// window (or the single global window for non-windowed aggregates).
type groupState struct {
	keyVals []any      // values of the GROUP BY columns, in order
	aggs    []aggState // parallel to the aggregate select items
	last    Row        // last emitted row, for change detection
	hasLast bool
}

// windowState holds all groups of one open window.
type windowState struct {
	win    Window
	groups map[string]*groupState
}

// queryState is the runtime state of one continuous query.
type queryState struct {
	q      *sqlparse.Query
	kind   windowKind
	size   time.Duration
	slide  time.Duration
	aggIdx []int // indexes into q.Items of aggregate items

	allowedLateness time.Duration
	watermark       time.Time

	// Windowed aggregation state. Key: window start nanos.
	windows map[int64]*windowState
	// Non-windowed aggregation state.
	groups map[string]*groupState

	// seen maps event ID -> event time, for exactly-once dedup.
	// Entries older than the lateness horizon are pruned on watermark
	// advances; such events would be rejected as late anyway.
	seen map[string]time.Time
}

// newQueryState builds runtime state for a parsed query.
// It validates semantic constraints of the subset and returns a
// distinguishable *sqlparse.Error (Kind ErrUnsupported) on violations.
func newQueryState(q *sqlparse.Query, allowedLateness time.Duration) (*queryState, error) {
	s := &queryState{
		q:               q,
		allowedLateness: allowedLateness,
		windows:         map[int64]*windowState{},
		groups:          map[string]*groupState{},
		seen:            map[string]time.Time{},
	}
	switch q.Window.Kind {
	case sqlparse.Tumble:
		s.kind = kindTumble
		s.size = q.Window.Size
	case sqlparse.Hop:
		s.kind = kindHop
		s.size = q.Window.Size
		s.slide = q.Window.Slide
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *queryState) isProjection() bool {
	return len(s.aggIdx) == 0 && len(s.q.GroupBy) == 0 && s.q.Window.Kind == sqlparse.NoWindow
}

func (s *queryState) validate() error {
	hasStar := false
	for i, it := range s.q.Items {
		if it.Star {
			hasStar = true
		}
		if it.Agg != "" {
			s.aggIdx = append(s.aggIdx, i)
		}
		if it.WindowFunc != "" && s.q.Window.Kind == sqlparse.NoWindow {
			return &sqlparse.Error{Kind: sqlparse.ErrUnsupported, Pos: -1,
				Msg: it.WindowFunc + "() requires a windowed GROUP BY"}
		}
	}
	if hasStar && len(s.q.Items) > 1 {
		return &sqlparse.Error{Kind: sqlparse.ErrUnsupported, Pos: -1,
			Msg: "SELECT * cannot be combined with other select items"}
	}
	if hasStar && !s.isProjection() {
		return &sqlparse.Error{Kind: sqlparse.ErrUnsupported, Pos: -1,
			Msg: "SELECT * is only supported for projection queries"}
	}
	if len(s.aggIdx) > 0 && len(s.q.GroupBy) == 0 && s.q.Window.Kind == sqlparse.NoWindow {
		return &sqlparse.Error{Kind: sqlparse.ErrUnsupported, Pos: -1,
			Msg: "aggregates require GROUP BY or a window function"}
	}
	// Plain columns in an aggregate query must be GROUP BY columns.
	if len(s.aggIdx) > 0 {
		for _, it := range s.q.Items {
			if it.Col == "" {
				continue
			}
			found := false
			for _, g := range s.q.GroupBy {
				if strings.EqualFold(g, it.Col) {
					found = true
				}
			}
			if !found {
				return &sqlparse.Error{Kind: sqlparse.ErrUnsupported, Pos: -1,
					Msg: fmt.Sprintf("column %q must appear in GROUP BY", it.Col)}
			}
		}
	}
	// ORDER BY / LIMIT apply to window-close emissions only.
	if (len(s.q.OrderBy) > 0 || s.q.Limit >= 0) && s.q.Window.Kind == sqlparse.NoWindow {
		return &sqlparse.Error{Kind: sqlparse.ErrUnsupported, Pos: -1,
			Msg: "ORDER BY / LIMIT require a windowed aggregation"}
	}
	return nil
}

// horizon returns the lateness horizon: events with EventTime before it
// are rejected as late.
func (s *queryState) horizon() time.Time {
	return s.watermark.Add(-s.allowedLateness)
}

// processEvent folds one event into the state, returning updates to emit.
func (s *queryState) processEvent(ev *event.Event) []Update {
	// Exactly-once: duplicates get a distinguishable verdict.
	if _, dup := s.seen[ev.ID]; dup {
		return []Update{{Kind: UpdateDuplicateEvent, EventID: ev.ID}}
	}
	// Late events beyond the allowed lateness get a distinguishable verdict.
	if ev.EventTime.Before(s.horizon()) {
		return []Update{{Kind: UpdateLateEvent, EventID: ev.ID}}
	}
	s.seen[ev.ID] = ev.EventTime

	// WHERE filter.
	if s.q.Where != nil {
		ok, err := evalExpr(s.q.Where, ev.Fields)
		if err != nil || !truthy(ok) {
			return nil
		}
	}

	if s.isProjection() {
		return []Update{s.projectRow(ev)}
	}
	if s.q.Window.Kind == sqlparse.NoWindow {
		return s.aggregateInto(s.groups, ev, Window{})
	}
	var out []Update
	for _, w := range assignWindows(s.kind, s.size, s.slide, ev.EventTime) {
		ws, ok := s.windows[w.Start.UnixNano()]
		if !ok {
			ws = &windowState{win: w, groups: map[string]*groupState{}}
			s.windows[w.Start.UnixNano()] = ws
		}
		out = append(out, s.aggregateInto(ws.groups, ev, w)...)
	}
	return out
}

// projectRow builds a projection output row from an event.
func (s *queryState) projectRow(ev *event.Event) Update {
	row := Row{Cols: map[string]any{}}
	for _, it := range s.q.Items {
		if it.Star {
			for k, v := range ev.Fields {
				row.Cols[k] = v
			}
			continue
		}
		row.Cols[outputName(it)] = lookupField(ev.Fields, it.Col)
	}
	return Update{Kind: UpdateRow, Row: row}
}

// aggregateInto adds the event to its group in groups and emits an
// update if the group's output row changed.
func (s *queryState) aggregateInto(groups map[string]*groupState, ev *event.Event, w Window) []Update {
	key, keyVals := groupKeyOf(s.q.GroupBy, ev.Fields)
	gs, ok := groups[key]
	if !ok {
		gs = &groupState{keyVals: keyVals}
		for _, idx := range s.aggIdx {
			gs.aggs = append(gs.aggs, newAggState(s.q.Items[idx].Agg))
		}
		groups[key] = gs
	}
	for i, idx := range s.aggIdx {
		it := s.q.Items[idx]
		var v any
		if it.AggCol == "*" {
			v = int64(1)
		} else {
			v = lookupField(ev.Fields, it.AggCol)
		}
		gs.aggs[i].add(v)
	}
	row := s.buildRow(gs, w)
	if gs.hasLast && rowsEqual(gs.last, row) {
		return nil // unchanged group: no update (incremental materialization)
	}
	gs.last = row
	gs.hasLast = true
	return []Update{{Kind: UpdateRow, Row: row, WindowStart: w.Start, WindowEnd: w.End}}
}

// buildRow materializes the current output row of a group.
func (s *queryState) buildRow(gs *groupState, w Window) Row {
	row := Row{Cols: map[string]any{}}
	aggN := 0
	for _, it := range s.q.Items {
		name := outputName(it)
		switch {
		case it.Agg != "":
			row.Cols[name] = gs.aggs[aggN].result()
			aggN++
		case it.WindowFunc == "WINDOW_START":
			row.Cols[name] = w.Start
		case it.WindowFunc == "WINDOW_END":
			row.Cols[name] = w.End
		case it.Col != "":
			// GROUP BY column: take the value from the group key.
			for i, g := range s.q.GroupBy {
				if strings.EqualFold(g, it.Col) {
					row.Cols[name] = gs.keyVals[i]
				}
			}
		}
	}
	return row
}

// advanceWatermark triggers windows whose end + allowedLateness <= wm.T,
// emits their final rows (ordered per ORDER BY with deterministic
// tie-breaks, limited per LIMIT), releases their state, and prunes the
// dedup set. It returns the updates to emit.
func (s *queryState) advanceWatermark(wm event.Watermark) []Update {
	if wm.T.After(s.watermark) {
		s.watermark = wm.T
	}
	horizon := s.horizon()
	var out []Update
	var closes []int64
	for startNs, ws := range s.windows {
		if !ws.win.End.After(horizon) { // End <= horizon: close
			closes = append(closes, startNs)
		}
	}
	sort.Slice(closes, func(i, j int) bool { return closes[i] < closes[j] })
	for _, startNs := range closes {
		ws := s.windows[startNs]
		out = append(out, s.closeWindow(ws)...)
		delete(s.windows, startNs) // release closed-window state
	}
	// Prune dedup entries beyond the lateness horizon.
	for id, t := range s.seen {
		if t.Before(horizon) {
			delete(s.seen, id)
		}
	}
	return out
}

// closeWindow emits the final rows of a window in deterministic order.
func (s *queryState) closeWindow(ws *windowState) []Update {
	rows := make([]Row, 0, len(ws.groups))
	for _, gs := range ws.groups {
		rows = append(rows, s.buildRow(gs, ws.win))
	}
	s.sortRows(rows)
	if s.q.Limit >= 0 && len(rows) > s.q.Limit {
		rows = rows[:s.q.Limit]
	}
	out := make([]Update, 0, len(rows))
	for _, r := range rows {
		out = append(out, Update{
			Kind:        UpdateRow,
			Row:         r,
			WindowStart: ws.win.Start,
			WindowEnd:   ws.win.End,
			Final:       true,
		})
	}
	return out
}

// sortRows orders rows per ORDER BY, breaking ties deterministically by
// the canonical encoding of all output columns (sorted by column name).
// This makes ties on keys or event times fully reproducible.
func (s *queryState) sortRows(rows []Row) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, ord := range s.q.OrderBy {
			c := compareValues(rows[i].Cols[ord.Col], rows[j].Cols[ord.Col])
			if c != 0 {
				if ord.Desc {
					return c > 0
				}
				return c < 0
			}
		}
		// Deterministic tie-break over all columns in name order.
		keys := make([]string, 0, len(rows[i].Cols))
		for k := range rows[i].Cols {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if c := compareValues(rows[i].Cols[k], rows[j].Cols[k]); c != 0 {
				return c < 0
			}
		}
		return false
	})
}

// compareValues returns -1, 0 or 1 with a total, type-aware order.
func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if fa, ok := asFloat(a); ok {
		if fb, ok := asFloat(b); ok {
			switch {
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			}
			return 0
		}
	}
	sa, aok := a.(string)
	sb, bok := b.(string)
	if aok && bok {
		return strings.Compare(sa, sb)
	}
	// Fallback: canonical formatted form, prefixed by type name for a
	// total order across types.
	ea := fmt.Sprintf("%T:%v", a, a)
	eb := fmt.Sprintf("%T:%v", b, b)
	return strings.Compare(ea, eb)
}

func rowsEqual(a, b Row) bool {
	if len(a.Cols) != len(b.Cols) {
		return false
	}
	for k, va := range a.Cols {
		vb, ok := b.Cols[k]
		if !ok || compareValues(va, vb) != 0 {
			return false
		}
	}
	return true
}

// groupKeyOf builds a deterministic group key and the key values.
func groupKeyOf(cols []string, fields map[string]any) (string, []any) {
	vals := make([]any, len(cols))
	var sb strings.Builder
	for i, c := range cols {
		v := lookupField(fields, c)
		vals[i] = v
		fmt.Fprintf(&sb, "%T:%v\x00", v, v)
	}
	return sb.String(), vals
}

func lookupField(fields map[string]any, name string) any {
	for k, v := range fields {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return nil
}

// outputName derives the output column name of a select item.
func outputName(it sqlparse.SelectItem) string {
	if it.Alias != "" {
		return it.Alias
	}
	switch {
	case it.Agg != "":
		return fmt.Sprintf("%s(%s)", it.Agg, it.AggCol)
	case it.WindowFunc != "":
		return it.WindowFunc
	default:
		return it.Col
	}
}
