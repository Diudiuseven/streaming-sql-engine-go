package engine

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Diudiuseven/streaming-sql-engine-go/checkpoint"
	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

func init() {
	// Concrete types that may appear behind interface values in Rows
	// and group keys.
	gob.Register(time.Time{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register("")
	gob.Register(true)
}

// CheckpointInfo describes one subscription restored from a checkpoint.
type CheckpointInfo struct {
	SubID int
	Stmt  string
}

// --- serializable forms ---

type engineSnap struct {
	LastSeq int64
	NextID  int
	Subs    []subSnap
}

type subSnap struct {
	ID    int
	Stmt  string
	State queryStateSnap
}

type queryStateSnap struct {
	Watermark time.Time
	Windows   []windowSnap // sorted by window start for deterministic bytes
	Groups    []groupSnap  // non-windowed aggregates, sorted by key
	Seen      map[string]time.Time
}

type windowSnap struct {
	Win    Window
	Groups []groupSnap // sorted by key
}

type groupSnap struct {
	Key     string
	KeyVals []any
	Aggs    []aggSnap
	Last    Row
	HasLast bool
}

// Snapshot atomically persists the engine's committed state (all
// subscription states and the stream position) to store.
func (e *Engine) Snapshot(store *checkpoint.Store) error {
	e.procMu.Lock()
	defer e.procMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("engine: closed")
	}
	snap := engineSnap{LastSeq: e.lastSeq, NextID: e.nextID}
	ids := make([]int, 0, len(e.subs))
	for id := range e.subs {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		s := e.subs[id]
		snap.Subs = append(snap.Subs, subSnap{
			ID:    id,
			Stmt:  s.stmt,
			State: snapshotQuery(s.state),
		})
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		return fmt.Errorf("engine: encode snapshot: %w", err)
	}
	return store.Commit(buf.Bytes())
}

// Restore loads the latest committed checkpoint and recreates all
// subscriptions (with their output buffers) exactly as they were.
// It returns infos so the application can re-attach via Resume.
// No committed results are re-emitted and no events are skipped as
// long as the source resumes after the returned LastSeq.
func (e *Engine) Restore(store *checkpoint.Store) ([]CheckpointInfo, int64, error) {
	data, err := store.Load()
	if err != nil {
		return nil, 0, err
	}
	var snap engineSnap
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return nil, 0, fmt.Errorf("engine: decode snapshot: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, 0, errors.New("engine: closed")
	}
	e.lastSeq = snap.LastSeq
	e.nextID = snap.NextID
	var infos []CheckpointInfo
	for _, ss := range snap.Subs {
		q, err := sqlparse.Parse(ss.Stmt)
		if err != nil {
			return nil, 0, fmt.Errorf("engine: restore sub %d: %w", ss.ID, err)
		}
		st, err := newQueryState(q, e.cfg.AllowedLateness)
		if err != nil {
			return nil, 0, fmt.Errorf("engine: restore sub %d: %w", ss.ID, err)
		}
		restoreQuery(st, ss.State)
		sub := &subscription{
			id:     ss.ID,
			engine: e,
			stmt:   ss.Stmt,
			state:  st,
			out:    make(chan Update, e.cfg.SubBuffer),
			done:   make(chan struct{}),
		}
		e.subs[sub.id] = sub
		infos = append(infos, CheckpointInfo{SubID: ss.ID, Stmt: ss.Stmt})
	}
	return infos, snap.LastSeq, nil
}

// Resume attaches a Subscription handle to a restored subscription.
func (e *Engine) Resume(id int) (*Subscription, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.subs[id]
	if !ok {
		return nil, fmt.Errorf("engine: no subscription %d", id)
	}
	return &Subscription{Updates: s.out, sub: s}, nil
}

// LastSeq returns the highest stream sequence number processed so far.
func (e *Engine) LastSeq() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastSeq
}

// --- queryState <-> snapshot conversion ---

func snapshotQuery(s *queryState) queryStateSnap {
	snap := queryStateSnap{
		Watermark: s.watermark,
		Seen:      s.seen,
	}
	starts := make([]int64, 0, len(s.windows))
	for k := range s.windows {
		starts = append(starts, k)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for _, k := range starts {
		ws := s.windows[k]
		snap.Windows = append(snap.Windows, windowSnap{
			Win:    ws.win,
			Groups: snapshotGroups(ws.groups),
		})
	}
	snap.Groups = snapshotGroups(s.groups)
	return snap
}

func snapshotGroups(groups map[string]*groupState) []groupSnap {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]groupSnap, 0, len(keys))
	for _, k := range keys {
		gs := groups[k]
		g := groupSnap{Key: k, KeyVals: gs.keyVals, Last: gs.last, HasLast: gs.hasLast}
		for _, a := range gs.aggs {
			g.Aggs = append(g.Aggs, a.export())
		}
		out = append(out, g)
	}
	return out
}

func restoreQuery(s *queryState, snap queryStateSnap) {
	s.watermark = snap.Watermark
	s.seen = snap.Seen
	if s.seen == nil {
		s.seen = map[string]time.Time{}
	}
	for _, ws := range snap.Windows {
		st := &windowState{win: ws.Win, groups: restoreGroups(s, ws.Groups)}
		s.windows[ws.Win.Start.UnixNano()] = st
	}
	s.groups = restoreGroups(s, snap.Groups)
}

func restoreGroups(s *queryState, snaps []groupSnap) map[string]*groupState {
	out := map[string]*groupState{}
	for _, g := range snaps {
		gs := &groupState{
			keyVals: g.KeyVals,
			last:    g.Last,
			hasLast: g.HasLast,
		}
		for i, a := range g.Aggs {
			st := newAggState(s.q.Items[s.aggIdx[i]].Agg)
			st.importFrom(a)
			gs.aggs = append(gs.aggs, st)
		}
		out[g.Key] = gs
	}
	return out
}
