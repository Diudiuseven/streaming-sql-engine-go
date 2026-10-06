package streamsql

import (
	"sync"
	"time"
)

// Config controls engine behaviour.
type Config struct {
	// AllowedLateness: events whose timestamp is more than this far behind
	// the maximum event time seen so far are "late" and rejected with a
	// distinguishable ErrKindLateEvent conclusion.
	AllowedLateness time.Duration
	// BufferSize is the per-query input channel capacity. When full, the
	// engine drops the event for that query only and reports
	// ErrKindBackpressure on the query's error channel (other queries and
	// the submitter are never blocked).
	BufferSize int
	// OutputBufferSize is the per-query output channel capacity.
	OutputBufferSize int
	// ErrorBufferSize is the per-query error channel capacity. When full,
	// error notifications are coalesced (counted in Stats, not delivered).
	ErrorBufferSize int
}

func (c Config) withDefaults() Config {
	if c.BufferSize <= 0 {
		c.BufferSize = 1024
	}
	if c.OutputBufferSize <= 0 {
		c.OutputBufferSize = 1024
	}
	if c.ErrorBufferSize <= 0 {
		c.ErrorBufferSize = 256
	}
	return c
}

// Engine owns one event stream and any number of registered queries.
type Engine struct {
	cfg     Config
	mu      sync.Mutex
	queries map[string]*Query
	offset  int64 // sequence number of last submitted event
	closed  bool
}

func NewEngine(cfg Config) *Engine {
	return &Engine{cfg: cfg.withDefaults(), queries: map[string]*Query{}}
}

// Register compiles sql and starts a query goroutine. Parse/validation
// failures return ErrKindParse or ErrKindUnsupported.
func (e *Engine) Register(name, sql string) (*Query, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errorf(ErrKindInternal, "engine is closed")
	}
	if _, exists := e.queries[name]; exists {
		return nil, errorf(ErrKindParse, "query name %q is already registered", name)
	}
	q, err := newQuery(name, sql, e)
	if err != nil {
		return nil, err
	}
	e.queries[name] = q
	return q, nil
}

// newQuery compiles sql and starts the query goroutine.
func newQuery(name, sql string, e *Engine) (*Query, error) {
	stmt, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	pl, err := compile(stmt)
	if err != nil {
		return nil, err
	}
	q := &Query{
		name:      name,
		plan:      pl,
		eng:       e,
		in:        make(chan qmsg, e.cfg.BufferSize),
		out:       make(chan Batch, e.cfg.OutputBufferSize),
		errs:      make(chan *Error, e.cfg.ErrorBufferSize),
		done:      make(chan struct{}),
		stop:      make(chan struct{}),
		watermark: mathMinInt64(),
		maxSeen:   mathMinInt64(),
		groups:    map[string]*groupState{},
		windows:   map[int64]*windowEntry{},
		seenIDs:   map[string]int64{},
	}
	go q.run()
	return q, nil
}

// Submit assigns the event a sequence number and dispatches it to every
// registered query. A query whose input buffer is full never blocks the
// submitter or other queries: the event is dropped for that query only and
// an ErrKindBackpressure conclusion is reported on its error channel.
func (e *Engine) Submit(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errorf(ErrKindInternal, "engine is closed")
	}
	e.offset++
	ev = Event{ID: ev.ID, Timestamp: ev.Timestamp, Fields: ev.Fields}
	for _, q := range e.queries {
		e.dispatch(q, &ev)
	}
	return nil
}

// dispatch delivers one event to one query, non-blocking. Caller holds
// e.mu; q.stats is updated under q.mu.
func (e *Engine) dispatch(q *Query, ev *Event) {
	if q.isClosed() {
		return
	}
	select {
	case q.in <- qmsg{ev: ev}:
	case <-q.stop:
	default:
		q.mu.Lock()
		q.stats.Backpressured++
		q.mu.Unlock()
		q.reportErr(errorf(ErrKindBackpressure, "query %q: input buffer full (%d), event %q dropped for this query only", q.name, cap(q.in), ev.ID))
	}
}

// Offset returns the sequence number of the last submitted event; it is
// the position recorded by Checkpoint.
func (e *Engine) Offset() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.offset
}

// Sync blocks until every query has processed every event submitted so far.
func (e *Engine) Sync() {
	e.mu.Lock()
	qs := make([]*Query, 0, len(e.queries))
	for _, q := range e.queries {
		qs = append(qs, q)
	}
	e.mu.Unlock()
	barriers := make([]chan struct{}, 0, len(qs))
	for _, q := range qs {
		b := make(chan struct{})
		q.in <- qmsg{barrier: b}
		barriers = append(barriers, b)
	}
	for _, b := range barriers {
		<-b
	}
}

// Close stops all query goroutines.
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	qs := make([]*Query, 0, len(e.queries))
	for _, q := range e.queries {
		qs = append(qs, q)
	}
	e.mu.Unlock()
	for _, q := range qs {
		q.Close()
	}
}
