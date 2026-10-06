package engine

import (
	"errors"
	"sync"
	"time"

	"github.com/Diudiuseven/streaming-sql-engine-go/event"
	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

// ErrBackpressure is reported on a Subscription whose output buffer
// overflowed: the subscription is terminated with this distinguishable
// error while other subscriptions and the upstream stream continue.
var ErrBackpressure = errors.New("engine: subscription buffer full (backpressure)")

// Config controls engine behaviour.
type Config struct {
	// AllowedLateness is how far behind the watermark an event may lag
	// and still be counted. Events older than watermark-AllowedLateness
	// produce UpdateLateEvent verdicts.
	AllowedLateness time.Duration
	// SubBuffer is the per-subscription output buffer capacity.
	// Zero uses a sane default.
	SubBuffer int
	// BlockOnFull makes delivery to a full subscription buffer block
	// (trading upstream slowdown for no data loss) instead of
	// terminating the subscription with ErrBackpressure.
	BlockOnFull bool
}

const defaultSubBuffer = 1024

// Subscription is one running continuous query.
type Subscription struct {
	// Updates delivers the query's output. It is closed when the
	// subscription terminates; call Err to learn why.
	Updates <-chan Update

	sub *subscription
}

// Err returns the terminal error (e.g. ErrBackpressure), or nil for a
// normal Cancel.
func (s *Subscription) Err() error {
	s.sub.mu.Lock()
	defer s.sub.mu.Unlock()
	return s.sub.err
}

// Cancel stops the subscription and closes its Updates channel.
func (s *Subscription) Cancel() { s.sub.engine.unsubscribe(s.sub.id, nil) }

type subscription struct {
	id     int
	engine *Engine
	stmt   string
	state  *queryState
	out    chan Update
	done   chan struct{}

	mu  sync.Mutex
	err error
}

// Engine hosts multiple continuous queries over one shared stream.
// Each query keeps its own state and progress; Publish never blocks on
// a slow subscriber unless Config.BlockOnFull is set.
type Engine struct {
	cfg Config

	mu      sync.Mutex
	subs    map[int]*subscription
	nextID  int
	lastSeq int64
	closed  bool

	// procMu serializes event/watermark processing so that the order of
	// Publish calls defines a single deterministic processing order.
	procMu sync.Mutex
}

// New creates an Engine.
func New(cfg Config) *Engine {
	if cfg.SubBuffer <= 0 {
		cfg.SubBuffer = defaultSubBuffer
	}
	return &Engine{
		cfg:  cfg,
		subs: map[int]*subscription{},
	}
}

// Subscribe registers a continuous query and returns its Subscription.
// Parse and semantic-validation errors are returned immediately and are
// distinguishable via *sqlparse.Error.
func (e *Engine) Subscribe(stmt string) (*Subscription, error) {
	q, err := sqlparse.Parse(stmt)
	if err != nil {
		return nil, err
	}
	st, err := newQueryState(q, e.cfg.AllowedLateness)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errors.New("engine: closed")
	}
	e.nextID++
	sub := &subscription{
		id:     e.nextID,
		engine: e,
		stmt:   stmt,
		state:  st,
		out:    make(chan Update, e.cfg.SubBuffer),
		done:   make(chan struct{}),
	}
	e.subs[sub.id] = sub
	return &Subscription{Updates: sub.out, sub: sub}, nil
}

// Publish feeds one item (event or watermark) into the stream.
// All registered queries process it with their own state and progress.
// It returns an error only for invalid input or a closed engine;
// per-subscription backpressure is reported on the Subscription itself.
func (e *Engine) Publish(it event.Item) error {
	e.procMu.Lock()
	defer e.procMu.Unlock()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("engine: closed")
	}
	subs := make([]*subscription, 0, len(e.subs))
	for _, s := range e.subs {
		subs = append(subs, s)
	}
	e.mu.Unlock()

	switch {
	case it.Event != nil:
		if it.Event.Seq > e.lastSeq {
			e.lastSeq = it.Event.Seq
		}
		for _, s := range subs {
			for _, u := range s.state.processEvent(it.Event) {
				e.deliver(s, u)
			}
		}
	case it.Watermark != nil:
		if it.Watermark.Seq > e.lastSeq {
			e.lastSeq = it.Watermark.Seq
		}
		for _, s := range subs {
			for _, u := range s.state.advanceWatermark(*it.Watermark) {
				e.deliver(s, u)
			}
		}
	default:
		return errors.New("engine: empty item")
	}
	return nil
}

// deliver sends one update to a subscription, honouring the
// backpressure policy.
func (e *Engine) deliver(s *subscription, u Update) {
	select {
	case <-s.done:
		return
	default:
	}
	if e.cfg.BlockOnFull {
		select {
		case s.out <- u:
		case <-s.done:
		}
		return
	}
	select {
	case s.out <- u:
	default:
		// Buffer full: terminate this subscription with a
		// distinguishable backpressure error; others continue.
		e.unsubscribe(s.id, ErrBackpressure)
	}
}

// unsubscribe removes a subscription, records its terminal error and
// closes its channels.
func (e *Engine) unsubscribe(id int, err error) {
	e.mu.Lock()
	s, ok := e.subs[id]
	if ok {
		delete(e.subs, id)
	}
	e.mu.Unlock()
	if !ok {
		return
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	close(s.done)
	close(s.out)
}

// Close terminates all subscriptions.
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	ids := make([]int, 0, len(e.subs))
	for id := range e.subs {
		ids = append(ids, id)
	}
	e.mu.Unlock()
	for _, id := range ids {
		e.unsubscribe(id, nil)
	}
}

// Compile validates a statement without subscribing.
func Compile(stmt string) (*sqlparse.Query, error) {
	q, err := sqlparse.Parse(stmt)
	if err != nil {
		return nil, err
	}
	if _, err := newQueryState(q, 0); err != nil {
		return nil, err
	}
	return q, nil
}
