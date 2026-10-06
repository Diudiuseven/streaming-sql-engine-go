package streamsql

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	checkpointFile  = "checkpoint.snap"
	checkpointTmp   = "checkpoint.snap.tmp"
	checkpointMagic = "SSQLCKPT\x01"
)

// querySnapshot is the serializable state of one query.
type querySnapshot struct {
	Name      string
	SQL       string
	Watermark int64
	MaxSeen   int64
	Groups    map[string]*groupState
	Windows   map[int64]*windowEntry
	SeenIDs   map[string]int64
	Stats     Stats
}

// snapshot is the serializable state of the whole engine.
type snapshot struct {
	Offset  int64
	Queries []*querySnapshot
}

// Checkpoint atomically persists the engine offset and every query's state
// into dir. It first waits until all submitted events are processed, so the
// checkpoint reflects exactly Offset(). Interrupted writes never leave an
// unreadable checkpoint behind: data is written to a temp file, fsynced,
// then renamed over the previous checkpoint.
func (e *Engine) Checkpoint(dir string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errorf(ErrKindCheckpoint, "engine is closed")
	}
	qs := make([]*Query, 0, len(e.queries))
	for _, q := range e.queries {
		qs = append(qs, q)
	}
	offset := e.offset
	e.mu.Unlock()

	// barrier every query so its state is quiescent and reflects all events
	// up to offset
	barriers := make([]chan struct{}, 0, len(qs))
	for _, q := range qs {
		b := make(chan struct{})
		q.in <- qmsg{barrier: b}
		barriers = append(barriers, b)
	}
	for _, b := range barriers {
		<-b
	}

	snap := &snapshot{Offset: offset}
	for _, q := range qs {
		snap.Queries = append(snap.Queries, q.snapshot())
	}

	var buf bytes.Buffer
	buf.WriteString(checkpointMagic)
	payload := new(bytes.Buffer)
	if err := gob.NewEncoder(payload).Encode(snap); err != nil {
		return errorf(ErrKindCheckpoint, "encode snapshot: %v", err)
	}
	crc := crc32.ChecksumIEEE(payload.Bytes())
	var crcBuf [4]byte
	crcBuf[0] = byte(crc >> 24)
	crcBuf[1] = byte(crc >> 16)
	crcBuf[2] = byte(crc >> 8)
	crcBuf[3] = byte(crc)
	buf.Write(crcBuf[:])
	buf.Write(payload.Bytes())

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errorf(ErrKindCheckpoint, "create dir: %v", err)
	}
	tmp := filepath.Join(dir, checkpointTmp)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return errorf(ErrKindCheckpoint, "open temp file: %v", err)
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		os.Remove(tmp)
		return errorf(ErrKindCheckpoint, "write temp file: %v", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return errorf(ErrKindCheckpoint, "fsync: %v", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return errorf(ErrKindCheckpoint, "close temp file: %v", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, checkpointFile)); err != nil {
		os.Remove(tmp)
		return errorf(ErrKindCheckpoint, "rename: %v", err)
	}
	return nil
}

// snapshot captures the query's mutable state. Called after a barrier, so
// the run goroutine is idle.
func (q *Query) snapshot() *querySnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return &querySnapshot{
		Name:      q.name,
		SQL:       q.plan.stmt.Raw,
		Watermark: q.watermark,
		MaxSeen:   q.maxSeen,
		Groups:    q.groups,
		Windows:   q.windows,
		SeenIDs:   q.seenIDs,
		Stats:     q.stats,
	}
}

// HasCheckpoint reports whether a committed checkpoint exists in dir.
func HasCheckpoint(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, checkpointFile))
	return err == nil
}

// RecoverEngine loads the last committed checkpoint from dir and returns an
// engine with all queries restored. The caller must replay events starting
// at Offset()+1; committed output is not produced twice and no event is
// skipped. A missing checkpoint yields ErrKindCheckpoint; a corrupt one
// (e.g. an interrupted write that somehow replaced the file) also yields
// ErrKindCheckpoint and never a half-readable state.
func RecoverEngine(dir string, cfg Config) (*Engine, error) {
	data, err := os.ReadFile(filepath.Join(dir, checkpointFile))
	if err != nil {
		return nil, errorf(ErrKindCheckpoint, "read checkpoint: %v", err)
	}
	if len(data) < len(checkpointMagic)+4 || string(data[:len(checkpointMagic)]) != checkpointMagic {
		return nil, errorf(ErrKindCheckpoint, "bad checkpoint magic")
	}
	body := data[len(checkpointMagic):]
	crc := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
	payload := body[4:]
	if crc32.ChecksumIEEE(payload) != crc {
		return nil, errorf(ErrKindCheckpoint, "checkpoint checksum mismatch")
	}
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&snap); err != nil {
		return nil, errorf(ErrKindCheckpoint, "decode checkpoint: %v", err)
	}

	eng := NewEngine(cfg)
	eng.offset = snap.Offset
	for _, qs := range snap.Queries {
		q, err := newQuery(qs.Name, qs.SQL, eng)
		if err != nil {
			return nil, fmt.Errorf("restore query %q: %w", qs.Name, err)
		}
		q.mu.Lock()
		q.watermark = qs.Watermark
		q.maxSeen = qs.MaxSeen
		q.groups = qs.Groups
		q.windows = qs.Windows
		q.seenIDs = qs.SeenIDs
		q.stats = qs.Stats
		if q.groups == nil {
			q.groups = map[string]*groupState{}
		}
		if q.windows == nil {
			q.windows = map[int64]*windowEntry{}
		}
		if q.seenIDs == nil {
			q.seenIDs = map[string]int64{}
		}
		q.mu.Unlock()
		eng.queries[q.name] = q
	}
	return eng, nil
}
