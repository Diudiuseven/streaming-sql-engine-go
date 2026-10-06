// Package checkpoint provides atomic snapshot persistence and recovery.
package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotFound is returned when no committed checkpoint exists.
var ErrNotFound = errors.New("checkpoint: no committed checkpoint found")

const (
	committedName = "checkpoint.bin"
	tmpName       = "checkpoint.bin.tmp"
)

// Store persists snapshots atomically: Commit writes to a temporary
// file, fsyncs it, then renames it over the committed file, so a crash
// at any point never leaves a half-written readable state.
type Store struct {
	dir string
}

// NewStore creates a Store rooted at dir, creating it if needed.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Commit atomically persists data as the latest checkpoint.
func (s *Store) Commit(data []byte) error {
	tmp := filepath.Join(s.dir, tmpName)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: open tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: close tmp: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, committedName)); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	// Fsync the directory so the rename itself is durable.
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("checkpoint: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("checkpoint: sync dir: %w", err)
	}
	return nil
}

// Load returns the latest committed checkpoint. A leftover temporary
// file from an interrupted Commit is ignored and removed.
func (s *Store) Load() ([]byte, error) {
	tmp := filepath.Join(s.dir, tmpName)
	if _, err := os.Stat(tmp); err == nil {
		os.Remove(tmp) // interrupted commit: discard the partial file
	}
	data, err := os.ReadFile(filepath.Join(s.dir, committedName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read: %w", err)
	}
	return data, nil
}
