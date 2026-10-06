package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommitLoad(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err != ErrNotFound {
		t.Fatalf("Load before commit = %v, want ErrNotFound", err)
	}
	if err := s.Commit([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit([]byte("two")); err != nil {
		t.Fatal(err)
	}
	data, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "two" {
		t.Fatalf("Load = %q, want %q", data, "two")
	}
}

func TestInterruptedCommitLeavesNoHalfState(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit([]byte("good")); err != nil {
		t.Fatal(err)
	}
	// Simulate a crashed commit: a partial tmp file exists.
	if err := os.WriteFile(filepath.Join(dir, tmpName), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "good" {
		t.Fatalf("Load = %q, want committed %q", data, "good")
	}
	// The partial tmp file must be cleaned up.
	if _, err := os.Stat(filepath.Join(dir, tmpName)); !os.IsNotExist(err) {
		t.Fatal("tmp file not cleaned up")
	}
}
