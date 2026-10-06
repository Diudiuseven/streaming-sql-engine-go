package streamsql

import (
	"errors"
	"fmt"
)

// ErrorKind classifies engine errors so callers can distinguish them.
type ErrorKind int

const (
	ErrKindParse ErrorKind = iota
	ErrKindUnsupported
	ErrKindLateEvent
	ErrKindDuplicate
	ErrKindBackpressure
	ErrKindCheckpoint
	ErrKindInternal
)

func (k ErrorKind) String() string {
	switch k {
	case ErrKindParse:
		return "parse error"
	case ErrKindUnsupported:
		return "unsupported feature"
	case ErrKindLateEvent:
		return "late event"
	case ErrKindDuplicate:
		return "duplicate event"
	case ErrKindBackpressure:
		return "backpressure"
	case ErrKindCheckpoint:
		return "checkpoint error"
	default:
		return "internal error"
	}
}

// Error is the single error type produced by the engine; Kind is
// machine-distinguishable via IsKind.
type Error struct {
	Kind ErrorKind
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Msg, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Msg)
}

func (e *Error) Unwrap() error { return e.Err }

func errorf(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// IsKind reports whether err (or any error it wraps) is an *Error of the
// given kind.
func IsKind(err error, kind ErrorKind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == kind
	}
	return false
}
