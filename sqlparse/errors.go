package sqlparse

import "fmt"

// ErrorKind distinguishes parse failures from unsupported-syntax failures.
type ErrorKind int

const (
	// ErrParse means the statement could not be tokenized/parsed.
	ErrParse ErrorKind = iota
	// ErrUnsupported means the statement parsed but uses syntax outside
	// the supported subset.
	ErrUnsupported
)

// Error is a distinguishable SQL error.
type Error struct {
	Kind ErrorKind
	Msg  string
	Pos  int // byte offset in the statement, -1 if unknown
}

func (e *Error) Error() string {
	kind := "parse error"
	if e.Kind == ErrUnsupported {
		kind = "unsupported syntax"
	}
	return fmt.Sprintf("%s at offset %d: %s", kind, e.Pos, e.Msg)
}

func parseErr(pos int, format string, args ...any) *Error {
	return &Error{Kind: ErrParse, Pos: pos, Msg: fmt.Sprintf(format, args...)}
}

func unsupportedErr(pos int, format string, args ...any) *Error {
	return &Error{Kind: ErrUnsupported, Pos: pos, Msg: fmt.Sprintf(format, args...)}
}
