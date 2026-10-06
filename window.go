package streamsql

import "fmt"

type WindowKind int

const (
	Tumbling WindowKind = iota
	Hopping
)

// WindowSpec describes an event-time window. Size and Slide are
// milliseconds. Tumbling: windows [k*Size, k*Size+Size). Hopping: windows
// of Size starting every Slide.
type WindowSpec struct {
	Kind    WindowKind
	TimeCol string
	Size    int64
	Slide   int64 // hopping only; must be > 0 and <= Size
}

// Window is a half-open interval [Start, End) in event-time millis.
type Window struct{ Start, End int64 }

func (w *WindowSpec) String() string {
	if w.Kind == Tumbling {
		return fmt.Sprintf("TUMBLE(%s,%dms)", w.TimeCol, w.Size)
	}
	return fmt.Sprintf("HOP(%s,%dms,%dms)", w.TimeCol, w.Slide, w.Size)
}

// floorDiv is integer division rounding towards negative infinity, so
// window assignment is well-defined for negative timestamps too.
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// Assign returns all windows containing event time ts, ordered by start
// time. Tumbling: exactly one window. Hopping: ceil(Size/Slide) windows.
func (w *WindowSpec) Assign(ts int64) []Window {
	if w.Kind == Tumbling {
		start := floorDiv(ts, w.Size) * w.Size
		return []Window{{Start: start, End: start + w.Size}}
	}
	// hopping: window k covers [k*Slide, k*Slide+Size); it contains ts iff
	// k*Slide <= ts < k*Slide+Size.
	kmax := floorDiv(ts, w.Slide)
	var out []Window
	for k := kmax; k*w.Slide+w.Size > ts; k-- {
		out = append(out, Window{Start: k * w.Slide, End: k*w.Slide + w.Size})
	}
	// reverse to ascending start order for determinism
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
