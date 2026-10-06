package engine

import "time"

// Window is a half-open event-time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
}

// windowKind mirrors sqlparse.WindowKind without import cycles.
type windowKind int

const (
	kindTumble windowKind = iota
	kindHop
)

// assignWindows returns all windows containing event time t.
// Windows are aligned to the Unix epoch, so results are deterministic
// and independent of when the engine started.
func assignWindows(kind windowKind, size, slide time.Duration, t time.Time) []Window {
	ns := t.UnixNano()
	switch kind {
	case kindTumble:
		s := ns - mod(ns, int64(size))
		return []Window{{Start: time.Unix(0, s).UTC(), End: time.Unix(0, s+int64(size)).UTC()}}
	case kindHop:
		n := int(size / slide)
		latest := ns - mod(ns, int64(slide))
		wins := make([]Window, 0, n)
		for k := 0; k < n; k++ {
			s := latest - int64(k)*int64(slide)
			e := s + int64(size)
			if s <= ns && ns < e {
				wins = append(wins, Window{Start: time.Unix(0, s).UTC(), End: time.Unix(0, e).UTC()})
			}
		}
		return wins
	}
	return nil
}

// mod is a floor-mod that handles negative values correctly.
func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
