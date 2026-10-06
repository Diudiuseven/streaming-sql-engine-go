// Package engine executes continuous queries over an event stream.
package engine

import "time"

// Row is one output record of a continuous query.
type Row struct {
	// Cols maps output column names (aliases) to values.
	Cols map[string]any
}

// UpdateKind distinguishes the kinds of emitted results.
type UpdateKind int

const (
	// UpdateRow is an incremental (re-)materialization of a group whose
	// aggregate changed. The row carries the full current values.
	UpdateRow UpdateKind = iota
	// UpdateLateEvent reports an event that arrived beyond the allowed
	// lateness and was not counted (distinguishable late-arrival verdict).
	UpdateLateEvent
	// UpdateDuplicateEvent reports an event dropped by deduplication.
	UpdateDuplicateEvent
)

// Update is one emitted unit from a running query.
type Update struct {
	Kind UpdateKind
	// Final is true for the last, ordered emission of a closed window.
	// Non-final UpdateRow updates are incremental upserts: the latest
	// update per (window, group) is the current result.
	Final bool
	// Window bounds, set for windowed aggregates.
	WindowStart time.Time
	WindowEnd   time.Time
	// Row is set for UpdateRow.
	Row Row
	// EventID is set for UpdateLateEvent / UpdateDuplicateEvent.
	EventID string
}
