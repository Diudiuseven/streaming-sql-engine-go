// Package event defines the event and watermark types flowing through the engine.
package event

import "time"

// Value is a dynamically typed field value carried by an event.
// Supported kinds: nil, bool, int64, float64, string, time.Time.
type Value = any

// Event is a single record in the stream.
type Event struct {
	// Seq is the monotonically increasing arrival sequence assigned by the
	// source. It defines the total arrival order and is used for checkpoints.
	Seq int64
	// ID uniquely identifies the event for deduplication. Events with the
	// same ID delivered more than once are counted exactly once.
	ID string
	// EventTime is the event-time timestamp used for windowing.
	EventTime time.Time
	// Fields holds the payload accessible from SQL statements.
	Fields map[string]Value
}

// Watermark announces that no events with EventTime <= T are expected
// (except allowed late arrivals). It drives window triggering.
type Watermark struct {
	Seq int64
	T   time.Time
}

// Item is a unit of data flowing through the pipeline: exactly one of
// Event or Watermark is set.
type Item struct {
	Event     *Event
	Watermark *Watermark
}
