package streamsql

// Event is a single input record. Timestamp is event time in epoch
// milliseconds. ID must be unique per logical event; duplicates (same ID)
// are detected and reported, never double-counted.
type Event struct {
	ID        string
	Timestamp int64
	Fields    map[string]any
}

// Row is one output record. Values align with the query's output schema
// (Query.Schema). Final is true when the row is emitted because its window
// closed (watermark passed the window end).
type Row struct {
	Values []any
	Final  bool
}

// Batch is a set of rows emitted together after processing one input event
// (or after the watermark advancement triggered by it). Rows are sorted
// according to the query's ORDER BY plus the documented tie-break rules.
type Batch struct {
	Query string
	Rows  []Row
}
