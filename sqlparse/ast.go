// Package sqlparse parses the supported SQL subset into an AST.
package sqlparse

import "time"

// WindowKind distinguishes tumbling and hopping windows.
type WindowKind int

const (
	NoWindow WindowKind = iota
	Tumble
	Hop
)

// WindowSpec describes an event-time window over a time column.
type WindowSpec struct {
	Kind    WindowKind
	Size    time.Duration
	Slide   time.Duration // Hop only
	TimeCol string
}

// AggFunc is a supported aggregate function name.
type AggFunc string

const (
	AggCount AggFunc = "COUNT"
	AggSum   AggFunc = "SUM"
	AggMin   AggFunc = "MIN"
	AggMax   AggFunc = "MAX"
	AggAvg   AggFunc = "AVG"
)

// SelectItem is one entry in the SELECT list.
type SelectItem struct {
	// Star is true for SELECT *.
	Star bool
	// Col is a plain column reference (when Agg == "").
	Col string
	// Agg is the aggregate function name, empty for plain columns.
	Agg AggFunc
	// AggCol is the argument column of the aggregate ("*" for COUNT(*)).
	AggCol string
	// WindowFunc is "WINDOW_START" or "WINDOW_END" when the item refers
	// to the window bounds pseudo-columns.
	WindowFunc string
	// Alias is the output column name; defaults derived when empty.
	Alias string
}

// OrderItem is one ORDER BY key.
type OrderItem struct {
	Col  string // output column name or alias
	Desc bool
}

// Query is a parsed SELECT statement of the supported subset.
type Query struct {
	Items   []SelectItem
	Source  string
	Where   Expr // nil when absent
	GroupBy []string
	Window  WindowSpec // Window.Kind == NoWindow when absent
	OrderBy []OrderItem
	Limit   int // -1 when absent
}

// Expr is a boolean/scalar expression tree for WHERE.
type Expr interface{ exprNode() }

// BinaryExpr is a comparison or logical combination.
type BinaryExpr struct {
	Op          string // =, !=, <, <=, >, >=, AND, OR
	Left, Right Expr
}

func (*BinaryExpr) exprNode() {}

// ColRef references an event field.
type ColRef struct{ Name string }

func (*ColRef) exprNode() {}

// Literal is a constant value.
type Literal struct{ Val any }

func (*Literal) exprNode() {}
