package streamsql

import (
	"fmt"
	"strings"
)

// compile validates a parsed statement and builds an executable plan.
// Anything outside the supported subset is rejected with
// ErrKindUnsupported; malformed statements yield ErrKindParse. Nothing is
// silently ignored.
func compile(stmt *Statement) (*plan, error) {
	p := &plan{stmt: stmt, window: stmt.Window}

	// SELECT * is rejected (except inside COUNT(*)): output schema must be
	// explicit and stable.
	for _, it := range stmt.Select {
		if _, ok := it.Expr.(Star); ok {
			return nil, errorf(ErrKindUnsupported, "SELECT * is not supported; list columns explicitly")
		}
	}

	// collect aggregate calls from SELECT, reject calls everywhere else
	if err := rejectCalls(stmt.Where, "WHERE"); err != nil {
		return nil, err
	}
	hasAgg := false
	for i := range stmt.Select {
		calls, err := collectCalls(stmt.Select[i].Expr)
		if err != nil {
			return nil, err
		}
		if len(calls) > 0 {
			hasAgg = true
		}
	}

	grouped := hasAgg || len(stmt.GroupBy) > 0 || stmt.Window != nil
	p.stateless = !grouped
	if grouped && stmt.Window == nil {
		return nil, errorf(ErrKindUnsupported, "aggregations require an event-time window: use GROUP BY TUMBLE(ts, '...') or HOP(ts, '...', '...')")
	}

	// group keys must be plain column references
	for _, e := range stmt.GroupBy {
		id, ok := e.(Ident)
		if !ok {
			return nil, errorf(ErrKindUnsupported, "GROUP BY supports plain column references only")
		}
		p.groupKeys = append(p.groupKeys, id.Name)
	}

	// rewrite SELECT: aggregate calls become marker identifiers
	for i := range stmt.Select {
		rew, err := p.rewriteExpr(stmt.Select[i].Expr, grouped)
		if err != nil {
			return nil, err
		}
		p.sel = append(p.sel, SelectItem{Expr: rew, Alias: stmt.Select[i].Alias})
		p.schema = append(p.schema, columnName(stmt.Select[i]))
	}

	// validate non-aggregate select identifiers against group keys
	if grouped {
		for _, it := range p.sel {
			if err := p.validateScopedIdents(it.Expr); err != nil {
				return nil, err
			}
		}
	}

	// ORDER BY: each term must name an output column
	for _, ob := range stmt.OrderBy {
		id, ok := ob.Expr.(Ident)
		if !ok {
			return nil, errorf(ErrKindUnsupported, "ORDER BY supports output column names only")
		}
		idx := -1
		for i, name := range p.schema {
			if strings.EqualFold(name, id.Name) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, errorf(ErrKindParse, "ORDER BY column %q is not in the SELECT list", id.Name)
		}
		p.orderIdx = append(p.orderIdx, idx)
		p.orderDesc = append(p.orderDesc, ob.Desc)
	}

	if stmt.Limit != nil {
		p.limit = *stmt.Limit
	}
	return p, nil
}

// rewriteExpr replaces aggregate calls with marker identifiers and records
// them in p.aggCalls. Any other call is rejected.
func (p *plan) rewriteExpr(e Expr, grouped bool) (Expr, error) {
	switch x := e.(type) {
	case Call:
		up := strings.ToUpper(x.Name)
		if !aggregateNames[up] {
			return nil, errorf(ErrKindUnsupported, "function %s is not supported (supported aggregates: COUNT, SUM, AVG, MIN, MAX)", up)
		}
		if !grouped {
			return nil, errorf(ErrKindInternal, "aggregate without grouping")
		}
		if err := validateAggArgs(x); err != nil {
			return nil, err
		}
		idx := len(p.aggCalls)
		p.aggCalls = append(p.aggCalls, x)
		return Ident{Name: fmt.Sprintf("%s%d", aggMarkerPrefix, idx)}, nil
	case Binary:
		l, err := p.rewriteExpr(x.L, grouped)
		if err != nil {
			return nil, err
		}
		r, err := p.rewriteExpr(x.R, grouped)
		if err != nil {
			return nil, err
		}
		return Binary{Op: x.Op, L: l, R: r}, nil
	case Unary:
		u, err := p.rewriteExpr(x.X, grouped)
		if err != nil {
			return nil, err
		}
		return Unary{Op: x.Op, X: u}, nil
	}
	return e, nil
}

func validateAggArgs(c Call) error {
	up := strings.ToUpper(c.Name)
	if c.Star {
		if up != "COUNT" {
			return errorf(ErrKindParse, "%s(*) is not valid; only COUNT(*) takes *", up)
		}
		if len(c.Args) != 0 {
			return errorf(ErrKindParse, "COUNT(*) takes no arguments")
		}
		return nil
	}
	if len(c.Args) != 1 {
		return errorf(ErrKindParse, "%s expects exactly one argument", up)
	}
	if err := rejectCalls(c.Args[0], up+" argument"); err != nil {
		return err
	}
	return nil
}

// validateScopedIdents ensures every identifier in a grouped select
// expression is a group key, a window bound, or an aggregate marker.
func (p *plan) validateScopedIdents(e Expr) error {
	switch x := e.(type) {
	case Ident:
		name := x.Name
		if strings.HasPrefix(name, aggMarkerPrefix) {
			return nil
		}
		if strings.EqualFold(name, "window_start") || strings.EqualFold(name, "window_end") {
			return nil
		}
		for _, k := range p.groupKeys {
			if k == name || stripQualifier(k) == name || k == stripQualifier(name) {
				return nil
			}
		}
		return errorf(ErrKindParse, "column %q must appear in GROUP BY or be aggregated", name)
	case Binary:
		if err := p.validateScopedIdents(x.L); err != nil {
			return err
		}
		return p.validateScopedIdents(x.R)
	case Unary:
		return p.validateScopedIdents(x.X)
	}
	return nil
}

func rejectCalls(e Expr, where string) error {
	if e == nil {
		return nil
	}
	switch x := e.(type) {
	case Call:
		return errorf(ErrKindUnsupported, "function calls are not allowed in %s", where)
	case Binary:
		if err := rejectCalls(x.L, where); err != nil {
			return err
		}
		return rejectCalls(x.R, where)
	case Unary:
		return rejectCalls(x.X, where)
	}
	return nil
}

func collectCalls(e Expr) ([]Call, error) {
	var out []Call
	var walk func(x Expr) error
	walk = func(x Expr) error {
		switch n := x.(type) {
		case Call:
			out = append(out, n)
			for _, a := range n.Args {
				if err := walk(a); err != nil {
					return err
				}
			}
		case Binary:
			if err := walk(n.L); err != nil {
				return err
			}
			return walk(n.R)
		case Unary:
			return walk(n.X)
		}
		return nil
	}
	if err := walk(e); err != nil {
		return nil, err
	}
	return out, nil
}

// columnName derives the output column name: the alias if given, otherwise
// a canonical rendering of the expression.
func columnName(it SelectItem) string {
	if it.Alias != "" {
		return it.Alias
	}
	return exprString(it.Expr)
}

func exprString(e Expr) string {
	switch x := e.(type) {
	case Ident:
		return x.Name
	case Literal:
		return fmt.Sprintf("%v", x.Value)
	case Star:
		return "*"
	case Call:
		parts := make([]string, len(x.Args))
		for i, a := range x.Args {
			parts[i] = exprString(a)
		}
		if x.Star {
			parts = []string{"*"}
		}
		return strings.ToUpper(x.Name) + "(" + strings.Join(parts, ",") + ")"
	case Binary:
		return exprString(x.L) + x.Op + exprString(x.R)
	case Unary:
		return x.Op + exprString(x.X)
	}
	return "?"
}
