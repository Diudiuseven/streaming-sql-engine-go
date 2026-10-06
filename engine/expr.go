package engine

import (
	"fmt"
	"strings"

	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

// evalExpr evaluates a WHERE expression against an event's fields.
// Field lookup is case-insensitive.
func evalExpr(e sqlparse.Expr, fields map[string]any) (any, error) {
	switch ex := e.(type) {
	case *sqlparse.Literal:
		return ex.Val, nil
	case *sqlparse.ColRef:
		for k, v := range fields {
			if strings.EqualFold(k, ex.Name) {
				return v, nil
			}
		}
		return nil, nil // unknown column evaluates to NULL
	case *sqlparse.BinaryExpr:
		switch ex.Op {
		case "AND", "OR":
			l, err := evalExpr(ex.Left, fields)
			if err != nil {
				return nil, err
			}
			lb := truthy(l)
			if ex.Op == "AND" && !lb {
				return false, nil
			}
			if ex.Op == "OR" && lb {
				return true, nil
			}
			r, err := evalExpr(ex.Right, fields)
			if err != nil {
				return nil, err
			}
			return truthy(r), nil
		default: // comparison
			l, err := evalExpr(ex.Left, fields)
			if err != nil {
				return nil, err
			}
			r, err := evalExpr(ex.Right, fields)
			if err != nil {
				return nil, err
			}
			return compare(ex.Op, l, r)
		}
	}
	return nil, fmt.Errorf("unknown expression %T", e)
}

func truthy(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

// compare evaluates a comparison. NULL (nil) operands yield false,
// matching SQL three-valued logic in WHERE position.
func compare(op string, l, r any) (bool, error) {
	if l == nil || r == nil {
		return false, nil
	}
	// Numeric comparison across int64/float64.
	if lf, lok := asFloat(l); lok {
		if rf, rok := asFloat(r); rok {
			switch op {
			case "=":
				return lf == rf, nil
			case "!=":
				return lf != rf, nil
			case "<":
				return lf < rf, nil
			case "<=":
				return lf <= rf, nil
			case ">":
				return lf > rf, nil
			case ">=":
				return lf >= rf, nil
			}
			return false, fmt.Errorf("unknown operator %q", op)
		}
	}
	// String / bool equality and string ordering.
	ls, lok := l.(string)
	rs, rok := r.(string)
	if lok && rok {
		switch op {
		case "=":
			return ls == rs, nil
		case "!=":
			return ls != rs, nil
		case "<":
			return ls < rs, nil
		case "<=":
			return ls <= rs, nil
		case ">":
			return ls > rs, nil
		case ">=":
			return ls >= rs, nil
		}
	}
	if lb, ok := l.(bool); ok {
		if rb, ok := r.(bool); ok {
			switch op {
			case "=":
				return lb == rb, nil
			case "!=":
				return lb != rb, nil
			}
		}
	}
	return false, fmt.Errorf("cannot compare %T with %T", l, r)
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
