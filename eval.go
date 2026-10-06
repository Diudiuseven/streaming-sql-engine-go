package streamsql

import "strings"

// lookup resolves a column name to a value. ok=false means "missing",
// which evaluates to nil (SQL-ish NULL semantics for comparisons).
type lookup func(name string) (any, bool)

// fieldLookup resolves identifiers against event fields; a qualifier
// prefix ("s.x") is tolerated and stripped if the full name is absent.
func fieldLookup(fields map[string]any) lookup {
	return func(name string) (any, bool) {
		if v, ok := fields[name]; ok {
			return v, true
		}
		if i := strings.LastIndex(name, "."); i >= 0 {
			v, ok := fields[name[i+1:]]
			return v, ok
		}
		return nil, false
	}
}

// evalExpr evaluates a scalar (non-aggregate) expression.
func evalExpr(e Expr, lk lookup) (any, error) {
	switch x := e.(type) {
	case Literal:
		return x.Value, nil
	case Ident:
		v, _ := lk(x.Name)
		return v, nil
	case Star:
		return nil, errorf(ErrKindInternal, "* is only valid in SELECT list or COUNT(*)")
	case Call:
		return nil, errorf(ErrKindUnsupported, "function %s cannot be used here", x.Name)
	case Unary:
		v, err := evalExpr(x.X, lk)
		if err != nil {
			return nil, err
		}
		switch x.Op {
		case "NOT":
			b, ok := v.(bool)
			if !ok {
				return nil, errorf(ErrKindParse, "NOT requires a boolean operand")
			}
			return !b, nil
		case "-":
			switch n := v.(type) {
			case int64:
				return -n, nil
			case float64:
				return -n, nil
			}
			return nil, errorf(ErrKindParse, "unary - requires a numeric operand")
		}
	case Binary:
		return evalBinary(x, lk)
	}
	return nil, errorf(ErrKindInternal, "unknown expression node %T", e)
}

func evalBinary(b Binary, lk lookup) (any, error) {
	// short-circuit boolean operators
	if b.Op == "AND" || b.Op == "OR" {
		l, err := evalExpr(b.L, lk)
		if err != nil {
			return nil, err
		}
		lb, ok := l.(bool)
		if !ok {
			return nil, errorf(ErrKindParse, "%s requires boolean operands", b.Op)
		}
		if b.Op == "AND" && !lb {
			return false, nil
		}
		if b.Op == "OR" && lb {
			return true, nil
		}
		r, err := evalExpr(b.R, lk)
		if err != nil {
			return nil, err
		}
		rb, ok := r.(bool)
		if !ok {
			return nil, errorf(ErrKindParse, "%s requires boolean operands", b.Op)
		}
		if b.Op == "AND" {
			return lb && rb, nil
		}
		return lb || rb, nil
	}
	l, err := evalExpr(b.L, lk)
	if err != nil {
		return nil, err
	}
	r, err := evalExpr(b.R, lk)
	if err != nil {
		return nil, err
	}
	switch b.Op {
	case "+", "-", "*", "/":
		return arith(b.Op, l, r)
	case "=", "!=", "<", "<=", ">", ">=":
		return compare(b.Op, l, r), nil
	}
	return nil, errorf(ErrKindInternal, "unknown operator %q", b.Op)
}

func arith(op string, l, r any) (any, error) {
	if l == nil || r == nil {
		return nil, nil
	}
	li, lok := l.(int64)
	ri, rok := r.(int64)
	if lok && rok {
		switch op {
		case "+":
			return li + ri, nil
		case "-":
			return li - ri, nil
		case "*":
			return li * ri, nil
		case "/":
			if ri == 0 {
				return nil, nil // division by zero yields nil, documented
			}
			return li / ri, nil
		}
	}
	lf, lok := toNumber(l)
	rf, rok := toNumber(r)
	if !lok || !rok {
		return nil, errorf(ErrKindParse, "arithmetic requires numeric operands, got %T and %T", l, r)
	}
	switch op {
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "/":
		if rf == 0 {
			return nil, nil
		}
		return lf / rf, nil
	}
	return nil, errorf(ErrKindInternal, "unknown arithmetic operator %q", op)
}

// compare gives a deterministic, total ordering within a type: numbers
// numerically, strings lexically, bools false<true. Cross-type comparisons
// and nil operands are false except != which is true for differing values.
func compare(op string, l, r any) bool {
	if l == nil || r == nil {
		switch op {
		case "=":
			return l == nil && r == nil
		case "!=":
			return !(l == nil && r == nil)
		}
		return false
	}
	c, ok := compareValues(l, r)
	if !ok {
		// differing, non-numeric types: only = and != are defined
		if op == "=" {
			return false
		}
		if op == "!=" {
			return true
		}
		return false
	}
	switch op {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}

// compareValues returns -1/0/1 for same-kind values (int64 and float64 are
// one numeric kind). ok=false for incomparable kinds.
func compareValues(l, r any) (int, bool) {
	if lf, ok := toNumber(l); ok {
		if rf, ok := toNumber(r); ok {
			switch {
			case lf < rf:
				return -1, true
			case lf > rf:
				return 1, true
			}
			return 0, true
		}
		return 0, false
	}
	switch lv := l.(type) {
	case string:
		rv, ok := r.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(lv, rv), true
	case bool:
		rv, ok := r.(bool)
		if !ok {
			return 0, false
		}
		switch {
		case lv == rv:
			return 0, true
		case !lv:
			return -1, true
		}
		return 1, true
	}
	return 0, false
}
