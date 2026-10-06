package sqlparse

import (
	"strconv"
	"strings"
	"time"
)

// unsupportedKeywords are valid SQL constructs outside the supported
// subset. Encountering one yields ErrUnsupported, not ErrParse.
var unsupportedKeywords = map[string]bool{
	"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"CROSS": true, "ON": true, "HAVING": true, "UNION": true,
	"INTERSECT": true, "EXCEPT": true, "DISTINCT": true, "OVER": true,
	"PARTITION": true, "CASE": true, "WHEN": true, "IN": true,
	"BETWEEN": true, "LIKE": true, "IS": true, "NOT": true, "NULL": true,
	"EXISTS": true, "WITH": true, "INSERT": true, "UPDATE": true,
	"DELETE": true, "CREATE": true, "DROP": true, "ALTER": true,
	"VALUES": true, "INTO": true, "SET": true,
}

// aggregate and window function names recognized by the parser.
var aggFuncs = map[string]AggFunc{
	"COUNT": AggCount, "SUM": AggSum, "MIN": AggMin, "MAX": AggMax, "AVG": AggAvg,
}

type parser struct {
	toks []token
	i    int
}

// Parse parses a SELECT statement of the supported subset.
// It returns *Error with Kind ErrParse for malformed input and
// ErrUnsupported for valid SQL outside the supported subset.
func Parse(stmt string) (*Query, error) {
	toks, err := lex(stmt)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tokEOF {
		return nil, parseErr(t.pos, "unexpected trailing input %q", t.text)
	}
	return q, nil
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) next() token { t := p.toks[p.i]; p.i++; return t }

func (p *parser) expectIdent(kw string) (token, error) {
	t := p.peek()
	if t.kind != tokIdent || !strings.EqualFold(t.text, kw) {
		return t, parseErr(t.pos, "expected %s, got %q", kw, t.text)
	}
	return p.next(), nil
}

func (p *parser) checkUnsupported(t token) error {
	if t.kind == tokIdent && unsupportedKeywords[strings.ToUpper(t.text)] {
		return unsupportedErr(t.pos, "%s is not supported by this engine", strings.ToUpper(t.text))
	}
	return nil
}

func (p *parser) parseQuery() (*Query, error) {
	t := p.peek()
	if err := p.checkUnsupported(t); err != nil {
		return nil, err
	}
	if _, err := p.expectIdent("SELECT"); err != nil {
		if t.kind == tokIdent {
			return nil, unsupportedErr(t.pos, "only SELECT statements are supported, got %q", t.text)
		}
		return nil, parseErr(t.pos, "expected SELECT")
	}
	q := &Query{Limit: -1}
	if err := p.parseSelectList(q); err != nil {
		return nil, err
	}
	if _, err := p.expectIdent("FROM"); err != nil {
		return nil, err
	}
	src := p.next()
	if src.kind != tokIdent {
		return nil, parseErr(src.pos, "expected source name after FROM")
	}
	if err := p.checkUnsupported(src); err != nil {
		return nil, err
	}
	q.Source = src.text
	// Optional clauses.
	for {
		t := p.peek()
		if t.kind != tokIdent {
			return q, nil
		}
		if err := p.checkUnsupported(t); err != nil {
			return nil, err
		}
		switch strings.ToUpper(t.text) {
		case "WHERE":
			p.next()
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			q.Where = expr
		case "GROUP":
			p.next()
			if _, err := p.expectIdent("BY"); err != nil {
				return nil, err
			}
			if err := p.parseGroupBy(q); err != nil {
				return nil, err
			}
		case "ORDER":
			p.next()
			if _, err := p.expectIdent("BY"); err != nil {
				return nil, err
			}
			if err := p.parseOrderBy(q); err != nil {
				return nil, err
			}
		case "LIMIT":
			p.next()
			n := p.next()
			if n.kind != tokNumber {
				return nil, parseErr(n.pos, "expected number after LIMIT")
			}
			v, err := strconv.Atoi(n.text)
			if err != nil || v < 0 {
				return nil, parseErr(n.pos, "invalid LIMIT value %q", n.text)
			}
			q.Limit = v
		default:
			return nil, parseErr(t.pos, "unexpected clause %q", t.text)
		}
	}
}

func (p *parser) parseSelectList(q *Query) error {
	for {
		t := p.peek()
		if err := p.checkUnsupported(t); err != nil {
			return err
		}
		item := SelectItem{}
		switch {
		case t.kind == tokStar:
			p.next()
			item.Star = true
		case t.kind == tokIdent:
			name := strings.ToUpper(t.text)
			if fn, ok := aggFuncs[name]; ok {
				p.next()
				if _, err := p.expectLParen(); err != nil {
					return err
				}
				arg := p.next()
				switch {
				case arg.kind == tokStar && fn == AggCount:
					item.AggCol = "*"
				case arg.kind == tokIdent:
					item.AggCol = arg.text
				default:
					return parseErr(arg.pos, "invalid aggregate argument %q", arg.text)
				}
				if err := p.expectRParen(); err != nil {
					return err
				}
				item.Agg = fn
			} else if name == "WINDOW_START" || name == "WINDOW_END" {
				p.next()
				if _, err := p.expectLParen(); err != nil {
					return err
				}
				if err := p.expectRParen(); err != nil {
					return err
				}
				item.WindowFunc = name
			} else {
				p.next()
				item.Col = t.text
			}
		default:
			return parseErr(t.pos, "expected column, function or * in SELECT list, got %q", t.text)
		}
		// Optional alias: AS ident or bare ident.
		nt := p.peek()
		if nt.kind == tokIdent && strings.EqualFold(nt.text, "AS") {
			p.next()
			a := p.next()
			if a.kind != tokIdent {
				return parseErr(a.pos, "expected alias after AS")
			}
			item.Alias = a.text
		}
		q.Items = append(q.Items, item)
		if p.peek().kind != tokComma {
			return nil
		}
		p.next()
	}
}

func (p *parser) expectLParen() (token, error) {
	t := p.peek()
	if t.kind != tokLParen {
		return t, parseErr(t.pos, "expected '('")
	}
	return p.next(), nil
}

func (p *parser) expectRParen() error {
	t := p.peek()
	if t.kind != tokRParen {
		return parseErr(t.pos, "expected ')'")
	}
	p.next()
	return nil
}

// parseDurationLit parses a string literal as a Go duration ("5m", "1h30m").
func parseDurationLit(t token) (time.Duration, error) {
	if t.kind != tokString {
		return 0, parseErr(t.pos, "expected duration string like '5m'")
	}
	d, err := time.ParseDuration(t.text)
	if err != nil || d <= 0 {
		return 0, parseErr(t.pos, "invalid duration %q", t.text)
	}
	return d, nil
}

func (p *parser) parseGroupBy(q *Query) error {
	for {
		t := p.next()
		if t.kind != tokIdent {
			return parseErr(t.pos, "expected column or window function in GROUP BY")
		}
		name := strings.ToUpper(t.text)
		if name == "TUMBLE" || name == "HOP" {
			if q.Window.Kind != NoWindow {
				return unsupportedErr(t.pos, "only one window function is allowed")
			}
			spec, err := p.parseWindow(name, t.pos)
			if err != nil {
				return err
			}
			q.Window = spec
		} else {
			if err := p.checkUnsupported(t); err != nil {
				return err
			}
			q.GroupBy = append(q.GroupBy, t.text)
		}
		if p.peek().kind != tokComma {
			return nil
		}
		p.next()
	}
}

func (p *parser) parseWindow(fn string, pos int) (WindowSpec, error) {
	spec := WindowSpec{Kind: Tumble}
	if fn == "HOP" {
		spec.Kind = Hop
	}
	if _, err := p.expectLParen(); err != nil {
		return spec, err
	}
	col := p.next()
	if col.kind != tokIdent {
		return spec, parseErr(col.pos, "expected time column in %s()", fn)
	}
	spec.TimeCol = col.text
	if p.peek().kind != tokComma {
		return spec, parseErr(p.peek().pos, "expected ',' in %s()", fn)
	}
	p.next()
	size, err := parseDurationLit(p.next())
	if err != nil {
		return spec, err
	}
	spec.Size = size
	if spec.Kind == Hop {
		if p.peek().kind != tokComma {
			return spec, parseErr(p.peek().pos, "HOP requires a slide argument")
		}
		p.next()
		slide, err := parseDurationLit(p.next())
		if err != nil {
			return spec, err
		}
		if size%slide != 0 {
			return spec, unsupportedErr(pos, "HOP size must be a multiple of slide")
		}
		spec.Slide = slide
	}
	if err := p.expectRParen(); err != nil {
		return spec, err
	}
	return spec, nil
}

func (p *parser) parseOrderBy(q *Query) error {
	for {
		t := p.next()
		if t.kind != tokIdent {
			return parseErr(t.pos, "expected column in ORDER BY")
		}
		item := OrderItem{Col: t.text}
		if nt := p.peek(); nt.kind == tokIdent {
			switch strings.ToUpper(nt.text) {
			case "DESC":
				p.next()
				item.Desc = true
			case "ASC":
				p.next()
			}
		}
		q.OrderBy = append(q.OrderBy, item)
		if p.peek().kind != tokComma {
			return nil
		}
		p.next()
	}
}

// parseExpr parses OR > AND > comparison > primary.
func (p *parser) parseExpr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tokIdent && strings.EqualFold(t.text, "OR") {
			p.next()
			right, err := p.parseAnd()
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Op: "OR", Left: left, Right: right}
			continue
		}
		return left, nil
	}
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseCmp()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tokIdent && strings.EqualFold(t.text, "AND") {
			p.next()
			right, err := p.parseCmp()
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Op: "AND", Left: left, Right: right}
			continue
		}
		return left, nil
	}
}

func (p *parser) parseCmp() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind != tokOp {
		if err := p.checkUnsupported(t); err != nil {
			return nil, err
		}
		return nil, parseErr(t.pos, "expected comparison operator")
	}
	p.next()
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Op: t.text, Left: left, Right: right}, nil
}

func (p *parser) parsePrimary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case tokIdent:
		if err := p.checkUnsupported(t); err != nil {
			return nil, err
		}
		if strings.EqualFold(t.text, "TRUE") {
			return &Literal{Val: true}, nil
		}
		if strings.EqualFold(t.text, "FALSE") {
			return &Literal{Val: false}, nil
		}
		return &ColRef{Name: t.text}, nil
	case tokNumber:
		if strings.Contains(t.text, ".") {
			f, err := strconv.ParseFloat(t.text, 64)
			if err != nil {
				return nil, parseErr(t.pos, "invalid number %q", t.text)
			}
			return &Literal{Val: f}, nil
		}
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, parseErr(t.pos, "invalid number %q", t.text)
		}
		return &Literal{Val: n}, nil
	case tokString:
		return &Literal{Val: t.text}, nil
	case tokLParen:
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectRParen(); err != nil {
			return nil, err
		}
		return e, nil
	}
	return nil, parseErr(t.pos, "unexpected token %q in expression", t.text)
}
