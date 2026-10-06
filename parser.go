package streamsql

import (
	"strconv"
	"strings"
	"time"
)

// reservedWords may not be used as bare aliases.
var reservedWords = map[string]bool{
	"FROM": true, "WHERE": true, "GROUP": true, "ORDER": true,
	"LIMIT": true, "AND": true, "OR": true, "NOT": true, "AS": true,
	"ASC": true, "DESC": true, "BY": true,
}

// unsupportedKeywords are recognized SQL features outside the supported
// subset; encountering them yields ErrKindUnsupported, never silent
// degradation.
var unsupportedKeywords = map[string]bool{
	"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"CROSS": true, "ON": true, "HAVING": true, "UNION": true,
	"INTERSECT": true, "EXCEPT": true, "DISTINCT": true, "CASE": true,
	"WHEN": true, "IN": true, "LIKE": true, "BETWEEN": true, "OVER": true,
	"PARTITION": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	"CREATE": true, "DROP": true, "ALTER": true, "EXISTS": true,
	"IS": true, "NULL": true, "CAST": true,
}

var aggregateNames = map[string]bool{
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
}

type parser struct {
	toks []token
	i    int
}

// Parse parses a SELECT statement of the supported subset. Unparseable
// input yields ErrKindParse; valid SQL outside the subset yields
// ErrKindUnsupported.
func Parse(input string) (*Statement, error) {
	toks, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	stmt.Raw = input
	return stmt, nil
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) next() token { t := p.toks[p.i]; p.i++; return t }
func (p *parser) atEOF() bool { return p.toks[p.i].kind == tEOF }
func (p *parser) isIdent(s string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, s)
}

func (p *parser) expectIdent(s string) error {
	if !p.isIdent(s) {
		t := p.peek()
		return errorf(ErrKindParse, "expected %s but found %q at position %d", s, t.text, t.pos)
	}
	p.next()
	return nil
}

func (p *parser) checkUnsupported() error {
	t := p.peek()
	if t.kind == tIdent && unsupportedKeywords[strings.ToUpper(t.text)] {
		return errorf(ErrKindUnsupported, "%s is not supported by this engine", strings.ToUpper(t.text))
	}
	return nil
}

func (p *parser) parseStatement() (*Statement, error) {
	if err := p.checkUnsupported(); err != nil {
		return nil, err
	}
	if !p.isIdent("SELECT") {
		t := p.peek()
		if t.kind == tEOF {
			return nil, errorf(ErrKindParse, "empty statement")
		}
		return nil, errorf(ErrKindParse, "expected SELECT at position %d, found %q", t.pos, t.text)
	}
	p.next()
	stmt := &Statement{}

	// SELECT list
	for {
		if err := p.checkUnsupported(); err != nil {
			return nil, err
		}
		var item SelectItem
		if p.peek().kind == tStar {
			p.next()
			item.Expr = Star{}
		} else {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			item.Expr = e
		}
		// alias
		if p.isIdent("AS") {
			p.next()
			t := p.next()
			if t.kind != tIdent {
				return nil, errorf(ErrKindParse, "expected alias after AS at position %d", t.pos)
			}
			item.Alias = t.text
		} else if t := p.peek(); t.kind == tIdent && !reservedWords[strings.ToUpper(t.text)] {
			if unsupportedKeywords[strings.ToUpper(t.text)] {
				return nil, errorf(ErrKindUnsupported, "%s is not supported by this engine", strings.ToUpper(t.text))
			}
			item.Alias = p.next().text
		}
		stmt.Select = append(stmt.Select, item)
		if p.peek().kind == tComma {
			p.next()
			continue
		}
		break
	}

	// FROM
	if err := p.expectIdent("FROM"); err != nil {
		return nil, err
	}
	t := p.next()
	if t.kind != tIdent {
		return nil, errorf(ErrKindParse, "expected stream name after FROM at position %d", t.pos)
	}
	if unsupportedKeywords[strings.ToUpper(t.text)] {
		return nil, errorf(ErrKindUnsupported, "%s is not supported by this engine", strings.ToUpper(t.text))
	}
	stmt.From = t.text

	// clauses
	for !p.atEOF() {
		if err := p.checkUnsupported(); err != nil {
			return nil, err
		}
		switch {
		case p.isIdent("WHERE"):
			if stmt.Where != nil {
				return nil, errorf(ErrKindParse, "duplicate WHERE clause")
			}
			p.next()
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Where = e
		case p.isIdent("GROUP"):
			if stmt.GroupBy != nil {
				return nil, errorf(ErrKindParse, "duplicate GROUP BY clause")
			}
			p.next()
			if err := p.expectIdent("BY"); err != nil {
				return nil, err
			}
			for {
				e, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				stmt.GroupBy = append(stmt.GroupBy, e)
				if p.peek().kind == tComma {
					p.next()
					continue
				}
				break
			}
		case p.isIdent("ORDER"):
			if stmt.OrderBy != nil {
				return nil, errorf(ErrKindParse, "duplicate ORDER BY clause")
			}
			p.next()
			if err := p.expectIdent("BY"); err != nil {
				return nil, err
			}
			for {
				e, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				item := OrderItem{Expr: e}
				if p.isIdent("ASC") {
					p.next()
				} else if p.isIdent("DESC") {
					p.next()
					item.Desc = true
				}
				stmt.OrderBy = append(stmt.OrderBy, item)
				if p.peek().kind == tComma {
					p.next()
					continue
				}
				break
			}
		case p.isIdent("LIMIT"):
			if stmt.Limit != nil {
				return nil, errorf(ErrKindParse, "duplicate LIMIT clause")
			}
			p.next()
			t := p.next()
			if t.kind != tNumber {
				return nil, errorf(ErrKindParse, "expected integer after LIMIT at position %d", t.pos)
			}
			v, err := strconv.ParseInt(t.text, 10, 64)
			if err != nil || v <= 0 {
				return nil, errorf(ErrKindParse, "LIMIT must be a positive integer, got %q", t.text)
			}
			stmt.Limit = &v
		default:
			t := p.peek()
			return nil, errorf(ErrKindParse, "unexpected token %q at position %d", t.text, t.pos)
		}
	}
	if err := resolveWindow(stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// resolveWindow extracts the single window function from GROUP BY into
// stmt.Window.
func resolveWindow(stmt *Statement) error {
	var rest []Expr
	for _, e := range stmt.GroupBy {
		if c, ok := e.(Call); ok && (strings.EqualFold(c.Name, "TUMBLE") || strings.EqualFold(c.Name, "HOP")) {
			if stmt.Window != nil {
				return errorf(ErrKindUnsupported, "only one window function per query is supported")
			}
			w, err := windowFromCall(c)
			if err != nil {
				return err
			}
			stmt.Window = w
			continue
		}
		rest = append(rest, e)
	}
	stmt.GroupBy = rest
	return nil
}

func windowFromCall(c Call) (*WindowSpec, error) {
	name := strings.ToUpper(c.Name)
	argErr := func() error {
		if name == "TUMBLE" {
			return errorf(ErrKindParse, "TUMBLE expects (time_column, size) e.g. TUMBLE(ts, '10s')")
		}
		return errorf(ErrKindParse, "HOP expects (time_column, slide, size) e.g. HOP(ts, '5s', '10s')")
	}
	need := 2
	if name == "HOP" {
		need = 3
	}
	if len(c.Args) != need {
		return nil, argErr()
	}
	id, ok := c.Args[0].(Ident)
	if !ok {
		return nil, argErr()
	}
	dur := func(e Expr) (int64, error) {
		switch v := e.(type) {
		case Literal:
			switch x := v.Value.(type) {
			case int64:
				return x, nil
			case string:
				d, err := time.ParseDuration(x)
				if err != nil {
					return 0, errorf(ErrKindParse, "invalid window duration %q: use Go-style durations like '10s', '5m'", x)
				}
				return d.Milliseconds(), nil
			}
		}
		return 0, argErr()
	}
	w := &WindowSpec{TimeCol: id.Name}
	if name == "TUMBLE" {
		w.Kind = Tumbling
		sz, err := dur(c.Args[1])
		if err != nil {
			return nil, err
		}
		w.Size = sz
		w.Slide = sz
	} else {
		w.Kind = Hopping
		sl, err := dur(c.Args[1])
		if err != nil {
			return nil, err
		}
		sz, err := dur(c.Args[2])
		if err != nil {
			return nil, err
		}
		w.Slide, w.Size = sl, sz
	}
	if w.Size <= 0 || w.Slide <= 0 {
		return nil, errorf(ErrKindParse, "window size and slide must be positive")
	}
	if w.Slide > w.Size {
		return nil, errorf(ErrKindUnsupported, "HOP slide greater than size (gaps) is not supported")
	}
	return w, nil
}

// --- expression grammar ---
// expr     = orExpr
// orExpr   = andExpr (OR andExpr)*
// andExpr  = notExpr (AND notExpr)*
// notExpr  = NOT notExpr | cmp
// cmp      = add ( (=|!=|<>|<|<=|>|>=) add )?
// add      = mul ((+|-) mul)*
// mul      = unary ((*|/) unary)*
// unary    = - unary | primary
// primary  = number | string | TRUE | FALSE | ( expr ) | ident | call | *

func (p *parser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (Expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isIdent("OR") {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = Binary{Op: "OR", L: l, R: r}
	}
	return l, nil
}

func (p *parser) parseAnd() (Expr, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.isIdent("AND") {
		p.next()
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l = Binary{Op: "AND", L: l, R: r}
	}
	return l, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.isIdent("NOT") {
		p.next()
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return Unary{Op: "NOT", X: x}, nil
	}
	return p.parseCmp()
}

func (p *parser) parseCmp() (Expr, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind == tOp {
		switch t.text {
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			p.next()
			r, err := p.parseAdd()
			if err != nil {
				return nil, err
			}
			op := t.text
			if op == "<>" {
				op = "!="
			}
			return Binary{Op: op, L: l, R: r}, nil
		}
	}
	return l, nil
}

func (p *parser) parseAdd() (Expr, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tOp && (t.text == "+" || t.text == "-") {
			p.next()
			r, err := p.parseMul()
			if err != nil {
				return nil, err
			}
			l = Binary{Op: t.text, L: l, R: r}
			continue
		}
		return l, nil
	}
}

func (p *parser) parseMul() (Expr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if (t.kind == tOp && t.text == "/") || t.kind == tStar {
			p.next()
			r, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			op := "*"
			if t.kind == tOp {
				op = "/"
			}
			l = Binary{Op: op, L: l, R: r}
			continue
		}
		return l, nil
	}
}

func (p *parser) parseUnary() (Expr, error) {
	t := p.peek()
	if t.kind == tOp && t.text == "-" {
		p.next()
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return Unary{Op: "-", X: x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case tNumber:
		if strings.Contains(t.text, ".") {
			f, err := strconv.ParseFloat(t.text, 64)
			if err != nil {
				return nil, errorf(ErrKindParse, "invalid number %q at position %d", t.text, t.pos)
			}
			return Literal{Value: f}, nil
		}
		v, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, errorf(ErrKindParse, "invalid number %q at position %d", t.text, t.pos)
		}
		return Literal{Value: v}, nil
	case tString:
		return Literal{Value: t.text}, nil
	case tLParen:
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tRParen {
			return nil, errorf(ErrKindParse, "missing closing parenthesis at position %d", p.peek().pos)
		}
		p.next()
		return e, nil
	case tIdent:
		up := strings.ToUpper(t.text)
		if unsupportedKeywords[up] {
			return nil, errorf(ErrKindUnsupported, "%s is not supported by this engine", up)
		}
		switch up {
		case "TRUE":
			return Literal{Value: true}, nil
		case "FALSE":
			return Literal{Value: false}, nil
		}
		if p.peek().kind == tLParen {
			p.next()
			call := Call{Name: up}
			if p.peek().kind == tStar {
				p.next()
				call.Star = true
			} else if p.peek().kind != tRParen {
				for {
					e, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					call.Args = append(call.Args, e)
					if p.peek().kind == tComma {
						p.next()
						continue
					}
					break
				}
			}
			if p.peek().kind != tRParen {
				return nil, errorf(ErrKindParse, "missing closing parenthesis in call to %s at position %d", t.text, p.peek().pos)
			}
			p.next()
			return call, nil
		}
		return Ident{Name: t.text}, nil
	case tEOF:
		return nil, errorf(ErrKindParse, "unexpected end of statement")
	default:
		return nil, errorf(ErrKindParse, "unexpected token %q at position %d", t.text, t.pos)
	}
}
