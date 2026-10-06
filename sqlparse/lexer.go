package sqlparse

import "strings"

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokStar
	tokComma
	tokLParen
	tokRParen
	tokOp // comparison operator: = != < <= > >=
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '.'
}

// lex tokenizes the input statement.
func lex(input string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case isIdentStart(c):
			j := i + 1
			for j < len(input) && isIdentChar(input[j]) {
				j++
			}
			toks = append(toks, token{kind: tokIdent, text: input[i:j], pos: i})
			i = j
		case c >= '0' && c <= '9':
			j := i + 1
			for j < len(input) && ((input[j] >= '0' && input[j] <= '9') || input[j] == '.') {
				j++
			}
			toks = append(toks, token{kind: tokNumber, text: input[i:j], pos: i})
			i = j
		case c == '\'':
			j := i + 1
			var sb strings.Builder
			for {
				if j >= len(input) {
					return nil, parseErr(i, "unterminated string literal")
				}
				if input[j] == '\'' {
					// '' is an escaped quote
					if j+1 < len(input) && input[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(input[j])
				j++
			}
			toks = append(toks, token{kind: tokString, text: sb.String(), pos: i})
			i = j + 1
		case c == '*':
			toks = append(toks, token{kind: tokStar, text: "*", pos: i})
			i++
		case c == ',':
			toks = append(toks, token{kind: tokComma, text: ",", pos: i})
			i++
		case c == '(':
			toks = append(toks, token{kind: tokLParen, text: "(", pos: i})
			i++
		case c == ')':
			toks = append(toks, token{kind: tokRParen, text: ")", pos: i})
			i++
		case c == '=' || c == '!' || c == '<' || c == '>':
			j := i + 1
			if j < len(input) && input[j] == '=' {
				j++
			}
			op := input[i:j]
			if op == "!" {
				return nil, parseErr(i, "unexpected '!'; did you mean '!='?")
			}
			toks = append(toks, token{kind: tokOp, text: op, pos: i})
			i = j
		default:
			return nil, parseErr(i, "unexpected character %q", c)
		}
	}
	toks = append(toks, token{kind: tokEOF, pos: len(input)})
	return toks, nil
}
