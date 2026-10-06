package streamsql

import "strings"

type tokenKind int

const (
	tEOF tokenKind = iota
	tIdent
	tNumber
	tString
	tStar
	tComma
	tLParen
	tRParen
	tOp // = != <> < <= > >= + - * /  (* has its own kind)
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
	return isIdentChar0(c)
}
func isIdentChar0(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func lex(input string) ([]token, error) {
	var toks []token
	i := 0
	n := len(input)
	for i < n {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case isIdentStart(c):
			j := i + 1
			for j < n && isIdentChar(input[j]) {
				j++
			}
			// qualified name: ident.ident
			for j+1 < n && input[j] == '.' && isIdentStart(input[j+1]) {
				j += 2
				for j < n && isIdentChar(input[j]) {
					j++
				}
			}
			toks = append(toks, token{tIdent, input[i:j], i})
			i = j
		case c >= '0' && c <= '9':
			j := i
			for j < n && ((input[j] >= '0' && input[j] <= '9') || input[j] == '.') {
				j++
			}
			toks = append(toks, token{tNumber, input[i:j], i})
			i = j
		case c == '\'':
			j := i + 1
			var sb strings.Builder
			for {
				if j >= n {
					return nil, errorf(ErrKindParse, "unterminated string literal at position %d", i)
				}
				if input[j] == '\'' {
					if j+1 < n && input[j+1] == '\'' { // escaped quote ''
						sb.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(input[j])
				j++
			}
			toks = append(toks, token{tString, sb.String(), i})
			i = j + 1
		case c == '*':
			toks = append(toks, token{tStar, "*", i})
			i++
		case c == ',':
			toks = append(toks, token{tComma, ",", i})
			i++
		case c == '(':
			toks = append(toks, token{tLParen, "(", i})
			i++
		case c == ')':
			toks = append(toks, token{tRParen, ")", i})
			i++
		case c == '=' || c == '+' || c == '-' || c == '/':
			toks = append(toks, token{tOp, string(c), i})
			i++
		case c == '<' || c == '>':
			if i+1 < n && input[i+1] == '=' {
				toks = append(toks, token{tOp, input[i : i+2], i})
				i += 2
			} else if c == '<' && i+1 < n && input[i+1] == '>' {
				toks = append(toks, token{tOp, "<>", i})
				i += 2
			} else {
				toks = append(toks, token{tOp, string(c), i})
				i++
			}
		case c == '!':
			if i+1 < n && input[i+1] == '=' {
				toks = append(toks, token{tOp, "!=", i})
				i += 2
			} else {
				return nil, errorf(ErrKindParse, "unexpected character '!' at position %d (did you mean !=?)", i)
			}
		default:
			return nil, errorf(ErrKindParse, "unexpected character %q at position %d", c, i)
		}
	}
	toks = append(toks, token{tEOF, "", n})
	return toks, nil
}
