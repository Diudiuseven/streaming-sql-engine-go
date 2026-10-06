package streamsql

// Statement is the parsed form of a supported SELECT query.
type Statement struct {
	Select  []SelectItem
	From    string
	Where   Expr
	GroupBy []Expr
	Window  *WindowSpec // exactly one window function in GROUP BY, if any
	OrderBy []OrderItem
	Limit   *int64
	Raw     string
}

type SelectItem struct {
	Expr  Expr
	Alias string
}

type OrderItem struct {
	Expr Expr
	Desc bool
}

// Expr is a scalar expression node.
type Expr interface{ exprNode() }

type Ident struct{ Name string }
type Literal struct{ Value any } // int64, float64, string, bool
type Star struct{}
type Call struct {
	Name string
	Args []Expr
	Star bool // COUNT(*)
}
type Binary struct {
	Op   string // = != < <= > >= AND OR + - * /
	L, R Expr
}
type Unary struct {
	Op string // NOT, -
	X  Expr
}

func (Ident) exprNode()   {}
func (Literal) exprNode() {}
func (Star) exprNode()    {}
func (Call) exprNode()    {}
func (Binary) exprNode()  {}
func (Unary) exprNode()   {}
