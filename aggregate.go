package streamsql

import (
	"encoding/gob"
	"strings"
)

// accumulator holds the running state of one aggregate function for one
// group. Concrete implementations are gob-registered for checkpoints.
type accumulator interface {
	Add(v any)
	Value() any
}

func init() {
	gob.Register(&countAcc{})
	gob.Register(&sumAcc{})
	gob.Register(&avgAcc{})
	gob.Register(&minMaxAcc{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register("")
	gob.Register(true)
}

// newAccumulator builds an accumulator for an aggregate call name
// (COUNT, SUM, AVG, MIN, MAX).
func newAccumulator(name string) (accumulator, error) {
	switch strings.ToUpper(name) {
	case "COUNT":
		return &countAcc{}, nil
	case "SUM":
		return &sumAcc{}, nil
	case "AVG":
		return &avgAcc{}, nil
	case "MIN":
		return &minMaxAcc{Min: true}, nil
	case "MAX":
		return &minMaxAcc{Min: false}, nil
	}
	return nil, errorf(ErrKindUnsupported, "aggregate function %s is not supported (supported: COUNT, SUM, AVG, MIN, MAX)", strings.ToUpper(name))
}

// toNumber converts int64/float64 to float64. ok is false for other types;
// SUM/AVG ignore non-numeric values (documented behaviour).
func toNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	}
	return 0, false
}

type countAcc struct {
	N int64
}

func (a *countAcc) Add(v any) {
	if v != nil { // COUNT(col) skips missing fields; COUNT(*) never passes nil
		a.N++
	}
}
func (a *countAcc) Value() any { return a.N }

type sumAcc struct {
	I       int64
	F       float64
	IsFloat bool
}

func (a *sumAcc) Add(v any) {
	switch x := v.(type) {
	case int64:
		a.I += x
		a.F += float64(x)
	case int:
		a.I += int64(x)
		a.F += float64(x)
	case float64:
		a.IsFloat = true
		a.F += x
	}
}
func (a *sumAcc) Value() any {
	if a.IsFloat {
		return a.F
	}
	return a.I
}

type avgAcc struct {
	Sum   float64
	Count int64
}

func (a *avgAcc) Add(v any) {
	if f, ok := toNumber(v); ok {
		a.Sum += f
		a.Count++
	}
}
func (a *avgAcc) Value() any {
	if a.Count == 0 {
		return nil
	}
	return a.Sum / float64(a.Count)
}

type minMaxAcc struct {
	Min   bool
	Has   bool
	F     float64
	S     string
	IsStr bool
}

func (a *minMaxAcc) Add(v any) {
	switch x := v.(type) {
	case string:
		if !a.Has || !a.IsStr {
			if !a.Has {
				a.S, a.IsStr, a.Has = x, true, true
			}
			return
		}
		if a.Min {
			if x < a.S {
				a.S = x
			}
		} else if x > a.S {
			a.S = x
		}
	case int64, int, float64:
		f, _ := toNumber(v)
		if !a.Has || a.IsStr {
			if !a.Has {
				a.F, a.IsStr, a.Has = f, false, true
			}
			return
		}
		if a.Min {
			if f < a.F {
				a.F = f
			}
		} else if f > a.F {
			a.F = f
		}
	}
}
func (a *minMaxAcc) Value() any {
	if !a.Has {
		return nil
	}
	if a.IsStr {
		return a.S
	}
	return a.F
}
