package engine

import (
	"fmt"

	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

// aggState accumulates one aggregate column for one group/window.
type aggState interface {
	add(v any)
	result() any
	clone() aggState
	export() aggSnap
	importFrom(aggSnap)
}

// newAggState creates the accumulator for the given function.
func newAggState(fn sqlparse.AggFunc) aggState {
	switch fn {
	case sqlparse.AggCount:
		return &countState{}
	case sqlparse.AggSum:
		return &sumState{}
	case sqlparse.AggMin:
		return &minmaxState{isMin: true}
	case sqlparse.AggMax:
		return &minmaxState{isMin: false}
	case sqlparse.AggAvg:
		return &avgState{}
	}
	return nil
}

// toNumber converts a value to float64, tracking whether it was an int64.
func toNumber(v any) (float64, bool, error) {
	switch n := v.(type) {
	case int64:
		return float64(n), true, nil
	case int:
		return float64(n), true, nil
	case float64:
		return n, false, nil
	}
	return 0, false, fmt.Errorf("value %v (%T) is not numeric", v, v)
}

type countState struct{ n int64 }

func (s *countState) add(v any)       { s.n++ }
func (s *countState) result() any     { return s.n }
func (s *countState) clone() aggState { c := *s; return &c }

// sumState keeps integer sums exact; switches to float64 on first float.
type sumState struct {
	i       int64
	f       float64
	isFloat bool
}

func (s *sumState) add(v any) {
	f, isInt, err := toNumber(v)
	if err != nil {
		return // non-numeric values are ignored, matching SQL NULL semantics
	}
	if isInt && !s.isFloat {
		s.i += int64(f)
		return
	}
	if !s.isFloat {
		s.f = float64(s.i)
		s.isFloat = true
	}
	s.f += f
}

func (s *sumState) result() any {
	if s.isFloat {
		return s.f
	}
	return s.i
}

func (s *sumState) clone() aggState { c := *s; return &c }

type minmaxState struct {
	isMin   bool
	set     bool
	i       int64
	f       float64
	isFloat bool
}

func (s *minmaxState) add(v any) {
	f, isInt, err := toNumber(v)
	if err != nil {
		return
	}
	if !s.set {
		s.set = true
		s.isFloat = !isInt
		if isInt {
			s.i = int64(f)
		}
		s.f = f
		return
	}
	if !isInt {
		s.isFloat = true
	}
	better := f < s.f
	if !s.isMin {
		better = f > s.f
	}
	if better {
		s.f = f
		if isInt {
			s.i = int64(f)
		}
	}
}

func (s *minmaxState) result() any {
	if !s.set {
		return nil
	}
	if !s.isFloat {
		return s.i
	}
	return s.f
}

func (s *minmaxState) clone() aggState { c := *s; return &c }

type avgState struct {
	sum sumState
	n   int64
}

func (s *avgState) add(v any) {
	if _, _, err := toNumber(v); err != nil {
		return
	}
	s.sum.add(v)
	s.n++
}

func (s *avgState) result() any {
	if s.n == 0 {
		return nil
	}
	var total float64
	switch v := s.sum.result().(type) {
	case int64:
		total = float64(v)
	case float64:
		total = v
	}
	return total / float64(s.n)
}

func (s *avgState) clone() aggState { c := *s; return &c }

// aggSnap is a neutral, serializable form of any aggState.
type aggSnap struct {
	N          int64   // count value / avg element count
	I          int64   // integer sum / min / max
	F          float64 // float sum / min / max
	IsFloat    bool
	Set        bool
	SumI       int64 // avg inner integer sum
	SumF       float64
	SumIsFloat bool
}

func (s *countState) export() aggSnap      { return aggSnap{N: s.n} }
func (s *countState) importFrom(a aggSnap) { s.n = a.N }

func (s *sumState) export() aggSnap {
	return aggSnap{I: s.i, F: s.f, IsFloat: s.isFloat}
}
func (s *sumState) importFrom(a aggSnap) {
	s.i, s.f, s.isFloat = a.I, a.F, a.IsFloat
}

func (s *minmaxState) export() aggSnap {
	return aggSnap{I: s.i, F: s.f, IsFloat: s.isFloat, Set: s.set}
}
func (s *minmaxState) importFrom(a aggSnap) {
	s.i, s.f, s.isFloat, s.set = a.I, a.F, a.IsFloat, a.Set
}

func (s *avgState) export() aggSnap {
	return aggSnap{N: s.n, SumI: s.sum.i, SumF: s.sum.f, SumIsFloat: s.sum.isFloat}
}
func (s *avgState) importFrom(a aggSnap) {
	s.n = a.N
	s.sum.i, s.sum.f, s.sum.isFloat = a.SumI, a.SumF, a.SumIsFloat
}
