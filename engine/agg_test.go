package engine

import (
	"testing"

	"github.com/Diudiuseven/streaming-sql-engine-go/sqlparse"
)

func TestAggStates(t *testing.T) {
	c := newAggState(sqlparse.AggCount)
	c.add(nil)
	c.add("x")
	if c.result() != int64(2) {
		t.Errorf("count = %v", c.result())
	}

	s := newAggState(sqlparse.AggSum)
	s.add(int64(3))
	s.add(int64(4))
	if s.result() != int64(7) {
		t.Errorf("int sum = %v", s.result())
	}
	s.add(0.5)
	if s.result() != 7.5 {
		t.Errorf("float sum = %v", s.result())
	}

	mn := newAggState(sqlparse.AggMin)
	mn.add(int64(5))
	mn.add(int64(3))
	mn.add(int64(9))
	if mn.result() != int64(3) {
		t.Errorf("min = %v", mn.result())
	}

	mx := newAggState(sqlparse.AggMax)
	mx.add(1.5)
	mx.add(2.5)
	if mx.result() != 2.5 {
		t.Errorf("max = %v", mx.result())
	}

	a := newAggState(sqlparse.AggAvg)
	a.add(int64(2))
	a.add(int64(4))
	if a.result() != 3.0 {
		t.Errorf("avg = %v", a.result())
	}

	// clone must be independent
	c2 := newAggState(sqlparse.AggSum)
	c2.add(int64(1))
	cl := c2.clone()
	c2.add(int64(10))
	if cl.result() != int64(1) || c2.result() != int64(11) {
		t.Errorf("clone not independent: %v %v", cl.result(), c2.result())
	}
}
