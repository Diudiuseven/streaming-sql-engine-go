package sqlparse

import (
	"testing"
	"time"
)

func TestParseWindowAgg(t *testing.T) {
	q, err := Parse(`SELECT user, COUNT(*) AS c, SUM(amount), WINDOW_START() AS ws
		FROM orders
		WHERE amount > 10 AND status = 'ok'
		GROUP BY user, TUMBLE(ts, '5m')
		ORDER BY ws DESC, user
		LIMIT 10`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if q.Source != "orders" {
		t.Errorf("source = %q", q.Source)
	}
	if q.Window.Kind != Tumble || q.Window.Size != 5*time.Minute || q.Window.TimeCol != "ts" {
		t.Errorf("window = %+v", q.Window)
	}
	if len(q.Items) != 4 || q.Items[1].Agg != AggCount || q.Items[1].Alias != "c" {
		t.Errorf("items = %+v", q.Items)
	}
	if len(q.GroupBy) != 1 || q.GroupBy[0] != "user" {
		t.Errorf("groupby = %v", q.GroupBy)
	}
	if len(q.OrderBy) != 2 || !q.OrderBy[0].Desc {
		t.Errorf("orderby = %+v", q.OrderBy)
	}
	if q.Limit != 10 {
		t.Errorf("limit = %d", q.Limit)
	}
	if q.Where == nil {
		t.Error("where is nil")
	}
}

func TestParseHop(t *testing.T) {
	q, err := Parse(`SELECT COUNT(*) FROM s GROUP BY HOP(ts, '10m', '2m')`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if q.Window.Kind != Hop || q.Window.Size != 10*time.Minute || q.Window.Slide != 2*time.Minute {
		t.Errorf("window = %+v", q.Window)
	}
}

func TestParseErrorsDistinguishable(t *testing.T) {
	cases := []struct {
		stmt string
		kind ErrorKind
	}{
		{"SELECT * FROM", ErrParse},
		{"SELECT * FROM s WHERE x =", ErrParse},
		{"SELECT * FROM s JOIN t", ErrUnsupported},
		{"SELECT DISTINCT x FROM s", ErrUnsupported},
		{"DELETE FROM s", ErrUnsupported},
		{"SELECT * FROM s HAVING x > 1", ErrUnsupported},
		{"SELECT * FROM s WHERE x IN (1,2)", ErrUnsupported},
		{"SELECT * FROM s GROUP BY HOP(ts, '10m', '3m')", ErrUnsupported},
	}
	for _, c := range cases {
		_, err := Parse(c.stmt)
		if err == nil {
			t.Errorf("%q: expected error", c.stmt)
			continue
		}
		se, ok := err.(*Error)
		if !ok {
			t.Errorf("%q: error is %T, want *Error", c.stmt, err)
			continue
		}
		if se.Kind != c.kind {
			t.Errorf("%q: kind = %v, want %v", c.stmt, se.Kind, c.kind)
		}
	}
}
