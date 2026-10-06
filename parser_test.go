package streamsql

import "testing"

func TestParseFullStatement(t *testing.T) {
	s, err := Parse(`SELECT user AS u, COUNT(*) AS c, SUM(amount) AS total, AVG(amount), MIN(amount), MAX(amount), window_start, window_end
		FROM events
		WHERE amount > 10 AND (ok = TRUE OR tier != 'free')
		GROUP BY TUMBLE(ts, '10s'), user
		ORDER BY total DESC, u ASC
		LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	if s.From != "events" {
		t.Fatalf("from: %q", s.From)
	}
	if s.Window == nil || s.Window.Kind != Tumbling || s.Window.Size != 10000 || s.Window.TimeCol != "ts" {
		t.Fatalf("window: %+v", s.Window)
	}
	if len(s.GroupBy) != 1 {
		t.Fatalf("group by: %+v", s.GroupBy)
	}
	if len(s.Select) != 8 {
		t.Fatalf("select: %d items", len(s.Select))
	}
	if len(s.OrderBy) != 2 || !s.OrderBy[0].Desc || s.OrderBy[1].Desc {
		t.Fatalf("order by: %+v", s.OrderBy)
	}
	if s.Limit == nil || *s.Limit != 3 {
		t.Fatalf("limit: %v", s.Limit)
	}
}

func TestParseHoppingWindow(t *testing.T) {
	s, err := Parse(`SELECT user, COUNT(*) FROM s GROUP BY HOP(ts, '5s', '30s'), user`)
	if err != nil {
		t.Fatal(err)
	}
	if s.Window.Kind != Hopping || s.Window.Slide != 5000 || s.Window.Size != 30000 {
		t.Fatalf("window: %+v", s.Window)
	}
}

func TestParseDurationsAndNumericMillis(t *testing.T) {
	if _, err := Parse(`SELECT a, COUNT(*) FROM s GROUP BY TUMBLE(ts, '1m30s'), a`); err != nil {
		t.Fatal(err)
	}
	s, err := Parse(`SELECT a, COUNT(*) FROM s GROUP BY TUMBLE(ts, 10000), a`)
	if err != nil {
		t.Fatal(err)
	}
	if s.Window.Size != 10000 {
		t.Fatalf("size: %d", s.Window.Size)
	}
}

func TestParseErrorsAreDistinguishable(t *testing.T) {
	parseErrs := []string{
		``,
		`SELEC a FROM s`,
		`SELECT a FROM`,
		`SELECT a FROM s WHERE`,
		`SELECT a FROM s LIMIT 0`,
		`SELECT a FROM s GROUP BY TUMBLE(ts)`,
		`SELECT a FROM s GROUP BY TUMBLE(ts, 'notaduration'), a`,
		`SELECT a FROM s s LIMIT 'x'`,
	}
	for _, q := range parseErrs {
		if err := mustErr(q); !IsKind(err, ErrKindParse) {
			t.Errorf("%q: want parse error, got %v", q, err)
		}
	}
	unsupported := []string{
		`SELECT a FROM s JOIN t ON s.x = t.x`,
		`SELECT DISTINCT a FROM s`,
		`SELECT a FROM s GROUP BY TUMBLE(ts, '10s'), a HAVING COUNT(*) > 1`,
		`SELECT a FROM s UNION SELECT b FROM t`,
		`SELECT a FROM s WHERE a IN (1,2)`,
		`SELECT a FROM s WHERE a LIKE 'x%'`,
		`INSERT INTO s VALUES (1)`,
		`SELECT a FROM s GROUP BY HOP(ts, '30s', '5s'), a`, // slide > size
	}
	for _, q := range unsupported {
		if err := mustErr(q); !IsKind(err, ErrKindUnsupported) {
			t.Errorf("%q: want unsupported error, got %v", q, err)
		}
	}
	// compile-time rejections (syntactically valid, outside the subset)
	eng := NewEngine(Config{})
	defer eng.Close()
	compileUnsupported := []string{
		`SELECT * FROM s`,
		`SELECT a FROM s GROUP BY a`, // aggregation without window
		`SELECT MEDIAN(a) FROM s GROUP BY TUMBLE(ts, '10s')`,
		`SELECT COUNT(*) FROM s GROUP BY TUMBLE(ts, '10s') HAVING COUNT(*) > 1`,
	}
	for i, q := range compileUnsupported {
		if _, err := eng.Register(string(rune('a'+i))+q[:1], q); !IsKind(err, ErrKindUnsupported) {
			t.Errorf("%q: want unsupported error, got %v", q, err)
		}
	}
}

func mustErr(q string) error {
	_, err := Parse(q)
	if err == nil {
		return nil
	}
	return err
}

func TestCompileValidation(t *testing.T) {
	eng := NewEngine(Config{})
	defer eng.Close()
	if _, err := eng.Register("bad1", `SELECT b, COUNT(*) FROM s GROUP BY TUMBLE(ts, '10s'), a`); !IsKind(err, ErrKindParse) {
		t.Fatalf("ungrouped column: %v", err)
	}
	if _, err := eng.Register("bad2", `SELECT a, COUNT(*) FROM s GROUP BY TUMBLE(ts, '10s'), a ORDER BY nope`); !IsKind(err, ErrKindParse) {
		t.Fatalf("bad order by: %v", err)
	}
	if _, err := eng.Register("ok1", `SELECT a, COUNT(*) AS c FROM s GROUP BY TUMBLE(ts, '10s'), a ORDER BY c DESC`); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Register("ok2", `SELECT a, b FROM s WHERE a > 1`); err != nil {
		t.Fatalf("stateless projection: %v", err)
	}
}
