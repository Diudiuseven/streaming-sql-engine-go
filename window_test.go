package streamsql

import (
	"reflect"
	"testing"
)

func TestTumblingAssign(t *testing.T) {
	w := &WindowSpec{Kind: Tumbling, Size: 10000, Slide: 10000}
	got := w.Assign(12345)
	want := []Window{{10000, 20000}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// boundary: exactly at window start belongs to the new window
	got = w.Assign(20000)
	want = []Window{{20000, 30000}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("boundary: got %v want %v", got, want)
	}
	// negative timestamps are well-defined
	got = w.Assign(-1)
	want = []Window{{-10000, 0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("negative: got %v want %v", got, want)
	}
}

func TestHoppingAssign(t *testing.T) {
	w := &WindowSpec{Kind: Hopping, Size: 30000, Slide: 10000}
	got := w.Assign(25000)
	want := []Window{{0, 30000}, {10000, 40000}, {20000, 50000}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// event at 30000: window [0,30000) no longer contains it
	got = w.Assign(30000)
	want = []Window{{10000, 40000}, {20000, 50000}, {30000, 60000}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("boundary: got %v want %v", got, want)
	}
}
