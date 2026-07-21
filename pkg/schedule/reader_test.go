package schedule

import (
	"context"
	"testing"
)

func TestParseOffsets(t *testing.T) {
	got := ParseOffsets(map[string][]int64{
		"0": {1, 2},
		"3": {9},
	})
	if len(got[0]) != 2 || got[0][0] != 1 || got[3][0] != 9 {
		t.Fatalf("got %#v", got)
	}
}

func TestReaderCloseNilClient(t *testing.T) {
	r := &Reader{}
	r.Close() // must not panic
}

func TestReaderReadEmpty(t *testing.T) {
	r := &Reader{topic: "sched"}
	res, err := r.Read(context.Background(), nil)
	if err != nil || len(res.Found) != 0 || len(res.Lost) != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	res, err = r.Read(context.Background(), map[int32][]int64{})
	if err != nil || len(res.Found) != 0 {
		t.Fatalf("empty map res=%+v err=%v", res, err)
	}
}
