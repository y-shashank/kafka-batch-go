package schedule

import (
	"context"
	"testing"
)

func TestMysqlStoreRecordReadMissIncrements(t *testing.T) {
	st := &MysqlStore{}
	n1, err := st.RecordReadMiss(context.Background(), "j1:0:1")
	if err != nil || n1 != 1 {
		t.Fatalf("first miss=%d err=%v", n1, err)
	}
	n2, err := st.RecordReadMiss(context.Background(), "j1:0:1")
	if err != nil || n2 != 2 {
		t.Fatalf("second miss=%d err=%v", n2, err)
	}
	if err := st.ClearReadMiss(context.Background(), "j1:0:1"); err != nil {
		t.Fatal(err)
	}
	n3, err := st.RecordReadMiss(context.Background(), "j1:0:1")
	if err != nil || n3 != 1 {
		t.Fatalf("after clear miss=%d err=%v", n3, err)
	}
}
