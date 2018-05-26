package mysqldb

import (
	"testing"
)

func TestRow(t *testing.T) {
	db, err := CreateMysqlDB("root", "root123", "192.168.1.249:3306", "activity", "utf8")
	if err != nil {
		t.Errorf("connect db failed")
	}
	rows, err := db.Query("select * from s_activity limit 10")
	if err != nil {
		t.Errorf("select failed")
	}
	r := NewRow()
	for _, row := range rows {
		r.Reset(row)
		id := r.Uint64("id")
		ID := r.Uint64("activity_id")
		App := r.String("app")
		Title := r.String("title")
		Mark := r.Uint32("mark")
		Force := r.Bool("force_pop")
		StartAt := r.Int64("start_at")
		EndAt := r.Int64("end_at")
		Content := r.String("content")
		Extra := r.String("extra")
		Tasks := r.String("tasks")
		SpecialID := r.Int32("special_id")
		EventID := r.Int32("event_id")
		t.Log(id, ID, App, Title, Mark, Force, StartAt, EndAt, Content, Extra, Tasks, SpecialID, EventID)
		if r.Err != nil {
			t.Errorf("row parse err: %v", r.Err)
			continue
		}
		_ = r.Bytes("__________sasasas")
		if r.Err == nil {
			t.Errorf("row parse err: unexcept find key success")
			continue
		}
		_ = r.Int32("app")
		if r.Err == nil {
			t.Errorf("row parse err: unexcept transfer success")
			continue
		}
	}
}
