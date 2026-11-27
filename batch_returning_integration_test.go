package zorm

import (
	"context"
	"testing"
)

func TestCreateMany_MissingReturnedRowsDoesNotAssignWrongIDs(t *testing.T) {
	for _, names := range [][]string{{"skip", "keep"}, {"keep", "skip"}, {"skip", "skip"}} {
		db := setupSaveDB(t)
		_, err := db.Exec(`CREATE TRIGGER skip_batch BEFORE INSERT ON save_items
			WHEN NEW.name = 'skip' BEGIN SELECT RAISE(IGNORE); END`)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		items := []*saveItem{{Name: names[0]}, {Name: names[1]}}
		err = New[saveItem]().SetDB(db).CreateMany(context.Background(), items)
		if err == nil {
			t.Errorf("%v: missing RETURNING rows reported success", names)
		}
		for _, item := range items {
			if item.ID != 0 {
				t.Errorf("%v: assigned ID %d before validating returned rows", names, item.ID)
			}
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM save_items").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Errorf("%v: failed batch left %d inserted rows", names, count-2)
		}
		db.Close()
	}
}
