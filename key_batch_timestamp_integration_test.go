package zorm

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestUpdateManyByKey_PreservesExplicitTimestamps(t *testing.T) {
	for _, size := range []int{1, 501} {
		t.Run(fmt.Sprintf("rows=%d", size), func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if size > 1 {
				_, err := db.Exec(`WITH RECURSIVE ids(id) AS (SELECT 2 UNION ALL SELECT id + 1 FROM ids WHERE id < 501)
					INSERT INTO save_items_touch (id, name, updated_at) SELECT id, 'batch', '2000-01-01 00:00:00' FROM ids`)
				if err != nil {
					t.Fatal(err)
				}
			}
			updates := make(map[int]time.Time, size)
			base := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
			for id := 1; id <= size; id++ {
				updates[id] = base.Add(time.Duration(id) * time.Second)
			}
			m := New[saveItemWithTouch]().SetDB(db)
			if err := m.UpdateManyByKey(context.Background(), "id", "updated_at", updates); err != nil {
				t.Fatal(err)
			}
			rows, err := db.Query(`SELECT id, updated_at FROM save_items_touch`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var id int
				var at time.Time
				if err := rows.Scan(&id, &at); err != nil {
					t.Fatal(err)
				}
				if !at.Equal(updates[id]) {
					t.Fatalf("row %d timestamp=%v, want %v", id, at, updates[id])
				}
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != size {
				t.Fatalf("loaded %d rows, want %d", count, size)
			}
			rows.Close()
			if err := m.UpdateManyByKey(context.Background(), "id", "name", map[int]string{1: "changed"}); err != nil {
				t.Fatal(err)
			}
			var touched time.Time
			if err := db.QueryRow(`SELECT updated_at FROM save_items_touch WHERE id = 1`).Scan(&touched); err != nil {
				t.Fatal(err)
			}
			if !touched.After(updates[1]) {
				t.Error("updating another column no longer touches updated_at")
			}
		})
	}
}

// PostgreSQL does not permit assigning the same target column twice.
func TestUpdateManyByKey_TimestampAssignedOnce(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()
	err := New[saveItemWithTouch]().SetDB(db).UpdateManyByKey(context.Background(), "id", "updated_at", map[int]time.Time{1: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(log.last(), "updated_at =") != 1 {
		t.Fatalf("duplicate timestamp assignments: %s", log.last())
	}
}
