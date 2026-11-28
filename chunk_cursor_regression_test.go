package zorm

import (
	"context"
	"reflect"
	"testing"
)

type chunkNullableRow struct {
	ID   int
	Rank *int
}

func (chunkNullableRow) TableName() string { return "chunk_nullable_rows" }

func TestChunk_CursorHandlesOrFiltersAndNullableOrder(t *testing.T) {
	for _, direction := range []string{"ASC", "DESC", "OR"} {
		t.Run(direction, func(t *testing.T) {
			db := setupSaveDB(t)
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE chunk_nullable_rows (id INTEGER PRIMARY KEY, rank INTEGER);
			INSERT INTO chunk_nullable_rows VALUES (1, NULL), (2, 10), (3, 10), (4, NULL), (5, 20)`); err != nil {
				t.Fatal(err)
			}
			m := New[chunkNullableRow]().SetDB(db)
			want := []int{1, 4, 2, 3, 5}
			if direction == "DESC" {
				want = []int{5, 2, 3, 1, 4}
			}
			if direction == "OR" {
				m.Where("id", 1).OrWhere("id", 2)
				want = []int{1, 2}
			} else {
				m.OrderBy("rank", direction)
			}
			var seen []int
			err := m.Chunk(context.Background(), 1, func(rows []*chunkNullableRow) error {
				for _, row := range rows {
					seen = append(seen, row.ID)
					ClearOriginals(row)
				}
				if len(seen) > len(want) {
					t.Fatalf("cursor repeated rows: %v", seen)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("cursor order: %v, want %v", seen, want)
			}
		})
	}
}
