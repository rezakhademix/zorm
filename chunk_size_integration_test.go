package zorm

import (
	"context"
	"errors"
	"strconv"
	"testing"
)

// Invalid sizes must fail before querying or invoking the callback. The
// callback returns an error so the regression cannot hang on repeated rows.
func TestChunk_RejectsNonPositiveSize(t *testing.T) {
	for _, size := range []int{0, -1, -100} {
		for _, empty := range []bool{false, true} {
			t.Run(strconv.Itoa(size)+map[bool]string{false: "/rows", true: "/empty"}[empty], func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				if empty {
					if _, err := db.Exec(`DELETE FROM save_items`); err != nil {
						t.Fatal(err)
					}
				}
				calls := 0
				stop := errors.New("callback reached")
				err := New[saveItem]().SetDB(db).Chunk(context.Background(), size, func([]*saveItem) error {
					calls++
					return stop
				})
				if err == nil || errors.Is(err, stop) || calls != 0 {
					t.Fatalf("expected size validation before callback: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}
