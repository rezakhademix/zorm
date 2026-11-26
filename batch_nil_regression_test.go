package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
)

func TestCreateMany_NilEntriesReturnError(t *testing.T) {
	for _, freshPK := range []bool{false, true} {
		for _, position := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("freshPK=%v/position=%d", freshPK, position), func(t *testing.T) {
				defer func() {
					if value := recover(); value != nil {
						t.Errorf("nil batch entry panicked instead of returning ErrNilPointer: %v", value)
					}
				}()
				writes := 0
				db := openFailureDB(t, &failureDB{query: func(string) (driver.Rows, error) { writes++; return nil, errDatabaseFailure }, onExec: func(string) { writes++ }})
				m := New[ExModel]().SetDB(db)
				entities := []*ExModel{{Name: "first"}, {Name: "second"}, {Name: "third"}}
				entities[position] = nil
				var err error
				if freshPK {
					err = m.CreateManyNewPK(context.Background(), entities)
				} else {
					err = m.CreateMany(context.Background(), entities)
				}
				if !errors.Is(err, ErrNilPointer) || writes != 0 {
					t.Errorf("invalid batch accepted: err=%v writes=%d", err, writes)
				}
			})
		}
	}
}
