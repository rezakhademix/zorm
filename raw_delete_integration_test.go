package zorm

import (
	"context"
	"errors"
	"testing"
)

// Raw SQL must be executed through Exec. Delete builds its own statement and
// must not treat a discarded raw query as a valid deletion condition.
func TestRawDelete_RejectsConflictingQueryState(t *testing.T) {
	for _, operation := range []string{"Delete", "DeleteMany", "ForceDeleteAll"} {
		for _, scoped := range []bool{false, true} {
			t.Run(operation+"/"+map[bool]string{false: "unscoped", true: "scoped"}[scoped], func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				ctx := context.Background()
				m := New[saveItem]().SetDB(db).Raw("SELECT * FROM save_items WHERE id = ?", 1)
				if scoped {
					m.Where("id", 1)
				}
				var err error
				switch operation {
				case "Delete":
					err = m.Delete(ctx)
				case "DeleteMany":
					err = m.DeleteMany(ctx)
				case "ForceDeleteAll":
					err = m.ForceDeleteAll(ctx)
				}
				if !errors.Is(err, ErrInvalidModel) {
					t.Errorf("raw builder delete should fail with ErrInvalidModel, got %v", err)
				}
				count, err := New[saveItem]().SetDB(db).Count(ctx)
				if err != nil || count != 2 {
					t.Fatalf("raw builder delete changed rows: count=%d, err=%v", count, err)
				}
			})
		}
	}
}
