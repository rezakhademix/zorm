package zorm

import (
	"context"
	"errors"
	"testing"
)

func TestCTE_PropagatesNestedAndLateBuilderErrors(t *testing.T) {
	for _, nested := range []bool{false, true} {
		db := setupSaveDB(t)
		ctx := context.Background()
		child := New[saveItem]().SetDB(db)
		cte := child
		if nested {
			cte = New[saveItem]().WithCTE("inner_scope", child).Table("inner_scope")
		}
		m := New[saveItem]().SetDB(db).WithCTE("scope", cte).Where("id IN (SELECT id FROM scope)")
		// CTEs retain builders, so validation must also catch edits made after
		// the CTE was added to its parent.
		child.Where("id; invalid", 1)
		if !errors.Is(m.GetBuildErr(), ErrInvalidColumnName) {
			t.Errorf("nested=%v: child error missing from GetBuildErr", nested)
		}
		if err := m.Delete(ctx); !errors.Is(err, ErrInvalidColumnName) {
			t.Errorf("nested=%v: child error missing from Delete: %v", nested, err)
		}
		count, err := New[saveItem]().SetDB(db).Count(ctx)
		if err != nil || count != 2 {
			t.Errorf("nested=%v: invalid CTE changed rows: %d, %v", nested, count, err)
		}
		db.Close()
	}
}

func TestWhereHas_PropagatesCallbackAndBuilderErrors(t *testing.T) {
	for _, callback := range []bool{false, true} {
		db := setupRelCallbackDB(t)
		ctx := context.Background()
		m := New[RcAuthor]().SetDB(db)
		var constraint any = New[RcBook]().Where("title; invalid", "published")
		if callback {
			constraint = func(q *Model[RcBook]) { q.Where("title; invalid", "published") }
		}
		m.WhereHas("Books", constraint)
		if err := m.Delete(ctx); !errors.Is(err, ErrInvalidColumnName) {
			t.Errorf("callback=%v: rejected constraint was ignored: %v", callback, err)
		}
		count, err := New[RcAuthor]().SetDB(db).Count(ctx)
		if err != nil || count != 1 {
			t.Errorf("callback=%v: invalid relation filter changed rows: %d, %v", callback, count, err)
		}
	}
}
