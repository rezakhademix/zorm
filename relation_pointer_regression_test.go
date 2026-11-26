package zorm

import (
	"context"
	"reflect"
	"testing"
)

func TestNestedHasMany_PointerConfiguration(t *testing.T) {
	defer func() {
		if value := recover(); value != nil {
			t.Errorf("pointer relation configuration panicked: %v", value)
		}
	}()
	m := New[failureParent]().SetDB(setupFailureRelations(t))
	parent := failureParentEntity()
	config := parent.ChildrenRelation()
	if err := m.loadHasManyDynamic(context.Background(), []any{parent}, reflect.TypeFor[failureParent](), &config, "Children", "", nil); err != nil {
		t.Fatal(err)
	}
	if len(parent.Children) != 1 || parent.Children[0].ID != 1 {
		t.Errorf("pointer config did not load children: %v", parent.Children)
	}
}
