package zorm

import "testing"

func TestDirtyUnknownColumn_IsNotReportedAsChanged(t *testing.T) {
	m := New[ExModel]()
	for _, tracked := range []bool{false, true} {
		entity := &ExModel{ID: 1, Name: "before"}
		if tracked {
			trackOriginals(entity, m.modelInfo)
		}
		if m.IsDirtyField(entity, "missing_column") {
			t.Errorf("nonexistent field reported dirty: tracked=%v", tracked)
		}
		ClearOriginals(entity)
	}
}
