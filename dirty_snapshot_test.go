package zorm

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

type snapshotValueItem struct {
	ID    int `zorm:"primaryKey"`
	Value any
}

type snapshotNamedInt int
type snapshotNamedPointer *int
type snapshotBlob []byte

func snapshotPtr[T any](value T) *T { return &value }

func TestDirtySnapshot_MutableValues(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name   string
		value  any
		mutate func(any)
	}{
		{"int", snapshotPtr(1), func(v any) { *v.(*int) = 2 }},
		{"int8", snapshotPtr(int8(1)), func(v any) { *v.(*int8) = 2 }},
		{"int16", snapshotPtr(int16(1)), func(v any) { *v.(*int16) = 2 }},
		{"int32", snapshotPtr(int32(1)), func(v any) { *v.(*int32) = 2 }},
		{"int64", snapshotPtr(int64(1)), func(v any) { *v.(*int64) = 2 }},
		{"uint", snapshotPtr(uint(1)), func(v any) { *v.(*uint) = 2 }},
		{"uint8", snapshotPtr(uint8(1)), func(v any) { *v.(*uint8) = 2 }},
		{"uint16", snapshotPtr(uint16(1)), func(v any) { *v.(*uint16) = 2 }},
		{"uint32", snapshotPtr(uint32(1)), func(v any) { *v.(*uint32) = 2 }},
		{"uint64", snapshotPtr(uint64(1)), func(v any) { *v.(*uint64) = 2 }},
		{"float32", snapshotPtr(float32(1)), func(v any) { *v.(*float32) = 2 }},
		{"float64", snapshotPtr(float64(1)), func(v any) { *v.(*float64) = 2 }},
		{"string", snapshotPtr("before"), func(v any) { *v.(*string) = "after" }},
		{"bool", snapshotPtr(false), func(v any) { *v.(*bool) = true }},
		{"time", &now, func(v any) { p := v.(*time.Time); *p = p.Add(time.Hour) }},
		{"named scalar", snapshotPtr(snapshotNamedInt(1)), func(v any) { *v.(*snapshotNamedInt) = 2 }},
		{"named pointer", snapshotNamedPointer(snapshotPtr(1)), func(v any) { *v.(snapshotNamedPointer) = 2 }},
		{"pointer chain", snapshotPtr(snapshotPtr(1)), func(v any) { **v.(**int) = 2 }},
		{"bytes", []byte("before"), func(v any) { v.([]byte)[0] = 'X' }},
		{"named bytes", snapshotBlob("before"), func(v any) { v.(snapshotBlob)[0] = 'X' }},
		{"slice of pointers", []*int{snapshotPtr(1)}, func(v any) { *v.([]*int)[0] = 2 }},
		{"array of pointers", [1]*int{snapshotPtr(1)}, func(v any) { *v.([1]*int)[0] = 2 }},
		{"map", map[string]int{"value": 1}, func(v any) { v.(map[string]int)["value"] = 2 }},
		{"map pointer values", map[string]*int{"value": snapshotPtr(1)}, func(v any) { *v.(map[string]*int)["value"] = 2 }},
		{"nested interface", map[string]any{"value": []byte("before")}, func(v any) { v.(map[string]any)["value"].([]byte)[0] = 'X' }},
		{"struct references", struct{ Value *int }{snapshotPtr(1)}, func(v any) { *v.(struct{ Value *int }).Value = 2 }},
		{"scanner value", snapshotPtr(sql.NullString{String: "before", Valid: true}), func(v any) { v.(*sql.NullString).String = "after" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity := &snapshotValueItem{ID: 1, Value: tt.value}
			m := New[snapshotValueItem]()
			trackOriginals(entity, m.modelInfo)
			defer ClearOriginals(entity)
			if m.HasDirtyFields(entity) || !m.IsCleanField(entity, "value") {
				t.Fatal("freshly tracked entity is dirty")
			}
			before := GetOriginal(entity, "value")
			if !reflect.DeepEqual(before, tt.value) {
				t.Fatalf("snapshot changed value or type: got %#v, want %#v", before, tt.value)
			}
			tt.mutate(entity.Value)
			if !m.HasDirtyFields(entity) || !m.IsDirtyField(entity, "value") || len(m.GetDirtyFields(entity)) != 1 {
				t.Error("in-place change was not detected by all dirty APIs")
			}
			if after := GetOriginal(entity, "value"); !reflect.DeepEqual(after, before) {
				t.Errorf("mutation changed original: got %#v, want %#v", after, before)
			}
			entity.Value = before
			if m.HasDirtyFields(entity) {
				t.Error("restoring the original must make the entity clean")
			}
		})
	}
}

func TestDirtySnapshot_NilEmptyAndReplacement(t *testing.T) {
	tests := []struct {
		name    string
		initial any
		next    any
		dirty   bool
	}{
		{"nil interface", nil, nil, false},
		{"typed nil pointer", (*int)(nil), (*int)(nil), false},
		{"nil to pointer", (*int)(nil), snapshotPtr(1), true},
		{"pointer to nil", snapshotPtr(1), (*int)(nil), true},
		{"equal replacement", snapshotPtr(1), snapshotPtr(1), false},
		{"different replacement", snapshotPtr(1), snapshotPtr(2), true},
		{"typed nil differs from interface nil", nil, (*int)(nil), true},
		{"nil slice", []byte(nil), []byte(nil), false},
		{"empty slice", []byte{}, []byte{}, false},
		{"nil to empty slice", []byte(nil), []byte{}, true},
		{"nil map", map[string]int(nil), map[string]int(nil), false},
		{"empty map", map[string]int{}, map[string]int{}, false},
		{"nil to empty map", map[string]int(nil), map[string]int{}, true},
		{"scalar", 1, 1, false},
		{"numeric type change", int(1), int64(1), true},
		{"time zone only", time.Unix(1, 0).UTC(), time.Unix(1, 0).In(time.FixedZone("other", 3600)), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity := &snapshotValueItem{ID: 1, Value: tt.initial}
			m := New[snapshotValueItem]()
			trackOriginals(entity, m.modelInfo)
			defer ClearOriginals(entity)
			entity.Value = tt.next
			if got := m.IsDirtyField(entity, "value"); got != tt.dirty {
				t.Errorf("dirty = %v, want %v", got, tt.dirty)
			}
		})
	}
}

func TestDirtySnapshot_OriginalAPIsReturnDetachedValues(t *testing.T) {
	for _, api := range []string{"GetOriginal", "GetOriginals"} {
		t.Run(api, func(t *testing.T) {
			entity := &snapshotValueItem{ID: 1, Value: map[string]any{"bytes": []byte("before"), "pointer": snapshotPtr(1)}}
			m := New[snapshotValueItem]()
			trackOriginals(entity, m.modelInfo)
			defer ClearOriginals(entity)
			var returned map[string]any
			if api == "GetOriginal" {
				returned = GetOriginal(entity, "value").(map[string]any)
			} else {
				all := GetOriginals(entity)
				returned = all["value"].(map[string]any)
				delete(all, "id")
			}
			returned["bytes"].([]byte)[0] = 'X'
			*returned["pointer"].(*int) = 2
			returned["added"] = true
			want := map[string]any{"bytes": []byte("before"), "pointer": snapshotPtr(1)}
			if got := GetOriginal(entity, "value"); !reflect.DeepEqual(got, want) {
				t.Errorf("caller modified tracking baseline: %#v", got)
			}
			if !reflect.DeepEqual(entity.Value, want) || m.HasDirtyFields(entity) {
				t.Error("caller modified entity through returned originals")
			}
			if GetOriginal(entity, "id") != 1 {
				t.Error("caller deleted baseline primary key")
			}
		})
	}
}

func TestDirtySnapshot_CyclesAndSharedReferences(t *testing.T) {
	t.Run("pointer cycle", func(t *testing.T) {
		type node struct {
			Value int
			Next  *node
		}
		value := &node{Value: 1}
		value.Next = value
		entity := &snapshotValueItem{ID: 1, Value: value}
		m := New[snapshotValueItem]()
		trackOriginals(entity, m.modelInfo)
		defer ClearOriginals(entity)
		original := GetOriginal(entity, "value").(*node)
		if original == value || original.Next != original || m.HasDirtyFields(entity) {
			t.Fatal("snapshot must preserve the cycle without retaining the original node")
		}
		value.Next.Value = 2
		if original.Value != 1 || !m.HasDirtyFields(entity) {
			t.Error("cycle mutation changed baseline or went undetected")
		}
	})
	t.Run("map cycle", func(t *testing.T) {
		value := map[string]any{"value": 1}
		value["self"] = value
		copy := snapshotValue(value).(map[string]any)
		copy["self"].(map[string]any)["value"] = 2
		if copy["value"] != 2 || value["value"] != 1 {
			t.Error("map cycle was not detached and preserved")
		}
	})
	t.Run("slice cycle", func(t *testing.T) {
		value := make([]any, 2)
		value[0], value[1] = 1, value
		copy := snapshotValue(value).([]any)
		copy[1].([]any)[0] = 2
		if copy[0] != 2 || value[0] != 1 {
			t.Error("slice cycle was not detached and preserved")
		}
	})
	t.Run("shared pointer", func(t *testing.T) {
		value := snapshotPtr(1)
		copy := snapshotValue([]*int{value, value}).([]*int)
		if copy[0] != copy[1] || copy[0] == value {
			t.Error("shared reference was not detached and preserved")
		}
	})
	t.Run("same start different slice lengths", func(t *testing.T) {
		bytes := []byte("abc")
		copy := snapshotValue([][]byte{bytes[:1], bytes[:2]}).([][]byte)
		if string(copy[0]) != "a" || string(copy[1]) != "ab" {
			t.Error("slice lengths were conflated")
		}
	})
	t.Run("pointer map key", func(t *testing.T) {
		key := snapshotPtr(1)
		value := map[*int][]byte{key: []byte("before")}
		copy := snapshotValue(value).(map[*int][]byte)
		if !reflect.DeepEqual(copy, value) {
			t.Fatal("snapshot changed map key identity")
		}
		copy[key][0] = 'X'
		if string(value[key]) != "before" {
			t.Error("map values still alias the entity")
		}
	})
}

func TestDirtySnapshot_EntitiesHaveIndependentBaselines(t *testing.T) {
	shared := snapshotPtr(1)
	first := &snapshotValueItem{ID: 1, Value: shared}
	second := &snapshotValueItem{ID: 2, Value: shared}
	m := New[snapshotValueItem]()
	trackOriginals(first, m.modelInfo)
	trackOriginals(second, m.modelInfo)
	defer ClearOriginals(first)
	defer ClearOriginals(second)
	*shared = 2
	syncOriginalColumns(first, m.modelInfo, []string{"value"}, nil)
	if m.HasDirtyFields(first) || !m.HasDirtyFields(second) || *GetOriginal(second, "value").(*int) != 1 {
		t.Error("synchronizing one entity changed another entity's baseline")
	}
	*shared = 3
	if !m.HasDirtyFields(first) || *GetOriginal(first, "value").(*int) != 2 {
		t.Error("new baseline shares the original entity's pointer")
	}
}
