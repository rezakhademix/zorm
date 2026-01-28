package zorm

import (
	"reflect"
	"sync"
)

// originalValues stores original field values for dirty tracking.
// Key: pointer to entity (uintptr), Value: map[columnName]originalValue
var originalValues sync.Map

// TrackOriginals stores the current field values of an entity as originals.
// Called automatically by Get, First, Find when loading from database.
func TrackOriginals[T any](entity *T, modelInfo *ModelInfo) {
	if entity == nil {
		return
	}

	originals := make(map[string]any)
	val := reflect.ValueOf(entity).Elem()

	for _, field := range modelInfo.Fields {
		fVal := val.FieldByIndex(field.Index)
		// Store a copy of the value
		originals[field.Column] = fVal.Interface()
	}

	// Use pointer address as key
	ptr := reflect.ValueOf(entity).Pointer()
	originalValues.Store(ptr, originals)
}

// ClearOriginals removes tracking for an entity.
// Should be called when entity is deleted or no longer needed.
func ClearOriginals[T any](entity *T) {
	if entity == nil {
		return
	}
	ptr := reflect.ValueOf(entity).Pointer()
	originalValues.Delete(ptr)
}

// GetOriginal returns the original value of a field before any modifications.
// Returns nil if the entity is not tracked or field doesn't exist.
func GetOriginal[T any](entity *T, column string) any {
	if entity == nil {
		return nil
	}

	ptr := reflect.ValueOf(entity).Pointer()
	if originals, ok := originalValues.Load(ptr); ok {
		if orig, exists := originals.(map[string]any)[column]; exists {
			return orig
		}
	}
	return nil
}

// GetOriginals returns all original values for the entity.
func GetOriginals[T any](entity *T) map[string]any {
	if entity == nil {
		return nil
	}

	ptr := reflect.ValueOf(entity).Pointer()
	if originals, ok := originalValues.Load(ptr); ok {
		// Return a copy to prevent modification
		orig := originals.(map[string]any)
		result := make(map[string]any, len(orig))
		for k, v := range orig {
			result[k] = v
		}
		return result
	}
	return nil
}

// IsDirty checks if a specific field has changed from its original value.
// Returns true if changed, false if unchanged or not tracked.
func IsDirty[T any](entity *T, column string, modelInfo *ModelInfo) bool {
	if entity == nil {
		return false
	}

	ptr := reflect.ValueOf(entity).Pointer()
	originals, ok := originalValues.Load(ptr)
	if !ok {
		return true // Not tracked = treat as dirty (new entity)
	}

	original, exists := originals.(map[string]any)[column]
	if !exists {
		return true // Column not in originals
	}

	// Get current value
	field, ok := modelInfo.Columns[column]
	if !ok {
		return false
	}

	val := reflect.ValueOf(entity).Elem()
	current := val.FieldByIndex(field.Index).Interface()

	return !reflect.DeepEqual(original, current)
}

// IsClean checks if a specific field has NOT changed from its original value.
func IsClean[T any](entity *T, column string, modelInfo *ModelInfo) bool {
	return !IsDirty(entity, column, modelInfo)
}

// GetDirty returns a map of all dirty (changed) fields and their current values.
func GetDirty[T any](entity *T, modelInfo *ModelInfo) map[string]any {
	if entity == nil {
		return nil
	}

	dirty := make(map[string]any)
	val := reflect.ValueOf(entity).Elem()

	ptr := reflect.ValueOf(entity).Pointer()
	originals, tracked := originalValues.Load(ptr)
	var orig map[string]any
	if tracked {
		orig = originals.(map[string]any)
	}

	for _, field := range modelInfo.Fields {
		if field.IsPrimary {
			continue
		}

		current := val.FieldByIndex(field.Index).Interface()

		if !tracked {
			// Not tracked = all fields are dirty
			dirty[field.Column] = current
		} else if original, exists := orig[field.Column]; !exists || !reflect.DeepEqual(original, current) {
			dirty[field.Column] = current
		}
	}

	return dirty
}

// IsTracked returns true if the entity has original values stored.
func IsTracked[T any](entity *T) bool {
	if entity == nil {
		return false
	}
	ptr := reflect.ValueOf(entity).Pointer()
	_, ok := originalValues.Load(ptr)
	return ok
}

// SyncOriginals updates the stored originals to match current values.
// Call after a successful save to mark entity as clean.
func SyncOriginals[T any](entity *T, modelInfo *ModelInfo) {
	TrackOriginals(entity, modelInfo)
}

// IsDirtyField checks if a specific field on an entity is dirty.
// This is a convenience method on Model.
func (m *Model[T]) IsDirtyField(entity *T, column string) bool {
	return IsDirty(entity, column, m.modelInfo)
}

// IsCleanField checks if a specific field on an entity is clean (unchanged).
// This is a convenience method on Model.
func (m *Model[T]) IsCleanField(entity *T, column string) bool {
	return IsClean(entity, column, m.modelInfo)
}

// GetDirtyFields returns all changed fields on an entity.
// This is a convenience method on Model.
func (m *Model[T]) GetDirtyFields(entity *T) map[string]any {
	return GetDirty(entity, m.modelInfo)
}

// GetOriginalValue returns the original value of a field.
// This is a convenience method on Model.
func (m *Model[T]) GetOriginalValue(entity *T, column string) any {
	return GetOriginal(entity, column)
}

// IsEntityTracked returns true if the entity is being tracked for dirty checking.
// This is a convenience method on Model.
func (m *Model[T]) IsEntityTracked(entity *T) bool {
	return IsTracked(entity)
}
