package zorm

import (
	"container/list"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// fastEqual compares two values for equality using type-specific comparison
// for common types, falling back to reflect.DeepEqual for complex types.
// This is significantly faster than reflect.DeepEqual for primitive types.
func fastEqual(a, b any) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Fast path for common types
	switch av := a.(type) {
	case int:
		if bv, ok := b.(int); ok {
			return av == bv
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return av == bv
		}
	case int32:
		if bv, ok := b.(int32); ok {
			return av == bv
		}
	case int16:
		if bv, ok := b.(int16); ok {
			return av == bv
		}
	case int8:
		if bv, ok := b.(int8); ok {
			return av == bv
		}
	case uint:
		if bv, ok := b.(uint); ok {
			return av == bv
		}
	case uint64:
		if bv, ok := b.(uint64); ok {
			return av == bv
		}
	case uint32:
		if bv, ok := b.(uint32); ok {
			return av == bv
		}
	case uint16:
		if bv, ok := b.(uint16); ok {
			return av == bv
		}
	case uint8:
		if bv, ok := b.(uint8); ok {
			return av == bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
	case float32:
		if bv, ok := b.(float32); ok {
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.Equal(bv)
		}
	case *int:
		if bv, ok := b.(*int); ok {
			if av == nil && bv == nil {
				return true
			}
			if av == nil || bv == nil {
				return false
			}
			return *av == *bv
		}
	case *int64:
		if bv, ok := b.(*int64); ok {
			if av == nil && bv == nil {
				return true
			}
			if av == nil || bv == nil {
				return false
			}
			return *av == *bv
		}
	case *string:
		if bv, ok := b.(*string); ok {
			if av == nil && bv == nil {
				return true
			}
			if av == nil || bv == nil {
				return false
			}
			return *av == *bv
		}
	case *bool:
		if bv, ok := b.(*bool); ok {
			if av == nil && bv == nil {
				return true
			}
			if av == nil || bv == nil {
				return false
			}
			return *av == *bv
		}
	case *time.Time:
		if bv, ok := b.(*time.Time); ok {
			if av == nil && bv == nil {
				return true
			}
			if av == nil || bv == nil {
				return false
			}
			return av.Equal(*bv)
		}
	}

	// Fallback to reflect.DeepEqual for complex types
	return reflect.DeepEqual(a, b)
}

// trackerEntry represents a single entity's tracking data in the LRU cache.
type trackerEntry struct {
	key       uintptr
	originals map[string]any
	element   *list.Element // Position in LRU list
}

// lruTrackerShard is a single shard of the tracker with its own lock.
type lruTrackerShard struct {
	mu       sync.Mutex
	capacity int
	items    map[uintptr]*trackerEntry
	lruList  *list.List // Front = most recently used, Back = least recently used
}

// shardCount is the number of shards for the tracker.
// Using 256 shards provides good distribution while keeping memory overhead low.
const shardCount = 256

// lruTracker provides bounded dirty tracking with LRU eviction.
// When the tracker reaches capacity, the least recently used entries are evicted.
// Uses sharded locking to reduce contention under high concurrency.
type lruTracker struct {
	shards   [shardCount]*lruTrackerShard
	capacity int // total capacity across all shards
}

// newLRUTracker creates a new LRU tracker with the specified capacity.
// A capacity of 0 means unbounded (no eviction).
func newLRUTracker(capacity int) *lruTracker {
	// Distribute capacity across shards
	shardCapacity := capacity / shardCount
	if shardCapacity < 1 && capacity > 0 {
		shardCapacity = 1
	}

	t := &lruTracker{
		capacity: capacity,
	}

	for i := 0; i < shardCount; i++ {
		t.shards[i] = &lruTrackerShard{
			capacity: shardCapacity,
			items:    make(map[uintptr]*trackerEntry),
			lruList:  list.New(),
		}
	}

	return t
}

// getShard returns the shard for the given key.
func (t *lruTracker) getShard(key uintptr) *lruTrackerShard {
	return t.shards[key%shardCount]
}

// Store adds or updates tracking data for an entity.
func (t *lruTracker) Store(key uintptr, originals map[string]any) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, exists := shard.items[key]; exists {
		// Update existing entry and move to front
		entry.originals = originals
		shard.lruList.MoveToFront(entry.element)
		return
	}

	// Evict if at capacity
	if shard.capacity > 0 && len(shard.items) >= shard.capacity {
		shard.evictLRU()
	}

	// Add new entry
	entry := &trackerEntry{
		key:       key,
		originals: originals,
	}
	entry.element = shard.lruList.PushFront(entry)
	shard.items[key] = entry
}

// Load retrieves tracking data for an entity.
func (t *lruTracker) Load(key uintptr) (map[string]any, bool) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.items[key]
	if !exists {
		return nil, false
	}

	// Move to front (mark as recently used)
	shard.lruList.MoveToFront(entry.element)
	return entry.originals, true
}

// Delete removes tracking data for an entity.
func (t *lruTracker) Delete(key uintptr) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.items[key]
	if !exists {
		return
	}

	shard.lruList.Remove(entry.element)
	delete(shard.items, key)
}

// Clear removes all tracking data.
func (t *lruTracker) Clear() {
	for i := 0; i < shardCount; i++ {
		shard := t.shards[i]
		shard.mu.Lock()
		shard.items = make(map[uintptr]*trackerEntry)
		shard.lruList.Init()
		shard.mu.Unlock()
	}
}

// Len returns the number of tracked entities.
func (t *lruTracker) Len() int {
	total := 0
	for i := 0; i < shardCount; i++ {
		shard := t.shards[i]
		shard.mu.Lock()
		total += len(shard.items)
		shard.mu.Unlock()
	}
	return total
}

// evictLRU removes the least recently used entry. Must be called with lock held.
func (s *lruTrackerShard) evictLRU() {
	back := s.lruList.Back()
	if back == nil {
		return
	}

	entry := back.Value.(*trackerEntry)
	s.lruList.Remove(back)
	delete(s.items, entry.key)
}

// TrackingScope provides scoped dirty tracking for batch operations.
// Entities tracked within a scope are automatically cleaned up when the scope is closed.
// This is useful for processing large batches of entities where you don't want
// tracking data to persist beyond the operation.
type TrackingScope struct {
	mu      sync.Mutex
	keys    map[uintptr]struct{}
	closed  atomic.Bool
}

// NewTrackingScope creates a new tracking scope.
func NewTrackingScope() *TrackingScope {
	return &TrackingScope{
		keys: make(map[uintptr]struct{}),
	}
}

// track adds a key to the scope's tracked set.
func (s *TrackingScope) track(key uintptr) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check keys != nil inside lock to prevent race with Close()
	if s.keys != nil {
		s.keys[key] = struct{}{}
	}
}

// Close clears all tracking data for entities in this scope.
// After Close is called, the scope should not be reused.
func (s *TrackingScope) Close() {
	if s == nil || s.closed.Swap(true) {
		return // Already closed
	}

	s.mu.Lock()
	keys := s.keys
	s.keys = nil
	s.mu.Unlock()

	// Clear all tracked entities from the global tracker
	tracker := globalTracker.Load()
	for key := range keys {
		tracker.Delete(key)
	}
}

// Len returns the number of entities tracked in this scope.
func (s *TrackingScope) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// globalTracker is the default tracker used for dirty tracking.
// By default, it tracks up to 10,000 entities. Use ConfigureDirtyTracking to change capacity.
var globalTracker atomic.Pointer[lruTracker]

func init() {
	globalTracker.Store(newLRUTracker(10000))
}

// Memory management considerations for dirty tracking:
// - Use ConfigureDirtyTracking() to set a maximum capacity for bounded tracking
// - Call ClearOriginals() when an entity is no longer needed to prevent memory leaks
// - Use TrackingScope for batch operations with automatic cleanup
// - In long-running services, consider using LRU-bounded tracking or scoped tracking

// ConfigureDirtyTracking sets the maximum number of entities to track.
// A capacity of 0 means unbounded (no eviction).
// The default capacity is 10,000 entities.
// This function is thread-safe and can be called at any time.
//
// Example:
//
//	zorm.ConfigureDirtyTracking(10000) // Track at most 10,000 entities
func ConfigureDirtyTracking(maxEntities int) {
	globalTracker.Store(newLRUTracker(maxEntities))
}

// ClearAllOriginals removes all tracking data from the global tracker.
// This can be used for periodic cleanup in long-running services.
func ClearAllOriginals() {
	globalTracker.Load().Clear()
}

// TrackedEntityCount returns the number of currently tracked entities.
// Useful for monitoring memory usage in long-running services.
func TrackedEntityCount() int {
	return globalTracker.Load().Len()
}

// getEntityKey returns the unique key for an entity pointer.
// This is extracted to a helper to ensure consistent key generation
// and reduce repeated reflection calls.
func getEntityKey[T any](entity *T) uintptr {
	return reflect.ValueOf(entity).Pointer()
}

// trackOriginals stores the current field values of an entity as originals.
// Called automatically by Get, First, Find when loading from database.
//
// Note: This uses the entity's pointer address as a key. If the entity is
// garbage collected and another entity reuses the same address, tracking
// data may become stale. Always call ClearOriginals when done with an entity.
func trackOriginals[T any](entity *T, modelInfo *ModelInfo) {
	trackOriginalsWithScope(entity, modelInfo, nil)
}

// trackOriginalsWithScope stores originals and optionally registers with a scope.
func trackOriginalsWithScope[T any](entity *T, modelInfo *ModelInfo, scope *TrackingScope) {
	if entity == nil {
		return
	}

	val := reflect.ValueOf(entity).Elem()
	originals := make(map[string]any, len(modelInfo.Fields))

	for _, field := range modelInfo.Fields {
		fVal := val.FieldByIndex(field.Index)
		originals[field.Column] = fVal.Interface()
	}

	key := getEntityKey(entity)
	globalTracker.Load().Store(key, originals)

	// Register with scope if provided
	if scope != nil {
		scope.track(key)
	}
}

// ClearOriginals removes tracking for an entity.
// Should be called when entity is deleted or no longer needed to prevent memory leaks.
func ClearOriginals[T any](entity *T) {
	if entity == nil {
		return
	}
	globalTracker.Load().Delete(getEntityKey(entity))
}

// GetOriginal returns the original value of a field before any modifications.
// Returns nil if the entity is not tracked or field doesn't exist.
func GetOriginal[T any](entity *T, column string) any {
	if entity == nil {
		return nil
	}

	if originals, ok := globalTracker.Load().Load(getEntityKey(entity)); ok {
		if orig, exists := originals[column]; exists {
			return orig
		}
	}
	return nil
}

// GetOriginals returns all original values for the entity.
// Returns a copy to prevent external modification of tracking data.
func GetOriginals[T any](entity *T) map[string]any {
	if entity == nil {
		return nil
	}

	if originals, ok := globalTracker.Load().Load(getEntityKey(entity)); ok {
		result := make(map[string]any, len(originals))
		maps.Copy(result, originals)
		return result
	}
	return nil
}

// isDirty checks if a specific field has changed from its original value.
// Returns true if the field was modified, or if the entity is not tracked (new entity).
// Returns false if the field is unchanged or if the entity/column is nil/invalid.
func isDirty[T any](entity *T, column string, modelInfo *ModelInfo) bool {
	if entity == nil {
		return false
	}

	orig, ok := globalTracker.Load().Load(getEntityKey(entity))
	if !ok {
		return true // Not tracked = treat as dirty (new entity)
	}

	original, exists := orig[column]
	if !exists {
		return true // Column not in originals
	}

	field, ok := modelInfo.Columns[column]
	if !ok {
		return false
	}

	current := reflect.ValueOf(entity).Elem().FieldByIndex(field.Index).Interface()
	return !fastEqual(original, current)
}

// isClean checks if a specific field has NOT changed from its original value.
func isClean[T any](entity *T, column string, modelInfo *ModelInfo) bool {
	return !isDirty(entity, column, modelInfo)
}

// getDirty returns a map of all dirty (changed) fields and their current values.
// For untracked entities, all non-primary fields are considered dirty.
// Primary key fields are always excluded from the result.
func getDirty[T any](entity *T, modelInfo *ModelInfo) map[string]any {
	if entity == nil {
		return nil
	}

	val := reflect.ValueOf(entity).Elem()
	orig, tracked := globalTracker.Load().Load(getEntityKey(entity))

	// Pre-allocate with reasonable capacity
	dirty := make(map[string]any, 4)

	for _, field := range modelInfo.Fields {
		if field.IsPrimary {
			continue
		}

		current := val.FieldByIndex(field.Index).Interface()

		if !tracked {
			dirty[field.Column] = current
			continue
		}

		if original, exists := orig[field.Column]; !exists || !fastEqual(original, current) {
			dirty[field.Column] = current
		}
	}

	return dirty
}

// IsTracked returns true if the entity has original values stored.
// An entity becomes tracked when loaded from the database via Get, First, or Find.
func IsTracked[T any](entity *T) bool {
	if entity == nil {
		return false
	}
	_, ok := globalTracker.Load().Load(getEntityKey(entity))
	return ok
}

// syncOriginals updates the stored originals to match current values.
// Call after a successful save to mark entity as clean.
// This is called automatically by Update and UpdateColumns after successful execution.
func syncOriginals[T any](entity *T, modelInfo *ModelInfo) {
	trackOriginals(entity, modelInfo)
}

// hasDirtyFields returns true if any non-primary field has changed.
// This is more efficient than getDirty when you only need to know if changes exist.
func hasDirtyFields[T any](entity *T, modelInfo *ModelInfo) bool {
	if entity == nil {
		return false
	}

	orig, tracked := globalTracker.Load().Load(getEntityKey(entity))
	if !tracked {
		return true // Untracked entities are considered dirty
	}

	val := reflect.ValueOf(entity).Elem()

	for _, field := range modelInfo.Fields {
		if field.IsPrimary {
			continue
		}

		current := val.FieldByIndex(field.Index).Interface()
		if original, exists := orig[field.Column]; !exists || !fastEqual(original, current) {
			return true
		}
	}

	return false
}

// IsDirtyField checks if a specific field on an entity is dirty.
// This is a convenience method on Model.
func (m *Model[T]) IsDirtyField(entity *T, column string) bool {
	return isDirty(entity, column, m.modelInfo)
}

// IsCleanField checks if a specific field on an entity is clean (unchanged).
// This is a convenience method on Model.
func (m *Model[T]) IsCleanField(entity *T, column string) bool {
	return isClean(entity, column, m.modelInfo)
}

// GetDirtyFields returns all changed fields on an entity.
// This is a convenience method on Model.
func (m *Model[T]) GetDirtyFields(entity *T) map[string]any {
	return getDirty(entity, m.modelInfo)
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

// HasDirtyFields returns true if the entity has any dirty (changed) fields.
// This is more efficient than GetDirtyFields when you only need to check existence.
func (m *Model[T]) HasDirtyFields(entity *T) bool {
	return hasDirtyFields(entity, m.modelInfo)
}
