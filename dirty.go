package zorm

import (
	"container/list"
	"database/sql"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
	"weak"
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

// entityRef identifies the entity a tracking entry was created for.
//
// Entries are keyed by the entity's pointer address, and an address can be
// recycled: once an entity is garbage collected, a newly allocated entity may
// land on the same address. Holding a weak reference lets a lookup tell the two
// apart — a collected entity's reference resolves to nil, so the new occupant
// is correctly reported as untracked instead of inheriting a dead baseline.
type entityRef interface {
	// matches reports whether this reference still points at entity.
	matches(entity any) bool
}

// weakRef is the entityRef implementation. It is generic so weak.Pointer can
// hold the concrete entity type, and is stored behind the non-generic
// entityRef interface because the tracker itself is not generic.
type weakRef[T any] struct {
	p weak.Pointer[T]
}

func (w weakRef[T]) matches(entity any) bool {
	e, ok := entity.(*T)
	return ok && w.p.Value() == e
}

// trackerEntry represents a single entity's tracking data in the LRU cache.
type trackerEntry struct {
	key       uintptr
	originals map[string]any
	ref       entityRef     // Identifies the entity this baseline belongs to
	tx        *sql.Tx       // Caller-owned transaction that produced the baseline
	element   *list.Element // Position in LRU list
}

// lruTrackerShard is a single shard of the tracker with its own lock.
type lruTrackerShard struct {
	mu      sync.Mutex
	items   map[uintptr]*trackerEntry
	lruList *list.List // Front = most recently used, Back = least recently used
}

// shardCount is the number of shards for the tracker.
// Using 256 shards provides good distribution while keeping memory overhead low.
const shardCount = 256

// lruTracker provides bounded dirty tracking with LRU eviction.
// At the shared capacity, it evicts an LRU entry within a shard.
// Uses sharded locking to reduce contention under high concurrency.
type lruTracker struct {
	storeMu  sync.Mutex
	shards   [shardCount]*lruTrackerShard
	capacity int          // total capacity across all shards
	count    atomic.Int64 // total count across all shards for O(1) Len()
}

// newLRUTracker creates a new LRU tracker with the specified capacity.
// A capacity of 0 means unbounded (no eviction).
func newLRUTracker(capacity int) *lruTracker {
	t := &lruTracker{
		capacity: capacity,
	}

	for i := 0; i < shardCount; i++ {
		t.shards[i] = &lruTrackerShard{
			items:   make(map[uintptr]*trackerEntry),
			lruList: list.New(),
		}
	}

	return t
}

// getShard returns the shard for the given key.
func (t *lruTracker) getShard(key uintptr) *lruTrackerShard {
	// Pointer alignment leaves the low bits constant for many entity sizes.
	// Mix the whole address before selecting a shard.
	hash := uint64(key)
	hash = (hash ^ (hash >> 30)) * 0xbf58476d1ce4e5b9
	hash = (hash ^ (hash >> 27)) * 0x94d049bb133111eb
	hash ^= hash >> 31
	return t.shards[hash%shardCount]
}

// Store adds or updates tracking data for an entity. ref identifies the entity
// the baseline belongs to; see entityRef.
func (t *lruTracker) Store(key uintptr, originals map[string]any, ref entityRef) {
	t.storeMu.Lock()
	defer t.storeMu.Unlock()
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, exists := shard.items[key]; exists {
		// Update existing entry and move to front. The ref must be replaced
		// too: when this address has been recycled, the entry belongs to the
		// new entity now, and keeping the old ref would make every subsequent
		// lookup report untracked.
		entry.originals = originals
		entry.ref = ref
		entry.tx = nil
		shard.lruList.MoveToFront(entry.element)
		return
	}

	// Shards share the total capacity, so a busy shard can use free slots.
	evicted := false
	if t.capacity > 0 && t.count.Load() >= int64(t.capacity) {
		if len(shard.items) > 0 {
			shard.evictLRU()
			evicted = true
		} else {
			for _, other := range t.shards {
				if other == shard {
					continue
				}
				other.mu.Lock()
				if len(other.items) > 0 {
					other.evictLRU()
					evicted = true
				}
				other.mu.Unlock()
				if evicted {
					break
				}
			}
		}
	}

	// Add new entry
	entry := &trackerEntry{
		key:       key,
		originals: originals,
		ref:       ref,
	}
	entry.element = shard.lruList.PushFront(entry)
	shard.items[key] = entry

	// Only increment if we didn't evict (net +1)
	if !evicted {
		t.count.Add(1)
	}
}

// Load retrieves tracking data for entity, which must be the entity the key
// was derived from. An entry whose ref no longer matches belongs to a different
// entity that previously occupied this address, so it is not returned.
func (t *lruTracker) Load(key uintptr, entity any) (map[string]any, bool) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.items[key]
	if !exists {
		return nil, false
	}
	if entry.ref != nil && !entry.ref.matches(entity) {
		return nil, false
	}

	// Move to front (mark as recently used)
	shard.lruList.MoveToFront(entry.element)
	return entry.originals, true
}

// LoadForTransaction rejects a baseline produced by another caller-owned
// transaction. Its commit/rollback is invisible to ZORM, so it cannot be reused
// outside that transaction without reloading the entity.
func (t *lruTracker) LoadForTransaction(key uintptr, entity any, tx *sql.Tx) (map[string]any, bool) {
	originals, _, ok := t.loadTransactionBaseline(key, entity, tx)
	return originals, ok
}

func (t *lruTracker) loadTransactionBaseline(key uintptr, entity any, tx *sql.Tx) (map[string]any, *sql.Tx, bool) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, exists := shard.items[key]
	if !exists || (entry.ref != nil && !entry.ref.matches(entity)) || (entry.tx != nil && entry.tx != tx) {
		return nil, nil, false
	}
	shard.lruList.MoveToFront(entry.element)
	return entry.originals, entry.tx, true
}

func (t *lruTracker) BindTransaction(key uintptr, entity any, tx *sql.Tx) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if entry, exists := shard.items[key]; exists && (entry.ref == nil || entry.ref.matches(entity)) {
		entry.tx = tx
	}
}

// Delete removes tracking data for an entity.
func (t *lruTracker) Delete(key uintptr) {
	t.deleteMatching(key, nil)
}

// deleteMatching removes the entry at key only when it is still the entry that
// ref was stored with. A nil ref deletes unconditionally.
//
// This matters for deferred cleanup such as TrackingScope.Close: by the time it
// runs, the scoped entity may have been collected and its address reused by an
// unrelated entity, whose baseline must not be dropped.
func (t *lruTracker) deleteMatching(key uintptr, ref entityRef) {
	shard := t.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.items[key]
	if !exists {
		return
	}
	if ref != nil && entry.ref != ref {
		return
	}

	shard.lruList.Remove(entry.element)
	delete(shard.items, key)
	t.count.Add(-1)
}

// Clear removes all tracking data.
func (t *lruTracker) Clear() {
	t.storeMu.Lock()
	defer t.storeMu.Unlock()
	for i := 0; i < shardCount; i++ {
		shard := t.shards[i]
		shard.mu.Lock()
		t.count.Add(-int64(len(shard.items)))
		shard.items = make(map[uintptr]*trackerEntry)
		shard.lruList.Init()
		shard.mu.Unlock()
	}
}

// Len returns the number of tracked entities.
// Uses atomic counter for O(1) performance instead of O(shardCount) with locks.
func (t *lruTracker) Len() int {
	return int(t.count.Load())
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
	mu     sync.Mutex
	keys   map[uintptr]entityRef
	closed atomic.Bool
}

// NewTrackingScope creates a new tracking scope.
func NewTrackingScope() *TrackingScope {
	return &TrackingScope{
		keys: make(map[uintptr]entityRef),
	}
}

// track records a key and the entity reference it was stored with, so Close can
// delete only the entries this scope actually created.
func (s *TrackingScope) track(key uintptr, ref entityRef) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check keys != nil inside lock to prevent race with Close()
	if s.keys != nil {
		s.keys[key] = ref
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

	// Clear all tracked entities from the global tracker, leaving alone any
	// address that has since been taken over by a different entity.
	tracker := globalTracker.Load()
	for key, ref := range keys {
		tracker.deleteMatching(key, ref)
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
// By default, it tracks up to 50,000 entities. Use ConfigureDirtyTracking to change capacity.
// The higher default reduces the chance of evicting actively-used entities.
var globalTracker atomic.Pointer[lruTracker]

func init() {
	globalTracker.Store(newLRUTracker(50000))
}

// Memory management considerations for dirty tracking:
// - Use ConfigureDirtyTracking() to set a maximum capacity for bounded tracking
// - Call ClearOriginals() when an entity is no longer needed to prevent memory leaks
// - Use TrackingScope for batch operations with automatic cleanup
// - In long-running services, consider using LRU-bounded tracking or scoped tracking

// ConfigureDirtyTracking sets the maximum number of entities to track.
// A capacity of 0 means unbounded (no eviction).
// The default capacity is 50,000 entities.
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
// Tracking data is bounded by the LRU capacity (default 10,000 entities).
func trackOriginals[T any](entity *T, modelInfo *ModelInfo) {
	trackOriginalsWithScope(entity, modelInfo, nil)
}

// trackOriginalsWithScope stores originals and optionally registers with a scope.
//
// Memory management: The LRU tracker is capacity-bounded (default 10,000 entries)
// and evicts the least-recently-used entry when full. For long-running services
// that process more entities than the capacity, callers should use TrackingScope
// (defer scope.Close()) or call ClearOriginals(entity) explicitly when the entity
// is no longer needed.
func trackOriginalsWithScope[T any](entity *T, modelInfo *ModelInfo, scope *TrackingScope) {
	if entity == nil {
		return
	}

	val := reflect.ValueOf(entity).Elem()
	originals := make(map[string]any, len(modelInfo.OrderedFields))

	for _, field := range modelInfo.OrderedFields {
		fVal := val.FieldByIndex(field.Index)
		originals[field.Column] = snapshotValue(fVal.Interface())
	}

	key := getEntityKey(entity)
	ref := weakRef[T]{p: weak.Make(entity)}
	tracker := globalTracker.Load()
	tracker.Store(key, originals, ref)

	// Register with scope if provided
	if scope != nil {
		scope.track(key, ref)
	}
}

// rememberOriginals records rollback metadata before a transaction changes an
// entity's baseline. User edits remain pending; only the managed version is reset.
func (m *Model[T]) rememberOriginals(entity *T) {
	if m.txState == nil {
		return
	}
	key := getEntityKey(entity)
	originals, originalTx, tracked := globalTracker.Load().loadTransactionBaseline(key, entity, m.tx)
	ref := weakRef[T]{p: weak.Make(entity)}
	m.txState.remember(key, func(rolledBack bool) {
		if !rolledBack || !tracked {
			ClearOriginals(entity)
			return
		}
		globalTracker.Load().Store(key, originals, ref)
		globalTracker.Load().BindTransaction(key, entity, originalTx)
		if field := m.modelInfo.VersionField; field != nil {
			if version, ok := originals[field.Column]; ok {
				reflect.ValueOf(entity).Elem().FieldByIndex(field.Index).Set(reflect.ValueOf(version))
			}
		}
	})
}

// trackOriginals also arranges cleanup when a transaction-created baseline is
// rolled back. The same rule applies to reads, cursors, and newly created rows.
func (m *Model[T]) trackOriginals(entity *T) {
	m.rememberOriginals(entity)
	trackOriginalsWithScope(entity, m.modelInfo, m.trackingScope)
	m.bindOriginals(entity)
}

// bindOriginals keeps caller-owned transaction baselines usable for repeated
// writes inside that transaction, while Save rejects them outside it.
func (m *Model[T]) bindOriginals(entity *T) {
	if m.tx != nil && (m.txState == nil || m.txState.callerOwned) {
		globalTracker.Load().BindTransaction(getEntityKey(entity), entity, m.tx)
	}
}

func (m *Model[T]) syncOriginalColumns(entity *T, columns []string) {
	syncOriginalColumns(entity, m.modelInfo, columns, m.tx)
	m.bindOriginals(entity)
}

// snapshotValue detaches mutable column values from the entity. A shallow copy
// would let in-place pointer, slice, or map edits also change the baseline.
func snapshotValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneSnapshot(reflect.ValueOf(value), make(map[snapshotVisit]reflect.Value)).Interface()
}

type snapshotVisit struct {
	typ reflect.Type
	ptr uintptr
	len int // Slices sharing a start address can have different lengths.
}

func cloneSnapshot(v reflect.Value, seen map[snapshotVisit]reflect.Value) reflect.Value {
	// Treat time as a value, preserving its location and monotonic clock data.
	if v.Type() == reflect.TypeFor[time.Time]() {
		return v
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		visit := snapshotVisit{typ: v.Type(), ptr: uintptr(v.UnsafePointer())}
		if v.Kind() == reflect.Slice {
			visit.len = v.Len()
		}
		if previous, ok := seen[visit]; ok {
			return previous
		}
		var result reflect.Value
		switch v.Kind() {
		case reflect.Pointer:
			result = reflect.New(v.Type().Elem()).Convert(v.Type())
			seen[visit] = result
			result.Elem().Set(cloneSnapshot(v.Elem(), seen))
		case reflect.Slice:
			result = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
			seen[visit] = result
			for i := 0; i < v.Len(); i++ {
				result.Index(i).Set(cloneSnapshot(v.Index(i), seen))
			}
		case reflect.Map:
			result = reflect.MakeMapWithSize(v.Type(), v.Len())
			seen[visit] = result
			iter := v.MapRange()
			for iter.Next() {
				// Keys retain their identity, as required by Go map equality.
				result.SetMapIndex(iter.Key(), cloneSnapshot(iter.Value(), seen))
			}
		}
		return result
	case reflect.Interface:
		result := reflect.New(v.Type()).Elem()
		if !v.IsNil() {
			result.Set(cloneSnapshot(v.Elem(), seen))
		}
		return result
	case reflect.Struct:
		result := reflect.New(v.Type()).Elem()
		result.Set(v)
		for i := 0; i < v.NumField(); i++ {
			field := result.Field(i)
			if !field.CanInterface() {
				// Access only our addressable copy. Private scanner fields may
				// hold mutable references that must not alias the entity.
				field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
			}
			field.Set(cloneSnapshot(field, seen))
		}
		return result
	case reflect.Array:
		result := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			result.Index(i).Set(cloneSnapshot(v.Index(i), seen))
		}
		return result
	default:
		return v
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
// Mutable values are returned as detached copies.
// Returns nil if the entity is not tracked or field doesn't exist.
func GetOriginal[T any](entity *T, column string) any {
	if entity == nil {
		return nil
	}

	if originals, ok := globalTracker.Load().Load(getEntityKey(entity), entity); ok {
		if orig, exists := originals[column]; exists {
			return snapshotValue(orig)
		}
	}
	return nil
}

// GetOriginals returns all original values for the entity.
// Returns detached copies to prevent external modification of tracking data.
func GetOriginals[T any](entity *T) map[string]any {
	if entity == nil {
		return nil
	}

	if originals, ok := globalTracker.Load().Load(getEntityKey(entity), entity); ok {
		result := make(map[string]any, len(originals))
		for column, value := range originals {
			result[column] = snapshotValue(value)
		}
		return result
	}
	return nil
}

// isDirty checks if a specific field has changed from its original value.
// Returns true if the field was modified, or if the entity is not tracked (new entity).
// Returns false if the field is unchanged or if the entity/column is nil/invalid.
//
// Note: This function touches the LRU entry to prevent actively-used entities
// from being evicted during modification-then-update cycles.
func isDirty[T any](entity *T, column string, modelInfo *ModelInfo) bool {
	if entity == nil {
		return false
	}
	field, ok := modelInfo.Columns[column]
	if !ok {
		return false
	}

	// Load also touches the LRU entry, preventing eviction of actively-used entities
	orig, ok := globalTracker.Load().Load(getEntityKey(entity), entity)
	if !ok {
		return true // Not tracked = treat as dirty (new entity)
	}

	original, exists := orig[column]
	if !exists {
		return true // Column not in originals
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
	originals, _ := globalTracker.Load().Load(getEntityKey(entity), entity)
	return getDirtyFromOriginals(entity, modelInfo, originals)
}

// getDirtyFromOriginals uses one retained baseline for the whole comparison.
// Save holds this snapshot across hooks so cache eviction cannot widen a write.
func getDirtyFromOriginals[T any](entity *T, modelInfo *ModelInfo, originals map[string]any) map[string]any {
	val := reflect.ValueOf(entity).Elem()
	dirty := make(map[string]any, 4)
	for _, field := range modelInfo.OrderedFields {
		if field.IsPrimary {
			continue
		}
		current := val.FieldByIndex(field.Index).Interface()
		if original, exists := originals[field.Column]; !exists || !fastEqual(original, current) {
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
	_, ok := globalTracker.Load().Load(getEntityKey(entity), entity)
	return ok
}

// syncOriginals updates the stored originals to match current values.
// Call after a successful save to mark entity as clean.
func syncOriginals[T any](entity *T, modelInfo *ModelInfo) {
	trackOriginals(entity, modelInfo)
}

// syncOriginalColumns marks only persisted columns clean. Excluded values must
// retain their previous baseline so a later Save can still write them. Only a
// baseline belonging to the writer can describe the unwritten columns safely.
func syncOriginalColumns[T any](entity *T, modelInfo *ModelInfo, columns []string, tx *sql.Tx) {
	originals, tracked := globalTracker.Load().LoadForTransaction(getEntityKey(entity), entity, tx)
	if !tracked {
		// A partial write cannot establish a safe baseline for an entity whose
		// other values were never loaded. A full write can, as Update always did.
		written := make(map[string]bool, len(columns))
		for _, column := range columns {
			written[column] = true
		}
		for _, field := range modelInfo.OrderedFields {
			if !field.IsPrimary && !written[field.Column] {
				return
			}
		}
		syncOriginals(entity, modelInfo)
		return
	}

	syncOriginalColumnsFromOriginals(entity, modelInfo, columns, originals)
}

// syncOriginalColumnsFromOriginals preserves unwritten values even when the
// retained baseline has been evicted from the global tracker during a write.
func syncOriginalColumnsFromOriginals[T any](entity *T, modelInfo *ModelInfo, columns []string, originals map[string]any) {
	// Replace the map instead of mutating it: readers can still hold the old
	// baseline after the tracker's lock has been released.
	next := make(map[string]any, len(originals))
	for column, value := range originals {
		next[column] = value
	}
	val := reflect.ValueOf(entity).Elem()
	for _, column := range columns {
		field := modelInfo.Columns[column]
		next[column] = snapshotValue(val.FieldByIndex(field.Index).Interface())
	}
	globalTracker.Load().Store(getEntityKey(entity), next, weakRef[T]{p: weak.Make(entity)})
}

// hasDirtyFields returns true if any non-primary field has changed.
// This is more efficient than getDirty when you only need to know if changes exist.
func hasDirtyFields[T any](entity *T, modelInfo *ModelInfo) bool {
	if entity == nil {
		return false
	}

	orig, tracked := globalTracker.Load().Load(getEntityKey(entity), entity)
	if !tracked {
		return true // Untracked entities are considered dirty
	}

	val := reflect.ValueOf(entity).Elem()

	for _, field := range modelInfo.OrderedFields {
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
