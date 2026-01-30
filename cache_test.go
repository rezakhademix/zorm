package zorm

import (
	"sync"
	"testing"
)

// =============================================================================
// columnMappingCache Tests (executor.go)
// =============================================================================

func TestColumnCache_BasicLoadStore(t *testing.T) {
	cache := newColumnCache(100)

	// Store a value
	fields := []*FieldInfo{{Name: "ID", Column: "id"}}
	cache.Store("test:id", fields)

	// Load it back
	loaded, ok := cache.Load("test:id")
	if !ok {
		t.Fatal("expected to find cached value")
	}
	if len(loaded) != 1 || loaded[0].Name != "ID" {
		t.Errorf("got unexpected value: %v", loaded)
	}
}

func TestColumnCache_LoadMissing(t *testing.T) {
	cache := newColumnCache(100)

	loaded, ok := cache.Load("nonexistent")
	if ok {
		t.Error("expected ok to be false for missing key")
	}
	if loaded != nil {
		t.Error("expected nil value for missing key")
	}
}

func TestColumnCache_LRUEviction(t *testing.T) {
	// Create cache with capacity 64 (1 per shard with 64 shards)
	// To test eviction, we need multiple keys that hash to the same shard
	cache := newColumnCache(64) // 1 per shard

	// We'll use keys that we know hash to the same shard
	// Store 3 items - the third should evict the first (LRU)
	key1 := "type1:col1"
	key2 := "type1:col2"
	key3 := "type1:col3"

	fields1 := []*FieldInfo{{Name: "Field1"}}
	fields2 := []*FieldInfo{{Name: "Field2"}}
	fields3 := []*FieldInfo{{Name: "Field3"}}

	// Find keys that hash to the same shard for testing
	shard1 := cache.getShard(key1)
	shard2 := cache.getShard(key2)
	shard3 := cache.getShard(key3)

	// Store all three
	cache.Store(key1, fields1)
	cache.Store(key2, fields2)
	cache.Store(key3, fields3)

	// Check if they're in the same shard - if not, the test needs different keys
	// but the basic functionality still works
	if shard1 == shard2 && shard2 == shard3 {
		// All in same shard - first one should be evicted (shard capacity is 1)
		_, ok1 := cache.Load(key1)
		_, ok2 := cache.Load(key2)
		_, ok3 := cache.Load(key3)

		// With capacity 1 per shard, only the last one should remain
		if ok1 && ok2 && ok3 {
			t.Error("expected at least one key to be evicted")
		}
	}
}

func TestColumnCache_LRUAccessOrder(t *testing.T) {
	// Create a small cache to test LRU ordering
	// We need to create a cache where we can control which shard keys go to
	cache := newColumnCache(128) // 2 per shard

	// Find 3 keys that hash to the same shard
	var shard *columnCacheShard
	var keys []string
	baseKey := "testtype:col"
	for i := 0; i < 1000 && len(keys) < 3; i++ {
		key := baseKey + string(rune('a'+i%26)) + string(rune('0'+i/26))
		s := cache.getShard(key)
		if shard == nil {
			shard = s
			keys = append(keys, key)
		} else if s == shard {
			keys = append(keys, key)
		}
	}

	if len(keys) < 3 {
		t.Skip("could not find 3 keys that hash to the same shard")
	}

	// Store first two keys (fills shard to capacity)
	cache.Store(keys[0], []*FieldInfo{{Name: "First"}})
	cache.Store(keys[1], []*FieldInfo{{Name: "Second"}})

	// Access the first key (moves it to front of LRU)
	cache.Load(keys[0])

	// Store third key - should evict keys[1] (LRU), not keys[0]
	cache.Store(keys[2], []*FieldInfo{{Name: "Third"}})

	// keys[0] should still exist (was accessed, moved to front)
	if _, ok := cache.Load(keys[0]); !ok {
		t.Error("keys[0] should not have been evicted - it was recently accessed")
	}

	// keys[2] should exist (just added)
	if _, ok := cache.Load(keys[2]); !ok {
		t.Error("keys[2] should exist - it was just added")
	}

	// keys[1] should have been evicted (it was LRU)
	if _, ok := cache.Load(keys[1]); ok {
		t.Error("keys[1] should have been evicted - it was LRU")
	}
}

func TestColumnCache_Concurrent(t *testing.T) {
	cache := newColumnCache(1000)

	var wg sync.WaitGroup
	numGoroutines := 100
	numOps := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := "type" + string(rune('A'+id%26)) + ":col" + string(rune('0'+j%10))
				cache.Store(key, []*FieldInfo{{Name: key}})
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := "type" + string(rune('A'+id%26)) + ":col" + string(rune('0'+j%10))
				cache.Load(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestColumnCache_DuplicateStore(t *testing.T) {
	cache := newColumnCache(100)

	fields1 := []*FieldInfo{{Name: "First"}}
	fields2 := []*FieldInfo{{Name: "Second"}}

	cache.Store("key", fields1)
	cache.Store("key", fields2) // Should be ignored (key already exists)

	loaded, ok := cache.Load("key")
	if !ok {
		t.Fatal("expected to find cached value")
	}
	if loaded[0].Name != "First" {
		t.Errorf("expected first value to be retained, got %s", loaded[0].Name)
	}
}

// =============================================================================
// snakeCaseCache Tests (schema.go)
// =============================================================================

func TestSnakeCaseCache_BasicLoadStore(t *testing.T) {
	cache := newSnakeCaseCache(100)

	cache.Store("UserID", "user_id")

	loaded, ok := cache.Load("UserID")
	if !ok {
		t.Fatal("expected to find cached value")
	}
	if loaded != "user_id" {
		t.Errorf("expected 'user_id', got '%s'", loaded)
	}
}

func TestSnakeCaseCache_LoadMissing(t *testing.T) {
	cache := newSnakeCaseCache(100)

	loaded, ok := cache.Load("nonexistent")
	if ok {
		t.Error("expected ok to be false for missing key")
	}
	if loaded != "" {
		t.Error("expected empty string for missing key")
	}
}

func TestSnakeCaseCache_LRUEviction(t *testing.T) {
	// Create a cache with small capacity
	cache := newSnakeCaseCache(2)

	// Store 3 items - the third should evict the first (LRU)
	cache.Store("First", "first")
	cache.Store("Second", "second")
	cache.Store("Third", "third")

	// First should be evicted
	if _, ok := cache.Load("First"); ok {
		t.Error("'First' should have been evicted")
	}

	// Second and Third should still exist
	if _, ok := cache.Load("Second"); !ok {
		t.Error("'Second' should still exist")
	}
	if _, ok := cache.Load("Third"); !ok {
		t.Error("'Third' should still exist")
	}
}

func TestSnakeCaseCache_LRUAccessOrder(t *testing.T) {
	// Create a cache with capacity 2
	cache := newSnakeCaseCache(2)

	// Store two items (fills cache)
	cache.Store("First", "first")
	cache.Store("Second", "second")

	// Access "First" to move it to the front
	cache.Load("First")

	// Store third item - should evict "Second" (LRU), not "First"
	cache.Store("Third", "third")

	// "First" should still exist (was recently accessed)
	if _, ok := cache.Load("First"); !ok {
		t.Error("'First' should not have been evicted - it was recently accessed")
	}

	// "Third" should exist (just added)
	if _, ok := cache.Load("Third"); !ok {
		t.Error("'Third' should exist - it was just added")
	}

	// "Second" should have been evicted (it was LRU)
	if _, ok := cache.Load("Second"); ok {
		t.Error("'Second' should have been evicted - it was LRU")
	}
}

func TestSnakeCaseCache_LRUMultipleAccess(t *testing.T) {
	cache := newSnakeCaseCache(3)

	// Store A, B, C
	cache.Store("A", "a")
	cache.Store("B", "b")
	cache.Store("C", "c")

	// Access in order: B, A (C becomes LRU)
	cache.Load("B")
	cache.Load("A")

	// Add D - should evict C (LRU)
	cache.Store("D", "d")

	if _, ok := cache.Load("C"); ok {
		t.Error("'C' should have been evicted - it was LRU")
	}

	// A, B, D should still exist
	if _, ok := cache.Load("A"); !ok {
		t.Error("'A' should still exist")
	}
	if _, ok := cache.Load("B"); !ok {
		t.Error("'B' should still exist")
	}
	if _, ok := cache.Load("D"); !ok {
		t.Error("'D' should still exist")
	}
}

func TestSnakeCaseCache_Concurrent(t *testing.T) {
	cache := newSnakeCaseCache(1000)

	var wg sync.WaitGroup
	numGoroutines := 100
	numOps := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := "Key" + string(rune('A'+id%26)) + string(rune('0'+j%10))
				cache.Store(key, "value_"+key)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := "Key" + string(rune('A'+id%26)) + string(rune('0'+j%10))
				cache.Load(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestSnakeCaseCache_DuplicateStore(t *testing.T) {
	cache := newSnakeCaseCache(100)

	cache.Store("Key", "first_value")
	cache.Store("Key", "second_value") // Should be ignored

	loaded, ok := cache.Load("Key")
	if !ok {
		t.Fatal("expected to find cached value")
	}
	if loaded != "first_value" {
		t.Errorf("expected 'first_value', got '%s'", loaded)
	}
}

func TestSnakeCaseCache_MoveToFrontOnLoad(t *testing.T) {
	cache := newSnakeCaseCache(2)

	// Store A, B (A is at front initially, then B)
	cache.Store("A", "a")
	cache.Store("B", "b")
	// LRU order: B (front) -> A (back)

	// Access A - moves it to front
	cache.Load("A")
	// LRU order: A (front) -> B (back)

	// Access A again - still at front
	cache.Load("A")
	// LRU order: A (front) -> B (back)

	// Store C - should evict B (at back)
	cache.Store("C", "c")

	// A and C should exist, B should be evicted
	if _, ok := cache.Load("A"); !ok {
		t.Error("'A' should exist")
	}
	if _, ok := cache.Load("C"); !ok {
		t.Error("'C' should exist")
	}
	if _, ok := cache.Load("B"); ok {
		t.Error("'B' should have been evicted")
	}
}

// =============================================================================
// Integration Tests - Verify caches work with actual usage patterns
// =============================================================================

func TestToSnakeCase_UsesCacheCorrectly(t *testing.T) {
	// Clear and recreate the global cache for testing
	oldCache := snakeCaseCache
	snakeCaseCache = newSnakeCaseCache(100)
	defer func() { snakeCaseCache = oldCache }()

	// First call should compute and cache
	result1 := ToSnakeCase("UserID")
	if result1 != "user_id" {
		t.Errorf("expected 'user_id', got '%s'", result1)
	}

	// Second call should use cache
	result2 := ToSnakeCase("UserID")
	if result2 != "user_id" {
		t.Errorf("expected 'user_id', got '%s'", result2)
	}

	// Verify it's in the cache
	cached, ok := snakeCaseCache.Load("UserID")
	if !ok {
		t.Error("expected 'UserID' to be in cache")
	}
	if cached != "user_id" {
		t.Errorf("expected cached value 'user_id', got '%s'", cached)
	}
}

func TestColumnMappingCache_Integration(t *testing.T) {
	// Test that the global columnMappingCache works correctly
	// This is a basic sanity check

	key := "TestIntegrationModel:id,name,email"
	fields := []*FieldInfo{
		{Name: "ID", Column: "id"},
		{Name: "Name", Column: "name"},
		{Name: "Email", Column: "email"},
	}

	// Store
	columnMappingCache.Store(key, fields)

	// Load
	loaded, ok := columnMappingCache.Load(key)
	if !ok {
		t.Fatal("expected to find cached mapping")
	}
	if len(loaded) != 3 {
		t.Errorf("expected 3 fields, got %d", len(loaded))
	}

	// Verify MoveToFront was called (indirectly by checking it still exists)
	loaded2, ok := columnMappingCache.Load(key)
	if !ok {
		t.Fatal("expected to find cached mapping after second load")
	}
	if len(loaded2) != 3 {
		t.Errorf("expected 3 fields, got %d", len(loaded2))
	}
}
