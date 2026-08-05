package zorm

import (
	"context"
	"database/sql"
	"runtime"
	"testing"
	"weak"

	_ "github.com/mattn/go-sqlite3"
)

// DirtyUser is a test model for dirty tracking tests
type DirtyUser struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Email string
}

func (u DirtyUser) TableName() string { return "dirty_users" }

func setupDirtyDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE dirty_users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func TestTrackOriginals(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()
	model := New[DirtyUser]().SetDB(db)

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := model.Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	loaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Verify it's tracked
	if !IsTracked(loaded) {
		t.Error("Expected loaded entity to be tracked")
	}

	// Verify original values match
	origName := GetOriginal(loaded, "name")
	if origName != "John Doe" {
		t.Errorf("Expected original name 'John Doe', got '%v'", origName)
	}

	origEmail := GetOriginal(loaded, "email")
	if origEmail != "john@example.com" {
		t.Errorf("Expected original email 'john@example.com', got '%v'", origEmail)
	}
}

func TestIsDirty(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	model := New[DirtyUser]().SetDB(db)
	loaded, err := model.Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Initially nothing is dirty
	if model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be clean initially")
	}
	if model.IsDirtyField(loaded, "email") {
		t.Error("Expected email to be clean initially")
	}

	// Modify name
	loaded.Name = "Jane Doe"

	// Now name should be dirty, email should be clean
	if !model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be dirty after modification")
	}
	if model.IsDirtyField(loaded, "email") {
		t.Error("Expected email to still be clean")
	}
}

func TestGetDirty(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	model := New[DirtyUser]().SetDB(db)
	loaded, err := model.Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Initially no dirty fields
	dirty := model.GetDirtyFields(loaded)
	if len(dirty) != 0 {
		t.Errorf("Expected no dirty fields initially, got %d", len(dirty))
	}

	// Modify name and email
	loaded.Name = "Jane Doe"
	loaded.Email = "jane@example.com"

	// Now should have 2 dirty fields
	dirty = model.GetDirtyFields(loaded)
	if len(dirty) != 2 {
		t.Errorf("Expected 2 dirty fields, got %d", len(dirty))
	}

	if dirty["name"] != "Jane Doe" {
		t.Errorf("Expected dirty name to be 'Jane Doe', got '%v'", dirty["name"])
	}
	if dirty["email"] != "jane@example.com" {
		t.Errorf("Expected dirty email to be 'jane@example.com', got '%v'", dirty["email"])
	}
}

func TestUpdateColumns(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	loaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	originalEmail := loaded.Email

	// Modify both name and email
	loaded.Name = "Jane Doe"
	loaded.Email = "jane@example.com"

	// Only update name using UpdateColumns
	err = New[DirtyUser]().SetDB(db).UpdateColumns(ctx, loaded, "name")
	if err != nil {
		t.Fatalf("Failed to update columns: %v", err)
	}

	// Reload from DB
	reloaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to reload user: %v", err)
	}

	// Verify name was updated
	if reloaded.Name != "Jane Doe" {
		t.Errorf("Expected name to be 'Jane Doe', got '%s'", reloaded.Name)
	}

	// Verify email is still the original (not the modified value)
	if reloaded.Email != originalEmail {
		t.Errorf("Expected email to remain '%s', got '%s'", originalEmail, reloaded.Email)
	}
}

func TestOmit(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	loaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	originalEmail := loaded.Email

	// Modify both name and email
	loaded.Name = "Jane Doe"
	loaded.Email = "jane@example.com"

	// Update using Omit to exclude email
	err = New[DirtyUser]().SetDB(db).Omit("email").Update(ctx, loaded)
	if err != nil {
		t.Fatalf("Failed to update with omit: %v", err)
	}

	// Reload from DB
	reloaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to reload user: %v", err)
	}

	// Verify name was updated
	if reloaded.Name != "Jane Doe" {
		t.Errorf("Expected name to be 'Jane Doe', got '%s'", reloaded.Name)
	}

	// Verify email is still the original (omitted from update)
	if reloaded.Email != originalEmail {
		t.Errorf("Expected email to remain '%s', got '%s'", originalEmail, reloaded.Email)
	}
}

func TestClearOriginals(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	loaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Verify it's tracked
	if !IsTracked(loaded) {
		t.Error("Expected loaded entity to be tracked")
	}

	// Clear tracking
	ClearOriginals(loaded)

	// Verify it's no longer tracked
	if IsTracked(loaded) {
		t.Error("Expected entity to not be tracked after ClearOriginals")
	}
}

func TestGetOriginals(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	loaded, err := New[DirtyUser]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Modify values
	loaded.Name = "Jane Doe"
	loaded.Email = "jane@example.com"

	// Get all originals
	originals := GetOriginals(loaded)

	// Verify originals contain the original values
	if originals["name"] != "John Doe" {
		t.Errorf("Expected original name 'John Doe', got '%v'", originals["name"])
	}
	if originals["email"] != "john@example.com" {
		t.Errorf("Expected original email 'john@example.com', got '%v'", originals["email"])
	}
}

func TestSyncOriginals(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	model := New[DirtyUser]().SetDB(db)
	loaded, err := model.Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Modify name
	loaded.Name = "Jane Doe"

	// Verify it's dirty
	if !model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be dirty")
	}

	// Sync originals (simulates what happens after a successful save)
	syncOriginals(loaded, model.modelInfo)

	// Now it should be clean
	if model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be clean after syncOriginals")
	}

	// And the original should now be the new value
	origName := GetOriginal(loaded, "name")
	if origName != "Jane Doe" {
		t.Errorf("Expected synced original name 'Jane Doe', got '%v'", origName)
	}
}

func TestUntrackedEntityIsDirty(t *testing.T) {
	// Create an entity without loading from DB (not tracked)
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}

	model := New[DirtyUser]()

	// Untracked entity should report all fields as dirty
	if !model.IsDirtyField(user, "name") {
		t.Error("Expected name to be dirty for untracked entity")
	}
	if !model.IsDirtyField(user, "email") {
		t.Error("Expected email to be dirty for untracked entity")
	}

	// GetDirtyFields should return all non-primary fields
	dirty := model.GetDirtyFields(user)
	if len(dirty) == 0 {
		t.Error("Expected dirty fields for untracked entity")
	}
}

func TestIsClean(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	model := New[DirtyUser]().SetDB(db)
	loaded, err := model.Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Initially everything is clean
	if !model.IsCleanField(loaded, "name") {
		t.Error("Expected name to be clean initially")
	}
	if !model.IsCleanField(loaded, "email") {
		t.Error("Expected email to be clean initially")
	}

	// Modify name
	loaded.Name = "Jane Doe"

	// Name should not be clean, email should still be clean
	if model.IsCleanField(loaded, "name") {
		t.Error("Expected name to not be clean after modification")
	}
	if !model.IsCleanField(loaded, "email") {
		t.Error("Expected email to still be clean")
	}
}

func TestHasDirtyFields(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	model := New[DirtyUser]().SetDB(db)
	loaded, err := model.Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Initially no dirty fields
	if model.HasDirtyFields(loaded) {
		t.Error("Expected no dirty fields initially")
	}

	// Modify name
	loaded.Name = "Jane Doe"

	// Now should have dirty fields
	if !model.HasDirtyFields(loaded) {
		t.Error("Expected dirty fields after modification")
	}
}

func TestEntityTrackedAfterCreate(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()
	model := New[DirtyUser]().SetDB(db)

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}

	// Before create, entity is not tracked
	if IsTracked(user) {
		t.Error("Expected entity to not be tracked before create")
	}

	err := model.Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// After create, entity should be tracked
	if !IsTracked(user) {
		t.Error("Expected entity to be tracked after create")
	}

	// Entity should be clean (no dirty fields)
	if model.HasDirtyFields(user) {
		t.Error("Expected no dirty fields after create")
	}
}

func TestUpdateSyncsOriginals(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user
	user := &DirtyUser{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err := New[DirtyUser]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	model := New[DirtyUser]().SetDB(db)
	loaded, err := model.Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Modify name
	loaded.Name = "Jane Doe"

	// Verify it's dirty before update
	if !model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be dirty before update")
	}

	// Update
	err = model.Update(ctx, loaded)
	if err != nil {
		t.Fatalf("Failed to update user: %v", err)
	}

	// After update, entity should be clean
	if model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be clean after update")
	}

	// Original should now be the new value
	origName := GetOriginal(loaded, "name")
	if origName != "Jane Doe" {
		t.Errorf("Expected original name 'Jane Doe' after update, got '%v'", origName)
	}
}

// DirtyUserWithUnique is a test model with a unique constraint
type DirtyUserWithUnique struct {
	ID          int `zorm:"primaryKey"`
	Name        string
	PhoneNumber string // This will be unique
}

func (u DirtyUserWithUnique) TableName() string { return "dirty_users_unique" }

func setupDirtyDBWithUnique(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE dirty_users_unique (
			id INTEGER PRIMARY KEY,
			name TEXT,
			phone_number TEXT UNIQUE
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

// TestUpdateWithOmitPreventsUniqueConflict demonstrates that even with
// the regular Update() method, using Omit() can prevent unique constraint issues.
func TestUpdateWithOmitPreventsUniqueConflict(t *testing.T) {
	db := setupDirtyDBWithUnique(t)
	defer db.Close()

	ctx := context.Background()

	// Create a user with a unique phone number
	user := &DirtyUserWithUnique{
		Name:        "John Doe",
		PhoneNumber: "555-1234",
	}
	err := New[DirtyUserWithUnique]().SetDB(db).Create(ctx, user)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Load user from DB
	loaded, err := New[DirtyUserWithUnique]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}

	// Only modify name
	loaded.Name = "Jane Doe"

	// Using Update() with Omit() to exclude the unique field
	err = New[DirtyUserWithUnique]().SetDB(db).Omit("phone_number").Update(ctx, loaded)
	if err != nil {
		t.Fatalf("Update with Omit should succeed: %v", err)
	}

	// Verify the update worked
	reloaded, err := New[DirtyUserWithUnique]().SetDB(db).Find(ctx, user.ID)
	if err != nil {
		t.Fatalf("Failed to reload user: %v", err)
	}

	if reloaded.Name != "Jane Doe" {
		t.Errorf("Expected name 'Jane Doe', got '%s'", reloaded.Name)
	}
	if reloaded.PhoneNumber != "555-1234" {
		t.Errorf("Expected phone_number '555-1234', got '%s'", reloaded.PhoneNumber)
	}
}

// ============================================================
// Memory Management Tests
// ============================================================

func TestLRUTracker_Eviction(t *testing.T) {
	// With sharded tracker (256 shards), a capacity of 512 gives 2 per shard.
	// Use keys that hash to the same shard to test eviction within a shard.
	tracker := newLRUTracker(512)

	// Keys 256, 512, 768 all map to shard 0 (key % 256 == 0)
	key1 := uintptr(256)
	key2 := uintptr(512)
	key3 := uintptr(768)

	// Add 2 entities to shard 0 (at capacity)
	tracker.Store(key1, map[string]any{"name": "user1"}, nil)
	tracker.Store(key2, map[string]any{"name": "user2"}, nil)

	// Verify both exist
	if _, ok := tracker.Load(key1, nil); !ok {
		t.Error("Expected key1 to exist")
	}
	if _, ok := tracker.Load(key2, nil); !ok {
		t.Error("Expected key2 to exist")
	}

	// Adding key3 should evict the oldest (key1)
	tracker.Store(key3, map[string]any{"name": "user3"}, nil)

	// key1 should be evicted (was LRU)
	if _, ok := tracker.Load(key1, nil); ok {
		t.Error("Expected key1 to be evicted")
	}

	// key2 and key3 should exist
	if _, ok := tracker.Load(key2, nil); !ok {
		t.Error("Expected key2 to exist")
	}
	if _, ok := tracker.Load(key3, nil); !ok {
		t.Error("Expected key3 to exist")
	}
}

func TestLRUTracker_AccessOrder(t *testing.T) {
	// Create a tracker with a larger capacity so each shard can hold multiple items
	// With 256 shards, a capacity of 512 gives each shard capacity of 2
	tracker := newLRUTracker(512)

	// Keys 256, 512, 768 all map to shard 0 (key % 256 == 0)
	key1 := uintptr(256)
	key2 := uintptr(512)
	key3 := uintptr(768)

	// Add 3 entities to the same shard (shard has capacity 2)
	tracker.Store(key1, map[string]any{"name": "user1"}, nil)
	tracker.Store(key2, map[string]any{"name": "user2"}, nil)

	// At this point, key1 is LRU and key2 is MRU
	// Access key1 (moves to front)
	tracker.Load(key1, nil)

	// Now key2 is LRU and key1 is MRU
	// Adding key3 should evict key2 (now the oldest)
	tracker.Store(key3, map[string]any{"name": "user3"}, nil)

	// key2 should be evicted (was LRU)
	if _, ok := tracker.Load(key2, nil); ok {
		t.Error("Expected key2 to be evicted")
	}

	// key1 should still exist (was accessed, moved to MRU)
	if _, ok := tracker.Load(key1, nil); !ok {
		t.Error("Expected key1 to exist")
	}

	// key3 should exist (just added)
	if _, ok := tracker.Load(key3, nil); !ok {
		t.Error("Expected key3 to exist")
	}
}

func TestLRUTracker_Update(t *testing.T) {
	tracker := newLRUTracker(3)

	// Add entity
	tracker.Store(1, map[string]any{"name": "user1"}, nil)

	// Update entity
	tracker.Store(1, map[string]any{"name": "user1_updated"}, nil)

	// Should still have only 1 entity
	if tracker.Len() != 1 {
		t.Errorf("Expected 1 entity after update, got %d", tracker.Len())
	}

	// Should have updated value
	originals, ok := tracker.Load(1, nil)
	if !ok {
		t.Fatal("Expected entity 1 to exist")
	}
	if originals["name"] != "user1_updated" {
		t.Errorf("Expected updated name, got %v", originals["name"])
	}
}

func TestLRUTracker_Delete(t *testing.T) {
	tracker := newLRUTracker(3)

	tracker.Store(1, map[string]any{"name": "user1"}, nil)
	tracker.Store(2, map[string]any{"name": "user2"}, nil)

	if tracker.Len() != 2 {
		t.Errorf("Expected 2 entities, got %d", tracker.Len())
	}

	tracker.Delete(1)

	if tracker.Len() != 1 {
		t.Errorf("Expected 1 entity after delete, got %d", tracker.Len())
	}

	if _, ok := tracker.Load(1, nil); ok {
		t.Error("Expected entity 1 to be deleted")
	}
}

func TestLRUTracker_Clear(t *testing.T) {
	tracker := newLRUTracker(3)

	tracker.Store(1, map[string]any{"name": "user1"}, nil)
	tracker.Store(2, map[string]any{"name": "user2"}, nil)

	tracker.Clear()

	if tracker.Len() != 0 {
		t.Errorf("Expected 0 entities after clear, got %d", tracker.Len())
	}
}

func TestLRUTracker_Unbounded(t *testing.T) {
	// Capacity of 0 means unbounded
	tracker := newLRUTracker(0)

	// Add many entities
	for i := 0; i < 100; i++ {
		tracker.Store(uintptr(i), map[string]any{"name": "user"}, nil)
	}

	// All should exist
	if tracker.Len() != 100 {
		t.Errorf("Expected 100 entities in unbounded tracker, got %d", tracker.Len())
	}
}

func TestTrackingScope_Cleanup(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Clear any existing tracking data
	ClearAllOriginals()

	// Create test users
	for i := 0; i < 5; i++ {
		user := &DirtyUser{
			Name:  "User " + string(rune('A'+i)),
			Email: "user@example.com",
		}
		err := New[DirtyUser]().SetDB(db).Create(ctx, user)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}
	}

	// Clear tracking from creation
	ClearAllOriginals()

	initialCount := TrackedEntityCount()

	// Create a scope and load entities
	scope := NewTrackingScope()
	users, err := New[DirtyUser]().SetDB(db).WithTrackingScope(scope).Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get users: %v", err)
	}

	if len(users) != 5 {
		t.Fatalf("Expected 5 users, got %d", len(users))
	}

	// Verify entities are tracked
	countAfterLoad := TrackedEntityCount()
	if countAfterLoad != initialCount+5 {
		t.Errorf("Expected %d tracked entities, got %d", initialCount+5, countAfterLoad)
	}

	// Verify scope tracks the entities
	if scope.Len() != 5 {
		t.Errorf("Expected scope to track 5 entities, got %d", scope.Len())
	}

	// Close scope - should clear tracking
	scope.Close()

	// Verify tracking is cleared
	countAfterClose := TrackedEntityCount()
	if countAfterClose != initialCount {
		t.Errorf("Expected %d tracked entities after scope close, got %d", initialCount, countAfterClose)
	}

	// Entities should no longer be tracked
	for _, user := range users {
		if IsTracked(user) {
			t.Error("Expected entity to not be tracked after scope close")
		}
	}
}

func TestTrackingScope_DoubleClose(t *testing.T) {
	scope := NewTrackingScope()

	// First close should work
	scope.Close()

	// Second close should not panic
	scope.Close()
}

func TestConfigureDirtyTracking(t *testing.T) {
	// Clear existing tracking data first
	ClearAllOriginals()

	// Configure with a larger capacity (768 = 3 per shard)
	// With 256 shards, this gives 768/256 = 3 per shard
	ConfigureDirtyTracking(768)

	// Add 1024 entities - this exceeds capacity but due to sharding,
	// entities are distributed. Just verify the mechanism works
	// and capacity is enforced within shards
	for i := 0; i < 1024; i++ {
		user := &DirtyUser{ID: i, Name: "User"}
		trackOriginals(user, ParseModel[DirtyUser]())
	}

	// With sharding, we can't guarantee exact total count due to
	// uneven distribution, but should be less than total entities added
	count := TrackedEntityCount()
	if count == 0 {
		t.Error("Expected some tracked entities")
	}
	if count >= 1024 {
		t.Errorf("Expected capacity limiting to reduce tracked count below 1024, got %d", count)
	}
	t.Logf("With capacity 768, tracked %d entities out of 1024 attempted", count)

	// Restore default capacity (10,000)
	ConfigureDirtyTracking(10000)
}

func TestClearAllOriginals(t *testing.T) {
	// Add some entities
	for i := 0; i < 5; i++ {
		user := &DirtyUser{ID: i, Name: "User"}
		trackOriginals(user, ParseModel[DirtyUser]())
	}

	if TrackedEntityCount() == 0 {
		t.Error("Expected some tracked entities")
	}

	// Clear all
	ClearAllOriginals()

	if TrackedEntityCount() != 0 {
		t.Errorf("Expected 0 tracked entities after ClearAllOriginals, got %d", TrackedEntityCount())
	}
}

func TestConcurrentTracking(t *testing.T) {
	// Clear any existing tracking
	ClearAllOriginals()

	modelInfo := ParseModel[DirtyUser]()

	// Run concurrent tracking operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				user := &DirtyUser{ID: id*100 + j, Name: "User"}
				trackOriginals(user, modelInfo)

				// Also do some reads
				_ = IsTracked(user)
				_ = GetOriginals(user)

				// And some clears
				if j%10 == 0 {
					ClearOriginals(user)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and tracker should be in consistent state
	count := TrackedEntityCount()
	t.Logf("Final tracked count after concurrent operations: %d", count)
}

func TestCursorWithTrackingScope(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()

	// Create test users
	for i := 0; i < 5; i++ {
		user := &DirtyUser{
			Name:  "User " + string(rune('A'+i)),
			Email: "user@example.com",
		}
		err := New[DirtyUser]().SetDB(db).Create(ctx, user)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}
	}

	// Clear tracking from creation
	ClearAllOriginals()

	// Create a scope and use cursor
	scope := NewTrackingScope()
	cursor, err := New[DirtyUser]().SetDB(db).WithTrackingScope(scope).Cursor(ctx)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}
	defer cursor.Close()

	var users []*DirtyUser
	for cursor.Next() {
		user, err := cursor.Scan(ctx)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		users = append(users, user)
	}

	if len(users) != 5 {
		t.Fatalf("Expected 5 users from cursor, got %d", len(users))
	}

	// Verify scope tracks the entities
	if scope.Len() != 5 {
		t.Errorf("Expected scope to track 5 entities, got %d", scope.Len())
	}

	// Close scope - should clear tracking
	scope.Close()

	// Entities should no longer be tracked
	for _, user := range users {
		if IsTracked(user) {
			t.Error("Expected entity to not be tracked after scope close")
		}
	}
}

// =============================================================================
// DIRTY TRACKING MEMORY MANAGEMENT TESTS
// =============================================================================

// TestDirtyTracking_NoFinalizerAutoCleanup documents that dirty tracking does NOT
// use runtime finalizers. Tracking data persists until either the LRU capacity
// evicts it, ClearOriginals is called explicitly, or a TrackingScope is closed.
//
// The ABA problem this avoids — a finalizer firing after a new entity reuses the
// collected entity's address — is handled instead by the weak reference each entry
// carries: the entry stays put, but a lookup from a different entity at that
// address does not match it. So occupancy is what this test asserts; retrievability
// through the collected entity is gone by design (see
// TestDirtyTracking_RecycledAddressDoesNotInheritBaseline).
func TestDirtyTracking_NoFinalizerAutoCleanup(t *testing.T) {
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	var key uintptr
	func() {
		user := &DirtyUser{ID: 999, Name: "Temporary", Email: "temp@example.com"}
		trackOriginals(user, modelInfo)
		key = getEntityKey(user)

		if _, ok := globalTracker.Load().Load(key, user); !ok {
			t.Fatal("expected entity to be tracked before going out of scope")
		}
	}()

	if got := TrackedEntityCount(); got != 1 {
		t.Fatalf("expected 1 tracked entity before GC, got %d", got)
	}

	// Without finalizers, GC does NOT remove tracking data.
	runtime.GC()
	runtime.GC()

	if got := TrackedEntityCount(); got != 1 {
		t.Errorf("tracking entry was unexpectedly removed (count=%d); no finalizer should be running", got)
	}

	// Explicit cleanup is the correct approach.
	globalTracker.Load().Delete(key)
	if got := TrackedEntityCount(); got != 0 {
		t.Errorf("expected tracking entry to be gone after explicit Delete, count=%d", got)
	}
}

// TestDirtyTracking_RetrackDoesNotPanic verifies that calling syncOriginals on
// an already-tracked entity does not panic.
func TestDirtyTracking_RetrackDoesNotPanic(t *testing.T) {
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	user := &DirtyUser{ID: 100, Name: "John", Email: "john@example.com"}
	trackOriginals(user, modelInfo)

	if !IsTracked(user) {
		t.Fatal("expected entity to be tracked after trackOriginals")
	}

	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				t.Errorf("syncOriginals panicked: %v", r)
			}
		}()
		syncOriginals(user, modelInfo)
	}()

	if panicked {
		t.Fatal("syncOriginals should not panic when re-tracking an already-tracked entity")
	}

	if !IsTracked(user) {
		t.Error("expected entity to still be tracked after syncOriginals")
	}
}

// =============================================================================
// ADDRESS-REUSE (ABA) TESTS
// =============================================================================

// TestDirtyTracking_RecycledAddressDoesNotInheritBaseline verifies that an entity
// occupying an address previously held by a different, now-collected entity does
// not inherit that entity's baseline.
//
// Entries are keyed by pointer address, so once an entity is garbage collected a
// new allocation can land on the same address. Storing the ghost's entry under the
// fresh entity's key reproduces that deterministically, without depending on the
// allocator actually recycling an address during the test.
func TestDirtyTracking_RecycledAddressDoesNotInheritBaseline(t *testing.T) {
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	ghost := &DirtyUser{ID: 1, Name: "Ghost", Email: "ghost@example.com"}
	trackOriginals(ghost, modelInfo)

	ghostOriginals, ok := globalTracker.Load().Load(getEntityKey(ghost), ghost)
	if !ok {
		t.Fatal("setup: expected ghost entity to be tracked")
	}
	ghostRef := weakRef[DirtyUser]{p: weak.Make(ghost)}

	fresh := &DirtyUser{ID: 2, Name: "Fresh", Email: "fresh@example.com"}
	globalTracker.Load().Store(getEntityKey(fresh), ghostOriginals, ghostRef)

	if IsTracked(fresh) {
		t.Error("a never-tracked entity reported as tracked: it inherited a recycled baseline")
	}
	if got := GetOriginals(fresh); got != nil {
		t.Errorf("fresh entity exposed a foreign baseline: %v", got)
	}
	if got := GetOriginal(fresh, "name"); got != nil {
		t.Errorf("fresh entity exposed a foreign original for name: %v", got)
	}

	// Untracked entities are dirty by definition; inheriting a baseline would
	// wrongly report them clean.
	model := New[DirtyUser]()
	if !model.IsDirtyField(fresh, "name") {
		t.Error("expected name to be dirty on an untracked entity")
	}

	runtime.KeepAlive(ghost)
}

// TestDirtyTracking_FreshEntitiesNeverInheritAfterGC is the end-to-end form of the
// same hazard: after a tracked entity is collected, no subsequently allocated
// entity may report as tracked. It cannot fail spuriously — it only fails if an
// address really is recycled and the baseline really is inherited.
func TestDirtyTracking_FreshEntitiesNeverInheritAfterGC(t *testing.T) {
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	func() {
		ghost := &DirtyUser{ID: 999, Name: "Ghost", Email: "ghost@example.com"}
		trackOriginals(ghost, modelInfo)
	}()

	runtime.GC()
	runtime.GC()

	inherited := 0
	for i := 0; i < 20000; i++ {
		fresh := &DirtyUser{ID: i, Name: "Fresh"}
		if IsTracked(fresh) {
			inherited++
		}
	}

	if inherited > 0 {
		t.Errorf("%d freshly allocated entities inherited a collected entity's baseline", inherited)
	}
}

// TestSave_RejectsEntityWithRecycledBaseline is the user-visible consequence:
// Save must refuse an entity that never carried a baseline of its own, rather
// than computing a dirty set against a foreign row and issuing a wrong UPDATE.
func TestSave_RejectsEntityWithRecycledBaseline(t *testing.T) {
	db := setupDirtyDB(t)
	defer db.Close()

	ctx := context.Background()
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	if _, err := db.Exec(`INSERT INTO dirty_users (id, name, email) VALUES (1, 'Original', 'original@example.com')`); err != nil {
		t.Fatal(err)
	}

	ghost := &DirtyUser{ID: 1, Name: "Ghost", Email: "ghost@example.com"}
	trackOriginals(ghost, modelInfo)
	ghostOriginals, ok := globalTracker.Load().Load(getEntityKey(ghost), ghost)
	if !ok {
		t.Fatal("setup: expected ghost entity to be tracked")
	}
	ghostRef := weakRef[DirtyUser]{p: weak.Make(ghost)}

	// A hand-built entity that happens to sit on the ghost's address.
	fresh := &DirtyUser{ID: 1, Name: "Fresh"}
	globalTracker.Load().Store(getEntityKey(fresh), ghostOriginals, ghostRef)

	err := New[DirtyUser]().SetDB(db).Save(ctx, fresh)
	if err != ErrSaveUntracked {
		t.Errorf("expected ErrSaveUntracked, got %v", err)
	}

	// The row must be untouched.
	var name string
	if err := db.QueryRow("SELECT name FROM dirty_users WHERE id = 1").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Original" {
		t.Errorf("Save wrote through a recycled baseline: name is now %q", name)
	}

	runtime.KeepAlive(ghost)
}

// TestTrackingScope_CloseSpareRecycledOccupant verifies that closing a scope
// deletes only the entries the scope itself created. Entries are keyed by
// address, so if a scoped entity is collected and its address is reused by an
// entity tracked elsewhere, Close must leave the newcomer's baseline alone —
// otherwise an unrelated entity silently loses tracking and its next Save
// fails with ErrSaveUntracked.
func TestTrackingScope_CloseSparesRecycledOccupant(t *testing.T) {
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	scope := NewTrackingScope()

	ghost := &DirtyUser{ID: 1, Name: "Ghost", Email: "ghost@example.com"}
	trackOriginalsWithScope(ghost, modelInfo, scope)
	key := getEntityKey(ghost)

	// The scoped entity is collected and a different entity, tracked outside the
	// scope, takes over its address.
	fresh := &DirtyUser{ID: 2, Name: "Fresh", Email: "fresh@example.com"}
	freshOriginals := map[string]any{"id": 2, "name": "Fresh", "email": "fresh@example.com"}
	globalTracker.Load().Store(key, freshOriginals, weakRef[DirtyUser]{p: weak.Make(fresh)})

	scope.Close()

	originals, ok := globalTracker.Load().Load(key, fresh)
	if !ok {
		t.Fatal("scope.Close deleted an entry belonging to a different entity")
	}
	if originals["name"] != "Fresh" {
		t.Errorf("expected the newcomer's baseline to survive, got %v", originals)
	}

	runtime.KeepAlive(ghost)
}

// TestTrackingScope_CloseStillClearsItsOwnEntities is the companion: normal
// scope cleanup must keep working.
func TestTrackingScope_CloseStillClearsItsOwnEntities(t *testing.T) {
	ClearAllOriginals()
	modelInfo := ParseModel[DirtyUser]()

	scope := NewTrackingScope()
	users := []*DirtyUser{
		{ID: 1, Name: "One"},
		{ID: 2, Name: "Two"},
	}
	for _, u := range users {
		trackOriginalsWithScope(u, modelInfo, scope)
		if !IsTracked(u) {
			t.Fatalf("setup: expected %s to be tracked", u.Name)
		}
	}

	scope.Close()

	for _, u := range users {
		if IsTracked(u) {
			t.Errorf("expected %s to be untracked after scope close", u.Name)
		}
	}
}
