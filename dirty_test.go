package zorm

import (
	"context"
	"database/sql"
	"testing"

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
	SyncOriginals(loaded, model.modelInfo)

	// Now it should be clean
	if model.IsDirtyField(loaded, "name") {
		t.Error("Expected name to be clean after SyncOriginals")
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
