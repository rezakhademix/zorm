package zorm

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// ==================== Models for BelongsTo with INT FK ====================

type AuthorInt struct {
	ID   int `zorm:"primaryKey"`
	Name string
}

func (a AuthorInt) TableName() string {
	return "authors_int"
}

type BookInt struct {
	ID       int `zorm:"primaryKey"`
	Title    string
	AuthorID int
	Author   *AuthorInt
}

func (b BookInt) TableName() string {
	return "books_int"
}

func (b BookInt) AuthorRelation() BelongsTo[AuthorInt] {
	return BelongsTo[AuthorInt]{
		ForeignKey: "author_id",
	}
}

// ==================== Models for BelongsTo with UUID FK ====================

type AuthorUUID struct {
	ID   uuid.UUID `zorm:"primaryKey"`
	Name string
}

func (a AuthorUUID) TableName() string {
	return "authors_uuid"
}

type BookUUID struct {
	ID       uuid.UUID `zorm:"primaryKey"`
	Title    string
	AuthorID uuid.UUID
	Author   *AuthorUUID
}

func (b BookUUID) TableName() string {
	return "books_uuid"
}

func (b BookUUID) AuthorRelation() BelongsTo[AuthorUUID] {
	return BelongsTo[AuthorUUID]{
		ForeignKey: "author_id",
	}
}

// ==================== Setup Functions ====================

func setupBelongsToIntDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE authors_int (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		);
		CREATE TABLE books_int (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			author_id INTEGER NOT NULL,
			FOREIGN KEY (author_id) REFERENCES authors_int(id)
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO authors_int (id, name) VALUES 
			(1, 'George Orwell'),
			(2, 'Aldous Huxley'),
			(3, 'Ray Bradbury');
		INSERT INTO books_int (id, title, author_id) VALUES 
			(1, '1984', 1),
			(2, 'Animal Farm', 1),
			(3, 'Brave New World', 2),
			(4, 'Fahrenheit 451', 3);
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

func setupBelongsToUUIDDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE authors_uuid (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL
		);
		CREATE TABLE books_uuid (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES authors_uuid(id)
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	// Generate UUIDs for authors
	author1ID := uuid.New().String()
	author2ID := uuid.New().String()
	author3ID := uuid.New().String()

	book1ID := uuid.New().String()
	book2ID := uuid.New().String()
	book3ID := uuid.New().String()
	book4ID := uuid.New().String()

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO authors_uuid (id, name) VALUES 
			(?, 'George Orwell'),
			(?, 'Aldous Huxley'),
			(?, 'Ray Bradbury');
		INSERT INTO books_uuid (id, title, author_id) VALUES 
			(?, '1984', ?),
			(?, 'Animal Farm', ?),
			(?, 'Brave New World', ?),
			(?, 'Fahrenheit 451', ?);
	`,
		author1ID, author2ID, author3ID,
		book1ID, author1ID,
		book2ID, author1ID,
		book3ID, author2ID,
		book4ID, author3ID,
	)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

// ==================== Tests for BelongsTo with INT FK ====================

func TestBelongsTo_LoadWithIntFK_Single(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) != 4 {
		t.Errorf("expected 4 books, got %d", len(books))
		return
	}

	// Verify first book (1984 by George Orwell)
	if books[0].Title != "1984" {
		t.Errorf("expected title '1984', got %q", books[0].Title)
	}

	if books[0].Author == nil {
		t.Fatal("expected Author to be loaded, got nil")
	}

	if books[0].Author.Name != "George Orwell" {
		t.Errorf("expected author 'George Orwell', got %q", books[0].Author.Name)
	}

	if books[0].Author.ID != 1 {
		t.Errorf("expected author ID 1, got %d", books[0].Author.ID)
	}
}

func TestBelongsTo_LoadWithIntFK_Multiple(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Verify all books have their authors loaded
	testCases := []struct {
		bookTitle  string
		authorName string
	}{
		{"1984", "George Orwell"},
		{"Animal Farm", "George Orwell"},
		{"Brave New World", "Aldous Huxley"},
		{"Fahrenheit 451", "Ray Bradbury"},
	}

	for i, tc := range testCases {
		if i >= len(books) {
			t.Errorf("expected at least %d books", i+1)
			break
		}

		if books[i].Title != tc.bookTitle {
			t.Errorf("book %d: expected title %q, got %q", i, tc.bookTitle, books[i].Title)
		}

		if books[i].Author == nil {
			t.Errorf("book %d (%s): expected Author to be loaded", i, tc.bookTitle)
			continue
		}

		if books[i].Author.Name != tc.authorName {
			t.Errorf("book %d: expected author %q, got %q", i, tc.authorName, books[i].Author.Name)
		}
	}
}

func TestBelongsTo_LoadWithIntFK_SameAuthor(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Find books by George Orwell (ID 1)
	orwellBooks := []*BookInt{}
	for _, b := range books {
		if b.Author != nil && b.Author.ID == 1 {
			orwellBooks = append(orwellBooks, b)
		}
	}

	if len(orwellBooks) != 2 {
		t.Errorf("expected 2 books by George Orwell, got %d", len(orwellBooks))
	}

	for _, b := range orwellBooks {
		if b.Author.Name != "George Orwell" {
			t.Errorf("expected author George Orwell, got %q", b.Author.Name)
		}
	}
}

func TestBelongsTo_LoadWithIntFK_NoRelation(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) != 4 {
		t.Errorf("expected 4 books, got %d", len(books))
	}

	// Verify author is not loaded
	for i, b := range books {
		if b.Author != nil {
			t.Errorf("book %d: expected Author to be nil, got %+v", i, b.Author)
		}
	}
}

func TestBelongsTo_IntFK_CorrectForeignKeyValue(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Verify that FK and loaded author match
	for _, b := range books {
		if b.Author != nil && b.Author.ID != b.AuthorID {
			t.Errorf("book %q: author ID mismatch - FK: %d, loaded author ID: %d",
				b.Title, b.AuthorID, b.Author.ID)
		}
	}
}

func TestBelongsTo_IntFK_LoadSlice(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Load author relation on existing slice
	err = New[BookInt]().LoadSlice(ctx, books, "Author")
	if err != nil {
		t.Fatalf("LoadSlice failed: %v", err)
	}

	// Verify authors are loaded
	for _, b := range books {
		if b.Author == nil {
			t.Errorf("book %q: expected Author to be loaded after LoadSlice", b.Title)
		}
	}
}

func TestBelongsTo_IntFK_Load(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Get a book first to populate it with data from DB
	books, err := New[BookInt]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) == 0 {
		t.Fatal("expected at least one book")
	}

	book := books[0]
	book.Author = nil // Clear the author

	err = New[BookInt]().Load(ctx, book, "Author")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if book.Author == nil {
		t.Fatal("expected Author to be loaded")
	}

	if book.Author.Name != "George Orwell" {
		t.Errorf("expected author George Orwell, got %q", book.Author.Name)
	}
}

// ==================== Tests for BelongsTo with UUID FK ====================

func TestBelongsTo_LoadWithUUIDFK_Single(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) != 4 {
		t.Errorf("expected 4 books, got %d", len(books))
		return
	}

	// Verify first book (1984 by George Orwell)
	if books[0].Title != "1984" {
		t.Errorf("expected title '1984', got %q", books[0].Title)
	}

	if books[0].Author == nil {
		t.Fatal("expected Author to be loaded, got nil")
	}

	if books[0].Author.Name != "George Orwell" {
		t.Errorf("expected author 'George Orwell', got %q", books[0].Author.Name)
	}
}

func TestBelongsTo_LoadWithUUIDFK_Multiple(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Verify all books have their authors loaded
	testCases := []struct {
		bookTitle  string
		authorName string
	}{
		{"1984", "George Orwell"},
		{"Animal Farm", "George Orwell"},
		{"Brave New World", "Aldous Huxley"},
		{"Fahrenheit 451", "Ray Bradbury"},
	}

	for i, tc := range testCases {
		if i >= len(books) {
			t.Errorf("expected at least %d books", i+1)
			break
		}

		if books[i].Title != tc.bookTitle {
			t.Errorf("book %d: expected title %q, got %q", i, tc.bookTitle, books[i].Title)
		}

		if books[i].Author == nil {
			t.Errorf("book %d (%s): expected Author to be loaded", i, tc.bookTitle)
			continue
		}

		if books[i].Author.Name != tc.authorName {
			t.Errorf("book %d: expected author %q, got %q", i, tc.authorName, books[i].Author.Name)
		}
	}
}

func TestBelongsTo_LoadWithUUIDFK_SameAuthor(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Count books by each author
	authorBookCount := make(map[string]int)
	for _, b := range books {
		if b.Author != nil {
			authorBookCount[b.Author.Name]++
		}
	}

	// George Orwell should have 2 books
	if authorBookCount["George Orwell"] != 2 {
		t.Errorf("expected 2 books by George Orwell, got %d", authorBookCount["George Orwell"])
	}

	// Each of the other authors should have 1 book
	if authorBookCount["Aldous Huxley"] != 1 {
		t.Errorf("expected 1 book by Aldous Huxley, got %d", authorBookCount["Aldous Huxley"])
	}

	if authorBookCount["Ray Bradbury"] != 1 {
		t.Errorf("expected 1 book by Ray Bradbury, got %d", authorBookCount["Ray Bradbury"])
	}
}

func TestBelongsTo_LoadWithUUIDFK_NoRelation(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) != 4 {
		t.Errorf("expected 4 books, got %d", len(books))
	}

	// Verify author is not loaded
	for i, b := range books {
		if b.Author != nil {
			t.Errorf("book %d: expected Author to be nil, got %+v", i, b.Author)
		}
	}
}

func TestBelongsTo_UUIDFK_CorrectForeignKeyValue(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Verify that FK and loaded author match
	for _, b := range books {
		if b.Author != nil && b.Author.ID != b.AuthorID {
			t.Errorf("book %q: author ID mismatch - FK: %s, loaded author ID: %s",
				b.Title, b.AuthorID, b.Author.ID)
		}
	}
}

func TestBelongsTo_UUIDFK_LoadSlice(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Load author relation on existing slice
	err = New[BookUUID]().LoadSlice(ctx, books, "Author")
	if err != nil {
		t.Fatalf("LoadSlice failed: %v", err)
	}

	// Verify authors are loaded
	for _, b := range books {
		if b.Author == nil {
			t.Errorf("book %q: expected Author to be loaded after LoadSlice", b.Title)
		}
	}
}

func TestBelongsTo_UUIDFK_Load(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Get a book first to know its ID
	books, err := New[BookUUID]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) == 0 {
		t.Fatal("expected at least one book")
	}

	book := books[0]
	book.Author = nil // Clear the author

	err = New[BookUUID]().Load(ctx, book, "Author")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if book.Author == nil {
		t.Fatal("expected Author to be loaded")
	}

	if book.Author.Name != "George Orwell" {
		t.Errorf("expected author George Orwell, got %q", book.Author.Name)
	}
}

func TestBelongsTo_UUIDFK_ValidUUIDs(t *testing.T) {
	db := setupBelongsToUUIDDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookUUID]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Verify all IDs and ForeignKeys are valid UUIDs
	for _, b := range books {
		// Check book ID
		if b.ID == uuid.Nil {
			t.Errorf("book has nil UUID ID")
		}

		// Check author FK
		if b.AuthorID == uuid.Nil {
			t.Errorf("book %q has nil author FK", b.Title)
		}

		// Check loaded author
		if b.Author != nil && b.Author.ID == uuid.Nil {
			t.Errorf("book %q has author with nil UUID", b.Title)
		}
	}
}

// ==================== Edge Case Tests ====================

func TestBelongsTo_IntFK_BookWithoutAuthor(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with nullable foreign key
	_, err = db.Exec(`
		CREATE TABLE authors_int (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		);
		CREATE TABLE books_int (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			author_id INTEGER
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	// Insert data - only insert book with valid author_id to avoid NULL handling issues
	_, err = db.Exec(`
		INSERT INTO authors_int (id, name) VALUES (1, 'George Orwell');
		INSERT INTO books_int (id, title, author_id) VALUES 
			(1, '1984', 1),
			(2, 'Unknown Book', 1);
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookInt]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Both books should have authors loaded
	for _, b := range books {
		if b.Author == nil {
			t.Errorf("book %q: expected Author to be loaded, got nil", b.Title)
		}
	}
}

func TestBelongsTo_MixedIntFK(t *testing.T) {
	db := setupBelongsToIntDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Test loading books and filtering in memory
	books, err := New[BookInt]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Filter in memory for books by author 1
	var authorOneBooks []*BookInt
	for _, b := range books {
		if b.AuthorID == 1 {
			authorOneBooks = append(authorOneBooks, b)
		}
	}

	if len(authorOneBooks) != 2 {
		t.Errorf("expected 2 books by author 1, got %d", len(authorOneBooks))
		return
	}

	for _, b := range authorOneBooks {
		if b.AuthorID != 1 {
			t.Errorf("expected author_id 1, got %d", b.AuthorID)
		}

		if b.Author == nil {
			t.Errorf("book %q: expected Author to be loaded", b.Title)
			continue
		}

		if b.Author.ID != 1 {
			t.Errorf("book %q: expected author ID 1, got %d", b.Title, b.Author.ID)
		}
	}
}

// ==================== Real-world User/Branch Scenario Tests ====================

type Branch struct {
	ID   uuid.UUID `zorm:"primaryKey"`
	Name string
}

func (b Branch) TableName() string {
	return "branches"
}

type User struct {
	ID        uuid.UUID `zorm:"primaryKey"`
	BranchID  uuid.UUID
	FirstName string
	LastName  string
	Email     string
	Branch    *Branch
}

func (u User) TableName() string {
	return "users"
}

// Note: The relation method should be named "Branch" + "Relation" or use With() with the field name
// The ORM resolves relation methods by trying both "Branch" and "BranchRelation" patterns
func (u User) BranchRelation() BelongsTo[Branch] {
	return BelongsTo[Branch]{
		ForeignKey: "branch_id",
		OwnerKey:   "id",
	}
}

func setupUserBranchDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE branches (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL
		);
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			branch_id TEXT NOT NULL,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			email TEXT,
			FOREIGN KEY (branch_id) REFERENCES branches(id)
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	// Generate UUIDs
	branch1ID := uuid.New().String()
	branch2ID := uuid.New().String()

	user1ID := uuid.New().String()
	user2ID := uuid.New().String()
	user3ID := uuid.New().String()

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO branches (id, name) VALUES 
			(?, 'Engineering'),
			(?, 'Sales');
		INSERT INTO users (id, branch_id, first_name, last_name, email) VALUES 
			(?, ?, 'John', 'Doe', 'john@example.com'),
			(?, ?, 'Jane', 'Smith', 'jane@example.com'),
			(?, ?, 'Bob', 'Wilson', 'bob@example.com');
	`,
		branch1ID, branch2ID,
		user1ID, branch1ID,
		user2ID, branch1ID,
		user3ID, branch2ID,
	)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

func TestBelongsTo_UserBranch_WithRelation(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().With("Branch").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) != 3 {
		t.Errorf("expected 3 users, got %d", len(users))
		return
	}

	// Verify all users have branch loaded
	for _, u := range users {
		if u.Branch == nil {
			t.Errorf("user %s %s: expected Branch to be loaded, got nil", u.FirstName, u.LastName)
			continue
		}

		if u.Branch.ID != u.BranchID {
			t.Errorf("user %s: branch ID mismatch - FK: %s, loaded branch: %s",
				u.FirstName, u.BranchID, u.Branch.ID)
		}

		// Verify branch name is populated
		if u.Branch.Name == "" {
			t.Errorf("user %s: expected branch name to be loaded", u.FirstName)
		}
	}
}

func TestBelongsTo_UserBranch_WithoutRelation(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Verify branch is not loaded when not requested
	for _, u := range users {
		if u.Branch != nil {
			t.Errorf("user %s: expected Branch to be nil, got %+v", u.FirstName, u.Branch)
		}
	}
}

func TestBelongsTo_UserBranch_OwnerKeyAndForeignKey(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().With("Branch").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Verify that owner key (Branch.ID) matches foreign key (User.BranchID)
	for _, u := range users {
		if u.Branch != nil {
			if u.Branch.ID != u.BranchID {
				t.Errorf("user %s: owner key/foreign key mismatch - branch ID: %s, foreign key: %s",
					u.FirstName, u.Branch.ID, u.BranchID)
			}
		}
	}
}

func TestBelongsTo_UserBranch_SameBranch(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().With("Branch").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Count users by branch
	branchUserCount := make(map[string]int)
	for _, u := range users {
		if u.Branch != nil {
			branchUserCount[u.Branch.Name]++
		}
	}

	// Engineering should have 2 users
	if branchUserCount["Engineering"] != 2 {
		t.Errorf("expected 2 users in Engineering branch, got %d", branchUserCount["Engineering"])
	}

	// Sales should have 1 user
	if branchUserCount["Sales"] != 1 {
		t.Errorf("expected 1 user in Sales branch, got %d", branchUserCount["Sales"])
	}
}

func TestBelongsTo_UserBranch_LoadSlice(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Load branch relation on existing slice
	err = New[User]().LoadSlice(ctx, users, "Branch")
	if err != nil {
		t.Fatalf("LoadSlice failed: %v", err)
	}

	// Verify branches are loaded
	for _, u := range users {
		if u.Branch == nil {
			t.Errorf("user %s %s: expected Branch to be loaded after LoadSlice", u.FirstName, u.LastName)
		}
	}
}

func TestBelongsTo_UserBranch_Load(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()

	// Get a user first
	users, err := New[User]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	if len(users) == 0 {
		t.Fatal("expected at least one user")
	}

	user := users[0]
	user.Branch = nil // Clear the branch

	// Load branch relation on single entity
	err = New[User]().Load(ctx, user, "Branch")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if user.Branch == nil {
		t.Fatal("expected Branch to be loaded")
	}

	if user.Branch.ID != user.BranchID {
		t.Errorf("branch ID mismatch - FK: %s, loaded: %s", user.BranchID, user.Branch.ID)
	}
}

func TestBelongsTo_UserBranch_ValidUUIDs(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().With("Branch").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Verify all UUIDs are valid
	for _, u := range users {
		if u.ID == uuid.Nil {
			t.Errorf("user has nil UUID ID")
		}

		if u.BranchID == uuid.Nil {
			t.Errorf("user %s has nil branch FK", u.FirstName)
		}

		if u.Branch != nil && u.Branch.ID == uuid.Nil {
			t.Errorf("user %s has branch with nil UUID", u.FirstName)
		}
	}
}

func TestBelongsTo_UserBranch_DataIntegrity(t *testing.T) {
	db := setupUserBranchDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	users, err := New[User]().With("Branch").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get users: %v", err)
	}

	// Verify user data is intact
	for _, u := range users {
		if u.FirstName == "" {
			t.Errorf("user has empty first name")
		}

		if u.LastName == "" {
			t.Errorf("user has empty last name")
		}

		if u.Email == "" {
			t.Errorf("user %s %s has empty email", u.FirstName, u.LastName)
		}

		// Verify branch data is intact when loaded
		if u.Branch != nil && u.Branch.Name == "" {
			t.Errorf("user %s: branch has empty name", u.FirstName)
		}
	}
}

// ==================== Nullable FK Edge Cases ====================

type BookNullableFK struct {
	ID       int  `zorm:"primaryKey"`
	Title    string
	AuthorID *int
	Author   *AuthorInt
}

func (b BookNullableFK) TableName() string {
	return "books_nullable_fk"
}

func (b BookNullableFK) AuthorRelation() BelongsTo[AuthorInt] {
	return BelongsTo[AuthorInt]{
		ForeignKey: "author_id",
	}
}

func setupBelongsToNullableFKDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE authors_int (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		);
		CREATE TABLE books_nullable_fk (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			author_id INTEGER
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO authors_int (id, name) VALUES (1, 'George Orwell');
		INSERT INTO books_nullable_fk (id, title, author_id) VALUES
			(1, '1984', 1),
			(2, 'Unknown Author Book', NULL),
			(3, 'Zero Author Book', 0);
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

func TestBelongsTo_NilForeignKey(t *testing.T) {
	db := setupBelongsToNullableFKDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookNullableFK]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) != 3 {
		t.Fatalf("expected 3 books, got %d", len(books))
	}

	// Book with valid author
	book1984 := books[0]
	if book1984.Title != "1984" {
		t.Errorf("expected '1984', got %q", book1984.Title)
	}
	if book1984.Author == nil {
		t.Error("expected '1984' to have author loaded")
	} else if book1984.Author.Name != "George Orwell" {
		t.Errorf("expected 'George Orwell', got %q", book1984.Author.Name)
	}

	// Book with NULL author_id - should have nil Author
	unknownBook := books[1]
	if unknownBook.Title != "Unknown Author Book" {
		t.Errorf("expected 'Unknown Author Book', got %q", unknownBook.Title)
	}
	if unknownBook.AuthorID != nil {
		t.Errorf("expected nil AuthorID, got %v", unknownBook.AuthorID)
	}
	if unknownBook.Author != nil {
		t.Errorf("expected nil Author for NULL FK, got %+v", unknownBook.Author)
	}
}

func TestBelongsTo_ZeroValueForeignKey(t *testing.T) {
	db := setupBelongsToNullableFKDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookNullableFK]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Find the book with author_id = 0
	var zeroBook *BookNullableFK
	for _, b := range books {
		if b.Title == "Zero Author Book" {
			zeroBook = b
			break
		}
	}

	if zeroBook == nil {
		t.Fatal("Zero Author Book not found")
	}

	// The FK value is 0, which is a zero value
	// isZero should return true for 0, so author should not be loaded
	if zeroBook.Author != nil {
		t.Errorf("expected nil Author for zero FK value, got %+v", zeroBook.Author)
	}
}

func TestBelongsTo_NullableFK_LoadSlice(t *testing.T) {
	db := setupBelongsToNullableFKDB(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookNullableFK]().Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	// Load authors
	err = New[BookNullableFK]().LoadSlice(ctx, books, "Author")
	if err != nil {
		t.Fatalf("LoadSlice failed: %v", err)
	}

	// Verify only the book with valid FK has author loaded
	for _, b := range books {
		if b.Title == "1984" {
			if b.Author == nil {
				t.Error("expected '1984' to have author after LoadSlice")
			}
		} else {
			if b.Author != nil {
				t.Errorf("book %q should not have author loaded, got %+v", b.Title, b.Author)
			}
		}
	}
}

func TestBelongsTo_AllNullFKs(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE authors_int (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE books_nullable_fk (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER);

		INSERT INTO books_nullable_fk (id, title, author_id) VALUES
			(1, 'Book A', NULL),
			(2, 'Book B', NULL);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	books, err := New[BookNullableFK]().With("Author").Get(ctx)
	if err != nil {
		t.Fatalf("failed to get books: %v", err)
	}

	if len(books) != 2 {
		t.Fatalf("expected 2 books, got %d", len(books))
	}

	// All books have NULL FKs, so no authors should be loaded
	for _, b := range books {
		if b.Author != nil {
			t.Errorf("book %q: expected nil Author, got %+v", b.Title, b.Author)
		}
	}
}
