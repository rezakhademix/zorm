[![Go Reference](https://pkg.go.dev/badge/github.com/rezakhademix/zorm.svg)](https://pkg.go.dev/github.com/rezakhademix/zorm) [![Go Report Card](https://goreportcard.com/badge/github.com/rezakhademix/zorm)](https://goreportcard.com/report/github.com/rezakhademix/zorm) [![codecov](https://codecov.io/gh/rezakhademix/zorm/graph/badge.svg?token=BDWNVIC670)](https://codecov.io/gh/rezakhademix/zorm) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

<div align="center">
  <h1>ZORM</h1>
  <p><strong>A Type-Safe, Production Ready Go ORM</strong></p>
  <p>One ORM To Query Them All</p>
</div>

---

ZORM is a powerful, type-safe, and developer-friendly Go ORM designed for modern applications. It leverages Go generics to provide compile-time type safety while offering a fluent, chainable API for building complex SQL queries with ease.

## Key Features

- **Type-Safe**: Full compile-time type safety powered by Go generics
- **Zero Dependencies**: Built on Go's `database/sql` package, works with any SQL driver
- **High Performance**: Prepared statement caching and connection pooling
- **Relations**: HasOne, HasMany, BelongsTo, BelongsToMany, Polymorphic relations
- **Fluent API**: Chainable query builder with intuitive method names
- **Advanced Queries**: CTEs, Subqueries, Full-Text Search, Window Functions
- **Database Splitting**: Automatic read/write split with replica support
- **Context Support**: All operations respect `context.Context` for cancellation & timeout
- **Debugging**: `Print()` method to inspect generated SQL without executing
- **Lifecycle Hooks**: BeforeCreate, BeforeUpdate, AfterUpdate hooks
- **Accessors**: Computed attributes via getter methods

## Installation

```bash
go get github.com/rezakhademix/zorm
```

## Quick Start

### 1. Connect to Database

#### PostgreSQL

```go
import (
    "github.com/rezakhademix/zorm"
)

// Using helper (with connection pooling)
db, err := zorm.ConnectPostgres(
    "postgres://user:password@localhost/dbname?sslmode=disable",
    &zorm.DBConfig{
        MaxOpenConns:    25,
        MaxIdleConns:    5,
        ConnMaxLifetime: time.Hour,
        ConnMaxIdleTime: 30 * time.Minute,
    },
)

zorm.GlobalDB = db
```

### 2. Define Models

Models are standard Go structs. **ZORM uses convention over configuration** - no tags required!

```go
type User struct {
    ID        int64      // Automatically detected as primary key with auto-increment
    Name      string     // Maps to "name" column
    Email     string     // Maps to "email" column
    Age       int        // Maps to "age" column
    CreatedAt time.Time  // Maps to "created_at" column
    UpdatedAt time.Time  // Maps to "updated_at" (auto-updated)
}
// Table name: "users" (auto-pluralized snake_case)
```

#### Custom Table Name & Primary Key

```go
// Custom table name
func (u User) TableName() string {
    return "app_users"
}

// Custom primary key
func (u User) PrimaryKey() string {
    return "user_id"
}
```

### 3. Basic CRUD

```go
ctx := context.Background()

// Create
user := &User{Name: "John", Email: "john@example.com"}
err := zorm.New[User]().Create(ctx, user)
fmt.Println(user.ID) // Auto-populated after insert

// Read - Single
user, err := zorm.New[User]().Find(ctx, 1)
user, err := zorm.New[User]().Where("email", "john@example.com").First(ctx)

// Read - Multiple
users, err := zorm.New[User]().Where("age", ">", 18).Get(ctx)

// Update
user.Name = "Jane"
err = zorm.New[User]().Update(ctx, user) // updated_at auto-set

// Delete
err = zorm.New[User]().Where("id", 1).Delete(ctx)
```

### 4. Bulk Operations

```go
// CreateMany - Insert multiple records in a single query
users := []*User{
    {Name: "Alice", Email: "alice@example.com"},
    {Name: "Bob", Email: "bob@example.com"},
    {Name: "Charlie", Email: "charlie@example.com"},
}
err := zorm.New[User]().CreateMany(ctx, users)
// All IDs are auto-populated after insert
fmt.Println(users[0].ID, users[1].ID, users[2].ID)

// UpdateMany - Update multiple records matching query
err = zorm.New[User]().
    Where("active", false).
    UpdateMany(ctx, map[string]any{"status": "inactive"})

// DeleteMany - Delete multiple records matching query
err = zorm.New[User]().Where("status", "inactive").DeleteMany(ctx)
```

**CreateMany Features:**
- Inserts all records in a single SQL statement for efficiency
- Automatically chunks large batches to stay within database limits (65535 parameters for PostgreSQL)
- Uses transactions for multi-chunk inserts to ensure atomicity
- Returns inserted IDs via `RETURNING` clause
- Works with all hooks (`BeforeCreate` is NOT called - use `BulkInsert` if you need hooks)

```go
// For very large datasets, CreateMany automatically chunks
largeDataset := make([]*User, 10000)
for i := range largeDataset {
    largeDataset[i] = &User{Name: fmt.Sprintf("User %d", i)}
}
err := zorm.New[User]().CreateMany(ctx, largeDataset)
// Automatically split into multiple INSERT statements within a transaction
```

---

## API Reference

### Query Methods

| Method                | Description                           | Returns          |
| --------------------- | ------------------------------------- | ---------------- |
| `Get(ctx)`            | Execute query and return all results  | `[]*T, error`    |
| `First(ctx)`          | Execute query and return first result | `*T, error`      |
| `Find(ctx, id)`       | Find record by primary key            | `*T, error`      |
| `FindOrFail(ctx, id)` | Find record or return error           | `*T, error`      |
| `Exists(ctx)`         | Check if any record matches           | `bool, error`    |
| `Count(ctx)`          | Count matching records                | `int64, error`   |
| `Sum(ctx, column)`    | Sum of column values                  | `float64, error` |
| `Avg(ctx, column)`    | Average of column values              | `float64, error` |
| `Pluck(ctx, column)`  | Get single column values              | `[]any, error`   |

### Write Methods

| Method                               | Description                            |
| ------------------------------------ | -------------------------------------- |
| `Create(ctx, entity)`                | Insert single record                   |
| `CreateMany(ctx, entities)`          | Insert multiple records                |
| `Update(ctx, entity)`                | Update single record by primary key    |
| `UpdateMany(ctx, values)`            | Update multiple records matching query |
| `Delete(ctx)`                        | Delete records matching query          |
| `DeleteMany(ctx)`                    | Alias for Delete                       |
| `FirstOrCreate(ctx, attrs, values)`  | Find first or create new               |
| `UpdateOrCreate(ctx, attrs, values)` | Update existing or create new          |

### Query Builder Methods

| Method                         | Description               |
| ------------------------------ | ------------------------- |
| `Select(columns...)`           | Specify columns to select |
| `Distinct()`                   | Add DISTINCT to query     |
| `DistinctBy(columns...)`       | PostgreSQL DISTINCT ON    |
| `Where(query, args...)`        | Add WHERE condition       |
| `OrWhere(query, args...)`      | Add OR WHERE condition    |
| `WhereIn(column, values)`      | WHERE column IN (...)     |
| `WhereNull(column)`            | WHERE column IS NULL      |
| `WhereNotNull(column)`         | WHERE column IS NOT NULL  |
| `OrWhereNull(column)`          | OR column IS NULL         |
| `OrWhereNotNull(column)`       | OR column IS NOT NULL     |
| `WhereHas(relation, callback)` | WHERE EXISTS subquery     |
| `OrderBy(column, direction)`   | Add ORDER BY              |
| `Latest(column?)`              | ORDER BY column DESC      |
| `Oldest(column?)`              | ORDER BY column ASC       |
| `GroupBy(columns...)`          | Add GROUP BY              |
| `Having(query, args...)`       | Add HAVING                |
| `Limit(n)`                     | Set LIMIT                 |
| `Offset(n)`                    | Set OFFSET                |
| `Lock(mode)`                   | Add FOR UPDATE/SHARE      |

### Utility Methods

| Method                 | Description                 |
| ---------------------- | --------------------------- |
| `Clone()`              | Deep copy the query builder |
| `Table(name)`          | Override table name         |
| `TableName()`          | Get current table name      |
| `SetDB(db)`            | Set custom DB connection    |
| `WithTx(tx)`           | Use transaction             |
| `WithContext(ctx)`     | Set context                 |
| `WithStmtCache(cache)` | Enable statement caching    |
| `Scope(fn)`            | Apply reusable query logic  |
| `Print()`              | Get SQL without executing   |
| `Raw(sql, args...)`    | Set raw SQL query           |
| `Exec(ctx)`            | Execute raw query           |

---

## Query Builder Details

### Where Conditions

```go
// Equality
zorm.New[User]().Where("name", "John").Get(ctx)

// Operators
zorm.New[User]().Where("age", ">", 18).Get(ctx)
zorm.New[User]().Where("email", "LIKE", "%@example.com").Get(ctx)
zorm.New[User]().Where("status", "!=", "inactive").Get(ctx)

// Map (multiple AND conditions)
zorm.New[User]().Where(map[string]any{
    "name": "John",
    "age":  25,
}).Get(ctx)

// Struct (non-zero fields)
zorm.New[User]().Where(&User{Name: "John", Age: 25}).Get(ctx)

// Nested/Grouped conditions
zorm.New[User]().Where(func(q *zorm.Model[User]) {
    q.Where("role", "admin").OrWhere("role", "manager")
}).Where("active", true).Get(ctx)
// WHERE (role = 'admin' OR role = 'manager') AND active = true

// NULL checks
zorm.New[User]().WhereNull("deleted_at").Get(ctx)
zorm.New[User]().WhereNotNull("verified_at").Get(ctx)

// IN clause
zorm.New[User]().WhereIn("id", []any{1, 2, 3}).Get(ctx)

// OR conditions
zorm.New[User]().Where("age", ">", 18).OrWhere("verified", true).Get(ctx)
```

### Exists Check

```go
// Check if any matching record exists (efficient - uses SELECT 1 LIMIT 1)
exists, err := zorm.New[User]().Where("email", "john@example.com").Exists(ctx)
if exists {
    fmt.Println("User exists!")
}
```

### Pluck (Single Column)

```go
// Get just the email column from all users
emails, err := zorm.New[User]().Where("active", true).Pluck(ctx, "email")
for _, email := range emails {
    fmt.Println(email)
}
```

### Cursor (Memory-Efficient Iteration)

For large datasets, use `Cursor` to iterate row by row without loading everything into memory:

```go
cursor, err := zorm.New[User]().Where("active", true).Cursor(ctx)
if err != nil {
    return err
}
defer cursor.Close()

for cursor.Next() {
    user, err := cursor.Scan(ctx)
    if err != nil {
        return err
    }
    // Process user one at a time
    fmt.Println(user.Name)
}
```

### FirstOrCreate & UpdateOrCreate

```go
// Find first matching record, or create if not found
user, err := zorm.New[User]().FirstOrCreate(ctx,
    map[string]any{"email": "john@example.com"},  // Search attributes
    map[string]any{"name": "John", "age": 25},    // Values for creation
)

// Find and update, or create if not found
user, err := zorm.New[User]().UpdateOrCreate(ctx,
    map[string]any{"email": "john@example.com"},  // Search attributes
    map[string]any{"name": "John Updated"},       // Values to set
)
```

### Pagination

```go
// Full pagination (with total count - 2 queries)
result, err := zorm.New[User]().Paginate(ctx, 1, 15)
fmt.Println(result.Data)        // []*User
fmt.Println(result.Total)       // Total record count
fmt.Println(result.CurrentPage) // 1
fmt.Println(result.LastPage)    // Calculated last page
fmt.Println(result.PerPage)     // 15

// Simple pagination (no count - 1 query, faster)
result, err := zorm.New[User]().SimplePaginate(ctx, 1, 15)
// result.Total will be -1 (skipped)
```

### Clone (Reuse Queries Safely)

```go
baseQuery := zorm.New[User]().Where("active", true)

// Clone prevents modifying original
admins, _ := baseQuery.Clone().Where("role", "admin").Get(ctx)
users, _ := baseQuery.Clone().Limit(10).Get(ctx)

// Original is unchanged
all, _ := baseQuery.Get(ctx)
```

### Custom Table Name

```go
// Override table name for this query
users, _ := zorm.New[User]().Table("archived_users").Get(ctx)
```

---

## Lifecycle Hooks

ZORM supports lifecycle hooks that are automatically called during CRUD operations.

### Available Hooks

| Hook                | When Called   |
| ------------------- | ------------- |
| `BeforeCreate(ctx)` | Before INSERT |
| `BeforeUpdate(ctx)` | Before UPDATE |
| `AfterUpdate(ctx)`  | After UPDATE  |

### Implementing Hooks

```go
type User struct {
    ID        int64
    Name      string
    Email     string
    CreatedAt time.Time
    UpdatedAt time.Time
}

// BeforeCreate is called before inserting a new record
func (u *User) BeforeCreate(ctx context.Context) error {
    // Validate
    if u.Email == "" {
        return errors.New("email is required")
    }

    // Set defaults
    u.CreatedAt = time.Now()

    // Normalize data
    u.Email = strings.ToLower(u.Email)

    return nil
}

// BeforeUpdate is called before updating a record
func (u *User) BeforeUpdate(ctx context.Context) error {
    // Validate
    if u.Name == "" {
        return errors.New("name cannot be empty")
    }

    // updated_at is set automatically by ZORM

    return nil
}

// AfterUpdate is called after a successful update
func (u *User) AfterUpdate(ctx context.Context) error {
    // Log, send notifications, update cache, etc.
    log.Printf("User %d updated", u.ID)
    return nil
}
```

### Hook Execution Flow

```go
// Create flow:
// 1. BeforeCreate(ctx) called
// 2. INSERT executed
// 3. ID populated

user := &User{Name: "John", Email: "JOHN@EXAMPLE.COM"}
err := zorm.New[User]().Create(ctx, user)
// BeforeCreate lowercases email to "john@example.com"

// Update flow:
// 1. updated_at set automatically
// 2. BeforeUpdate(ctx) called
// 3. UPDATE executed
// 4. AfterUpdate(ctx) called

user.Name = "Jane"
err = zorm.New[User]().Update(ctx, user)
```

---

## Accessors (Computed Attributes)

Define getter methods to compute virtual attributes. Methods starting with `Get` are automatically called after scanning. The struct must have an `Attributes map[string]any` field to store computed values.

```go
type User struct {
    ID         int64
    FirstName  string
    LastName   string
    Attributes map[string]any // Holds computed values
}

// Accessor: GetFullName -> attributes["full_name"]
func (u *User) GetFullName() string {
    return u.FirstName + " " + u.LastName
}

// Accessor: GetInitials -> attributes["initials"]
func (u *User) GetInitials() string {
    return string(u.FirstName[0]) + string(u.LastName[0])
}

// Usage
user, _ := zorm.New[User]().Find(ctx, 1)
fmt.Println(user.Attributes["full_name"])  // "John Doe"
fmt.Println(user.Attributes["initials"])   // "JD"
```

---

## Relationships

### Defining Relations

Relations are defined as methods on your model that return a relation type. The method name can be either `RelationName` or `RelationNameRelation` (e.g., `Posts` or `PostsRelation`).

```go
type User struct {
    ID      int64
    Name    string
    Posts   []*Post  // HasMany
    Profile *Profile // HasOne
}

// HasMany: User has many Posts
// Method can be named "Posts" or "PostsRelation"
func (u User) PostsRelation() zorm.HasMany[Post] {
    return zorm.HasMany[Post]{
        ForeignKey: "user_id",  // Column in posts table
        LocalKey:   "id",       // Optional, defaults to primary key
    }
}

// HasOne: User has one Profile
func (u User) ProfileRelation() zorm.HasOne[Profile] {
    return zorm.HasOne[Profile]{
        ForeignKey: "user_id",
    }
}

type Post struct {
    ID     int64
    UserID int64
    Title  string
    Author *User    // BelongsTo
}

// BelongsTo: Post belongs to User
func (p Post) AuthorRelation() zorm.BelongsTo[User] {
    return zorm.BelongsTo[User]{
        ForeignKey: "user_id",  // Column in posts table
        OwnerKey:   "id",       // Optional, defaults to primary key
    }
}
```

### Custom Table Names in Relations

```go
func (u User) PostsRelation() zorm.HasMany[Post] {
    return zorm.HasMany[Post]{
        ForeignKey: "user_id",
        Table:      "blog_posts",  // Use custom table name
    }
}
```

### Eager Loading

```go
// Load single relation (use the relation name without "Relation" suffix)
users, _ := zorm.New[User]().With("Posts").Get(ctx)

// Load multiple relations
users, _ := zorm.New[User]().With("Posts", "Profile").Get(ctx)

// Load nested relations
users, _ := zorm.New[User]().With("Posts.Comments").Get(ctx)

// Load with constraints
users, _ := zorm.New[User]().WithCallback("Posts", func(q *zorm.Model[Post]) {
    q.Where("published", true).
      OrderBy("created_at", "DESC").
      Limit(5)
}).Get(ctx)
```

### Lazy Loading

```go
user, _ := zorm.New[User]().Find(ctx, 1)

// Load relation on existing entity
err := zorm.New[User]().Load(ctx, user, "Posts")

// Load on slice
users, _ := zorm.New[User]().Get(ctx)
err := zorm.New[User]().LoadSlice(ctx, users, "Posts", "Profile")
```

### Many-to-Many Relations

```go
type User struct {
    ID    int64
    Roles []*Role
}

func (u User) RolesRelation() zorm.BelongsToMany[Role] {
    return zorm.BelongsToMany[Role]{
        PivotTable: "role_user",   // Join table
        ForeignKey: "user_id",     // FK in pivot table
        RelatedKey: "role_id",     // Related FK in pivot table
    }
}
```

#### Managing Many-to-Many Associations

ZORM provides three methods to manage pivot table associations: `Attach`, `Detach`, and `Sync`.

```go
user := &User{ID: 1}

// Attach - Add new associations (inserts into pivot table)
err := zorm.New[User]().Attach(ctx, user, "Roles", []any{3, 4}, nil)
// Adds role_user entries: (1,3), (1,4)

// Attach with pivot data (extra columns in pivot table)
pivotData := map[any]map[string]any{
    3: {"assigned_at": time.Now(), "assigned_by": 1},
    4: {"assigned_at": time.Now(), "assigned_by": 1},
}
err = zorm.New[User]().Attach(ctx, user, "Roles", []any{3, 4}, pivotData)

// Detach - Remove specific associations
err = zorm.New[User]().Detach(ctx, user, "Roles", []any{2})
// Removes role_user entry: (1,2)

// Detach all - Remove all associations for the relation
err = zorm.New[User]().Detach(ctx, user, "Roles", nil)
// Removes all role_user entries where user_id = 1
```

#### Sync - Synchronize Associations

`Sync` is the most powerful method for managing many-to-many relations. It synchronizes the pivot table to match exactly the IDs you provide:
- **Attaches** IDs that are in the new list but not in the database
- **Detaches** IDs that are in the database but not in the new list
- **Keeps** IDs that exist in both (no duplicate entry errors)

```go
user := &User{ID: 1}
// Current roles in DB: [1, 2, 3]

// Sync to new set of roles
err := zorm.New[User]().Sync(ctx, user, "Roles", []any{1, 2, 4}, nil)
// Result:
// - Role 1: kept (exists in both)
// - Role 2: kept (exists in both)
// - Role 3: detached (was in DB, not in new list)
// - Role 4: attached (not in DB, is in new list)
// Final roles in DB: [1, 2, 4]

// Sync with pivot data for new attachments
pivotData := map[any]map[string]any{
    4: {"assigned_at": time.Now()},
}
err = zorm.New[User]().Sync(ctx, user, "Roles", []any{1, 2, 4}, pivotData)
```

**Common Sync Use Cases:**

```go
// Replace all user roles with a new set
err := zorm.New[User]().Sync(ctx, user, "Roles", []any{1, 2}, nil)

// Remove all roles (sync with empty list)
err = zorm.New[User]().Sync(ctx, user, "Roles", []any{}, nil)

// Form submission: update user roles from checkbox selection
selectedRoleIDs := []any{1, 3, 5}  // From form
err = zorm.New[User]().Sync(ctx, user, "Roles", selectedRoleIDs, nil)
```

### Polymorphic Relations

```go
type Image struct {
    ID            int64
    URL           string
    ImageableType string  // "users" or "posts"
    ImageableID   int64
}

// MorphOne: User has one Image
func (u User) AvatarRelation() zorm.MorphOne[Image] {
    return zorm.MorphOne[Image]{
        Type: "ImageableType",  // Type column
        ID:   "ImageableID",    // ID column
    }
}

// MorphMany: Post has many Images
func (p Post) ImagesRelation() zorm.MorphMany[Image] {
    return zorm.MorphMany[Image]{
        Type: "ImageableType",
        ID:   "ImageableID",
    }
}

// Loading with type constraints
images, _ := zorm.New[Image]().WithMorph("Imageable", map[string][]string{
    "users": {"Profile"},  // When type=users, also load Profile
    "posts": {},           // When type=posts, just load Post
}).Get(ctx)
```

---

## Transactions

```go
// Function-based transaction
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    user := &User{Name: "John"}
    if err := zorm.New[User]().WithTx(tx).Create(ctx, user); err != nil {
        return err // Rollback
    }

    post := &Post{UserID: user.ID, Title: "First Post"}
    if err := zorm.New[Post]().WithTx(tx).Create(ctx, post); err != nil {
        return err // Rollback
    }

    return nil // Commit
})

// Model-based transaction
err = zorm.New[User]().Transaction(ctx, func(tx *zorm.Tx) error {
    return zorm.New[User]().WithTx(tx).Create(ctx, &User{Name: "Jane"})
})
```

Transaction features:

- Auto-rollback on error return
- Auto-rollback on panic (re-panics after rollback)
- Auto-commit on nil return

---

## Error Handling

ZORM provides comprehensive error handling with categorized errors.

### Sentinel Errors

```go
import "github.com/rezakhademix/zorm"

// Query errors
zorm.ErrRecordNotFound     // No matching record

// Model errors
zorm.ErrInvalidModel       // Invalid model type
zorm.ErrNilPointer         // Nil pointer passed

// Relation errors
zorm.ErrRelationNotFound   // Relation method not found
zorm.ErrInvalidRelation    // Invalid relation type

// Constraint violations
zorm.ErrDuplicateKey       // Unique constraint violation
zorm.ErrForeignKey         // Foreign key constraint violation
zorm.ErrNotNullViolation   // NOT NULL constraint violation
zorm.ErrCheckViolation     // CHECK constraint violation

// Connection errors
zorm.ErrConnectionFailed   // Connection refused
zorm.ErrConnectionLost     // Connection lost during operation
zorm.ErrTimeout            // Operation timeout

// Transaction errors
zorm.ErrTransactionDeadlock    // Deadlock detected
zorm.ErrSerializationFailure  // Serialization failure

// Schema errors
zorm.ErrColumnNotFound     // Column doesn't exist
zorm.ErrTableNotFound      // Table doesn't exist
zorm.ErrInvalidSyntax      // SQL syntax error
```

### Error Helper Functions

```go
user, err := zorm.New[User]().Find(ctx, 999)

// Check specific error types
if zorm.IsNotFound(err) {
    // Handle not found
}

if zorm.IsDuplicateKey(err) {
    // Handle duplicate
}

if zorm.IsConstraintViolation(err) {
    // Any constraint violation
}

if zorm.IsConnectionError(err) {
    // Connection failed or lost
}

if zorm.IsTimeout(err) {
    // Operation timed out
}

if zorm.IsDeadlock(err) {
    // Transaction deadlock - retry
}

if zorm.IsSchemaError(err) {
    // Missing column, table, or syntax error
}
```

### QueryError Details

```go
user, err := zorm.New[User]().Create(ctx, &User{Email: "duplicate@example.com"})
if err != nil {
    if qe := zorm.GetQueryError(err); qe != nil {
        fmt.Println(qe.Query)      // The SQL that failed
        fmt.Println(qe.Args)       // Query arguments
        fmt.Println(qe.Operation)  // "INSERT", "SELECT", etc.
        fmt.Println(qe.Table)      // Table name (if detected)
        fmt.Println(qe.Constraint) // Constraint name (if detected)
    }
}
```

---

## Advanced Features

### Statement Caching

Improve performance by reusing prepared statements:

```go
cache := zorm.NewStmtCache(100)  // Cache up to 100 statements
defer cache.Close()

model := zorm.New[User]().WithStmtCache(cache)

// Statements are prepared once and reused
users, _ := model.Clone().Where("age", ">", 18).Get(ctx)
users, _ := model.Clone().Where("age", ">", 25).Get(ctx)  // Reuses prepared statement
```

### Read/Write Splitting

```go
// Configure resolver
zorm.ConfigureDBResolver(
    zorm.WithPrimary(primaryDB),
    zorm.WithReplicas(replica1, replica2),
    zorm.WithLoadBalancer(zorm.RoundRobinLB),
)

// Automatic routing
users, _ := zorm.New[User]().Get(ctx)          // Reads from replica
err := zorm.New[User]().Create(ctx, user)      // Writes to primary

// Force primary for consistency
users, _ := zorm.New[User]().UsePrimary().Get(ctx)

// Force specific replica
users, _ := zorm.New[User]().UseReplica(0).Get(ctx)
```

### Common Table Expressions (CTEs)

```go
// String CTE
users, _ := zorm.New[User]().
    WithCTE("active_users", "SELECT * FROM users WHERE active = true").
    Raw("SELECT * FROM active_users WHERE age > 18").
    Get(ctx)

// Subquery CTE
subQuery := zorm.New[User]().Where("active", true)
users, _ := zorm.New[User]().
    WithCTE("active_users", subQuery).
    Raw("SELECT * FROM active_users").
    Get(ctx)
```

### Full-Text Search (PostgreSQL)

```go
// Basic full-text search
articles, _ := zorm.New[Article]().
    WhereFullText("content", "database sql").Get(ctx)

// With language config
articles, _ := zorm.New[Article]().
    WhereFullTextWithConfig("content", "base de datos", "spanish").Get(ctx)

// Pre-computed tsvector column (fastest)
articles, _ := zorm.New[Article]().
    WhereTsVector("search_vector", "golang & performance").Get(ctx)

// Phrase search (word order matters)
articles, _ := zorm.New[Article]().
    WherePhraseSearch("title", "getting started").Get(ctx)
```

### Row Locking

```go
// Lock for update (exclusive)
user, _ := zorm.New[User]().Where("id", 1).Lock("UPDATE").First(ctx)

// Shared lock
user, _ := zorm.New[User]().Where("id", 1).Lock("SHARE").First(ctx)

// PostgreSQL-specific
user, _ := zorm.New[User]().Where("id", 1).Lock("NO KEY UPDATE").First(ctx)
```

### Advanced Grouping

```go
// ROLLUP
zorm.New[Order]().
    Select("region", "city", "SUM(amount)").
    GroupByRollup("region", "city").Get(ctx)

// CUBE
zorm.New[Order]().
    Select("year", "month", "SUM(amount)").
    GroupByCube("year", "month").Get(ctx)

// GROUPING SETS
zorm.New[Order]().
    GroupByGroupingSets(
        []string{"region"},
        []string{"city"},
        []string{},  // Grand total
    ).Get(ctx)
```

### Chunking Large Datasets

```go
err := zorm.New[User]().Chunk(ctx, 1000, func(users []*User) error {
    for _, user := range users {
        // Process each user
    }
    return nil  // Return error to stop chunking
})
```

### Scopes (Reusable Query Logic)

```go
func Active(q *zorm.Model[User]) *zorm.Model[User] {
    return q.Where("active", true).WhereNull("deleted_at")
}

func Verified(q *zorm.Model[User]) *zorm.Model[User] {
    return q.WhereNotNull("verified_at")
}

func RecentlyActive(q *zorm.Model[User]) *zorm.Model[User] {
    return q.Where("last_login", ">", time.Now().AddDate(0, -1, 0))
}

// Chain scopes
users, _ := zorm.New[User]().
    Scope(Active).
    Scope(Verified).
    Scope(RecentlyActive).
    Get(ctx)
```

### Query Debugging

```go
sql, args := zorm.New[User]().
    Where("age", ">", 18).
    OrderBy("name", "ASC").
    Limit(10).
    Print()

fmt.Println(sql)   // SELECT * FROM users WHERE 1=1 AND age > ? ORDER BY name ASC LIMIT 10
fmt.Println(args)  // [18]
```

---

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/rezakhademix/zorm"
)

type User struct {
    ID        int64
    Name      string
    Email     string
    Age       int
    Active    bool
    CreatedAt time.Time
    UpdatedAt time.Time
    Posts     []*Post
}

func (u *User) BeforeCreate(ctx context.Context) error {
    u.CreatedAt = time.Now()
    u.Active = true
    return nil
}

func (u User) PostsRelation() zorm.HasMany[Post] {
    return zorm.HasMany[Post]{ForeignKey: "user_id"}
}

type Post struct {
    ID        int64
    UserID    int64
    Title     string
    Published bool
}

func main() {
    ctx := context.Background()

    // Connect
    db, err := zorm.ConnectPostgres("postgres://...", nil)
    if err != nil {
        log.Fatal(err)
    }
    zorm.GlobalDB = db

    // Create with hook
    user := &User{Name: "John", Email: "john@example.com", Age: 25}
    if err := zorm.New[User]().Create(ctx, user); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Created user %d\n", user.ID)

    // Query with relations
    users, err := zorm.New[User]().
        Where("age", ">", 18).
        Where("active", true).
        WithCallback("Posts", func(q *zorm.Model[Post]) {
            q.Where("published", true).Limit(5)
        }).
        OrderBy("created_at", "DESC").
        Limit(10).
        Get(ctx)

    if err != nil {
        log.Fatal(err)
    }

    for _, u := range users {
        fmt.Printf("%s has %d published posts\n", u.Name, len(u.Posts))
    }

    // FirstOrCreate
    user, err = zorm.New[User]().FirstOrCreate(ctx,
        map[string]any{"email": "jane@example.com"},
        map[string]any{"name": "Jane", "age": 30},
    )

    // Pagination
    result, _ := zorm.New[User]().Paginate(ctx, 1, 15)
    fmt.Printf("Page 1 of %d, Total: %d\n", result.LastPage, result.Total)
}
```

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.
