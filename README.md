<div align="center">
  <h1>ZORM</h1>
  <p><strong>A Type-Safe, Production Ready Go ORM</strong></p>
  <p>One ORM To Query Them All</p>
</div>

---

ZORM is a powerful, type-safe, and developer-friendly Go ORM designed for modern applications. It leverages Go generics to provide compile-time type safety while offering a fluent, chainable API for building complex SQL queries with ease.

## ✨ Key Features

- **🔒 Type-Safe**: Full compile-time type safety powered by Go generics
- **🚀 Zero Dependencies**: Built on Go's `database/sql` package, works with any SQL driver
- **⚡ High Performance**: Prepared statement caching and connection pooling
- **🔄 Relations**: HasOne, HasMany, BelongsTo, BelongsToMany, Polymorphic relations
- **🎯 Fluent API**: Chainable query builder with intuitive method names
- **📊 Advanced Queries**: CTEs, Subqueries, Full-Text Search, Window Functions
- **💾 Database Splitting**: Automatic read/write split with replica support
- **🔐 Context Support**: All operations respect `context.Context` for cancellation & timeout
- **📝 Debugging**: `Print()` method to inspect generated SQL without executing

## 📦 Installation

```bash
go get github.com/rezakhademix/zorm
```

## 🚀 Quick Start

### 1. Connect to Database

#### PostgreSQL (Recommended)

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

#### SQLite

```go
import (
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

db, _ := sql.Open("sqlite3", "./database.db")
zorm.GlobalDB = db
```

### 2. Define Models

```go
type User struct {
    ID        int64
    Name      string
    Email     string
    Age       int
    CreatedAt time.Time
}
```

### 3. Basic CRUD

```go
ctx := context.Background()

// Create
user := &User{Name: "John", Email: "john@example.com"}
err := zorm.New[User]().Create(ctx, user)

// Read
user, err := zorm.New[User]().Find(ctx, 1)
users, err := zorm.New[User]().Where("age >", 18).Get(ctx)

// Update
user.Name = "Jane"
err = zorm.New[User]().Update(ctx, user)

// Delete
err = zorm.New[User]().Delete(ctx, user)
```

---

## 📚 Complete Feature Guide

### Query Builder

#### Select & Distinct

```go
// Select specific columns
users, _ := zorm.New[User]().Select("id", "name", "email").Get(ctx)

// Distinct
users, _ := zorm.New[User]().Distinct().Get(ctx)

// Distinct On (PostgreSQL)
users, _ := zorm.New[User]().DistinctBy("email").Get(ctx)
```

#### Where Conditions

**Basic**

```go
zorm.New[User]().Where("name", "John").Get(ctx)
zorm.New[User]().Where("age >", 18).Get(ctx)
zorm.New[User]().Where("email LIKE", "%@example.com").Get(ctx)
```

**Map**

```go
zorm.New[User]().Where(map[string]any{
    "name": "John",
    "age":  25,
}).Get(ctx)
```

**Struct**

```go
zorm.New[User]().Where(&User{Name: "John", Age: 25}).Get(ctx)
```

**Nested/Grouped**

```go
zorm.New[User]().Where(func(q *zorm.Model[User]) {
    q.Where("role", "admin").OrWhere("role", "manager")
}).Get(ctx)
// Generates: WHERE (role = 'admin' OR role = 'manager')
```

**Where In**

```go
zorm.New[User]().WhereIn("id", []any{1, 2, 3}).Get(ctx)
```

**Or Where**

```go
zorm.New[User]().Where("age >", 18).OrWhere("verified", true).Get(ctx)
```

#### Ordering & Pagination

```go
// Order By
zorm.New[User]().OrderBy("created_at", "DESC").Get(ctx)

// Helpers
zorm.New[User]().Latest().Get(ctx)        // created_at DESC
zorm.New[User]().Oldest().Get(ctx)        // created_at ASC
zorm.New[User]().Latest("updated_at").Get(ctx)

// Limit & Offset
zorm.New[User]().Limit(10).Offset(20).Get(ctx)

// Pagination (with total count)
result, _ := zorm.New[User]().Paginate(ctx, 1, 15)
fmt.Println(result.Data, result.Total, result.LastPage)

// Simple Pagination (no count query, 2x faster)
result, _ := zorm.New[User]().SimplePaginate(ctx, 1, 15)
```

#### Grouping & Aggregates

```go
// Group By
zorm.New[User]().GroupBy("role").Get(ctx)

// Advanced Grouping (PostgreSQL)
zorm.New[User]().GroupByRollup("region", "city").Get(ctx)
zorm.New[User]().GroupByCube("year", "month").Get(ctx)
zorm.New[User]().GroupByGroupingSets([]string{"region"}, []string{"city"}).Get(ctx)

// Having
zorm.New[User]().
    GroupBy("role").
    Having("COUNT(*) >", 5).
    Get(ctx)

// Aggregates
count, _ := zorm.New[User]().Count(ctx)
sum, _ := zorm.New[User]().Sum(ctx, "amount")
avg, _ := zorm.New[User]().Avg(ctx, "age")
```

#### Chunking

Process large datasets efficiently without loading everything into memory.

```go
err := zorm.New[User]().Chunk(ctx, 100, func(users []*User) error {
    for _, user := range users {
        // Process each user
        fmt.Println(user.Name)
    }
    return nil
})
```

---

### Advanced Querying

#### Raw Queries

Execute raw SQL when the query builder isn't enough.

```go
// Raw select
users, _ := zorm.New[User]().Raw("SELECT * FROM users WHERE age > ?", 18).Get(ctx)

// Raw execution (INSERT, UPDATE, DELETE)
_, err := zorm.New[User]().
    Raw("UPDATE users SET verified = ? WHERE email LIKE ?", true, "%@company.com").
    Exec(ctx)
```

#### Common Table Expressions (CTEs)

```go
users, _ := zorm.New[User]().
    WithCTE("active_users", "SELECT * FROM users WHERE active = true").
    Raw("SELECT * FROM active_users WHERE age > 18").
    Get(ctx)
```

#### Full-Text Search (PostgreSQL)

```go
// Standard full-text search
articles, _ := zorm.New[Article]().
    WhereFullText("content", "database OR sql").
    Get(ctx)

// With custom text search configuration
articles, _ := zorm.New[Article]().
    WhereFullTextWithConfig("content", "base de datos", "spanish").
    Get(ctx)

// Using pre-computed tsvector column
articles, _ := zorm.New[Article]().
    WhereTsVector("search_vector", "golang & performance").
    Get(ctx)

// Phrase search
articles, _ := zorm.New[Article]().
    WherePhraseSearch("title", "getting started").
    Get(ctx)
```

#### Locking (FOR UPDATE)

```go
// Lock rows for update
user, _ := zorm.New[User]().
    Where("id", 1).
    Lock("UPDATE").
    First(ctx)

// Share lock
user, _ := zorm.New[User]().
    Where("id", 1).
    Lock("SHARE").
    First(ctx)
```

#### Where Has (Query Relations)

```go
// Get users who have posts
users, _ := zorm.New[User]().
    WhereHas("Posts", func(q *zorm.Model[Post]) {
        q.Where("published", true)
    }).
    Get(ctx)
```

---

### Relationships

#### Defining Relations

```go
type User struct {
    ID    int64
    Name  string
    Posts []*Post  // Loaded via eager loading
}

// HasMany Relation
func (u User) Posts() zorm.HasMany[Post] {
    return zorm.HasMany[Post]{ForeignKey: "user_id"}
}

type Post struct {
    ID     int64
    UserID int64
    Title  string
    Author *User  // Loaded via eager loading
}

// BelongsTo Relation
func (p Post) Author() zorm.BelongsTo[User] {
    return zorm.BelongsTo[User]{ForeignKey: "user_id"}
}
```

#### Eager Loading

```go
// Load single relation
users, _ := zorm.New[User]().With("Posts").Get(ctx)

// Load nested relation
users, _ := zorm.New[User]().With("Posts.Comments").Get(ctx)

// Load multiple relations
users, _ := zorm.New[User]().
    With("Posts").
    With("Profile").
    Get(ctx)

// Load with constraints
users, _ := zorm.New[User]().
    WithCallback("Posts", func(q *zorm.Model[Post]) {
        q.Where("published", true).
          OrderBy("created_at", "DESC").
          Limit(5)
    }).
    Get(ctx)
```

#### Polymorphic Relations

```go
// MorphTo
type Image struct {
    ID            int64
    ImageableType string
    ImageableID   int64
    URL           string
    Imageable     any  // Can be User or Post
}

func (i Image) Imageable() zorm.MorphTo[any] {
    return zorm.MorphTo[any]{
        Type: "ImageableType",
        ID:   "ImageableID",
        TypeMap: map[string]any{
            "users": User{},
            "posts": Post{},
        },
    }
}

// Loading polymorphic relations
images, _ := zorm.New[Image]().
    WithMorph("Imageable", map[string][]string{
        "users": {"Profile"},  // Load User's Profile
        "posts": {},           // Just load Post
    }).
    Get(ctx)
```

#### Many-to-Many Relations

```go
type User struct {
    ID    int64
    Roles []*Role
}

func (u User) Roles() zorm.BelongsToMany[Role] {
    return zorm.BelongsToMany[Role]{
        PivotTable: "role_user",
        ForeignKey: "user_id",
        RelatedKey: "role_id",
    }
}

// Attach roles to user
user, _ := zorm.New[User]().Find(ctx, 1)
zorm.New[User]().Attach(ctx, user, "Roles", []any{1, 2, 3}, nil)

// Detach roles
zorm.New[User]().Detach(ctx, user, "Roles", []any{2})

// Sync roles (replace all)
zorm.New[User]().Sync(ctx, user, "Roles", []any{1, 3}, nil)

// With pivot data
pivotData := map[any]map[string]any{
    1: {"assigned_at": time.Now()},
}
zorm.New[User]().Attach(ctx, user, "Roles", []any{1}, pivotData)
```

---

### Transactions

```go
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    // Create user
    u := &User{Name: "John"}
    if err := zorm.New[User]().WithTx(tx).Create(ctx, u); err != nil {
        return err // Rollback
    }

    // Create post
    p := &Post{UserID: u.ID, Title: "First Post"}
    if err := zorm.New[Post]().WithTx(tx).Create(ctx, p); err != nil {
        return err // Rollback
    }

    return nil // Commit
})

// Or using Model.Transaction
err = zorm.New[User]().Transaction(ctx, func(m *zorm.Model[User]) error {
    return m.Create(ctx, &User{Name: "Jane"})
})
```

---

### Advanced Features

#### Read/Write Splitting

Automatically route read queries to replicas and write queries to primary.

```go
// Configure resolver
zorm.ConfigureDBResolver(
    zorm.WithPrimary(primaryDB),
    zorm.WithReplicas(replica1, replica2),
    zorm.WithLoadBalancer(zorm.RoundRobinLB),
)

// Usage is transparent
users, _ := zorm.New[User]().Get(ctx)      // Reads from replica
err := zorm.New[User]().Create(ctx, user)  // Writes to primary

// Force primary for next read
users, _ := zorm.New[User]().UsePrimary().Get(ctx)

// Force specific replica
users, _ := zorm.New[User]().UseReplica(0).Get(ctx)
```

#### Statement Caching

Improve performance by reusing prepared statements.

```go
cache := zorm.NewStmtCache(100)
defer cache.Close()

// Use cache for this model
model := zorm.New[User]().WithStmtCache(cache)

// All queries will reuse prepared statements
users, _ := model.Where("age >", 18).Get(ctx)
users, _ := model.Where("age >", 25).Get(ctx)  // Reuses statement
```

#### Context Support

All operations respect `context.Context` for cancellation and timeouts.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

users, err := zorm.New[User]().Get(ctx)
if err != nil {
    // Handle timeout or cancellation
}
```

#### Custom Scopes

Reusable query logic.

```go
func ActiveUsers(q *zorm.Model[User]) *zorm.Model[User] {
    return q.Where("active", true).Where("deleted_at IS NULL", nil)
}

func Verified(q *zorm.Model[User]) *zorm.Model[User] {
    return q.Where("verified", true)
}

// Use scopes
users, _ := zorm.New[User]().
    Scope(ActiveUsers).
    Scope(Verified).
    Get(ctx)
```

#### Query Debugging

```go
// Print SQL without executing
sql, args := zorm.New[User]().
    Where("age >", 18).
    Limit(10).
    Print()

fmt.Println(sql)   // SELECT * FROM users WHERE 1=1 AND (age > ?) LIMIT 10
fmt.Println(args)  // [18]
```

---

### 2. Define Models

Models are standard Go structs. **ZORM uses convention over configuration** - no tags required!

**Convention-based (recommended):**

```go
type User struct {
    ID        int64      // Automatically detected as primary key with auto-increment
    Name      string     // Maps to "name" column
    Email     string     // Maps to "email" column
    Age       int        // Maps to "age" column
    CreatedAt time.Time  // Maps to "created_at" column
}
// Table name: "users" (auto-pluralized snake_case)
```

---

## 🎯 Best Practices

### 1. Always Use Context

```go
ctx := context.Background()
users, err := zorm.New[User]().Get(ctx)  // ✅ Good
```

### 2. Reuse Model Instances with Clone

```go
baseQuery := zorm.New[User]().Where("active", true)

admin := baseQuery.Clone().Where("role", "admin").Get(ctx)
users := baseQuery.Clone().Limit(10).Get(ctx)
```

### 3. Use Transactions for Multiple Operations

```go
zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    // Multiple operations...
    return nil
})
```

### 4. Enable Statement Caching for High Traffic

```go
cache := zorm.NewStmtCache(100)
defer cache.Close()
model := zorm.New[User]().WithStmtCache(cache)
```

### 5. Use Chunk for Large Datasets

```go
zorm.New[User]().Chunk(ctx, 1000, func(users []*User) error {
    // Process batch
    return nil
})
```

---

## 📖 Examples

### Complex Query Example

```go
// Get top 10 verified users who have published posts,
// ordered by last login, with their 5 most recent posts
users, err := zorm.New[User]().
    Select("id", "name", "email", "last_login").
    Where("verified", true).
    WhereHas("Posts", func(q *zorm.Model[Post]) {
        q.Where("status", "published")
    }).
    WithCallback("Posts", func(q *zorm.Model[Post]) {
        q.Where("status", "published").
          OrderBy("created_at", "DESC").
          Limit(5)
    }).
    OrderBy("last_login", "DESC").
    Limit(10).
    Get(ctx)
```

### Batch Operations

```go
// Create many
users := []*User{
    {Name: "Alice"},
    {Name: "Bob"},
}
err := zorm.New[User]().CreateMany(ctx, users)

// Update many
err = zorm.New[User]().
    Where("last_login <", time.Now().AddDate(0, -6, 0)).
    UpdateMany(ctx, map[string]any{
        "active": false,
    })

// Delete many
err = zorm.New[User]().
    Where("active", false).
    DeleteMany(ctx)
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License - see LICENSE file for details.

---
