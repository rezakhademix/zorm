<div align="center">
  <h1>Z-ORM</h1>
  <p>One ORM To Query Them All</p>
</div>

ZORM is a powerful, type-safe, and developer-friendly Go ORM designed for modern applications. It provides a fluent API for building complex SQL queries, managing relationships, handling transactions, and supporting advanced database features like main/replica splitting.

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
  - [Setup Connection](#setup-connection)
  - [Define Models](#define-models)
- [Basic CRUD](#basic-crud)
  - [Create](#create)
  - [Read (Get, First, Find)](#read)
  - [Update](#update)
  - [Delete](#delete)
- [Query Builder](#query-builder)
  - [Select & Distinct](#select--distinct)
  - [Where Clauses](#where-clauses)
  - [Ordering & Pagination](#ordering--pagination)
  - [Grouping & Aggregates](#grouping--aggregates)
  - [Chunking](#chunking)
- [Advanced Querying](#advanced-querying)
  - [Raw Queries](#raw-queries)
  - [Subqueries & CTEs](#subqueries--ctes)
  - [Full-Text Search](#full-text-search)
- [Relationships](#relationships)
  - [Defining Relations](#defining-relations)
  - [Eager Loading](#eager-loading)
  - [Polymorphic Relations](#polymorphic-relations)
- [Transactions & Locking](#transactions--locking)
  - [Transactions](#transactions)
  - [Locking (For Update)](#locking)
- [Advanced Features](#advanced-features)
  - [Read/Write Splitting (Replicas)](#readwrite-splitting)
  - [Statement Caching](#statement-caching)
  - [Context Support](#context-support)

---

## Installation

```bash
go get github.com/rezakhademix/zorm
```

## Getting Started

### Setup Connection

Initialize the global database connection or configure a resolver.

```go
package main

import (
    "database/sql"
    "github.com/rezakhademix/zorm"
    _ "github.com/lib/pq" // PostgreSQL driver
)

func main() {
    db, _ := sql.Open("postgres", "postgres://user:pass@localhost/dbname?sslmode=disable")

    // Set Global DB
    zorm.GlobalDB = db

    // Optional: Configure Connection Pool
    zorm.ConfigureConnectionPool(db, 25, 5, 0, 0)
}
```

### Define Models

Models are standard Go structs. ZORM uses reflection to map struct fields to database columns.

```go
type User struct {
    ID        int       `json:"id"`
    Name      string    `json:"name"`
    Email     string    `json:"email"`
    CreatedAt time.Time `json:"created_at"`
}
```

---

## Basic CRUD

### Create

```go
user := &User{Name: "John Doe", Email: "john@example.com"}
err := zorm.New[User]().Create(ctx, user)
```

### Read

**Find by ID:**

```go
user, err := zorm.New[User]().Find(ctx, 1)
```

**Get All:**

```go
users, err := zorm.New[User]().Get(ctx)
```

**First Record:**

```go
user, err := zorm.New[User]().First(ctx)
```

### Update

```go
user.Name = "Jane Doe"
err := zorm.New[User]().Update(ctx, user)
```

### Delete

```go
err := zorm.New[User]().Delete(ctx, user)
```

---

## Query Builder

### Select & Distinct

```go
// Select specific columns
users, err := zorm.New[User]().Select("id", "name").Get(ctx)

// Distinct
users, err := zorm.New[User]().Distinct().Get(ctx)
```

### Where Clauses

ZORM supports multiple ways to add conditions.

**Basic:**

```go
zorm.New[User]().Where("name", "John").Get(ctx)
zorm.New[User]().Where("age >", 18).Get(ctx)
```

**Map:**

```go
zorm.New[User]().Where(map[string]any{
    "name": "John",
    "age":  30,
}).Get(ctx)
```

**Struct:**

```go
zorm.New[User]().Where(&User{Name: "John"}).Get(ctx)
```

**Nested (Grouped):**

```go
zorm.New[User]().Where(func(q *zorm.Model[User]) {
    q.Where("role", "admin").OrWhere("role", "manager")
}).Get(ctx)
// Generates: WHERE (role = 'admin' OR role = 'manager')
```

**Where In:**

```go
zorm.New[User]().WhereIn("id", []any{1, 2, 3}).Get(ctx)
```

### Ordering & Pagination

**Ordering:**

```go
zorm.New[User]().OrderBy("created_at", "DESC").Get(ctx)
// Helpers
zorm.New[User]().Latest().Get(ctx)
zorm.New[User]().Oldest().Get(ctx)
```

**Limit & Offset:**

```go
zorm.New[User]().Limit(10).Offset(5).Get(ctx)
```

**Pagination:**

```go
result, err := zorm.New[User]().Paginate(ctx, 1, 15) // Page 1, 15 per page
fmt.Println(result.Data, result.Total, result.LastPage)
```

### Grouping & Aggregates

**Grouping:**

```go
zorm.New[User]().GroupBy("role").Get(ctx)
```

**Aggregates:**

```go
count, _ := zorm.New[User]().Count(ctx)
sum, _ := zorm.New[User]().Sum(ctx, "amount")
avg, _ := zorm.New[User]().Avg(ctx, "age")
```

### Chunking

Process large datasets efficiently without loading everything into memory.

```go
err := zorm.New[User]().Chunk(ctx, 100, func(users []*User) error {
    for _, user := range users {
        // Process user
    }
    return nil
})
```

---

## Advanced Querying

### Raw Queries

Execute raw SQL when the query builder isn't enough.

```go
// Raw Select
users, err := zorm.New[User]().Raw("SELECT * FROM users WHERE id = ?", 1).Get(ctx)

// Raw Exec
err := zorm.New[User]().Exec(ctx, "UPDATE users SET active = ? WHERE last_login < ?", false, time.Now())
```

### Subqueries & CTEs

**Common Table Expressions (CTE):**

```go
zorm.New[User]().
    WithCTE("active_users", "SELECT * FROM users WHERE active = true").
    Raw("SELECT * FROM active_users").
    Get(ctx)
```

### Full-Text Search

PostgreSQL specific full-text search support.

```go
zorm.New[Article]().WhereFullText("content", "database OR sql").Get(ctx)
```

---

## Relationships

### Defining Relations

Define methods on your model struct to configure relationships.

```go
type User struct {
    ID int
    // ...
}

// HasMany Relation
func (u User) Posts() zorm.Relation {
    return zorm.HasMany[Post]{ForeignKey: "user_id"}
}

type Post struct {
    ID     int
    UserID int
    // ...
}

// BelongsTo Relation
func (p Post) Author() zorm.Relation {
    return zorm.BelongsTo[User]{ForeignKey: "user_id"}
}
```

### Eager Loading

Load relationships efficiently to avoid N+1 query problems.

```go
// Load single relation
users, err := zorm.New[User]().With("Posts").Get(ctx)

// Load nested relation
users, err := zorm.New[User]().With("Posts.Comments").Get(ctx)

// Load with constraints (Callback)
users, err := zorm.New[User]().WithCallback("Posts", func(q *zorm.Model[Post]) {
    q.Where("published", true).OrderBy("created_at", "DESC")
}).Get(ctx)
```

### Polymorphic Relations

Support for MorphTo, MorphOne, and MorphMany.

```go
// MorphTo
func (i Image) Imageable() zorm.Relation {
    return zorm.MorphTo[any]{Type: "imageable_type", ID: "imageable_id"}
}

// Loading MorphTo
images, err := zorm.New[Image]().WithMorph("Imageable", map[string][]string{
    "users": {"Profile"}, // Load Profile for Users
    "posts": {},          // Just load Post
}).Get(ctx)
```

---

## Transactions & Locking

### Transactions

Execute multiple operations atomically.

```go
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    u := &User{Name: "New User"}
    if err := zorm.New[User]().WithTx(tx).Create(ctx, u); err != nil {
        return err
    }

    p := &Post{UserID: u.ID, Title: "First Post"}
    if err := zorm.New[Post]().WithTx(tx).Create(ctx, p); err != nil {
        return err
    }

    return nil
})
```

### Locking

Lock rows for update to prevent race conditions.

```go
// SELECT * FROM users WHERE id = 1 FOR UPDATE
user, err := zorm.New[User]().Where("id", 1).Lock("UPDATE").First(ctx)
```

---

## Advanced Features

### Read/Write Splitting

Automatically route read queries to replicas and write queries to the primary database.

```go
// Configure Resolver
zorm.ConfigureDBResolver(
    zorm.WithPrimary(primaryDB),
    zorm.WithReplicas(replica1, replica2),
    zorm.WithLoadBalancer(zorm.RoundRobinLB),
)

// Usage is transparent
users, _ := zorm.New[User]().Get(ctx) // Reads from replica
err := zorm.New[User]().Create(ctx, u) // Writes to primary

// Force Primary for next read
users, _ := zorm.New[User]().UsePrimary().Get(ctx)
```

### Statement Caching

Improve performance by caching prepared statements.

```go
cache := zorm.NewStmtCache(100)
defer cache.Close()

// Use cache for this query
users, err := zorm.New[User]().WithStmtCache(cache).Where("id", 1).Get(ctx)
```

### Context Support

All operations support `context.Context` for cancellation and timeouts.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

users, err := zorm.New[User]().Get(ctx)
```

---

## Examples

### Multiple Combinations

Combine multiple features for complex logic.

```go
// Get top 10 active users who have published posts, ordered by last login, with their recent posts
users, err := zorm.New[User]().
    Select("id", "name", "last_login").
    Where("active", true).
    WhereHas("Posts", func(q *zorm.Model[Post]) {
        q.Where("published", true)
    }).
    WithCallback("Posts", func(q *zorm.Model[Post]) {
        q.Limit(5).OrderBy("created_at", "DESC")
    }).
    OrderBy("last_login", "DESC").
    Limit(10).
    Get(ctx)
```

### Complex Queries

Using Subqueries and CTEs.

```go
// Find users who have more than average number of posts
avgPostsQuery := "SELECT AVG(post_count) FROM (SELECT COUNT(*) as post_count FROM posts GROUP BY user_id) as counts"

users, err := zorm.New[User]().
    Where("id IN (SELECT user_id FROM posts GROUP BY user_id HAVING COUNT(*) > (" + avgPostsQuery + "))").
    Get(ctx)
```

### Raw Queries with Maps

```go
var results []map[string]any
// Execute raw query and map results manually if needed, or use struct scanning
// ZORM currently focuses on Struct scanning, but you can use sql.Rows directly from queryer() if needed.
```
