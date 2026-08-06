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
- **High Performance**: Prepared statement caching, connection pooling, model pooling — see [Advanced Start](#advanced-start)
- **Relations**: HasOne, HasMany, BelongsTo, BelongsToMany, Polymorphic relations
- **Fluent API**: Chainable query builder with intuitive method names
- **Advanced Queries**: CTEs, Subqueries, Full-Text Search, Window Functions
- **Database Splitting**: Automatic read/write split with replica support — see [Advanced Start](#advanced-start)
- **Context Support**: All operations respect `context.Context` for cancellation & timeout
- **Debugging**: `Print()` method to inspect generated SQL without executing
- **Lifecycle Hooks**: BeforeCreate / AfterCreate, BeforeUpdate / AfterUpdate, BeforeDelete / AfterDelete, AfterFind — plus `*Tx` variants for atomic side effects
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
    CreatedAt time.Time  // Maps to "created_at" (auto-set on Create when zero)
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

// UpdateManyByKey - Update multiple records by matching lookup column to map keys
// Each map key is matched against the lookup column, and its value is set in the target column
updates := map[string]string{
    "REF001": "pending",
    "REF002": "approved",
    "REF003": "rejected",
}
err = zorm.New[Order]().UpdateManyByKey(ctx, "reference_number", "status", updates)

// DeleteMany - Delete multiple records matching query
err = zorm.New[User]().Where("status", "inactive").DeleteMany(ctx)
```

**CreateMany Features:**
- Inserts all records in a single SQL statement for efficiency
- Automatically chunks large batches to stay within database limits (65535 parameters for PostgreSQL)
- Uses transactions for multi-chunk inserts to ensure atomicity
- Returns inserted IDs via `RETURNING` clause
- Splits a batch that mixes set and unset primary keys into two INSERTs inside one
  transaction, so explicit IDs are honored and zero-PK rows are still auto-assigned
- Auto-sets `created_at` (when the column exists and the field is zero), but runs
  **no create hooks** — use `BulkInsert` if you need `BeforeCreate` / `AfterCreate`

```go
// For very large datasets, CreateMany automatically chunks
largeDataset := make([]*User, 10000)
for i := range largeDataset {
    largeDataset[i] = &User{Name: fmt.Sprintf("User %d", i)}
}
err := zorm.New[User]().CreateMany(ctx, largeDataset)
// Automatically split into multiple INSERT statements within a transaction
```

**BulkInsert** - Insert many records *with* the create hooks:

```go
users := []*User{
    {Name: "Alice", Email: "alice@example.com"},
    {Name: "Bob", Email: "bob@example.com"},
}
err := zorm.New[User]().BulkInsert(ctx, users)
// BeforeCreate ran for both entities before the first INSERT;
// AfterCreate runs per row as it is inserted; IDs are populated.
```

`BulkInsert` reuses one prepared statement across the batch instead of building a
single multi-row INSERT. Pick it over `CreateMany` when you need hooks; pick
`CreateMany` when you want the fewest round trips.

|                                | `CreateMany`                              | `BulkInsert`                             |
| ------------------------------ | ----------------------------------------- | ---------------------------------------- |
| SQL shape                      | one multi-row INSERT (chunked)            | one prepared statement, executed per row |
| `created_at` auto-set          | yes                                       | yes                                      |
| `BeforeCreate` / `AfterCreate` | no                                        | yes                                      |
| Mixed set/unset primary keys   | split into two INSERTs in one transaction | inserted as two groups                   |

`BulkInsert` runs `BeforeCreate` for the whole batch *before* the first row is
written, so a hook may still change the values that get inserted.

**UpdateManyByKey** - Efficient batch updates using CASE WHEN syntax:

```go
// Example 1: Update order statuses by reference number
statusUpdates := map[string]string{
    "ORD-001": "shipped",
    "ORD-002": "delivered",
    "ORD-003": "cancelled",
}
err := zorm.New[Order]().UpdateManyByKey(ctx, "reference_number", "status", statusUpdates)
// Generates: UPDATE orders SET status = CASE reference_number
//            WHEN 'ORD-001' THEN 'shipped' WHEN 'ORD-002' THEN 'delivered' ... END
//            WHERE reference_number IN ('ORD-001', 'ORD-002', 'ORD-003')

// Example 2: Update product quantities by product code (int keys, int values)
quantityUpdates := map[int]int{
    100: 50,   // product code 100 -> quantity 50
    200: 75,   // product code 200 -> quantity 75
    300: 100,  // product code 300 -> quantity 100
}
err = zorm.New[Product]().UpdateManyByKey(ctx, "code", "quantity", quantityUpdates)

// Example 3: Combine with WHERE clause for conditional updates
// Only update orders that are in 'pending' status
statusUpdates := map[string]string{
    "ORD-001": "processing",
    "ORD-002": "processing",
}
err = zorm.New[Order]().
    Where("status", "pending").
    UpdateManyByKey(ctx, "reference_number", "status", statusUpdates)
// Only updates if both: reference_number matches AND status = 'pending'
```

**UpdateManyByKey Features:**
- Uses efficient CASE WHEN syntax (single query for all updates)
- Supports any map key/value types (string, int, float64, bool, etc.)
- Automatically chunks large maps (500+ entries) with transaction safety
- Combines with existing WHERE conditions
- Auto-updates `updated_at` timestamp if the column exists. `Create`/`CreateMany` also auto-populate `created_at` when present and unset.

---

## Advanced Start

Quick Start gets you querying. This section gets you to production: connection
pool sizing, prepared-statement caching, replica routing, and allocation /
memory control — in the order you should add them.

### 1. Tune the connection pool

```go
db, err := zorm.ConnectPostgres(
    "postgres://user:password@primary.internal/app?sslmode=require",
    &zorm.DBConfig{
        MaxOpenConns:    25,              // hard ceiling on concurrent connections
        MaxIdleConns:    5,               // kept warm between bursts
        ConnMaxLifetime: time.Hour,       // recycle before the server/proxy does
        ConnMaxIdleTime: 30 * time.Minute,
    },
)
if err != nil {
    log.Fatal(err)
}

zorm.SetGlobalDB(db)  // thread-safe; prefer over assigning zorm.GlobalDB directly
```

`ConnectPostgres` pings the database before returning, so a bad DSN or an
unreachable host fails at startup rather than on the first query. Passing `nil`
for the config leaves Go's `database/sql` defaults in place (unlimited open
connections, 2 idle).

### 2. Add a statement cache

`StmtCache` is an LRU of `*sql.Stmt` shared by every model that opts in. Build
one at startup, close it at shutdown:

```go
cache := zorm.NewStmtCache(200)  // capacity <= 0 defaults to 100
defer cache.Close()

// Build a base model once, clone it per query — the cache survives Clone()
base := zorm.New[User]().WithStmtCache(cache)

users, _ := base.Clone().Where("age", ">", 18).Get(ctx)
adults, _ := base.Clone().Where("age", ">", 21).Get(ctx)  // same SQL shape, reuses the prepared statement

fmt.Println(cache.Len())  // number of live cached statements
```

The cache pays off when the same *SQL shape* repeats — argument values differ,
the statement does not. A query built with a different set of clauses is a
different statement.

**Caching rules worth knowing:**

- **Keyed per connection handle**, not just per SQL string: the key includes the
  `*sql.DB` pointer. One cache can therefore be shared safely across a primary
  and every replica — a statement prepared against a replica can never be handed
  to the primary.
- **Transactions are never cached.** A statement prepared on a `*sql.Tx` dies
  with its transaction, so ZORM prepares it fresh and closes it at the end of
  the operation. Workloads that run almost entirely inside `Transaction(...)`
  get little benefit from the cache.
- **Capacity is sharded** across independently locked shards (the shard sizes
  sum to exactly the capacity you asked for), so concurrent lookups on different
  queries do not contend on one mutex.
- `Close()` closes every cached statement; `Clear()` empties the cache but keeps
  it usable.

### 3. Add replicas

```go
zorm.ConfigureDBResolver(
    zorm.WithPrimary(primaryDB),
    zorm.WithReplicas(replica1, replica2),
    zorm.WithLoadBalancer(zorm.RoundRobinLB),  // or zorm.RandomLB
)

// Routing is automatic from here
users, _ := zorm.New[User]().Get(ctx)      // -> load-balanced replica
err := zorm.New[User]().Create(ctx, user)  // -> always primary

// Manual overrides
users, _ = zorm.New[User]().UsePrimary().Get(ctx)   // force primary
users, _ = zorm.New[User]().UseReplica(0).Get(ctx)  // force a specific replica
```

**Routing rules worth knowing:**

- **There is no read-after-write stickiness.** A read issued right after a write
  goes to a replica and may not see the row yet. Call `UsePrimary()` on reads
  that must observe a just-committed write.
- **A configured resolver overrides `SetDB(db)`** for reads *and* writes — the
  resolver is consulted first and never falls back to the model's own handle.
  Only `WithTx(tx)` escapes the resolver: transactions always run on their own
  connection, and are therefore always on the primary.
- **Zero replicas is not an error.** `WithReplicas()` with an empty list routes
  reads to the primary, which makes it safe to configure the resolver
  unconditionally and add replicas later.
- **An out-of-range `UseReplica(i)` silently falls back** to a load-balanced
  replica rather than failing. Treat the index as a hint, not a guarantee.

### 4. Putting it together

Cache, pool and replicas are meant to be configured once at startup:

```go
func setup() (*zorm.StmtCache, error) {
    primary, err := zorm.ConnectPostgres(primaryDSN, &zorm.DBConfig{
        MaxOpenConns:    25,
        MaxIdleConns:    5,
        ConnMaxLifetime: time.Hour,
    })
    if err != nil {
        return nil, err
    }

    // Replicas usually want a larger pool — they take the read traffic
    replica, err := zorm.ConnectPostgres(replicaDSN, &zorm.DBConfig{
        MaxOpenConns:    50,
        MaxIdleConns:    10,
        ConnMaxLifetime: time.Hour,
    })
    if err != nil {
        return nil, err
    }

    zorm.SetGlobalDB(primary)  // fallback for code paths that bypass the resolver
    zorm.ConfigureDBResolver(
        zorm.WithPrimary(primary),
        zorm.WithReplicas(replica),
        zorm.WithLoadBalancer(zorm.RoundRobinLB),
    )

    // One cache for both handles — entries are keyed per handle
    return zorm.NewStmtCache(200), nil
}

// In a request handler
users, err := zorm.New[User]().
    WithStmtCache(cache).
    Where("active", true).
    Limit(50).
    Get(ctx)  // replica + cached statement
```

### 5. Allocation and memory control

**Model pooling.** `Acquire[T]()` takes a `Model[T]` from a `sync.Pool` instead
of allocating one; `Release()` returns it. Worth using on hot paths that build a
query per request:

```go
m := zorm.Acquire[User]()
defer m.Release()

users, err := m.Where("active", true).Limit(20).Get(ctx)
// `users` stays valid after Release — the entities are separate allocations
```

`Acquire` binds the current `GlobalDB` at acquire time. Never touch the model
after `Release()`: its state is cleared and it may already be in use elsewhere.

**Dirty-tracking memory.** Loading an entity stores a baseline so `Save` can
compute dirty columns. Baselines live in a bounded LRU — 50,000 entities by
default. A long-running service that streams through many distinct rows should
bound it explicitly:

```go
zorm.ConfigureDirtyTracking(10000)  // 0 means unbounded — not recommended for long-lived processes
```

For a batch job, scope the tracking instead so it is released deterministically:

```go
scope := zorm.NewTrackingScope()
defer scope.Close()  // drops the baselines for everything loaded through this model

model := zorm.New[User]().WithTrackingScope(scope)
users, _ := model.Get(ctx)
```

`zorm.TrackedEntityCount()` reports the current count (useful as a gauge metric);
`zorm.ClearAllOriginals()` drops everything at once.

**Large result sets.** Don't materialize what you don't need: use
[`Cursor`](#cursor-memory-efficient-iteration) to stream row by row, or
[`Chunk`](#chunking-large-datasets) to process in fixed-size batches.

### What to reach for

| Symptom                                    | Knob                                       |
| ------------------------------------------ | ------------------------------------------ |
| Connection exhaustion / "too many clients" | `DBConfig.MaxOpenConns`, `ConnMaxLifetime` |
| High CPU parsing the same SQL repeatedly   | `WithStmtCache`                            |
| Read traffic saturating the primary        | `ConfigureDBResolver` + replicas           |
| Stale reads right after a write            | `UsePrimary()` on that read                |
| Allocation churn in a hot handler          | `Acquire[T]()` / `Release()`               |
| Memory growth in a long-running worker     | `ConfigureDirtyTracking`, `TrackingScope`  |
| OOM on a large `Get`                       | `Cursor` or `Chunk`                        |

---

## API Reference

### Query Methods

| Method                   | Description                           | Returns                   |
| ------------------------ | ------------------------------------- | ------------------------- |
| `Get(ctx)`               | Execute query and return all results  | `[]*T, error`             |
| `First(ctx)`             | Execute query and return first result | `*T, error`               |
| `Find(ctx, id)`          | Find record by primary key            | `*T, error`               |
| `FindOrFail(ctx, id)`    | Find record or return error           | `*T, error`               |
| `Exists(ctx)`            | Check if any record matches           | `bool, error`             |
| `Count(ctx)`             | Count matching records                | `int64, error`            |
| `Sum(ctx, column)`       | Sum of column values                  | `float64, error`          |
| `Avg(ctx, column)`       | Average of column values              | `float64, error`          |
| `Pluck(ctx, column)`     | Get single column values              | `[]any, error`            |
| `CountOver(ctx, column)` | Count rows per partition of a column  | `map[string]int64, error` |

### Write Methods

| Method                                      | Description                                       |
| ------------------------------------------- | ------------------------------------------------- |
| `Create(ctx, entity)`                       | Insert single record                              |
| `CreateMany(ctx, entities)`                 | Insert multiple records in one statement          |
| `BulkInsert(ctx, entities)`                 | Insert multiple records, running the create hooks |
| `Update(ctx, entity)`                       | Update all non-PK columns by primary key          |
| `Save(ctx, entity)`                         | Update only dirty columns; optimistic-lock aware  |
| `UpdateMany(ctx, values)`                   | Update multiple records matching query            |
| `UpdateManyByKey(ctx, lookup, target, map)` | Update records by matching lookup column keys     |
| `Delete(ctx)`                               | Delete records matching query                     |
| `DeleteMany(ctx)`                           | Alias for Delete                                  |
| `FirstOrCreate(ctx, attrs, values)`         | Find first or create new                          |
| `UpdateOrCreate(ctx, attrs, values)`        | Update existing or create new                     |

### Query Builder Methods

| Method                         | Description                  |
| ------------------------------ | ---------------------------- |
| `Select(columns...)`           | Specify columns to select    |
| `Distinct()`                   | Add DISTINCT to query        |
| `DistinctBy(columns...)`       | PostgreSQL DISTINCT ON       |
| `Where(query, args...)`        | Add WHERE condition          |
| `OrWhere(query, args...)`      | Add OR WHERE condition       |
| `WhereIn(column, values)`      | WHERE column IN (...)        |
| `OrWhereIn(column, values)`    | OR WHERE column IN (...)     |
| `WhereNotIn(column, values)`   | WHERE column NOT IN (...)    |
| `OrWhereNotIn(column, values)` | OR WHERE column NOT IN (...) |
| `WhereNull(column)`            | WHERE column IS NULL         |
| `WhereNotNull(column)`         | WHERE column IS NOT NULL     |
| `OrWhereNull(column)`          | OR column IS NULL            |
| `OrWhereNotNull(column)`       | OR column IS NOT NULL        |
| `WhereHas(relation, callback)` | WHERE EXISTS subquery        |
| `OrderBy(column, direction)`   | Add ORDER BY                 |
| `Latest(column?)`              | ORDER BY column DESC         |
| `Oldest(column?)`              | ORDER BY column ASC          |
| `GroupBy(columns...)`          | Add GROUP BY                 |
| `Having(query, args...)`       | Add HAVING                   |
| `Limit(n)`                     | Set LIMIT                    |
| `Offset(n)`                    | Set OFFSET                   |
| `Lock(mode)`                   | Add FOR UPDATE/SHARE         |

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

// Nested/Grouped conditions — single-predicate grouping wraps the
// callback's predicate in parentheses
zorm.New[User]().Where(func(q *zorm.Model[User]) {
    q.Where("age", ">", 18)
}).Where("active", true).Get(ctx)
// WHERE (age > $1) AND active = $2

// NULL checks
zorm.New[User]().WhereNull("deleted_at").Get(ctx)
zorm.New[User]().WhereNotNull("verified_at").Get(ctx)

// IN clause
zorm.New[User]().WhereIn("id", []any{1, 2, 3}).Get(ctx)
zorm.New[User]().WhereNotIn("status", []any{"banned", "archived"}).Get(ctx)

// OR conditions
zorm.New[User]().Where("age", ">", 18).OrWhere("verified", true).Get(ctx)
zorm.New[User]().OrWhereNotIn("status", []any{"banned", "archived"}).Get(ctx)
zorm.New[User]().OrWhereIn("id", []any{1, 2, 3}).Get(ctx)

// Raw fragment with bound arguments — any fragment containing ? is treated as
// raw SQL, with the arguments bound in order
zorm.New[User]().
    Where("id IN (SELECT user_id FROM memberships WHERE role = ?)", "admin").
    Get(ctx)
```

**Security note on the raw form.** It is an escape hatch. The fragment is checked
for SQL comments and statement separators, but that cannot stop *logical*
injection (`"active = ? OR 1=1"`). Keep the fragment a trusted constant and pass
user input as bound arguments — never concatenate it into the string.

### Input validation

Builder methods validate what you give them. Invalid input — a column name,
operator, `ORDER BY` direction, lock mode, CTE name or `HAVING` expression that
fails validation — records a build error instead of quietly changing the query.
The error is returned by the terminal call:

```go
users, err := zorm.New[User]().
    Where("name; DROP TABLE users", "John").  // rejected here
    Get(ctx)                                   // reported here

if errors.Is(err, zorm.ErrInvalidColumnName) {
    // the query never ran
}
```

Rules worth knowing:

- **Write methods refuse to run with a build error.** This matters most for
  `UpdateMany` / `UpdateManyByKey`: a dropped `WHERE` would otherwise turn a
  targeted update into a table-wide one.
- **Errors propagate out of nested scopes.** A rejected condition inside a
  `Where(func)` group or a `WithCallback` callback fails the outer query rather
  than silently dropping the group or the relation filter.
- **`OrderBy` rejects an unrecognized direction.** Earlier versions coerced
  anything that was not `ASC`/`DESC` to `DESC`, which silently ordered results the
  opposite way from the request. `"asc"`, `"ASC"`, `"desc"` and `"DESC"` are
  unaffected.

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

### CountOver (Counts per Partition)

`CountOver` counts rows per distinct value of a column using
`COUNT(*) OVER (PARTITION BY ...)`:

```go
// How many orders does each customer have, among orders over $100?
counts, err := zorm.New[Order]().
    Where("amount", ">", 100).
    CountOver(ctx, "customer_id")

for customerID, n := range counts {
    fmt.Printf("customer %s has %d orders\n", customerID, n)
}
```

Keys are normalized to strings regardless of the type the driver scanned the
column into — SQLite hands back `int64` for integers, some drivers hand back
`[]byte` for binary and UUID columns, and a `[]byte` cannot be a map key at all.
The return type is `map[string]int64` for that reason; it was `map[any]int64` in
earlier versions, and making the key type explicit turns an out-of-date lookup
(`counts[int64(5)]`) into a compile error rather than a silent zero.

`CountOver` honors JOINs, CTEs, `WHERE` and the statement cache like the other
aggregates.

### Scalar Queries (Type-Safe Single Column)

`ScalarQuery[T]` provides a type-safe query builder for fetching single-column scalar values. Unlike `Model[T]` which returns full struct records, `ScalarQuery` returns simple typed values like `[]string`, `[]int64`, `[]float64`, etc.

```go
// Example 1: Get all usernames from users table
names, err := zorm.Query[string]().
    Table("users").
    Select("name").
    Where("active", true).
    Get(ctx)
// names is []string{"Alice", "Bob", "Charlie"}

// Example 2: Get user IDs ordered by creation date
ids, err := zorm.Query[int64]().
    Table("users").
    Select("id").
    OrderBy("created_at", "DESC").
    Limit(100).
    Get(ctx)
// ids is []int64{42, 41, 40, ...}

// Example 3: Get distinct roles with count filtering
roles, err := zorm.Query[string]().
    Table("users").
    Select("role").
    Distinct().
    GroupBy("role").
    Having("COUNT(*) >", 5).
    Get(ctx)
// roles is []string{"admin", "editor"} (roles with more than 5 users)
```

`ScalarQuery` supports the same query builder methods as `Model`:
- `Where`, `OrWhere`, `WhereIn`, `WhereNotIn`, `WhereNull`, `WhereNotNull`
- `OrderBy`, `Limit`, `Offset`
- `Distinct`, `GroupBy`, `Having`
- `First` (returns single value), `Count` (returns row count)
- `SetDB`, `WithTx`, `Clone`, `Print`

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

Find-then-create is not atomic: a concurrent caller can insert the same row
between the two statements. When the INSERT loses that race with a duplicate-key
error, the lookup is retried once and the winning row is returned (or, for
`UpdateOrCreate`, updated). A duplicate-key conflict on some *other* unique
constraint — one the search attributes do not select for — is returned to you
unchanged, since retrying could not resolve it.

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

| Hook                | When Called            | `*Tx` Variant                 |
| ------------------- | ---------------------- | ----------------------------- |
| `BeforeCreate(ctx)` | Before INSERT          | `BeforeCreateTx(ctx, tx *Tx)` |
| `AfterCreate(ctx)`  | After INSERT           | `AfterCreateTx(ctx, tx *Tx)`  |
| `BeforeUpdate(ctx)` | Before UPDATE          | `BeforeUpdateTx(ctx, tx *Tx)` |
| `AfterUpdate(ctx)`  | After UPDATE           | `AfterUpdateTx(ctx, tx *Tx)`  |
| `BeforeDelete(ctx)` | Before DELETE          | `BeforeDeleteTx(ctx, tx *Tx)` |
| `AfterDelete(ctx)`  | After DELETE           | `AfterDeleteTx(ctx, tx *Tx)`  |
| `AfterFind(ctx)`    | After SELECT (per row) | —                             |

If both a plain hook and its `*Tx` variant are defined on a model, **only the `*Tx` variant fires** — they never both run.

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
// 1. created_at auto-set if column exists and field is zero
// 2. BeforeCreate(ctx) called
// 3. INSERT executed
// 4. ID populated

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

### Transactional Hooks (`*Tx` variants)

Plain hooks receive only `context.Context`, so any DB work they do runs on a separate connection from the parent INSERT/UPDATE/DELETE. If the parent SQL fails, the hook's writes are **not** rolled back.

To do hook-side DB work atomically with the parent operation, implement the `*Tx` variant instead. It receives the active `*zorm.Tx` for the running operation:

```go
type User struct {
    ID    int64
    Email string
    Name  string
}

// AfterCreateTx writes an audit row through the SAME transaction as the INSERT.
// If anything later fails and the transaction rolls back, the audit row
// disappears with the user row.
func (u *User) AfterCreateTx(ctx context.Context, tx *zorm.Tx) error {
    _, err := tx.Tx.ExecContext(ctx,
        `INSERT INTO audit_log (entity, entity_id, action) VALUES (?, ?, ?)`,
        "user", u.ID, "created",
    )
    return err
}
```

#### Auto-opened transactions

If a model implements **any** `*Tx` variant relevant to the operation, and the call is made outside an existing transaction, ZORM **auto-opens one** for that call:

```go
// No outer transaction — ZORM opens one because User has AfterCreateTx.
// The INSERT and the hook's audit-log write commit together, or roll back together.
err := zorm.New[User]().Create(ctx, &User{Email: "a@b.com", Name: "Ada"})
```

Inside an existing transaction (`Transaction(...)` + `WithTx(tx)`), no extra transaction is opened — the hook receives the outer `*Tx` so several operations can share one atomic boundary:

```go
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    if err := zorm.New[User]().WithTx(tx).Create(ctx, user); err != nil {
        return err
    }
    return zorm.New[Order]().WithTx(tx).Create(ctx, order)
})
// AfterCreateTx on both User and Order see the same *Tx.
// If either fails, both inserts and both hook side-effects roll back together.
```

#### Calling ZORM from inside a `*Tx` hook

A `*Tx` hook can call ZORM itself bound to the same transaction. This is the idiomatic way to insert a related row atomically:

```go
func (u *User) AfterCreateTx(ctx context.Context, tx *zorm.Tx) error {
    profile := &Profile{UserID: u.ID, DisplayName: u.Name}
    return zorm.New[Profile]().WithTx(tx).Create(ctx, profile)
}
```

Note: in-memory mutations to Go fields are **never** rolled back regardless of variant. Only DB writes performed through the passed `*Tx` are atomic with the parent SQL.

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
// Limit applies per parent: each user gets up to 5 published posts, not 5
// posts shared across all users. OrderBy decides which 5 each user keeps.
users, _ := zorm.New[User]().WithCallback("Posts", func(q *zorm.Model[Post]) {
    q.Where("published", true).
      OrderBy("created_at", "DESC").
      Limit(5)
}).Get(ctx)
```

A validation failure inside the callback is returned by `Get` / `Load`; the
relation is not loaded unfiltered.

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

`Sync` is a handy method for managing many-to-many relations. It synchronizes the pivot table to match exactly the IDs you provide:
- **Attaches** IDs that are in the new list but not in the database
- **Detaches** IDs that are in the database but not in the new list
- **Keeps** IDs that exist in both (no duplicate entry errors)

The read, the DELETE and the INSERT run in a single transaction, so a failing
attach cannot leave the association half-synced. When the model is already bound
to a transaction (`WithTx`), that one is used instead of opening a nested one.

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

// The value stored in the type column is the parent's morph type: its
// MorphType() method when declared, otherwise its Go struct name ("User").
func (u User) MorphType() string { return "users" }
func (p Post) MorphType() string { return "posts" }

// MorphOne: User has one Image
func (u User) AvatarRelation() zorm.MorphOne[Image] {
    return zorm.MorphOne[Image]{
        Type: "imageable_type",  // Type column
        ID:   "imageable_id",    // ID column
    }
}

// MorphMany: Post has many Images
func (p Post) ImagesRelation() zorm.MorphMany[Image] {
    return zorm.MorphMany[Image]{
        Type: "imageable_type",
        ID:   "imageable_id",
    }
}

// MorphTo: the inverse. Type/ID are column names here too, and the TypeMap
// keys are the same morph type values written into the type column.
func (i Image) ImageableRelation() zorm.MorphTo[any] {
    return zorm.MorphTo[any]{
        Type: "imageable_type",
        ID:   "imageable_id",
        TypeMap: map[string]any{
            "users": User{},
            "posts": Post{},
        },
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
- Routed to the primary when a `DBResolver` is configured
- If the rollback *itself* fails, the returned error matches both your original
  error and `errors.Is(err, zorm.ErrRollbackFailed)` — worth branching on, since
  the connection state is then unknown

---

## Error Handling

ZORM provides comprehensive error handling with categorized errors.

### Sentinel Errors

```go
import "github.com/rezakhademix/zorm"

// Query errors
zorm.ErrRecordNotFound     // No matching record
zorm.ErrRequiresRawQuery   // Exec() called without Raw()

// Model errors
zorm.ErrInvalidModel       // Invalid model type
zorm.ErrNilPointer         // Nil pointer passed
zorm.ErrNoContext          // No context provided

// Relation errors
zorm.ErrRelationNotFound   // Relation method not found
zorm.ErrInvalidRelation    // Invalid relation type
zorm.ErrInvalidConfig      // Invalid relation config (e.g. missing pivot keys)

// Constraint violations
zorm.ErrDuplicateKey       // Unique constraint violation
zorm.ErrForeignKey         // Foreign key constraint violation
zorm.ErrNotNullViolation   // NOT NULL constraint violation
zorm.ErrCheckViolation     // CHECK constraint violation

// Connection errors
zorm.ErrConnectionFailed   // Connection refused
zorm.ErrConnectionLost     // Connection lost during operation
zorm.ErrTimeout            // Operation timeout
zorm.ErrNilDatabase        // No DB configured (GlobalDB, SetDB or resolver)

// Transaction errors
zorm.ErrTransactionDeadlock    // Deadlock detected
zorm.ErrSerializationFailure  // Serialization failure
zorm.ErrOptimisticLock         // Save() detected concurrent modification
zorm.ErrSaveUntracked          // Save() called on entity not loaded from DB
zorm.ErrVersionOverflow        // version column is at its type's maximum
zorm.ErrRollbackFailed         // rollback failed after a failed transaction

// Schema errors
zorm.ErrColumnNotFound     // Column doesn't exist
zorm.ErrTableNotFound      // Table doesn't exist
zorm.ErrInvalidSyntax      // SQL syntax error
zorm.ErrInvalidColumnName  // Builder input rejected by validation
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

### Save() vs Update()

`Update(ctx, entity)` writes every non-primary column. `Save(ctx, entity)`
inspects the dirty-tracking baseline (set when the entity was loaded via
`Find` / `First` / `Get`) and emits an `UPDATE` containing only columns that
actually changed. If nothing changed, `Save` is a no-op and issues no SQL.

```go
user, _ := zorm.New[User]().Find(ctx, 1)
user.Name = "Renamed"

err := zorm.New[User]().Save(ctx, user)
// UPDATE users SET name = ?, updated_at = ? WHERE id = ?
// (only the dirty columns; "value", "email", etc. are NOT rewritten)
```

`Save` requires:

- a non-zero primary key on the entity (use `Create` to insert); and
- a dirty-tracking baseline, i.e. the entity must have been loaded via
  `Find` / `First` / `Get`. A manually-constructed entity is rejected with
  `ErrSaveUntracked` so it cannot silently rewrite columns to zero values.
  Use `Update` for full-column writes from a hand-built entity.

Hooks: `Save` fires `BeforeUpdate` / `AfterUpdate` (and the `*Tx` variants)
in the same positions as `Update` — including when the entity is clean and no
SQL is issued, so audit-logging hooks observe every `Save` call. (`BeforeUpdate`
runs before the dirty set is computed, so a hook that mutates fields turns a
clean Save into a real UPDATE.) If the row was deleted between load and save
and no version column is configured, `Save` returns `ErrRecordNotFound`.

### Optimistic Concurrency

Add a `version` tag to a numeric field to enable optimistic locking on
`Save()`. The version is checked in `WHERE` and incremented in `SET`; if a
concurrent writer already bumped it, the UPDATE matches zero rows and `Save`
returns `ErrOptimisticLock`.

```go
type Order struct {
    ID      int64  `zorm:"primaryKey"`
    Status  string
    Version int64  `zorm:"version"`  // marks the optimistic-lock column
}

a, _ := zorm.New[Order]().Find(ctx, 1)  // Version == 1
b, _ := zorm.New[Order]().Find(ctx, 1)  // Version == 1

a.Status = "paid"
_ = zorm.New[Order]().Save(ctx, a)      // OK; a.Version == 2 in DB and memory

b.Status = "cancelled"
err := zorm.New[Order]().Save(ctx, b)
if zorm.IsOptimisticLock(err) {
    // re-load, reapply change, retry
}
```

Supported version-field kinds: `int`, `int32`, `int64`, `uint`, `uint32`,
`uint64`. At most one `version` field per model. The check applies only to
`Save`; `Update` and `UpdateColumns` are left as full-write escape hatches.

### Statement Caching

See [Advanced Start → Add a statement cache](#2-add-a-statement-cache).

### Read/Write Splitting

See [Advanced Start → Add replicas](#3-add-replicas).

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

#### Parameterized Scopes

Scopes can also take arguments. Return a closure that matches the
`func(*zorm.Model[T]) *zorm.Model[T]` signature expected by `Scope(...)`:

```go
func Role(role string) func(*zorm.Model[User]) *zorm.Model[User] {
    return func(q *zorm.Model[User]) *zorm.Model[User] {
        return q.Where("role", role)
    }
}

func RegisteredBetween(from, to time.Time) func(*zorm.Model[User]) *zorm.Model[User] {
    return func(q *zorm.Model[User]) *zorm.Model[User] {
        return q.
            Where("created_at", ">=", from).
            Where("created_at", "<=", to)
    }
}

// Apply via Scope(...) just like parameter-less scopes
zorm.New[User]().
    Scope(Role("admin")).
    Scope(RegisteredBetween(from, to)).
    Get(ctx)
```

Scopes are plain functions, not methods on `Model[T]` — they cannot be
chained directly (`.Role("admin")`); pass them to `Scope(...)`.

### Query Debugging

```go
sql, args := zorm.New[User]().
    Where("age", ">", 18).
    OrderBy("name", "ASC").
    Limit(10).
    Print()

fmt.Println(sql)   // SELECT * FROM users WHERE 1=1 AND age > $1 ORDER BY name ASC LIMIT 10
fmt.Println(args)  // [18]
```

Placeholder rewriting is not dialect-dependent — `?` becomes `$N` everywhere
(SQLite accepts `$N` too), and every execution path sends exactly what `Print`
reports, including `Exec` on a `Raw` query.

The generated SQL is also stable: clauses built from a map or a struct
(`Where(map[string]any{...})`, `Where(&User{...})`, `UpdateMany(values)`) are
emitted in a fixed order rather than Go's randomized map order. `Print()` output
is therefore safe to assert on in tests, and the statement cache sees one entry
per builder chain instead of one per iteration order.

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

## zorm benchmarks

Cross-ORM benchmark suite comparing **zorm** against:

- [ent](https://github.com/ent/ent)
- [gorm](https://github.com/go-gorm/gorm)
- [sqlx](https://github.com/jmoiron/sqlx)

All four run the same workload against the same in-memory SQLite database, with
identical seed data, identical row shapes, and identical iteration semantics so
the numbers compare apples to apples (with the caveats noted below).

## The model

A portable two-table schema (`users` and `posts`) chosen to exercise the
common Go datatypes without needing a Postgres-only column type:

| Column     | Go type     | SQLite affinity     |
| ---------- | ----------- | ------------------- |
| id         | `int64`     | INTEGER PK          |
| name       | `string`    | TEXT                |
| email      | `string`    | TEXT UNIQUE         |
| age        | `int64`     | INTEGER             |
| score      | `float64`   | REAL                |
| is_active  | `bool`      | INTEGER 0/1         |
| nickname   | `*string`   | TEXT NULL           |
| avatar     | `[]byte`    | BLOB                |
| metadata   | `string`    | TEXT (JSON-as-text) |
| created_at | `time.Time` | DATETIME            |

`posts` adds a `user_id` foreign key so `User HasMany Post` /
`Post BelongsTo User` is exercised by the eager-load benchmarks.

## Benchmark

Recorded on Apple M3 Pro / darwin/arm64, SQLite `:memory:`, single connection. Raw `go test -bench=. -benchmem` output:

### Side-by-side (ns/op · B/op · allocs/op)

| Operation           | gorm (ns/op · B/op · allocs/op) | zorm (ns/op · B/op · allocs/op) | Summary                                                |
| ------------------- | ------------------------------- | ------------------------------- | ------------------------------------------------------ |
| InsertOne           | 11,147 · 6,732 · 87             | 11,637 · 4,661 · 72             | **zorm** 31% less memory, 21% fewer allocs             |
| GetByPK             | 8,320 · 5,516 · 109             | 9,120 · 4,818 · 103             | **zorm** 14.5% less memory, 6% fewer allocs            |
| UpdateOne           | 9,112 · 10,040 · 101            | 8,063 · 4,461 · 63              | **zorm** 12% faster, 56% less memory, 38% fewer allocs |
| DeleteOne           | 6,161 · 3,106 · 40              | 5,876 · 1,879 · 29              | **zorm** 5% faster, 40% less memory, 28% fewer allocs  |
| BulkInsert100       | 309,817 · 213,593 · 3,203       | 294,982 · 165,799 · 2,660       | **zorm** 5% faster, 22% less memory, 17% fewer allocs  |
| BulkInsert1000      | 3,041,683 · 1,990,998 · 31,411  | 2,842,708 · 1,567,151 · 26,363  | **zorm** 7% faster, 21% less memory, 16% fewer allocs  |
| FindWhereOrderLimit | 248,997 · 56,515 · 1,387        | 258,790 · 67,684 · 1,627        | **gorm** 4% faster, 16% less memory, 15% fewer allocs  |
| TxInsert100         | 1,596,357 · 703,940 · 9,258     | 1,582,641 · 541,958 · 7,899     | **zorm** 1% faster, 23% less memory, 15% fewer allocs  |
| EagerLoadHasMany    | 1,466,133 · 627,644 · 17,223    | 1,147,633 · 442,953 · 11,536    | **zorm** 22% faster, 29% less memory, 33% fewer allocs |
| EagerLoadBelongsTo  | 322,497 · 177,939 · 3,798       | 304,350 · 153,179 · 3,491       | **zorm** 6% faster, 14% less memory, 8% fewer allocs   |


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.
