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
- **database/sql Integration**: Uses standard connection pools and transactions; the PostgreSQL helper uses pgx
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

Requires **Go 1.25.1 or later**. PostgreSQL is the primary target; SQLite is
also used by the integration tests and examples. PostgreSQL-specific features
are identified below.

```bash
go get github.com/rezakhademix/zorm
```

Run the command inside your application's Go module. Import ZORM with:

```go
import "github.com/rezakhademix/zorm"
```

### Contents

- [Quick Start](#quick-start)
- [Advanced Start](#advanced-start)
- [API Reference](#api-reference)
- [Query Builder Details](#query-builder-details)
- [Lifecycle Hooks](#lifecycle-hooks)
- [Accessors](#accessors-computed-attributes)
- [Relationships](#relationships)
- [Transactions](#transactions)
- [Error Handling](#error-handling)
- [Advanced Features](#advanced-features)
- [Complete Example](#complete-example)
- [Runnable SQLite Example](#runnable-sqlite-example)
- [Benchmarks](#zorm-benchmarks)
- [Contributing](#contributing)

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

```go
// Connect using a DSN from application configuration
db, err := zorm.ConnectPostgres(os.Getenv("DATABASE_URL"), nil)
if err != nil {
    return err
}
defer db.Close()

zorm.SetGlobalDB(db)  // Thread-safe setter
```

#### SQLite and an existing database pool

Use `SetDB` for a query-specific pool, or `SetGlobalDB` for an application-wide
pool. SQLite requires the driver import and CGO support when using
`github.com/mattn/go-sqlite3`:

```go
import (
    "database/sql"

    _ "github.com/mattn/go-sqlite3"
    "github.com/rezakhademix/zorm"
)

// Open a file-backed SQLite database
db, err := sql.Open("sqlite3", "app.db?_foreign_keys=on")
if err != nil {
    return err
}
defer db.Close()

if err := db.PingContext(ctx); err != nil {
    return err
}

users, err := zorm.New[User]().SetDB(db).Get(ctx)
```

Create tables with SQL migrations before querying them. ZORM does not create
tables, migrate schemas, or create relationship constraints automatically.
The [runnable example](#runnable-sqlite-example) includes the required SQL.

#### Context and cancellation

Pass a request or job context to every terminal method. `WithContext` stores
context on the builder; it does not replace the explicit context argument to
`Get`, `Create`, `Save`, or another terminal method.

```go
// Bound the query to two seconds
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

users, err := zorm.New[User]().
    WithContext(ctx).
    Where("active", true).
    Get(ctx)
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

#### Field mapping and optional tags

Exported scalar fields map to snake_case columns. An exported embedded value
struct contributes its fields to the same table. Unexported fields and relation
fields are excluded from normal column mapping. Use pointers for nullable
columns and `zorm:"-"` for application-only fields.

Fields implementing `sql.Scanner` or `driver.Valuer` are database values,
including pointer scanners such as `*sql.NullString`.

```go
type Timestamps struct {
    CreatedAt time.Time
    UpdatedAt time.Time
}

type Customer struct {
    Timestamps
    ID          int64
    DisplayName string  `zorm:"column:full_name"` // Explicit column mapping
    Nickname    *string                         // NULL becomes nil
    Age         *int                            // nil differs from zero
    Avatar      []byte
    Temporary   string  `zorm:"-"`               // Never read or written
}
```

| Tag                       | Meaning                                                         |
| ------------------------- | --------------------------------------------------------------- |
| `zorm:"column:full_name"` | Map the field to `full_name`                                    |
| `zorm:"full_name"`        | Shorthand column mapping                                        |
| `zorm:"primary"`          | Mark an application-supplied primary key                        |
| `zorm:"auto"`             | Mark a field as auto-generated when it is also the primary key  |
| `zorm:"primaryKey"`       | Mark the primary key; integer fields also become auto-generated |
| `zorm:"version"`          | Enable optimistic concurrency for `Save`                        |
| `zorm:"-"`                | Exclude the field                                               |

Separate multiple options with semicolons, for example
`zorm:"column:customer_id;primary;auto"`. Tags configure mapping; they do not
create database columns or constraints. Use a single primary key; composite
primary keys are not supported by the single-record helpers.

The `PrimaryKey()` method returns a **database column name**, and that column
must map to an actual field. It overrides conventional `ID` detection and tags:

```go
type Customer struct {
    CustomerID int64 `zorm:"primary;auto"`
    Name       string
}

func (c Customer) TableName() string { return "crm_customers" }
func (c Customer) PrimaryKey() string { return "customer_id" }

// Finds crm_customers.customer_id = 42
customer, err := zorm.New[Customer]().Find(ctx, 42)
```

For keys generated by the application, assign the key before `Create` and use
`primary` without `auto`:

```go
type Document struct {
    Key   string `zorm:"column:document_key;primary"`
    Title string
}

document := &Document{Key: "guide-001", Title: "Getting Started"}
err := zorm.New[Document]().Create(ctx, document)
```

`created_at` is populated on insert only when zero. A supplied non-zero value
is preserved. Update methods protect a zero `created_at` from replacing the
stored creation time; a supplied non-zero creation time can still be written.
`updated_at` is touched by update paths when the field exists. `Create` does
not automatically initialize `updated_at`; use an explicit value or a hook
when the schema requires it.

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

#### Reading one record or a collection

`First`, `Find`, and `FindOrFail` return `ErrRecordNotFound` when no row
matches. `FindOrFail` has the same behavior as `Find`. `Get` returns no error
for an empty result; check its length. Use `OrderBy` when the first matching
record must be deterministic.

```go
// Find the oldest active user
user, err := zorm.New[User]().
    Where("active", true).
    OrderBy("id", "ASC").
    First(ctx)
if zorm.IsNotFound(err) {
    return nil  // No active user
}
if err != nil {
    return err
}
fmt.Println(user.Name)

// Find by primary key while retaining another filter
user, err = zorm.New[User]().Where("active", true).FindOrFail(ctx, 42)

// Read a bounded collection
users, err := zorm.New[User]().OrderBy("id", "ASC").Limit(50).Get(ctx)
```

#### Updating selected fields, including zero values

`Update` writes the entity's mapped non-primary columns, including `false`,
zero, empty strings, and nil pointers. A partially populated struct can
therefore overwrite values you did not intend to change. Use `UpdateColumns`
for an explicit field list, or load the entity and use `Save` for dirty fields.
These entity methods combine the primary key with any builder `Where`
conditions. Every OR branch remains subject to the primary key. A write that
matches no row returns `ErrRecordNotFound` and leaves its changes dirty.
Use `UpdateMany` to update a filtered batch without an entity.

```go
// Change one field without rewriting the rest of the row
user := &User{ID: 42, Name: "Jane"}
err := zorm.New[User]().UpdateColumns(ctx, user, "name")

// Zero values are valid updates
user.Active = false
err = zorm.New[User]().UpdateColumns(ctx, user, "active")

// Exclude columns from a full entity update
user, err = zorm.New[User]().Find(ctx, 42)
if err != nil {
    return err
}
user.Name = "Jane Updated"
err = zorm.New[User]().Exclude("email", "created_at").Update(ctx, user)
```

`UpdateColumns` also touches `updated_at` unless explicitly included in the
column list. Passing no columns is a no-op. Pass database column names to
`UpdateColumns` and `Exclude`. `Exclude` applies to `Update` and `Save`, rather
than acting as an insert-field selector.

`Exclude` also works with `Save`: excluded changes stay dirty and can be saved
later through a builder without those exclusions. Repeated calls accumulate
exclusions; use `Clone` to keep a shared base query unchanged. Unknown column
names have no effect, so use the exact mapped database names.

When every change is excluded, `Save` issues no UPDATE and leaves `updated_at`
and the managed version unchanged. Update hooks still run.

```go
// Save the changed name while leaving the email change pending
user, err := zorm.New[User]().Find(ctx, 42)
if err != nil {
    return err
}
user.Name = "Jane Updated"
user.Email = "jane.updated@example.com"
err = zorm.New[User]().Exclude("email").Save(ctx, user)
```

Exclusions prevent column writes, not in-memory changes made by update hooks
or automatic timestamp handling. `Save` still manages its optimistic-lock
version column. `UpdateColumns` uses its own explicit column list and does not
apply exclusions.

#### Scoping an entity update

`Update`, `UpdateColumns`, and `Save` combine your filters with the entity's
primary key. For a users table with a `tenant_id` column:

```go
user, err := zorm.New[User]().Find(ctx, 42)
if err != nil {
    return err
}
user.Name += " (updated)"

// Update only user 42, and only if it belongs to tenant 1 or tenant 2
query := zorm.New[User]().
    Where("tenant_id", 1).
    OrWhere("tenant_id", 2)

err = query.Update(ctx, user)
if zorm.IsNotFound(err) {
    // No row matched: the name change has not been saved
    fmt.Println(query.IsDirtyField(user, "name")) // true
    return err
}
if err != nil {
    return err
}
```

The WHERE clause is equivalent to:

```sql
WHERE (tenant_id = 1 OR tenant_id = 2) AND id = 42
```

Every OR branch remains restricted to user 42. If the row was deleted or
belongs to another tenant, the write returns `ErrRecordNotFound` and leaves
the tracked entity's changes dirty. The same filtering applies when calling
`query.UpdateColumns(ctx, user, "name")` or `query.Save(ctx, user)`.
A clean `Save` remains a no-op and does not check whether a row matches.

To update every matching user, use `UpdateMany` without an entity:

```go
// Deactivate all users in tenant 1, regardless of their primary keys
err := zorm.New[User]().
    Where("tenant_id", 1).
    UpdateMany(ctx, map[string]any{"active": false})
```

#### Deleting records

`Delete` and `DeleteMany` execute a WHERE-based delete and require at least
one condition. They do not use an entity's ID unless you add that condition.
They perform physical deletion; soft deletion is an application convention.
Combining `Raw` with a delete builder returns `ErrInvalidModel`; execute raw
SQL through `Exec` instead.

```go
// Delete one row by its primary key
err := zorm.New[User]().Where("id", 42).Delete(ctx)

// Delete a filtered group
err = zorm.New[User]().
    Where("active", false).
    Where("created_at", "<", time.Now().AddDate(-1, 0, 0)).
    DeleteMany(ctx)

// Intentionally delete every row in an archive table
err = zorm.New[User]().Table("archived_users").ForceDeleteAll(ctx)
```

`ForceDeleteAll` bypasses the missing-WHERE guard; existing conditions are
still retained. Delete methods return an error, rather than a row count.
Use raw `Exec` when you need `RowsAffected`.

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

#### Fresh inserts and atomic batches

Use `CreateManyNewPK` only when every entity has an unset auto-generated
primary key. It omits that key from the INSERT without checking each value;
it is not appropriate for application-generated string or UUID keys.

```go
// Fast path for new rows with zero auto-increment IDs
users := []*User{
    {Name: "Alice", Email: "alice@example.com"},
    {Name: "Bob", Email: "bob@example.com"},
}
err := zorm.New[User]().CreateManyNewPK(ctx, users)

// Use CreateMany when importing a mix of explicit and generated IDs
imported := []*User{
    {ID: 5000, Name: "Imported", Email: "imported@example.com"},
    {Name: "New", Email: "new@example.com"},
}
err = zorm.New[User]().CreateMany(ctx, imported)
```

`CreateMany` and `CreateManyNewPK` skip create hooks. Use `BulkInsert` for
hook-driven validation or defaults. Outside an explicit transaction, plain
`BulkInsert` can leave earlier rows inserted if a later row fails. Wrap it
when the entire operation must succeed or fail together:

```go
// Insert every row and its transactional hook side effects atomically
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    return zorm.New[User]().WithTx(tx).BulkInsert(ctx, users)
})
```

An empty insert slice is a no-op; nil entities are rejected. Insert methods
populate primary keys in memory, but only `Create` records a dirty-tracking
baseline immediately. Reload batch-created entities before using `Save`.
Rollback does not restore IDs or other fields already changed in Go memory.

#### Updating a filtered batch

`UpdateMany` sets the same values on all matching rows, including zero and
nil values. `UpdateManyByKey` assigns a different value per lookup key.
Neither method runs per-entity update hooks or performs `Save`'s optimistic
version check. Unlike `Delete`, `UpdateMany` allows an empty WHERE; add the
conditions that define the intended batch.

`UpdateManyByKey` preserves supplied timestamps when `updated_at` is the target
column. Updates to other columns still touch `updated_at` automatically.

```go
// Deactivate all users in one department
err := zorm.New[User]().
    Where("department_id", 12).
    UpdateMany(ctx, map[string]any{
        "active": false,
        "nickname": nil,
    })

// Change different names while restricting the allowed rows
names := map[int64]string{1: "Alice", 2: "Bob"}
err = zorm.New[User]().
    Where("department_id", 12).
    UpdateManyByKey(ctx, "id", "name", names)
```

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

For a pool opened elsewhere, configure it without reconnecting. Inspect
`database/sql` statistics to see whether callers are waiting for connections:

```go
// Tune an existing pool
zorm.ConfigureConnectionPool(db, 25, 5, time.Hour, 30*time.Minute)

// Monitor pool pressure using the standard database handle
stats := db.Stats()
fmt.Println(stats.OpenConnections, stats.InUse, stats.Idle)
fmt.Println(stats.WaitCount, stats.WaitDuration)
```

These limits apply to each pool separately. Budget the combined connections
across application instances and replicas against the database's capacity.

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

Closing a tracking scope makes its entities untracked, so finish pending
`Save` calls before cleanup. `ConfigureDirtyTracking` replaces the global
tracker and clears existing baselines; configure it at startup.

```go
// Inspect the scope before releasing its baselines
fmt.Println(scope.Len())
scope.Close()  // Safe to call again when the deferred cleanup runs

// Release one entity without clearing tracking for other requests
zorm.ClearOriginals(user)
```

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

### Additional builder and execution methods

| Method                                                                           | Purpose                                               | Example section                                                    |
| -------------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------ |
| `CreateManyNewPK(ctx, entities)`                                                 | Insert rows known to have unset auto-generated keys   | [Fresh inserts](#fresh-inserts-and-atomic-batches)                 |
| `UpdateColumns(ctx, entity, columns...)`                                         | Write an explicit column list by primary key          | [Selected fields](#updating-selected-fields-including-zero-values) |
| `Exclude(columns...)`                                                            | Exclude columns from `Update` or `Save`               | [Dirty tracking](#inspecting-dirty-fields-and-pending-changes)     |
| `ForceDeleteAll(ctx)`                                                            | Allow a delete without WHERE                          | [Deleting records](#deleting-records)                              |
| `Cursor(ctx)`                                                                    | Stream mapped rows                                    | [Cursor](#cursor-memory-efficient-iteration)                       |
| `Chunk(ctx, size, callback)`                                                     | Process batches using LIMIT/OFFSET                    | [Chunking](#chunking-large-datasets)                               |
| `Paginate(ctx, page, perPage)`                                                   | Return data and total-count metadata                  | [Pagination](#pagination)                                          |
| `SimplePaginate(ctx, page, perPage)`                                             | Return data without a count query                     | [Pagination](#pagination)                                          |
| `Join`, `LeftJoin`, `RightJoin`, `CrossJoin`                                     | Build SQL joins                                       | [Joins](#joins-and-result-structs)                                 |
| `GroupByRollup`, `GroupByCube`, `GroupByGroupingSets`                            | Build subtotal and grand-total reports                | [Advanced grouping](#advanced-grouping)                            |
| `WithCTE(name, query)`                                                           | Add a named SQL subquery                              | [CTEs](#common-table-expressions-ctes)                             |
| `WhereFullText`, `WhereFullTextWithConfig`, `WhereTsVector`, `WherePhraseSearch` | Search PostgreSQL text                                | [Full-text search](#full-text-search-postgresql)                   |
| `UsePrimary()`, `UseReplica(index)`                                              | Override read routing                                 | [Replicas](#3-add-replicas)                                        |
| `WithTrackingScope(scope)`                                                       | Release dirty baselines together                      | [Memory control](#5-allocation-and-memory-control)                 |
| `Transaction(ctx, callback)`                                                     | Run operations on the model's selected write database | [Transactions](#transactions)                                      |
| `GetBuildErr()`                                                                  | Inspect validation before execution                   | [Validation](#checking-dynamic-query-input)                        |
| `GetWheres()`, `GetArgs()`, `GetOrderBys()`, `GetLimit()`                        | Inspect builder state                                 | [Debugging](#query-debugging)                                      |

### Relationship methods

| Method                                          | Purpose                                                            |
| ----------------------------------------------- | ------------------------------------------------------------------ |
| `With(relations...)`                            | Eager-load related entities, including dot-separated paths         |
| `WithCallback(relation, callback)`              | Eager-load a relation with a typed query constraint                |
| `WithMorph(relation, typeMap)`                  | Eager-load polymorphic targets with type-specific nested relations |
| `WhereHas(relation, callback)`                  | Filter parents using related-row existence                         |
| `Load(ctx, entity, relations...)`               | Load relations for an existing entity                              |
| `LoadSlice(ctx, entities, relations...)`        | Load relations for an existing slice                               |
| `LoadMorph(ctx, entities, relation, typeMap)`   | Load polymorphic targets on an existing slice                      |
| `Attach(ctx, entity, relation, ids, pivotData)` | Add pivot associations                                             |
| `Detach(ctx, entity, relation, ids)`            | Remove some or all pivot associations                              |
| `Sync(ctx, entity, relation, ids, pivotData)`   | Make pivot associations match the requested IDs                    |

---

## Query Builder Details

### Selecting columns and distinct rows

`Select` returns a partial entity: unselected fields keep their zero values.
Select the primary key when the result will be updated, and include the key
columns needed to match eager-loaded relations. Use `UpdateColumns` for
partial entities to avoid writing unselected fields back to the database.

```go
// Fetch only the fields needed for a user list
users, err := zorm.New[User]().
    Select("id", "name", "email").
    Where("active", true).
    Get(ctx)

// Return unique values as a typed slice
names, err := zorm.Query[string]().
    Table("users").Select("name").Distinct().OrderBy("name", "ASC").Get(ctx)

// PostgreSQL: latest order for each customer
orders, err := zorm.New[Order]().
    DistinctBy("customer_id").
    OrderBy("customer_id", "ASC").
    OrderBy("created_at", "DESC").
    OrderBy("id", "DESC").
    Get(ctx)
```

`Distinct()` applies to all selected columns together. `DistinctBy` emits
PostgreSQL `DISTINCT ON`; start `OrderBy` with the distinct columns and follow
with the columns that decide which row to keep.

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

#### Grouped OR conditions, ranges, and zero values

Group alternatives explicitly when combining them with mandatory filters.
Struct conditions skip zero-valued fields; map conditions and explicit
column/value calls let you filter for `false`, `0`, and empty strings.

```go
// Active users who are either administrators or editors
users, err := zorm.New[User]().
    Where("active", true).
    Where(func(q *zorm.Model[User]) {
        q.Where("role", "admin").
        OrWhere("role", "editor")
    }).
    Get(ctx)

// Inclusive lower bound, exclusive upper bound
users, err = zorm.New[User]().
    Where("created_at", ">=", from).
    Where("created_at", "<", to).
    Get(ctx)

// Include zero-valued conditions explicitly
users, err = zorm.New[User]().Where(map[string]any{
    "active": false,
    "age": 0,
}).Get(ctx)

// PostgreSQL: case-insensitive pattern matching
users, err = zorm.New[User]().Where("name", "ILIKE", "%alice%").Get(ctx)
```

Use `WhereNull` / `WhereNotNull` for SQL NULL predicates. Ordinary
`Where("column", nil)` is not a replacement for `IS NULL`. `LIKE` and
`ILIKE` treat `%` and `_` as pattern characters even when the value is bound.
Express ranges with two comparisons; `BETWEEN` and `IN` are not supported
as the operator in the three-argument `Where` form.

#### IN lists and NULL alternatives

The IN helpers accept `[]any`. Convert an existing typed slice explicitly:

```go
// Convert IDs without mixing their concrete types
values := []any{10, 20, 30}
users, err := zorm.New[User]().WhereIn("id", values).Get(ctx)

// Users with no verification time, or a verification time before the cutoff
users, err = zorm.New[User]().Where(func(q *zorm.Model[User]) {
    q.WhereNull("verified_at").OrWhere("verified_at", "<", cutoff)
}).Get(ctx)

// Users with a verification time or an alternative contact address
users, err = zorm.New[User]().
    WhereNotNull("verified_at").
    OrWhereNotNull("alternate_email").
    Get(ctx)

// Include unassigned users alongside members of a selected group
users, err = zorm.New[User]().Where(func(q *zorm.Model[User]) {
    q.WhereIn("department_id", []any{int64(12), int64(15)}).
      OrWhereNull("department_id")
}).Get(ctx)
```

An empty IN list matches nothing; an empty NOT IN list matches everything.
PostgreSQL homogeneous supported scalar values use one array parameter with
`ANY` / `ALL`. SQLite and mixed-type lists use individual placeholders and
remain subject to database parameter limits. Use a separate NULL predicate
when the desired result includes NULLs; SQL `NOT IN` with NULL values has
three-valued logic.

### Ordering and limiting results

`OrderBy` appends sort columns; `Limit` and `Offset` set their current values.
`Latest()` / `Oldest()` default to `created_at`, or accept one column name.
Add a unique tie-breaker when sorting by a non-unique field.

```go
// Most recent users, with stable ordering for identical timestamps
users, err := zorm.New[User]().
    Latest().
    OrderBy("id", "DESC").
    Limit(20).
    Get(ctx)

// Oldest users by ID
users, err = zorm.New[User]().Oldest("id").Limit(20).Get(ctx)

// Skip the first twenty names
users, err = zorm.New[User]().
    OrderBy("name", "ASC").OrderBy("id", "ASC").
    Limit(20).Offset(20).Get(ctx)
```

### Joins and result structs

Join conditions compare column names; WHERE values remain bound arguments.
Qualify ambiguous columns and alias joined results to fields on a result
struct. A join returns flat rows; use `With` to fill relation fields instead.

```go
type UserPostRow struct {
    ID        int64
    Name      *string  // A RIGHT JOIN may produce NULL
    PostTitle *string  // A LEFT JOIN may produce NULL
}

// Return users even when they have no posts
rows, err := zorm.New[UserPostRow]().
    Table("users").
    Select("users.id", "users.name", "posts.title AS post_title").
    LeftJoin("posts", "users.id", "=", "posts.user_id").
    OrderBy("users.id", "ASC").
    Get(ctx)

// INNER JOIN: users with at least one published post
users, err := zorm.New[User]().
    Select("users.*").
    Join("posts", "users.id", "=", "posts.user_id").
    Where("posts.published", true).
    Distinct().
    Get(ctx)

// RIGHT JOIN: retain every post, including those without a matching user
rows, err = zorm.New[UserPostRow]().
    Table("users").
    Select("posts.id AS id", "users.name", "posts.title AS post_title").
    RightJoin("posts", "users.id", "=", "posts.user_id").
    Get(ctx)
```

Make nullable fields on the non-preserved side of an outer join pointers
when NULL must be distinguished from a zero value. `RightJoin` requires
database support. `CrossJoin("other_table")` produces every combination:

```go
type ProductRegion struct {
    ID     int64
    Region string
}

// All product and region combinations
rows, err := zorm.New[ProductRegion]().
    Table("products").
    Select("products.id", "regions.name AS region").
    CrossJoin("regions").Get(ctx)
```

### Count, Sum, Avg, and grouped reports

`Count` returns `int64`; `Sum` and `Avg` return `float64` and return zero for
an empty aggregate. For exact decimal or financial arithmetic, select into an
appropriate decimal representation with SQL and a compatible scanner.

```go
// Count matching users
count, err := zorm.New[User]().Where("active", true).Count(ctx)

// Aggregate matching order amounts
total, err := zorm.New[Order]().Where("status", "paid").Sum(ctx, "amount")
average, err := zorm.New[Order]().Where("status", "paid").Avg(ctx, "amount")

// Count distinct names
count, err = zorm.New[User]().Select("name").Distinct().Count(ctx)
```

Use `GroupBy` with selected aggregate expressions to return several metrics
per group. Aliases must match the mapped result-field names:

```go
type CustomerSales struct {
    CustomerID int64
    OrderCount int64
    Total      float64
}

// Customers with at least five paid orders and more than 1000 in sales
sales, err := zorm.New[CustomerSales]().
    Table("orders").
    Select("customer_id", "COUNT(*) AS order_count", "SUM(amount) AS total").
    Where("status", "paid").
    GroupBy("customer_id").
    Having("COUNT(*) >= ?", 5).
    Having("SUM(amount) > ?", 1000).
    OrderBy("total", "DESC").
    Get(ctx)
```

Repeated `Having` calls are joined with AND. `Count` on a grouped query
counts the resulting groups; use a projected aggregate to obtain the count
inside each group.

### Raw SQL and affected rows

Use `Raw(...).Get` / `First` for queries returning mapped rows and
`Raw(...).Exec` for statements returning a `sql.Result`. Raw SQL supplies the
query itself: builder `Where`, `Select`, `OrderBy`, and `Limit` are not
appended to it. Put the required clauses and placeholders in the SQL string.

```go
// Read a custom projection
users, err := zorm.New[User]().Raw(
    "SELECT id, name, email FROM users WHERE age >= ? ORDER BY id LIMIT ?",
    18, 20,
).Get(ctx)

// Execute a database-side arithmetic update
result, err := zorm.New[Order]().Raw(
    "UPDATE orders SET amount = amount + ? WHERE id = ?",
    10, 42,
).Exec(ctx)
if err != nil {
    return err
}
affected, err := result.RowsAffected()
if err != nil {
    return err
}
fmt.Println(affected)

// PostgreSQL JSON key operator: ?? emits a literal ?
users, err = zorm.New[User]().Raw(
    "SELECT * FROM users WHERE metadata ?? ?", "newsletter",
).Get(ctx)
```

ZORM rewrites `?` bind markers to `$1`, `$2`, and so on. Use one placeholder
style per query. Raw statements do not run write hooks, manage timestamps,
apply optimistic locking, or refresh existing entity baselines. `Exec`
routes to the primary and requires `Raw`; it is not a general SELECT terminal.
Keep SQL text trusted and bind values separately. Identifier validation does
not replace an application allowlist for user-selectable columns or tables.

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

Column/value `Where` forms accept a single identifier, optionally qualified
with a table or schema name, and an optional recognized trailing operator.

```go
zorm.New[User]().Where("name", "Alice")           // name = ?
zorm.New[User]().Where("users.name", "Alice")     // users.name = ?
zorm.New[User]().Where("public.users.id", 42)     // public.users.id = ?
zorm.New[User]().Where("age", ">", 18)            // age > ?
zorm.New[User]().Where("age >", 18)               // age > ?

// Expressions use trusted SQL text with explicit placeholders
zorm.New[User]().Where("LOWER(name) = ?", "alice")

// An expression is not a column identifier
query := zorm.New[User]().Where("LOWER(name)", "alice")
_, err := query.Get(ctx)
// errors.Is(err, zorm.ErrInvalidColumnName) == true; the query never ran
```

Keep expression text fixed and pass user input as bound arguments.

Map-based assignment, such as `UpdateOrCreate`, rejects numeric range and
signedness overflow before writing. For example:

| Map value        | Target struct field type | Result                                         |
| ---------------- | ------------------------ | ---------------------------------------------- |
| `int64(300)`     | `int8`                   | Error; does not wrap to `44`                   |
| `int64(-1)`      | `uint64`                 | Error; does not wrap to a large positive value |
| `int64(127)`     | `int8`                   | Accepted as `127`                              |
| `float64(10.9)`  | `int`                    | Accepted as `10`                               |
| `float64(-10.9)` | `int`                    | Accepted as `-10`                              |

Valid float-to-integer conversions truncate toward zero: they remove the
fractional part without rounding. If `User.Age` is an `int`:

```go
user, err := zorm.New[User]().UpdateOrCreate(ctx,
    map[string]any{"id": 42},
    map[string]any{"age": float64(10.9)},
)
if err != nil {
    return err
}
fmt.Println(user.Age) // 10
```

Rules worth knowing:

- **Write methods refuse to run with a build error.** This matters most for
  `UpdateMany` / `UpdateManyByKey`: a dropped `WHERE` would otherwise turn a
  targeted update into a table-wide one.
- **Unsupported WHERE condition types produce a build error.** This includes
  nil conditions; use one of the documented string, map, struct, or callback forms.
- **Updates and deletes reject unsupported query state.** `Raw`, JOINs,
  positive limits or offsets, grouping, and `HAVING` return `ErrInvalidModel`
  instead of being discarded. Use `Raw(...).Exec(ctx)` for write SQL that needs
  those clauses.
- **Errors propagate out of nested scopes.** A rejected condition inside a
  `Where(func)` group, a `WhereHas` / `WithCallback` callback, or a CTE builder
  fails the outer query rather than silently dropping its filter. CTE builders
  are checked again at execution time, including edits made after attachment.
- **`OrderBy` rejects an unrecognized direction.** Earlier versions coerced
  anything that was not `ASC`/`DESC` to `DESC`, which silently ordered results the
  opposite way from the request. `"asc"`, `"ASC"`, `"desc"` and `"DESC"` are
  unaffected.

#### Checking dynamic query input

Read a build error before executing when constructing queries from application
options. Validation checks SQL syntax characters, not whether a client is
allowed to select a particular column; translate external names through an
allowlist first.

```go
// Map public sort options to known database columns
columns := map[string]string{"name": "name", "joined": "created_at"}
column, ok := columns[requestedSort]
if !ok {
    return errors.New("unsupported sort field")
}

query := zorm.New[User]().OrderBy(column, "ASC")
if err := query.GetBuildErr(); err != nil {
    return err
}
users, err := query.Get(ctx)
```

`ValidateColumnName` returns an error for a rejected identifier;
`MustValidateColumnName` panics and is intended for programmer-controlled
configuration. `ValidateRawQuery` checks a SQL fragment, not authorization
or the safety of interpolated user values.

### Exists Check

```go
// Check if any matching record exists (efficient - uses SELECT 1 LIMIT 1)
exists, err := zorm.New[User]().Where("email", "john@example.com").Exists(ctx)
if exists {
    fmt.Println("User exists!")
}
```

Check `err` before interpreting the boolean. An existence check is useful for
display decisions, but it does not reserve a row or prevent another caller
from inserting it. Enforce uniqueness with a database constraint.

```go
// Check for paid orders without fetching the order records
exists, err := zorm.New[Order]().
    Where("customer_id", 42).
    Where("status", "paid").
    Exists(ctx)
if err != nil {
    return err
}
fmt.Println("Has paid orders:", exists)
```

### Pluck (Single Column)

```go
// Get just the email column from all users
emails, err := zorm.New[User]().Where("active", true).Pluck(ctx, "email")
for _, email := range emails {
    fmt.Println(email)
}
```

`Pluck` returns `[]any`, with concrete values supplied by the SQL driver.
Use `Query[string]` or another scalar type when a typed result is required.

```go
// Fetch unique email values in a stable order
emails, err := zorm.New[User]().
    WhereNotNull("email").
    Distinct().
    OrderBy("email", "ASC").
    Pluck(ctx, "email")
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

```go
// Look up a partition using its string key
counts, err := zorm.New[Order]().CountOver(ctx, "customer_id")
if err != nil {
    return err
}
fmt.Println(counts["42"])  // Zero if that key is absent
```

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

Scalar `Count` ignores pagination and ordering, preserves distinct projections,
and counts groups that satisfy `HAVING`. Without grouping, `HAVING` filters the
aggregate row count, returning zero when its condition fails. Counting leaves
the original builder unchanged.

Always supply a table and select a single column compatible with `T`.
Scalar queries do not load model relations, run model hooks, or track dirty
fields. `First` returns `ErrRecordNotFound` when there is no row.

```go
// Example 4: Fetch one nullable scalar
nickname, err := zorm.Query[*string]().
    Table("users").
    Select("nickname").
    Where("id", 42).
    First(ctx)
if err != nil {
    return err
}
if nickname != nil {
    fmt.Println(*nickname)
}

// Example 5: Reuse a scalar query with independent filters
base := zorm.Query[int64]().Table("users").Select("id")

activeIDs, err := base.Clone().Where("active", true).Get(ctx)
inactiveCount, err := base.Clone().Where("active", false).Count(ctx)

// Example 6: Read within an existing transaction
err = zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    ids, err := zorm.Query[int64]().WithTx(tx).
        Table("users").
        Select("id").
        Where("active", true).
        Get(ctx)
    if err != nil {
        return err
    }
    fmt.Println(ids)
    return nil
})
```

`ScalarQuery.WithTx` changes that scalar builder; use `Clone().WithTx(tx)`
if you need to reuse the original outside the transaction. Use explicit
placeholders for HAVING expressions, for example `Having("COUNT(*) > ?", 5)`.

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
if err := cursor.Err(); err != nil {
    return err
}
```

Check `cursor.Err()` after iteration: cancellation, timeouts, and driver errors
also make `Next()` return false. Normal exhaustion returns a nil error.

`Cursor` supports `AfterFind`, accessors, and dirty tracking, but rejects
`With`, `WithCallback`, and `WithMorph`. Use `Get` or `Chunk` for eager-loaded
results. A cursor holds a database connection until closed or exhausted;
performing another query inside the loop or its `AfterFind` hook may wait
indefinitely on a pool with only one connection. Buffered reads through `Get`,
`First`, `Find`, and relation loading close their result sets before running
`AfterFind` hooks.

```go
// Stream only the columns needed for an export
cursor, err := zorm.New[User]().
    Select("id", "email").
    OrderBy("id", "ASC").
    Cursor(ctx)
if err != nil {
    return err
}
defer cursor.Close()

for cursor.Next() {
    user, err := cursor.Scan(ctx)
    if err != nil {
        return err
    }

    fmt.Printf("%d,%s\n", user.ID, user.Email)

    zorm.ClearOriginals(user)  // Export does not need a Save baseline
}
return cursor.Err()
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

Both the initial lookup and duplicate-key recovery read from the primary when
a resolver is configured, so replication lag cannot decide these writes.

Use search attributes backed by a database unique constraint. `values` are
defaults only for `FirstOrCreate`; an existing row is returned unchanged.
`UpdateOrCreate` applies only the columns in `values` (plus the automatic
`updated_at` timestamp) when the row already exists. Supply
explicit values for required columns on the creation path.

For an existing row, `UpdateOrCreate` rejects a primary-key change with
`ErrInvalidModel`. Repeating the same key after type conversion is allowed.

```go
// Preserve an existing name; set it only when the user is first created
user, err := zorm.New[User]().FirstOrCreate(ctx,
    map[string]any{"email": "alice@example.com"},
    map[string]any{"name": "Alice", "active": true},
)
if err != nil {
    return err
}

// Apply the latest profile values whether the row exists or is new
user, err = zorm.New[User]().UpdateOrCreate(ctx,
    map[string]any{"email": "alice@example.com"},
    map[string]any{"name": "Alice Updated", "active": false},
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

Both methods normalize `page < 1` to `1` and `perPage < 1` to `15`.
`SimplePaginate` also sets `LastPage` to `-1`; it does not fetch an extra row
or supply a `HasMore` field. Decide whether to request another page using
your application's policy, such as whether a full page was returned.
Neither method adds ordering automatically.

```go
// Stable pages with a filter and eager-loaded relations
result, err := zorm.New[User]().
    Where("active", true).
    With("Profile").
    OrderBy("created_at", "DESC").
    OrderBy("id", "DESC").
    Paginate(ctx, page, 25)
if err != nil {
    return err
}
fmt.Printf("Page %d of %d\n", result.CurrentPage, result.LastPage)

// Keyset pagination for a large feed ordered by an increasing ID
users, err := zorm.New[User]().
    Where("id", ">", lastSeenID).
    OrderBy("id", "ASC").
    Limit(25).
    Get(ctx)
```

Pagination uses LIMIT/OFFSET, so concurrent inserts or deletes can change
page membership. The keyset recipe avoids increasing offsets but does not
return total-count metadata.

### Clone (Reuse Queries Safely)

```go
baseQuery := zorm.New[User]().Where("active", true)

// Clone prevents modifying original
admins, _ := baseQuery.Clone().Where("role", "admin").Get(ctx)
users, _ := baseQuery.Clone().Limit(10).Get(ctx)

// Original is unchanged
all, _ := baseQuery.Get(ctx)
```

Builders are mutable. Build a common base once, then clone it before adding
request-specific filters. Do not modify the same builder concurrently.
`Clone` copies query configuration, while connection, statement-cache, and
tracking-scope handles remain shared. Treat objects supplied as argument
values as immutable while a query uses them.

```go
// Independent query branches
base := zorm.New[User]().Where("department_id", 12)

active := base.Clone().Where("active", true)
inactive := base.Clone().Where("active", false)

activeUsers, err := active.Get(ctx)
inactiveUsers, err := inactive.Get(ctx)
```

### Custom Table Name

```go
// Override table name for this query
users, _ := zorm.New[User]().Table("archived_users").Get(ctx)
```

`Table` changes the table for that builder without changing its field mapping.
The destination table must have compatible columns. `TableName` reports the
current table override or the model's configured table name.

```go
// Keep table selection under application control
query := zorm.New[User]().Table("archived_users")
fmt.Println(query.TableName())  // archived_users
users, err := query.Where("active", false).Get(ctx)
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

#### Read and delete hooks

`AfterFind` runs per loaded row before accessors are evaluated. It also runs
for eager-loaded relations and cursor results. Return an error to stop the
read operation. Keep relation and accessor dependencies in mind when selecting
only part of an entity.

```go
// Normalize a field after loading
func (u *User) AfterFind(ctx context.Context) error {
    u.Name = strings.TrimSpace(u.Name)

    return nil
}

// Delete hooks run once for the statement on a zero-value User
func (u *User) BeforeDelete(ctx context.Context) error {
    log.Println("Deleting matching users")

    return nil
}

func (u *User) AfterDelete(ctx context.Context) error {
    log.Println("User delete completed")

    return nil
}
```

Delete hooks do not receive the matching rows or the WHERE arguments, so
`u.ID` is not the ID of each deleted user. Batch update methods and raw SQL
do not invoke entity update hooks; `CreateMany` skips create hooks.

#### Driver placeholders inside hooks

The earlier `AfterCreateTx` audit example calls `tx.Tx.ExecContext` directly.
That call goes to `database/sql`, bypassing ZORM's placeholder rewriting.
On PostgreSQL, write `$1`, `$2`, and `$3` in direct driver calls:

```go
// PostgreSQL audit write using the transaction's native SQL handle
func (u *User) AfterCreateTx(ctx context.Context, tx *zorm.Tx) error {
    _, err := tx.Tx.ExecContext(ctx,
        "INSERT INTO audit_log (entity, entity_id, action) VALUES ($1, $2, $3)",
        "user", u.ID, "created",
    )

    return err
}
```

Alternatively, use a ZORM model with `WithTx(tx)` as in the profile example.
Plain after-hook errors can arrive after the SQL has committed. An explicit
transaction or a `*Tx` hook makes database writes through that same transaction
rollback together; external notifications and in-memory changes are not undone.

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

Accessor methods take no arguments and return exactly one value. Their
names become snake_case keys in `Attributes`; the attributes are not SQL
columns and cannot be used in `Where` or `OrderBy`. They are recomputed on
reads, rather than automatically after each Go field assignment.

The initials example assumes non-empty ASCII names. For optional names or
Unicode text, use a rune-aware implementation:

```go
// Accessor: GetInitials -> attributes["initials"]
func (u *User) GetInitials() string {
    var initials []rune

    for _, name := range []string{u.FirstName, u.LastName} {
        letters := []rune(strings.TrimSpace(name))
        if len(letters) > 0 {
            initials = append(initials, letters[0])
        }
    }

    return string(initials)
}
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

### Relation fields and key selection

Use exported pointer fields for a single related entity and exported slices
of pointers for collections. Prefer **value receivers** for relation methods
as shown above; eager loaders and pivot helpers discover those methods on
the value type. A field and a method cannot share a Go name, so use the
`Relation` suffix when the struct already has the destination field.

| Relation              | Destination field  | Key configuration                                                 |
| --------------------- | ------------------ | ----------------------------------------------------------------- |
| `HasOne[Profile]`     | `Profile *Profile` | Related `ForeignKey` matches parent `LocalKey`                    |
| `HasMany[Post]`       | `Posts []*Post`    | Related `ForeignKey` matches parent `LocalKey`                    |
| `BelongsTo[User]`     | `Author *User`     | Parent `ForeignKey` matches related `OwnerKey`                    |
| `BelongsToMany[Role]` | `Roles []*Role`    | Pivot `ForeignKey` / `RelatedKey` connect parent and related keys |
| `MorphOne[Image]`     | `Avatar *Image`    | Related type and ID columns identify the parent                   |
| `MorphMany[Image]`    | `Images []*Image`  | Related type and ID columns identify the parent                   |
| `MorphTo[any]`        | `Imageable any`    | TypeMap selects the concrete parent model                         |

Use explicit key configuration when joining through business keys instead
of IDs. `LocalKey`, `ForeignKey`, and `OwnerKey` are database column names:

```go
type Customer struct {
    ID     int64
    Code   string
    Orders []*Order
}

type Order struct {
    ID           int64
    CustomerCode string
    Customer     *Customer
}

// Match orders.customer_code to customers.code
func (c Customer) OrdersRelation() zorm.HasMany[Order] {
    return zorm.HasMany[Order]{ForeignKey: "customer_code", LocalKey: "code"}
}

// Resolve the inverse relationship through the same business key
func (o Order) CustomerRelation() zorm.BelongsTo[Customer] {
    return zorm.BelongsTo[Customer]{ForeignKey: "customer_code", OwnerKey: "code"}
}

customers, err := zorm.New[Customer]().With("Orders").Get(ctx)
orders, err := zorm.New[Order]().With("Customer").Get(ctx)
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

`Load` and `LoadSlice` replace previous relation values. A missing match or
cleared foreign key leaves an empty relation rather than retaining old data.

Eager loading queries related rows separately; it does not flatten all rows
with SQL JOINs. Select the parent matching keys and related foreign/owner
keys when restricting columns. `WithCallback` filters the loaded children,
rather than filtering out parents without a matching child.

```go
// Load names and their published post titles while retaining matching keys
users, err := zorm.New[User]().
    Select("id", "name").
    With("Posts:id,user_id,title").
    WithCallback("Posts", func(q *zorm.Model[Post]) {
        q.Where("published", true).
        OrderBy("id", "DESC").
        Limit(3)
    }).
    Get(ctx)
```

Use `With("Posts:id,user_id,title")` for relation column selection; retain
the matching keys. The comma-separated column list is trusted SQL text, so
keep it application-controlled. `WithCallback` applies WHERE, ordering, and
limit constraints; it does not transfer a callback's `Select`, `Offset`,
joins, or grouping into the relation query.

Use the exact exported field name in relation paths, such as `Posts.Author`.
Nested paths require each intermediate model to define the corresponding
field and relation method. `HasMany` callback limits are applied
per parent; ordering decides the selected children.

### Filtering parents with WhereHas

Use `WhereHas` for a direct `HasOne` / `HasMany` relation whose related
foreign key points to the parent's primary key:

```go
// Return only users who have at least one published post
users, err := zorm.New[User]().
    WhereHas("Posts", func(q *zorm.Model[Post]) {
        q.Where("published", true)
    }).
    WithCallback("Posts", func(q *zorm.Model[Post]) {
        q.Where("published", true)
    }).Get(ctx)

// Test existence without additional child conditions
users, err = zorm.New[User]().WhereHas("Posts", nil).Get(ctx)
```

The callback contributes WHERE predicates to an EXISTS subquery; it is not
a general child SELECT builder. This helper does not implement pivot-table
or polymorphic filtering, dot-separated relation paths, or custom LocalKey
matching. A missing relation method currently leaves the query unchanged,
so use known relation names and keep predicate columns and callback types
under application control. Use a bound raw EXISTS fragment for other shapes:

```go
// Filter through a many-to-many pivot table explicitly
users, err := zorm.New[User]().Where(
    "EXISTS (SELECT 1 FROM role_user WHERE role_user.user_id = users.id AND role_user.role_id = ?)",
    3,
).Get(ctx)
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

Keep the matching key populated on existing entities. Loading relations does
not save the parent or create missing children. Use `LoadSlice` rather than
one `Load` call per entity when you already have a collection.

```go
// Load constrained posts on a previously fetched user
user, err := zorm.New[User]().Find(ctx, 42)
if err != nil {
    return err
}
err = zorm.New[User]().WithCallback("Posts", func(q *zorm.Model[Post]) {
    q.Where("published", true).
    OrderBy("id", "DESC")
}).
Load(ctx, user, "Posts")
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

The related table and pivot table must already exist. Define foreign keys and
a unique constraint on `(user_id, role_id)` in your migration. A minimal
PostgreSQL pivot table for the preceding example is:

```sql
CREATE TABLE role_user (
    user_id BIGINT NOT NULL REFERENCES users(id),
    role_id BIGINT NOT NULL REFERENCES roles(id),
    assigned_at TIMESTAMPTZ,
    assigned_by BIGINT,
    PRIMARY KEY (user_id, role_id)
);
```

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

`Sync` uses pivot data for newly attached IDs; it does not update extra
columns on associations that are kept. Keys in `pivotData` must use the same
concrete Go type as the IDs being attached, for example `int64(3)` in both.
`Attach` inserts associations and may report a duplicate-key error for one
already present. An empty Attach list is a no-op; nil or empty Detach and
Sync lists remove all associations for that parent.

```go
// Assign roles with consistently typed IDs and pivot-data keys
ids := []any{int64(3), int64(4)}
pivotData := map[any]map[string]any{
    int64(3): {"assigned_at": time.Now(), "assigned_by": int64(1)},
    int64(4): {"assigned_at": time.Now(), "assigned_by": int64(1)},
}
err := zorm.New[User]().Sync(ctx, user, "Roles", ids, pivotData)
if err != nil {
    return err
}

// Refresh the in-memory collection after changing the pivot table
err = zorm.New[User]().Load(ctx, user, "Roles")
```

Pivot writes do not automatically mutate `user.Roles`. The `LocalKey` and
`RelatedPK` options on `BelongsToMany` configure eager-loading keys; pivot
write helpers identify the parent by its primary key.

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

Declare destination fields on the models in the polymorphic example:
`User.Avatar *Image`, `Post.Images []*Image`, and `Image.Imageable any`.
The type column stores the exact `MorphType()` value, not automatically a
table name. Use stable values consistently on both sides of the relation.

```go
// Store a polymorphic child explicitly
image := &Image{
    URL: "https://example.com/avatar.png",
    ImageableType: "users",
    ImageableID: user.ID,
}
err := zorm.New[Image]().Create(ctx, image)

// Load a parent's polymorphic collection
posts, err := zorm.New[Post]().With("Images").Get(ctx)

// Load polymorphic targets on an existing slice
images, err := zorm.New[Image]().Get(ctx)
if err != nil {
    return err
}
err = zorm.New[Image]().LoadMorph(ctx, images, "Imageable", map[string][]string{
    "users": {"Profile"},
    "posts": {},
})
```

After loading, use a type assertion or type switch on `image.Imageable` to
work with the concrete `*User` or `*Post`. Polymorphic targets are not given
a dirty-tracking baseline by the dynamic loader; load a typed target with
`New[User]().Find` or `New[Post]().Find` before using `Save` on it.

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
- Restores pre-transaction dirty-tracking baselines and managed versions on
  successful rollback, while retaining pending user edits
- Invalidates affected baselines if commit or rollback fails; reload before
  calling `Save` again when the transaction outcome is uncertain
- If the rollback *itself* fails, the returned error matches both your original
  error and `errors.Is(err, zorm.ErrRollbackFailed)` — worth branching on, since
  the connection state is then unknown

### Sharing a transaction across queries and raw SQL

Every participating query must use `WithTx(tx)`. A query that uses only the
global database runs outside the transaction even when called from inside
the callback. `Model.WithTx` returns a clone, so a base query can be reused.

```go
// Commit the user change and its related insert together
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    users := zorm.New[User]().WithTx(tx)
    user, err := users.Find(ctx, 42)
    if err != nil {
        return err
    }

    user.Name = "Alice Updated"
    if err := users.Save(ctx, user); err != nil {
        return err
    }

    _, err = zorm.New[User]().WithTx(tx).Raw(
        "INSERT INTO audit_log (entity_id, action) VALUES (?, ?)",
        user.ID, "renamed",
    ).Exec(ctx)
    return err
})
```

For a local database, start with `New[User]().SetDB(db).Transaction(...)`.
A configured resolver still selects its primary for transactions. ZORM's
transaction helpers begin with default `sql.TxOptions`; they do not expose
nested savepoints or a transaction-isolation option. Use native
`database/sql` when you need those controls, then bind a wrapper explicitly:

```go
// Use a transaction that was opened by application infrastructure
nativeTx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
if err != nil {
    return err
}
defer nativeTx.Rollback()

tx := &zorm.Tx{Tx: nativeTx}
if err := zorm.New[User]().WithTx(tx).Create(ctx, user); err != nil {
    return err
}
return nativeTx.Commit()
```

The application owns commit/rollback for a native transaction. Do not let a
transaction-bound model escape beyond the transaction's lifetime. Baselines
produced by native-transaction reads and writes are bound to that `sql.Tx`:
repeated saves work inside it, but saving through another transaction or an
unbound model returns `ErrSaveUntracked`. Reload after native commit or
rollback before saving outside that transaction.

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

`Create` returns only `error`; the entity is populated through the pointer
you pass. The full error-handling form of the preceding example is:

```go
user := &User{Email: "duplicate@example.com"}
err := zorm.New[User]().Create(ctx, user)
if err != nil {
    if qe := zorm.GetQueryError(err); qe != nil {
        log.Printf("%s failed on %s", qe.Operation, qe.Table)
    }

    return err
}
```

### Matching wrapped errors

Use `errors.Is` for sentinels and `errors.As` for structured errors. Query
errors preserve both ZORM's category and the underlying driver error when
classified. Not every validation, hook, relation, or connection-startup error
is a `QueryError`.

```go
// Inspect a failed insert without parsing the driver's message
err := zorm.New[User]().Create(ctx, &User{Email: "duplicate@example.com"})
if err != nil {
    var pgErr *pgconn.PgError
    if errors.As(err, &pgErr) {
        fmt.Println(pgErr.Code, pgErr.ConstraintName)
    }

    return fmt.Errorf("create user: %w", err)  // Preserve the error chain
}

// Inspect a relation failure
err = zorm.New[User]().Load(ctx, user, "Posts")
if err != nil {
    var relationErr *zorm.RelationError
    if errors.As(err, &relationErr) {
        fmt.Println(relationErr.Relation, relationErr.ModelType)
    }

    return err
}
```

The PostgreSQL error type is imported from
`github.com/jackc/pgx/v5/pgconn`. Avoid logging query
arguments that contain passwords, tokens, or private data.

Additional helpers are `IsForeignKeyViolation`, `IsNotNullViolation`,
`IsCheckViolation`, `IsSerializationFailure`, and `IsOptimisticLock`.
Connection errors, deadlocks, and serialization failures are reported to the
caller; ZORM does not automatically retry whole transactions. Any retry
policy should rerun the complete transaction with bounded attempts and
respect context cancellation.

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

- a non-zero, unchanged primary key on the entity (use `Create` to insert); and
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

#### Inspecting dirty fields and pending changes

Dirty tracking belongs to the loaded entity pointer. Keep the same pointer
when calling `Save`; copying a struct does not copy its tracking identity.
Inspect fields using database column names:

```go
model := zorm.New[User]()
user, err := model.Find(ctx, 42)
if err != nil {
    return err
}

user.Name = "Alice Updated"
fmt.Println(model.IsEntityTracked(user))         // true
fmt.Println(model.IsDirtyField(user, "name"))    // true
fmt.Println(model.IsCleanField(user, "email"))   // true
fmt.Println(model.HasDirtyFields(user))           // true
fmt.Println(model.GetOriginalValue(user, "name"))
fmt.Println(model.GetDirtyFields(user))

// Save one change while leaving another change pending
user.Email = "alice.updated@example.com"
if err := model.Clone().Exclude("email").Save(ctx, user); err != nil {
    return err
}
fmt.Println(model.IsDirtyField(user, "email"))   // true
err = model.Save(ctx, user)                       // Save the pending email
```

`UpdateColumns` also refreshes the baseline only for written columns, so
other pending edits remain dirty. `GetOriginal` and `GetOriginals` expose
detached baseline snapshots; modifying those snapshots does not change the
entity or its stored baseline.

```go
// Inspect tracking without a Model instance
fmt.Println(zorm.IsTracked(user))
fmt.Println(zorm.GetOriginal(user, "name"))
fmt.Println(zorm.GetOriginals(user))

// Release tracking when this entity is no longer needed
zorm.ClearOriginals(user)
// Save now requires reloading the entity to obtain a baseline
```

The global bounded tracker may evict a baseline under load. A tracking scope
provides deterministic cleanup, not immunity from eviction; handle
`ErrSaveUntracked` if the application keeps entities for a long time.

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

Reload from the primary after a conflict, then decide whether the requested
change is still valid. Do not overwrite the version in memory or retry a
stale entity blindly. `Save` compares against the loaded version and returns
`ErrOptimisticLock` if application code or a hook changes that version field.
A concurrently deleted row returns
`ErrRecordNotFound`; a version that cannot be incremented returns
`ErrVersionOverflow`.

```go
// Distinguish a concurrent edit from a deleted row
err := zorm.New[Order]().Save(ctx, order)
switch {
case zorm.IsOptimisticLock(err):
    current, loadErr := zorm.New[Order]().UsePrimary().Find(ctx, order.ID)
    if loadErr != nil {
        return loadErr
    }
    fmt.Println("Current version:", current.Version)
    return err  // Ask application logic to merge or reject the change
case zorm.IsNotFound(err):
    return err
case err != nil:
    return err
}
```

### Statement Caching

See [Advanced Start → Add a statement cache](#2-add-a-statement-cache).

The same cache can be passed to read and single-record write builders.
Clearing it is useful after planned schema changes; close the cache before
closing its database pools at shutdown.

```go
// Share a cache across independently constructed queries
cache := zorm.NewStmtCache(200)
defer cache.Close()

user, err := zorm.New[User]().WithStmtCache(cache).Find(ctx, 42)
if err != nil {
    return err
}
user.Name = "Cached update"
err = zorm.New[User]().WithStmtCache(cache).UpdateColumns(ctx, user, "name")

fmt.Println(cache.Len())
cache.Clear()  // Drop prepared statements; the cache remains reusable
```

`Get`, `Put`, and `PutAndGet` are low-level statement-cache operations.
If using `Get` or `PutAndGet` directly, call the returned release function
after use so eviction can safely close an unused statement. Ordinary ORM
queries manage this ownership internally.

### Read/Write Splitting

See [Advanced Start → Add replicas](#3-add-replicas).

```go
// Use random replica selection instead of round robin
zorm.ConfigureDBResolver(
    zorm.WithPrimary(primaryDB),
    zorm.WithReplicas(replica1, replica2),
    zorm.WithLoadBalancer(zorm.RandomLB),
)

// A read that must observe a just-completed write
user, err := zorm.New[User]().UsePrimary().Find(ctx, 42)

// Return to ordinary global/per-query database routing
zorm.ClearDBResolver()
```

Resolver helpers choose a pool; they do not monitor replication lag or health,
retry failed replicas, or automatically close database pools. The application
owns those pools. `GetGlobalDB` and `GetGlobalResolver` expose the configured
handles; resolver methods `Primary`, `Replica`, `ReplicaAt`, and `HasReplicas`
are useful for integration code. Custom load balancers implement
`Next([]*sql.DB) *sql.DB` and must support concurrent calls.

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

In the preceding examples, `Raw("SELECT * FROM active_users")` supplies the
whole SELECT and bypasses builder CTE assembly. Use `Table("active_users")`
to select from a CTE added with `WithCTE`:

```go
// Parameterized CTE: arguments from the nested builder are bound first
activeUsers := zorm.New[User]().Where("active", true)
users, err := zorm.New[User]().
    WithCTE("active_users", activeUsers).
    Table("active_users").
    Where("age", ">=", 18).
    OrderBy("id", "ASC").
    Get(ctx)

// String CTE: use a trusted constant without external interpolation
users, err = zorm.New[User]().
    WithCTE("active_users", "SELECT * FROM users WHERE active = true").
    Table("active_users").
    Get(ctx)
```

String CTEs have no separate arguments parameter; prefer a model builder
when the CTE needs bound values. To use recursive CTEs or more complex SQL,
supply the entire `WITH ... SELECT ...` statement through `Raw`.

### Full-Text Search (PostgreSQL)

```go
// Basic full-text search
articles, _ := zorm.New[Article]().
    WhereFullText("content", "database sql").Get(ctx)

// With language config
articles, _ := zorm.New[Article]().
    WhereFullTextWithConfig("content", "base de datos", "spanish").
    Get(ctx)

// Pre-computed tsvector column (fastest)
articles, _ := zorm.New[Article]().
    WhereTsVector("search_vector", "golang & performance").
    Get(ctx)

// Phrase search (word order matters)
articles, _ := zorm.New[Article]().
    WherePhraseSearch("title", "getting started").
    Get(ctx)
```

`WhereFullText` treats the input as plain search words using the `english`
configuration. `WherePhraseSearch` preserves word order.
`WhereFullTextWithConfig` selects the PostgreSQL text-search configuration;
`WhereTsVector` expects a tsquery expression such as `golang & performance`
and uses the `english` configuration.

For repeated searches, create a stored search vector and GIN index in your
PostgreSQL migration:

```sql
ALTER TABLE articles
ADD COLUMN search_vector tsvector
GENERATED ALWAYS AS (to_tsvector('english', coalesce(content, ''))) STORED;

CREATE INDEX articles_search_vector_idx ON articles USING GIN (search_vector);
```

```go
// Search published articles through the indexed vector
articles, err := zorm.New[Article]().
    Select("id", "title", "content").
    Where("published", true).
    WhereTsVector("search_vector", "golang & performance").
    Latest().
    Limit(20).
    Get(ctx)
```

The table and index are managed by your migrations. If the search-vector
column is mapped on the Go model, keep generated-column writes under your
control; the normal Create method sends every mapped non-auto field. These
helpers do not provide SQLite FTS syntax or automatic relevance ranking.

### Row Locking

```go
// Lock for update (exclusive)
user, _ := zorm.New[User]().Where("id", 1).Lock("UPDATE").First(ctx)

// Shared lock
user, _ := zorm.New[User]().Where("id", 1).Lock("SHARE").First(ctx)

// PostgreSQL-specific
user, _ := zorm.New[User]().Where("id", 1).Lock("NO KEY UPDATE").First(ctx)
```

Locks protect read-modify-write operations only while their transaction is
open. The standalone reads above do not retain a lock for a later write;
perform both operations inside the same transaction:

```go
// PostgreSQL: serialize updates to the selected user
err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
    users := zorm.New[User]().WithTx(tx)
    user, err := users.Clone().Where("id", 42).Lock("UPDATE").First(ctx)
    if err != nil {
        return err
    }
    user.Name = "Locked update"
    return users.UpdateColumns(ctx, user, "name")
})
```

The accepted modes are `UPDATE`, `NO KEY UPDATE`, `SHARE`, and `KEY SHARE`.
Outside a transaction, these reads use the resolver's primary even if
`UseReplica` was requested.
`SKIP LOCKED` and `NOWAIT` are not accepted by `Lock`; use trusted raw SQL
within a transaction when needed. Locking syntax is PostgreSQL-specific;
SQLite uses different concurrency controls.

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

ROLLUP, CUBE, and GROUPING SETS require database support; these examples
target PostgreSQL. Subtotal rows contain NULL grouping dimensions, so use
nullable fields on a report projection:

```go
type RegionalSales struct {
    Region *string
    City   *string
    Total  float64
}

// Per-city rows, region subtotals, and a grand total
sales, err := zorm.New[RegionalSales]().
    Table("orders").
    Select("region", "city", "SUM(amount) AS total").
    GroupByRollup("region", "city").Get(ctx)
```

Use `GroupByCube` when all combinations of the selected dimensions are
needed. Use `GroupByGroupingSets` to list just the combinations required by
the report. Alias aggregates to mapped output columns as above.

### Chunking Large Datasets

```go
err := zorm.New[User]().Chunk(ctx, 1000, func(users []*User) error {
    for _, user := range users {
        // Process each user
    }

    return nil  // Return error to stop chunking
})
```

Use a positive chunk size and stable ordering. A non-positive size returns
an error before any query or callback runs. `Chunk` uses LIMIT/OFFSET,
overrides its cloned query's limit/offset for each batch, and stops when the
callback returns an error or the final batch is exhausted. Modifying the
rows used by the filter while iterating can shift offsets and skip rows.

```go
// Export in ordered batches and release tracking after each batch
err := zorm.New[User]().OrderBy("id", "ASC").Chunk(ctx, 500, func(users []*User) error {
    for _, user := range users {
        if err := ctx.Err(); err != nil {
            return err
        }
        fmt.Println(user.ID, user.Email)
        zorm.ClearOriginals(user)
    }

    return nil
})
```

For jobs that change their own filters or delete processed rows, consider
the ID-based keyset recipe in [Pagination](#pagination) instead of offsets.

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

fmt.Println(sql)   // SELECT * FROM users WHERE age > $1 ORDER BY name ASC LIMIT 10
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

`Print` describes the configured SELECT, not a later `Create`, `Update`,
or `Save` statement. `GetBuildErr` is separate; inspect it before trusting
SQL generated from dynamic builder options. The state getters expose WHERE
fragments, their arguments, order clauses, and the current limit:

```go
// Inspect a query during development without contacting the database
query := zorm.New[User]().Where("active", true).OrderBy("id", "ASC").Limit(10)
if err := query.GetBuildErr(); err != nil {
    return err
}

sql, args := query.Print()
fmt.Println(sql, args)
fmt.Println(query.GetWheres(), query.GetArgs())
fmt.Println(query.GetOrderBys(), query.GetLimit())
```

### Dialects and instrumented drivers

ZORM recognizes pgx, lib/pq, and go-sqlite3 driver types. Unknown driver
types default to PostgreSQL behavior. Register the exact wrapper type name
when instrumentation hides the underlying driver:

```go
// Tell ZORM that this instrumented pool speaks SQLite
driverName := fmt.Sprintf("%T", db.Driver())
zorm.RegisterDriverDialect(driverName, zorm.DialectSQLite)

// Or set a process-wide dialect at application startup
zorm.SetDialect(zorm.DialectPostgres)
fmt.Println(zorm.GetDialect())
zorm.SetDialect(zorm.DialectAuto)  // Restore driver-based detection
```

`SetDialect` is process-wide. Prefer driver registration when multiple
database types share the process. Dialect selection affects IN-list and
default-insert SQL; `?` to `$N` placeholder rewriting still applies to both
supported dialects.

### Schema inspection and lower-level helpers

`ParseModel[T]` returns cached field metadata without contacting the
database. `ParseModelType` provides the same metadata when the Go type is
known only through reflection. Treat cached metadata as read-only.

```go
// Inspect model conventions during development
info := zorm.ParseModel[User]()
fmt.Println(info.TableName, info.PrimaryKey)
for _, field := range info.OrderedFields {
    fmt.Println(field.Name, field.Column, field.IsPrimary)
}

fmt.Println(zorm.ToSnakeCase("CreatedAt"))  // created_at
```

`ModelInfo.GetRelationField` supports reflection-based integrations.
`GetStringBuilder` / `PutStringBuilder` expose the internal builder pool;
return a borrowed builder after use and do not retain it afterward. Most
applications need only the query APIs rather than these integration helpers.

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

See [benchmarks/README.md](benchmarks/README.md) for the full benchmark setup.

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

For suspected vulnerabilities, follow the [security policy](SECURITY.md) and
avoid sharing sensitive details in public issues.

## License

MIT License - see LICENSE file for details.
