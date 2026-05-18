# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "review them to be correct"
- "Fix the bug" → "review the whole scenario to ensure it is fixed and write tests for bug scenario"
- "Refactor X" → "review the whole scenario to ensure nothing is broken"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## Project Overview

ZORM is a type-safe Go ORM (Go 1.25+) leveraging generics for compile-time type safety. It provides a fluent, chainable API for SQL queries, built on standard `database/sql`. Drivers in `go.mod`: `pgx/v5` (PostgreSQL) and `go-sqlite3` (used by tests).

## Common Commands

The Makefile is lowercased (`makefile`):

```bash
# Run all tests with race detection
make test

# Run a single test by name regex (passes -run ${name})
make test-one name=TestCreate

# Run tests with coverage percentage (top-level package only)
make test-cover

# Generate HTML coverage report
make test-profile

# Clean test cache
make clean-test-cache

# Tidy go modules
make tidy
```

Direct `go test` is fine for targeted runs:

```bash
go test -v -run TestCreate ./...
go test -v -run TestRelations ./...
```

## Architecture

### Core Components

- **model.go** — `Model[T]` generic struct that holds query state (columns, wheres, args, orderBys, relations, joins, CTEs, locks, etc.) and provides the entry point via `New[T]()`. Also defines `Acquire[T]()`/`Release()` for `sync.Pool`-based reuse.
- **query.go** — Query builder methods (Where, Select, OrderBy, GroupBy, Limit, Join, …) that mutate `Model` state.
- **executor.go** — SQL generation and execution: `Get`, `First`, `Find`, `Create`, `Update`, `Delete`, plus bulk variants. Handles row scanning into typed structs via reflection.
- **relations.go** — Relationship system: `HasOne`, `HasMany`, `BelongsTo`, `BelongsToMany`, `MorphOne`, `MorphMany`. Eager loading (`With`/`WithCallback`/`WithMorph`) and lazy loading (`Load`/`LoadSlice`). Pivot helpers `Attach`/`Detach`/`Sync`.
- **schema.go** — Reflection-based schema parsing via `ParseModel[T]()`. Caches `ModelInfo` (table name, primary key, field mappings, relation methods). Snake-case conversion is cached with a bounded LRU (`snakeCaseCache`).
- **scalar.go** — `ScalarQuery[T]` / `Query[T]()` for fetching single-column typed slices (`[]string`, `[]int64`, …) without materializing full structs.
- **resolver.go** — `DBResolver` for primary/replica routing with `RoundRobinLoadBalancer` / `RandomLoadBalancer`. Configured via `ConfigureDBResolver(...)`.
- **transaction.go** — `Transaction(ctx, fn)` and `(*Model[T]).Transaction(...)`. Auto-rollback on error or panic; auto-commit otherwise.
- **stmt_cache.go** — `StmtCache` LRU for prepared statements; thread-safe with atomic refcounting. Enable per-model via `WithStmtCache`.
- **errors.go** — Sentinel errors and `IsNotFound` / `IsDuplicateKey` / `IsConnectionError` / etc. helpers, plus `QueryError` with operation/table/constraint context.
- **postgres.go** — `ConnectPostgres(dsn, *DBConfig)` helper (uses `pgx/v5/stdlib`).
- **dirty.go** — Change tracking. `Print()` (defined in `query.go`, mirrored in `scalar.go` for `ScalarQuery`) returns SQL+args without executing for debugging.

### Key Patterns

**Convention over configuration**: Models are plain Go structs. Table names auto-pluralize to snake_case (`User` → `users`). Primary key defaults to `id`. Field names map to snake_case columns. Override with `TableName() string` / `PrimaryKey() string` methods on the type.

**Query state is mutable**: Builder methods modify the receiver in place. `Model[T]` is **not safe for concurrent modification**. For concurrent use, build a base in one goroutine and `Clone()` it per goroutine (or create a fresh `New[T]()` per goroutine). See the doc comment on `Model[T]` in `model.go`.

**Relation methods**: Define relations as methods returning `zorm.HasMany[T]`, `zorm.BelongsTo[T]`, etc. The schema parser accepts either bare name (`Posts`) or suffixed (`PostsRelation`).

**Global DB**: Set `zorm.GlobalDB` (or call `zorm.SetGlobalDB(db)` for thread-safety) at startup. Per-query override via `SetDB(db)` or `WithTx(tx)`.

**Placeholder rebinding**: `rebind()` in `query.go` converts `?` to `$1, $2, …` for PostgreSQL. SQLite (used by tests) keeps `?`. Most query paths apply this automatically; a few relation-loader paths call it explicitly. Relevant when reading generated SQL via `Print()` or hand-writing `Raw(...)` queries.

### Hooks & Accessors

**Hooks** are detected via type-assertion on an inline interface in `executor.go` (e.g. `any(entity).(interface{ BeforeCreate(context.Context) error })`). The seven supported hooks are:

- `BeforeCreate`, `AfterCreate`
- `BeforeUpdate`, `AfterUpdate`
- `BeforeDelete`, `AfterDelete`
- `AfterFind`

Note: README's hooks table only lists three — the code supports all seven.

**Transactional hooks (`*Tx` variants)**: each of the six **write** hooks has a parallel `Tx` variant that receives the active `*zorm.Tx`:

- `BeforeCreateTx(ctx, *Tx) error`, `AfterCreateTx(ctx, *Tx) error`
- `BeforeUpdateTx(ctx, *Tx) error`, `AfterUpdateTx(ctx, *Tx) error`
- `BeforeDeleteTx(ctx, *Tx) error`, `AfterDeleteTx(ctx, *Tx) error`

If both the plain and `Tx` variant exist on a model, **only the `Tx` variant fires** (no double-dispatch). When a model implements any `Tx` variant and the operation is called outside an existing transaction, the executor auto-opens one for that call so DB work performed through the passed `*Tx` (e.g. `tx.Tx.ExecContext(...)` or `model.WithTx(tx).<...>`) rolls back atomically with the parent SQL on error. `AfterFind` intentionally has no `Tx` variant — reads don't participate in write transactions.

Gotcha: in-memory mutations to entity fields are **never** rolled back regardless of variant — only DB writes via the passed `*Tx` are. Plain hooks (`BeforeCreate` etc.) still run on a separate connection from the parent SQL, so their DB side effects are not atomic; migrate to the `Tx` variant when atomicity matters.

**Accessors** (`schema.go` + `executor.go`): methods named `Get<Name>` with zero arguments returning exactly one value are treated as computed attributes. The attribute key is the snake_case of the name after `Get` (`GetFullName()` → `attributes["full_name"]`). Requires the struct to declare `Attributes map[string]any`; without that field, accessors silently no-op.

**Dirty-tracking memory** (`dirty.go`): originals are kept in a bounded LRU (default 50,000 entries; tunable via `ConfigureDirtyTracking()`). Long-running services that load many distinct entities should use `WithTrackingScope(scope)` (with `defer scope.Close()`) or explicitly `ClearOriginals(entity)` to avoid retaining originals indefinitely.

### Execution Flow

1. `New[T]()` creates a `Model` with parsed schema from `ParseModel[T]()`.
2. Builder methods (Where, Select, …) accumulate state on the receiver.
3. Terminal methods (Get, First, Create, …) call `queryer()` / `queryerForWrite()` to obtain a DB/Tx handle.
4. SQL is built from accumulated state and executed via `database/sql`.
5. Results are scanned into `*T` / `[]*T` using reflection (or a typed scalar slice for `ScalarQuery`).

### Database Routing

- `queryer()` — returns the read handle (respects `GlobalResolver` for replica routing; `forcePrimary` / `forceReplica` overrides on the model).
- `queryerForWrite()` — always returns the primary when a resolver is configured.
- Transactions (`WithTx(tx)`) bypass the resolver and use the transaction connection.

### Statement Caching

`StmtCache` is an LRU of prepared statements with atomic reference counting. Enable per-model via `WithStmtCache(cache)`. The cache is reused across `Clone()`.

## Pre-PR Checks (CI)

`make test` alone is **not** sufficient. `.github/workflows/audit.yml` runs the following on every push and PR to `main`:

```bash
go mod verify
go build -v ./...
go vet ./...
staticcheck ./...      # install: go install honnef.co/go/tools/cmd/staticcheck@latest
golint ./...           # install: go install golang.org/x/lint/golint@latest
go test -race -vet=off -coverprofile=./coverage.out ./...
```

`golint` is deprecated upstream but the workflow still gates on it — don't introduce new lint findings. To mirror CI locally before opening a PR:

```bash
go vet ./... && staticcheck ./... && golint ./... && make test
```

## Testing

- Integration tests live in files ending `_integration_test.go` and exercise full DB operations against an in-memory SQLite database.
- Unit tests test isolated logic without a DB.
- `make test` runs everything with `-race`. Prefer keeping new tests race-clean; if a test exposes a real race in production code, fix the code rather than the test.

## Contributing Conventions

From `CONTRIBUTING.md`:
- Branch names use **kebab-case** (e.g. `feature-kebab-case-example`, `feat-improve-join-query`). Recent merge commits follow this.
- Add tests for behavioral changes; `go test` must pass before opening a PR.
