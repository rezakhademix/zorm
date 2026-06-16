# zorm benchmarks

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

Seed: 1,000 users × 5 posts each = 1,000 user rows + 5,000 post rows. RNG seed
is fixed (`rand.NewSource(42)`) so every ORM under test reads identical bytes.

## Benchmarks

Each ORM has its own `*_bench_test.go` file. All declare the same nine
`Benchmark*` funcs (prefixed by the ORM name) so `go test -bench=.` produces a
single table:

| Bench                   | What it measures                                                            |
| ----------------------- | --------------------------------------------------------------------------- |
| `*_InsertOne`           | Insert one row per iteration                                                |
| `*_GetByPK`             | `SELECT … WHERE id = ?` against a seeded row                                |
| `*_UpdateOne`           | Update two columns of a preloaded row                                       |
| `*_DeleteOne`           | Delete a row by PK (re-seed step excluded from the timer via `b.StopTimer`) |
| `*_BulkInsert100`       | Insert 100 rows in one logical batch                                        |
| `*_BulkInsert1000`      | Insert 1,000 rows in one logical batch                                      |
| `*_TxInsert100`         | 100 single-row inserts inside one transaction (per-call cost + commit)      |
| `*_FindWhereOrderLimit` | `WHERE age > ? AND is_active = ? ORDER BY score DESC LIMIT 50`              |
| `*_EagerLoadHasMany`    | Load 100 users + their posts (`User → []Post`)                              |
| `*_EagerLoadBelongsTo`  | Load 100 posts + their author (`Post → User`)                               |

## Benchmark

Recorded on Apple M3 Pro / darwin/arm64, SQLite `:memory:`, single connection.
Lower is better. Raw `go test -bench=. -benchmem` output:

```
goos: darwin
goarch: arm64
pkg: github.com/rezakhademix/zorm/benchmarks
cpu: Apple M3 Pro
BenchmarkGorm_InsertOne-11                    108280         11047 ns/op        6732 B/op          87 allocs/op
BenchmarkGorm_GetByPK-11                      145824          8873 ns/op        5515 B/op         109 allocs/op
BenchmarkGorm_UpdateOne-11                    121521          9418 ns/op       10040 B/op         101 allocs/op
BenchmarkGorm_DeleteOne-11                    180073          6605 ns/op        3106 B/op          40 allocs/op
BenchmarkGorm_BulkInsert100-11                  4012        305729 ns/op      213582 B/op        3203 allocs/op
BenchmarkGorm_BulkInsert1000-11                  400       2988061 ns/op     1991097 B/op       31411 allocs/op
BenchmarkGorm_FindWhereOrderLimit-11            4824        249172 ns/op       56515 B/op        1387 allocs/op
BenchmarkGorm_TxInsert100-11                     746       1608794 ns/op      703747 B/op        9256 allocs/op
BenchmarkGorm_EagerLoadHasMany-11                836       1435784 ns/op      627648 B/op       17223 allocs/op
BenchmarkGorm_EagerLoadBelongsTo-11             3788        318111 ns/op      177928 B/op        3798 allocs/op

BenchmarkZorm_InsertOne-11                     93940         10911 ns/op        4661 B/op          72 allocs/op
BenchmarkZorm_GetByPK-11                      136419          8493 ns/op        4818 B/op         103 allocs/op
BenchmarkZorm_UpdateOne-11                    162715          7342 ns/op        4460 B/op          63 allocs/op
BenchmarkZorm_DeleteOne-11                    189541          5971 ns/op        1879 B/op          29 allocs/op
BenchmarkZorm_BulkInsert100-11                   996       1234804 ns/op      221158 B/op        3479 allocs/op
BenchmarkZorm_BulkInsert1000-11                   26      50519979 ns/op     2140176 B/op       35262 allocs/op
BenchmarkZorm_FindWhereOrderLimit-11            4491        247887 ns/op       67647 B/op        1626 allocs/op
BenchmarkZorm_TxInsert100-11                     795       1536454 ns/op      541881 B/op        7899 allocs/op
BenchmarkZorm_EagerLoadHasMany-11               1060       1124967 ns/op      444236 B/op       11573 allocs/op
BenchmarkZorm_EagerLoadBelongsTo-11             4074        289290 ns/op      152650 B/op        3476 allocs/op
```

### Side-by-side (ns/op · B/op · allocs/op)

| Op                  | gorm                           | zorm                            | Winner (ns/op) |
| ------------------- | ------------------------------ | ------------------------------- | -------------- |
| InsertOne           | 11,047 · 6,732 · 87            | 10,911 · 4,661 · 72             | zorm           |
| GetByPK             | 8,873 · 5,515 · 109            | 8,493 · 4,818 · 103             | zorm           |
| UpdateOne           | 9,418 · 10,040 · 101           | 7,342 · 4,460 · 63              | zorm           |
| DeleteOne           | 6,605 · 3,106 · 40             | 5,971 · 1,879 · 29              | zorm           |
| BulkInsert100       | 305,729 · 213,582 · 3,203      | 1,234,804 · 221,158 · 3,479     | gorm           |
| BulkInsert1000      | 2,988,061 · 1,991,097 · 31,411 | 50,519,979 · 2,140,176 · 35,262 | gorm           |
| FindWhereOrderLimit | 249,172 · 56,515 · 1,387       | 247,887 · 67,647 · 1,626        | zorm           |
| TxInsert100         | 1,608,794 · 703,747 · 9,256    | 1,536,454 · 541,881 · 7,899     | zorm           |
| EagerLoadHasMany    | 1,435,784 · 627,648 · 17,223   | 1,124,967 · 444,236 · 11,573    | zorm           |
| EagerLoadBelongsTo  | 318,111 · 177,928 · 3,798      | 289,290 · 152,650 · 3,476       | zorm           |

## Running

```bash
# from repo root
make bench

# or from this directory
go mod tidy                 # one-time
go test -bench=. -benchmem -run=^$ ./...
```

### Running the ent benches

Ent requires code generation. The ent bench file is gated behind the `ent`
build tag so the suite stays runnable until you opt in:

```bash
cd benchmarks
go generate ./entbench       # materializes ./entbench/<generated client>
go test -tags=ent -bench=BenchmarkEnt -benchmem -run=^$ ./...
```

### Filtering

```bash
# only zorm
go test -bench=BenchmarkZorm -benchmem -run=^$ ./...

# only the eager-load comparison across all ORMs
go test -bench=EagerLoad -benchmem -run=^$ ./...
```

## How to read a result row

```
BenchmarkZorm_GetByPK-10        50000     22431 ns/op    1184 B/op    35 allocs/op
```

- `-10`: GOMAXPROCS.
- `50000`: iterations the harness chose.
- `ns/op`: median-ish per-op latency. Lower is better.
- `B/op` + `allocs/op`: heap bytes and alloc count per op. Lower is better,
  but a library with 2× the allocs and 0.9× the latency is still faster in
  wall-clock terms — read both columns together.

