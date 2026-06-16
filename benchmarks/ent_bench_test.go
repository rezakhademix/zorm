//go:build ent

// This file is gated behind the `ent` build tag because it imports the
// generated ent client at ./entbench. Run `go generate ./entbench` once to
// materialize the client, then `go test -tags=ent -bench=BenchmarkEnt`.
package benchmarks

import (
	"context"
	"database/sql"
	"testing"

	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rezakhademix/zorm/benchmarks/entbench"
	"github.com/rezakhademix/zorm/benchmarks/shared"
)

func entOpen(b *testing.B) (*entbench.Client, *sql.DB, []int64) {
	b.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := shared.ApplyDDL(db); err != nil {
		b.Fatalf("ddl: %v", err)
	}
	ids, err := shared.SeedRaw(db)
	if err != nil {
		b.Fatalf("seed: %v", err)
	}
	drv := entsql.OpenDB("sqlite3", db)
	client := entbench.NewClient(entbench.Driver(drv))
	return client, db, ids
}

func BenchmarkEnt_InsertOne(b *testing.B) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		p := shared.MakeUser(shared.SeedSize + 1 + i)
		c := client.User.Create().
			SetName(p.Name).SetEmail(p.Email).SetAge(p.Age).SetScore(p.Score).
			SetIsActive(p.IsActive).SetAvatar(p.Avatar).
			SetMetadata(p.Metadata).SetCreatedAt(p.CreatedAt)
		if p.Nickname != nil {
			c = c.SetNickname(*p.Nickname)
		}
		if _, err := c.Save(ctx); err != nil {
			b.Fatalf("create: %v", err)
		}
	}
}

func BenchmarkEnt_GetByPK(b *testing.B) {
	client, db, ids := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := client.User.Get(ctx, int(ids[i%len(ids)])); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkEnt_UpdateOne(b *testing.B) {
	client, db, ids := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := client.User.UpdateOneID(int(ids[0])).
			SetScore(float64(i)).
			SetIsActive(i%2 == 0).
			Save(ctx); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

func BenchmarkEnt_DeleteOne(b *testing.B) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		p := shared.MakeUser(shared.SeedSize + 10_000 + i)
		c := client.User.Create().
			SetName(p.Name).SetEmail(p.Email).SetAge(p.Age).SetScore(p.Score).
			SetIsActive(p.IsActive).SetAvatar(p.Avatar).
			SetMetadata(p.Metadata).SetCreatedAt(p.CreatedAt)
		if p.Nickname != nil {
			c = c.SetNickname(*p.Nickname)
		}
		u, err := c.Save(ctx)
		if err != nil {
			b.Fatalf("seed-delete: %v", err)
		}
		b.StartTimer()

		if err := client.User.DeleteOneID(u.ID).Exec(ctx); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func entBulkInsert(b *testing.B, n int) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		bulk := make([]*entbench.UserCreate, n)
		for j := range n {
			p := shared.MakeUser(shared.SeedSize + 100_000 + i*n + j)
			c := client.User.Create().
				SetName(p.Name).SetEmail(p.Email).SetAge(p.Age).SetScore(p.Score).
				SetIsActive(p.IsActive).SetAvatar(p.Avatar).
				SetMetadata(p.Metadata).SetCreatedAt(p.CreatedAt)
			if p.Nickname != nil {
				c = c.SetNickname(*p.Nickname)
			}
			bulk[j] = c
		}
		if _, err := client.User.CreateBulk(bulk...).Save(ctx); err != nil {
			b.Fatalf("bulk: %v", err)
		}
	}
}

func BenchmarkEnt_BulkInsert100(b *testing.B)  { entBulkInsert(b, 100) }
func BenchmarkEnt_BulkInsert1000(b *testing.B) { entBulkInsert(b, 1000) }

func BenchmarkEnt_FindWhereOrderLimit(b *testing.B) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, err := client.User.Query().
			Where(entbench.UserAgeGT(30), entbench.UserIsActive(true)).
			Order(entbench.UserScoreDesc()).
			Limit(50).
			All(ctx)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		if len(rows) == 0 {
			b.Fatal("zero rows")
		}
	}
}

func BenchmarkEnt_TxInsert100(b *testing.B) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		tx, err := client.Tx(ctx)
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		for j := range 100 {
			p := shared.MakeUser(shared.SeedSize + 200_000 + i*100 + j)
			c := tx.User.Create().
				SetName(p.Name).SetEmail(p.Email).SetAge(p.Age).SetScore(p.Score).
				SetIsActive(p.IsActive).SetAvatar(p.Avatar).
				SetMetadata(p.Metadata).SetCreatedAt(p.CreatedAt)
			if p.Nickname != nil {
				c = c.SetNickname(*p.Nickname)
			}
			if _, err := c.Save(ctx); err != nil {
				tx.Rollback()
				b.Fatalf("tx insert: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

func BenchmarkEnt_EagerLoadHasMany(b *testing.B) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		users, err := client.User.Query().WithPosts().Limit(100).All(ctx)
		if err != nil {
			b.Fatalf("with posts: %v", err)
		}
		if len(users) != 100 {
			b.Fatalf("want 100, got %d", len(users))
		}
	}
}

func BenchmarkEnt_EagerLoadBelongsTo(b *testing.B) {
	client, db, _ := entOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		posts, err := client.Post.Query().WithUser().Limit(100).All(ctx)
		if err != nil {
			b.Fatalf("with user: %v", err)
		}
		if len(posts) != 100 {
			b.Fatalf("want 100, got %d", len(posts))
		}
	}
}
