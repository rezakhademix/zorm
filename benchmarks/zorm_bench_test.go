package benchmarks

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rezakhademix/zorm"
	"github.com/rezakhademix/zorm/benchmarks/shared"
)

type ZormUser struct {
	ID        int64 `zorm:"primaryKey"`
	Name      string
	Email     string
	Age       int64
	Score     float64
	IsActive  bool
	Nickname  *string
	Avatar    []byte
	Metadata  string
	CreatedAt time.Time
	Posts     []*ZormPost
}

func (ZormUser) TableName() string { return "users" }

func (ZormUser) PostsRelation() zorm.HasMany[ZormPost] {
	return zorm.HasMany[ZormPost]{ForeignKey: "user_id", LocalKey: "id"}
}

type ZormPost struct {
	ID        int64 `zorm:"primaryKey"`
	UserID    int64
	Title     string
	Body      string
	Views     int64
	Rating    float64
	Published bool
	Tags      string
	Cover     []byte
	CreatedAt time.Time
	User      *ZormUser
}

func (ZormPost) TableName() string { return "posts" }

func (ZormPost) UserRelation() zorm.BelongsTo[ZormUser] {
	return zorm.BelongsTo[ZormUser]{ForeignKey: "user_id", OwnerKey: "id"}
}

func zormOpen(b *testing.B) (*sql.DB, []int64) {
	b.Helper()
	zorm.SetDialect(zorm.DialectSQLite)
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
	zorm.SetGlobalDB(db)
	return db, ids
}

func BenchmarkZorm_InsertOne(b *testing.B) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		p := shared.MakeUser(shared.SeedSize + 1 + i)
		u := &ZormUser{
			Name: p.Name, Email: p.Email, Age: p.Age, Score: p.Score,
			IsActive: p.IsActive, Nickname: p.Nickname, Avatar: p.Avatar,
			Metadata: p.Metadata, CreatedAt: p.CreatedAt,
		}
		if err := zorm.New[ZormUser]().Create(ctx, u); err != nil {
			b.Fatalf("create: %v", err)
		}
	}
}

func BenchmarkZorm_GetByPK(b *testing.B) {
	db, ids := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := ids[i%len(ids)]
		if _, err := zorm.New[ZormUser]().Find(ctx, id); err != nil {
			b.Fatalf("find: %v", err)
		}
	}
}

func BenchmarkZorm_UpdateOne(b *testing.B) {
	db, ids := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	u, err := zorm.New[ZormUser]().Find(ctx, ids[0])
	if err != nil {
		b.Fatalf("preload: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		u.Score = float64(i)
		u.IsActive = i%2 == 0
		if err := zorm.New[ZormUser]().Update(ctx, u); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

func BenchmarkZorm_DeleteOne(b *testing.B) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		p := shared.MakeUser(shared.SeedSize + 10_000 + i)
		u := &ZormUser{
			Name: p.Name, Email: p.Email, Age: p.Age, Score: p.Score,
			IsActive: p.IsActive, Nickname: p.Nickname, Avatar: p.Avatar,
			Metadata: p.Metadata, CreatedAt: p.CreatedAt,
		}
		if err := zorm.New[ZormUser]().Create(ctx, u); err != nil {
			b.Fatalf("seed-delete: %v", err)
		}
		b.StartTimer()

		if err := zorm.New[ZormUser]().Where("id", u.ID).Delete(ctx); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func zormBulkInsert(b *testing.B, n int) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		batch := make([]*ZormUser, n)
		for j := range n {
			p := shared.MakeUser(shared.SeedSize + 100_000 + i*n + j)
			batch[j] = &ZormUser{
				Name: p.Name, Email: p.Email, Age: p.Age, Score: p.Score,
				IsActive: p.IsActive, Nickname: p.Nickname, Avatar: p.Avatar,
				Metadata: p.Metadata, CreatedAt: p.CreatedAt,
			}
		}
		if err := zorm.New[ZormUser]().CreateMany(ctx, batch); err != nil {
			b.Fatalf("createMany: %v", err)
		}
	}
}

func BenchmarkZorm_BulkInsert100(b *testing.B)  { zormBulkInsert(b, 100) }
func BenchmarkZorm_BulkInsert1000(b *testing.B) { zormBulkInsert(b, 1000) }

func BenchmarkZorm_FindWhereOrderLimit(b *testing.B) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, err := zorm.New[ZormUser]().
			Where("age", ">", 30).
			Where("is_active", true).
			OrderBy("score", "DESC").
			Limit(50).
			Get(ctx)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if len(rows) == 0 {
			b.Fatal("zero rows")
		}
	}
}

func BenchmarkZorm_TxInsert100(b *testing.B) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		err := zorm.Transaction(ctx, func(tx *zorm.Tx) error {
			for j := range 100 {
				p := shared.MakeUser(shared.SeedSize + 200_000 + i*100 + j)
				u := &ZormUser{
					Name: p.Name, Email: p.Email, Age: p.Age, Score: p.Score,
					IsActive: p.IsActive, Nickname: p.Nickname, Avatar: p.Avatar,
					Metadata: p.Metadata, CreatedAt: p.CreatedAt,
				}
				if err := zorm.New[ZormUser]().WithTx(tx).Create(ctx, u); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("tx: %v", err)
		}
	}
}

func BenchmarkZorm_EagerLoadHasMany(b *testing.B) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		users, err := zorm.New[ZormUser]().With("Posts").Limit(100).Get(ctx)
		if err != nil {
			b.Fatalf("eager has-many: %v", err)
		}
		if len(users) != 100 {
			b.Fatalf("want 100, got %d", len(users))
		}
	}
}

func BenchmarkZorm_EagerLoadBelongsTo(b *testing.B) {
	db, _ := zormOpen(b)
	defer db.Close()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		posts, err := zorm.New[ZormPost]().With("User").Limit(100).Get(ctx)
		if err != nil {
			b.Fatalf("eager belongs-to: %v", err)
		}
		if len(posts) != 100 {
			b.Fatalf("want 100, got %d", len(posts))
		}
	}
}
