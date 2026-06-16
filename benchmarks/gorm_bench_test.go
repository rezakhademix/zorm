package benchmarks

import (
	"testing"
	"time"

	"github.com/rezakhademix/zorm/benchmarks/shared"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type GormUser struct {
	ID        int64 `gorm:"primaryKey"`
	Name      string
	Email     string
	Age       int64
	Score     float64
	IsActive  bool
	Nickname  *string
	Avatar    []byte
	Metadata  string
	CreatedAt time.Time
	Posts     []GormPost `gorm:"foreignKey:UserID"`
}

func (GormUser) TableName() string { return "users" }

type GormPost struct {
	ID        int64 `gorm:"primaryKey"`
	UserID    int64
	Title     string
	Body      string
	Views     int64
	Rating    float64
	Published bool
	Tags      string
	Cover     []byte
	CreatedAt time.Time
	User      *GormUser `gorm:"foreignKey:UserID"`
}

func (GormPost) TableName() string { return "posts" }

func gormOpen(b *testing.B) (*gorm.DB, []int64) {
	b.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:?_busy_timeout=5000"), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		b.Fatalf("db: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := shared.ApplyDDL(sqlDB); err != nil {
		b.Fatalf("ddl: %v", err)
	}
	ids, err := shared.SeedRaw(sqlDB)
	if err != nil {
		b.Fatalf("seed: %v", err)
	}
	return db, ids
}

func gormUserFrom(p shared.UserPayload) *GormUser {
	return &GormUser{
		Name: p.Name, Email: p.Email, Age: p.Age, Score: p.Score,
		IsActive: p.IsActive, Nickname: p.Nickname, Avatar: p.Avatar,
		Metadata: p.Metadata, CreatedAt: p.CreatedAt,
	}
}

func BenchmarkGorm_InsertOne(b *testing.B) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		u := gormUserFrom(shared.MakeUser(shared.SeedSize + 1 + i))
		if err := db.Create(u).Error; err != nil {
			b.Fatalf("create: %v", err)
		}
	}
}

func BenchmarkGorm_GetByPK(b *testing.B) {
	db, ids := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		var u GormUser
		if err := db.First(&u, ids[i%len(ids)]).Error; err != nil {
			b.Fatalf("first: %v", err)
		}
	}
}

func BenchmarkGorm_UpdateOne(b *testing.B) {
	db, ids := gormOpen(b)
	var u GormUser
	if err := db.First(&u, ids[0]).Error; err != nil {
		b.Fatalf("preload: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		u.Score = float64(i)
		u.IsActive = i%2 == 0
		if err := db.Save(&u).Error; err != nil {
			b.Fatalf("save: %v", err)
		}
	}
}

func BenchmarkGorm_DeleteOne(b *testing.B) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		u := gormUserFrom(shared.MakeUser(shared.SeedSize + 10_000 + i))
		if err := db.Create(u).Error; err != nil {
			b.Fatalf("seed-delete: %v", err)
		}
		b.StartTimer()

		if err := db.Delete(&GormUser{}, u.ID).Error; err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func gormBulkInsert(b *testing.B, n int) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		batch := make([]GormUser, n)
		for j := range n {
			batch[j] = *gormUserFrom(shared.MakeUser(shared.SeedSize + 100_000 + i*n + j))
		}
		if err := db.CreateInBatches(batch, n).Error; err != nil {
			b.Fatalf("createInBatches: %v", err)
		}
	}
}

func BenchmarkGorm_BulkInsert100(b *testing.B)  { gormBulkInsert(b, 100) }
func BenchmarkGorm_BulkInsert1000(b *testing.B) { gormBulkInsert(b, 1000) }

func BenchmarkGorm_FindWhereOrderLimit(b *testing.B) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var rows []GormUser
		if err := db.Where("age > ? AND is_active = ?", 30, true).
			Order("score DESC").
			Limit(50).
			Find(&rows).Error; err != nil {
			b.Fatalf("find: %v", err)
		}
		if len(rows) == 0 {
			b.Fatal("zero rows")
		}
	}
}

func BenchmarkGorm_TxInsert100(b *testing.B) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		err := db.Transaction(func(tx *gorm.DB) error {
			for j := range 100 {
				u := gormUserFrom(shared.MakeUser(shared.SeedSize + 200_000 + i*100 + j))
				if err := tx.Create(u).Error; err != nil {
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

func BenchmarkGorm_EagerLoadHasMany(b *testing.B) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var users []GormUser
		if err := db.Preload("Posts").Limit(100).Find(&users).Error; err != nil {
			b.Fatalf("preload posts: %v", err)
		}
		if len(users) != 100 {
			b.Fatalf("want 100, got %d", len(users))
		}
	}
}

func BenchmarkGorm_EagerLoadBelongsTo(b *testing.B) {
	db, _ := gormOpen(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var posts []GormPost
		if err := db.Preload("User").Limit(100).Find(&posts).Error; err != nil {
			b.Fatalf("preload user: %v", err)
		}
		if len(posts) != 100 {
			b.Fatalf("want 100, got %d", len(posts))
		}
	}
}
