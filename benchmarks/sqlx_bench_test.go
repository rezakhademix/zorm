package benchmarks

import (
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rezakhademix/zorm/benchmarks/shared"
)

type SqlxUser struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	Age       int64     `db:"age"`
	Score     float64   `db:"score"`
	IsActive  bool      `db:"is_active"`
	Nickname  *string   `db:"nickname"`
	Avatar    []byte    `db:"avatar"`
	Metadata  string    `db:"metadata"`
	CreatedAt time.Time `db:"created_at"`
}

type SqlxPost struct {
	ID        int64     `db:"id"`
	UserID    int64     `db:"user_id"`
	Title     string    `db:"title"`
	Body      string    `db:"body"`
	Views     int64     `db:"views"`
	Rating    float64   `db:"rating"`
	Published bool      `db:"published"`
	Tags      string    `db:"tags"`
	Cover     []byte    `db:"cover"`
	CreatedAt time.Time `db:"created_at"`
}

func sqlxOpen(b *testing.B) (*sqlx.DB, []int64) {
	b.Helper()
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := shared.ApplyDDL(db.DB); err != nil {
		b.Fatalf("ddl: %v", err)
	}
	ids, err := shared.SeedRaw(db.DB)
	if err != nil {
		b.Fatalf("seed: %v", err)
	}
	return db, ids
}

const sqlxInsertUser = `INSERT INTO users
	(name, email, age, score, is_active, nickname, avatar, metadata, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

func BenchmarkSqlx_InsertOne(b *testing.B) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		p := shared.MakeUser(shared.SeedSize + 1 + i)
		if _, err := db.Exec(sqlxInsertUser, p.Name, p.Email, p.Age, p.Score,
			p.IsActive, p.Nickname, p.Avatar, p.Metadata, p.CreatedAt); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkSqlx_GetByPK(b *testing.B) {
	db, ids := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		var u SqlxUser
		if err := db.Get(&u, `SELECT * FROM users WHERE id = ?`, ids[i%len(ids)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkSqlx_UpdateOne(b *testing.B) {
	db, ids := sqlxOpen(b)
	defer db.Close()
	var u SqlxUser
	if err := db.Get(&u, `SELECT * FROM users WHERE id = ?`, ids[0]); err != nil {
		b.Fatalf("preload: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		u.Score = float64(i)
		u.IsActive = i%2 == 0
		if _, err := db.Exec(`UPDATE users SET score = ?, is_active = ? WHERE id = ?`,
			u.Score, u.IsActive, u.ID); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

func BenchmarkSqlx_DeleteOne(b *testing.B) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		p := shared.MakeUser(shared.SeedSize + 10_000 + i)
		res, err := db.Exec(sqlxInsertUser, p.Name, p.Email, p.Age, p.Score,
			p.IsActive, p.Nickname, p.Avatar, p.Metadata, p.CreatedAt)
		if err != nil {
			b.Fatalf("seed-delete: %v", err)
		}
		id, _ := res.LastInsertId()
		b.StartTimer()

		if _, err := db.Exec(`DELETE FROM users WHERE id = ?`, id); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func sqlxBulkInsert(b *testing.B, n int) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	const cols = "(?, ?, ?, ?, ?, ?, ?, ?, ?)"
	var sb strings.Builder
	sb.Grow(64 + n*len(cols)+1)
	sb.WriteString(`INSERT INTO users (name, email, age, score, is_active, nickname, avatar, metadata, created_at) VALUES `)
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(cols)
	}
	stmt := sb.String()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		args := make([]any, 0, n*9)
		for j := range n {
			p := shared.MakeUser(shared.SeedSize + 100_000 + i*n + j)
			args = append(args, p.Name, p.Email, p.Age, p.Score,
				p.IsActive, p.Nickname, p.Avatar, p.Metadata, p.CreatedAt)
		}
		if _, err := db.Exec(stmt, args...); err != nil {
			b.Fatalf("bulk insert: %v", err)
		}
	}
}

func BenchmarkSqlx_BulkInsert100(b *testing.B)  { sqlxBulkInsert(b, 100) }
func BenchmarkSqlx_BulkInsert1000(b *testing.B) { sqlxBulkInsert(b, 1000) }

func BenchmarkSqlx_FindWhereOrderLimit(b *testing.B) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var rows []SqlxUser
		if err := db.Select(&rows,
			`SELECT * FROM users WHERE age > ? AND is_active = ? ORDER BY score DESC LIMIT 50`,
			30, true); err != nil {
			b.Fatalf("select: %v", err)
		}
		if len(rows) == 0 {
			b.Fatal("zero rows")
		}
	}
}

func BenchmarkSqlx_TxInsert100(b *testing.B) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		tx, err := db.Beginx()
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		for j := range 100 {
			p := shared.MakeUser(shared.SeedSize + 200_000 + i*100 + j)
			if _, err := tx.Exec(sqlxInsertUser, p.Name, p.Email, p.Age, p.Score,
				p.IsActive, p.Nickname, p.Avatar, p.Metadata, p.CreatedAt); err != nil {
				tx.Rollback()
				b.Fatalf("tx insert: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

// BenchmarkSqlx_EagerLoadHasMany hand-stitches users + posts in two queries.
// sqlx has no ORM-level eager loader, so this is the idiomatic equivalent —
// label it that way in the results table.
func BenchmarkSqlx_EagerLoadHasMany(b *testing.B) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var users []SqlxUser
		if err := db.Select(&users, `SELECT * FROM users LIMIT 100`); err != nil {
			b.Fatalf("select users: %v", err)
		}
		idArgs := make([]any, len(users))
		idx := make(map[int64]int, len(users))
		holders := make([]string, len(users))
		for i, u := range users {
			idArgs[i] = u.ID
			idx[u.ID] = i
			holders[i] = "?"
		}
		postsByUser := make(map[int64][]SqlxPost, len(users))
		var posts []SqlxPost
		q := `SELECT * FROM posts WHERE user_id IN (` + strings.Join(holders, ",") + `)`
		if err := db.Select(&posts, q, idArgs...); err != nil {
			b.Fatalf("select posts: %v", err)
		}
		for _, p := range posts {
			postsByUser[p.UserID] = append(postsByUser[p.UserID], p)
		}
		if len(users) != 100 {
			b.Fatalf("want 100, got %d", len(users))
		}
	}
}

func BenchmarkSqlx_EagerLoadBelongsTo(b *testing.B) {
	db, _ := sqlxOpen(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var posts []SqlxPost
		if err := db.Select(&posts, `SELECT * FROM posts LIMIT 100`); err != nil {
			b.Fatalf("select posts: %v", err)
		}
		seen := make(map[int64]struct{}, len(posts))
		idArgs := make([]any, 0, len(posts))
		holders := make([]string, 0, len(posts))
		for _, p := range posts {
			if _, ok := seen[p.UserID]; ok {
				continue
			}
			seen[p.UserID] = struct{}{}
			idArgs = append(idArgs, p.UserID)
			holders = append(holders, "?")
		}
		var users []SqlxUser
		q := `SELECT * FROM users WHERE id IN (` + strings.Join(holders, ",") + `)`
		if err := db.Select(&users, q, idArgs...); err != nil {
			b.Fatalf("select users: %v", err)
		}
		if len(posts) != 100 {
			b.Fatalf("want 100, got %d", len(posts))
		}
	}
}
