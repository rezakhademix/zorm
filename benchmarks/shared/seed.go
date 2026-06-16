package shared

import (
	"database/sql"
	"fmt"
)

// SeedRaw populates users + posts via plain database/sql. Every ORM bench
// shares this seeder so the read-side workloads see identical rows regardless
// of which library is being measured.
//
// Returns the slice of user IDs (1..SeedSize) so callers can pick PKs without
// re-querying.
func SeedRaw(db *sql.DB) ([]int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	userStmt, err := tx.Prepare(`INSERT INTO users
		(name, email, age, score, is_active, nickname, avatar, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("prepare users: %w", err)
	}
	defer userStmt.Close()

	postStmt, err := tx.Prepare(`INSERT INTO posts
		(user_id, title, body, views, rating, published, tags, cover, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("prepare posts: %w", err)
	}
	defer postStmt.Close()

	ids := make([]int64, 0, SeedSize)
	for i := 1; i <= SeedSize; i++ {
		u := MakeUser(i)
		res, err := userStmt.Exec(u.Name, u.Email, u.Age, u.Score, boolToInt(u.IsActive),
			u.Nickname, u.Avatar, u.Metadata, u.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("insert user %d: %w", i, err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		for j := range PostsPerUser {
			p := MakePost(id, j)
			if _, err := postStmt.Exec(p.UserID, p.Title, p.Body, p.Views, p.Rating,
				boolToInt(p.Published), p.Tags, p.Cover, p.CreatedAt); err != nil {
				return nil, fmt.Errorf("insert post %d/%d: %w", id, j, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
