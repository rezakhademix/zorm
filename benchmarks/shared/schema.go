// Package shared holds bench-wide DDL, fixtures, and seed helpers so every
// ORM under test runs against bit-identical tables and bit-identical rows.
package shared

import "database/sql"

// DDL is executed once per fresh :memory: database. The schema is intentionally
// portable across SQLite/Postgres so the same datatype coverage carries over if
// the suite is later parameterized by driver.
const DDL = `
CREATE TABLE users (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT     NOT NULL,
    email        TEXT     NOT NULL UNIQUE,
    age          INTEGER  NOT NULL,
    score        REAL     NOT NULL,
    is_active    INTEGER  NOT NULL,
    nickname     TEXT,
    avatar       BLOB,
    metadata     TEXT     NOT NULL,
    created_at   DATETIME NOT NULL
);

CREATE TABLE posts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER  NOT NULL REFERENCES users(id),
    title        TEXT     NOT NULL,
    body         TEXT     NOT NULL,
    views        INTEGER  NOT NULL,
    rating       REAL     NOT NULL,
    published    INTEGER  NOT NULL,
    tags         TEXT     NOT NULL,
    cover        BLOB,
    created_at   DATETIME NOT NULL
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
`

// ApplyDDL runs DDL against db. Bench setup helpers call this immediately after
// opening :memory:.
func ApplyDDL(db *sql.DB) error {
	_, err := db.Exec(DDL)
	return err
}
