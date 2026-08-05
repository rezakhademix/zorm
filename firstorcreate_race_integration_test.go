package zorm

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// FirstOrCreate / UpdateOrCreate look the row up and then insert it, with no
// transaction and no upsert. A concurrent caller that inserts the same row in
// between made the INSERT fail with a duplicate-key error even though the row
// the caller asked for now exists. These tests reproduce that window
// deterministically: focRacer's BeforeCreate hook inserts the conflicting row
// out of band, which is exactly what the losing caller observes.

type FocUser struct {
	ID    int64  `zorm:"primaryKey"`
	Email string `zorm:"column:email"`
	Name  string `zorm:"column:name"`
}

func (FocUser) TableName() string { return "foc_users" }

// focRacer, when non-nil, runs once just before the next INSERT.
var focRacer func()

// FocRacedUser is FocUser plus the hook that simulates the losing race.
type FocRacedUser struct {
	ID    int64  `zorm:"primaryKey"`
	Email string `zorm:"column:email"`
	Name  string `zorm:"column:name"`
}

func (FocRacedUser) TableName() string { return "foc_users" }

func (u *FocRacedUser) BeforeCreate(ctx context.Context) error {
	if focRacer != nil {
		racer := focRacer
		focRacer = nil
		racer()
	}
	return nil
}

func setupFocDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		focRacer = nil
		db.Close()
	})

	_, err = db.Exec(`
		CREATE TABLE foc_users (
			id    INTEGER PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name  TEXT
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}
	return db
}

func focRowCount(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM foc_users`).Scan(&n); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	return n
}

// TestFirstOrCreate_LosesRaceReturnsWinnersRow covers the race: when the insert
// loses to a concurrent writer, FirstOrCreate must return the row that now
// exists rather than surfacing the duplicate-key error.
func TestFirstOrCreate_LosesRaceReturnsWinnersRow(t *testing.T) {
	db := setupFocDB(t)
	ctx := context.Background()

	focRacer = func() {
		if _, err := db.Exec(`INSERT INTO foc_users (email, name) VALUES ('a@example.com', 'winner')`); err != nil {
			t.Errorf("racing insert failed: %v", err)
		}
	}

	got, err := New[FocRacedUser]().SetDB(db).FirstOrCreate(ctx,
		map[string]any{"email": "a@example.com"},
		map[string]any{"name": "loser"},
	)
	if err != nil {
		t.Fatalf("FirstOrCreate failed after losing the race: %v", err)
	}
	if got.Name != "winner" {
		t.Errorf("expected the winner's row back, got %q", got.Name)
	}
	if got.ID == 0 {
		t.Error("expected the returned row to carry the winner's primary key")
	}
	if n := focRowCount(t, db); n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}

// TestUpdateOrCreate_LosesRaceUpdatesWinnersRow covers the same window for
// UpdateOrCreate, which must fall back to updating the row that won.
func TestUpdateOrCreate_LosesRaceUpdatesWinnersRow(t *testing.T) {
	db := setupFocDB(t)
	ctx := context.Background()

	focRacer = func() {
		if _, err := db.Exec(`INSERT INTO foc_users (email, name) VALUES ('b@example.com', 'winner')`); err != nil {
			t.Errorf("racing insert failed: %v", err)
		}
	}

	got, err := New[FocRacedUser]().SetDB(db).UpdateOrCreate(ctx,
		map[string]any{"email": "b@example.com"},
		map[string]any{"name": "updated"},
	)
	if err != nil {
		t.Fatalf("UpdateOrCreate failed after losing the race: %v", err)
	}
	if got.Name != "updated" {
		t.Errorf("expected the winner's row to be updated, got %q", got.Name)
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM foc_users WHERE email = 'b@example.com'`).Scan(&name); err != nil {
		t.Fatalf("failed to read back row: %v", err)
	}
	if name != "updated" {
		t.Errorf("expected the stored row to be updated, got %q", name)
	}
	if n := focRowCount(t, db); n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}

// TestFirstOrCreate_UnrelatedDuplicateStillErrors guards against swallowing a
// duplicate-key error that the retry cannot resolve: if the conflict is on a
// column the attributes do not select for, the caller must still hear about it.
func TestFirstOrCreate_UnrelatedDuplicateStillErrors(t *testing.T) {
	db := setupFocDB(t)
	ctx := context.Background()

	if _, err := db.Exec(`INSERT INTO foc_users (email, name) VALUES ('taken@example.com', 'existing')`); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}

	_, err := New[FocUser]().SetDB(db).FirstOrCreate(ctx,
		map[string]any{"name": "fresh"},
		map[string]any{"email": "taken@example.com"},
	)
	if err == nil {
		t.Fatal("expected the duplicate-key error to surface")
	}
	if !IsDuplicateKey(err) {
		t.Errorf("expected a duplicate-key error, got %v", err)
	}
}

// TestFirstOrCreate_NoRaceUnaffected guards the ordinary paths.
func TestFirstOrCreate_NoRaceUnaffected(t *testing.T) {
	db := setupFocDB(t)
	ctx := context.Background()

	m := New[FocUser]().SetDB(db)

	created, err := m.FirstOrCreate(ctx,
		map[string]any{"email": "c@example.com"},
		map[string]any{"name": "created"},
	)
	if err != nil {
		t.Fatalf("FirstOrCreate failed: %v", err)
	}
	if created.Name != "created" {
		t.Errorf("expected the created row, got %q", created.Name)
	}

	found, err := m.FirstOrCreate(ctx,
		map[string]any{"email": "c@example.com"},
		map[string]any{"name": "ignored"},
	)
	if err != nil {
		t.Fatalf("second FirstOrCreate failed: %v", err)
	}
	if found.Name != "created" {
		t.Errorf("expected the existing row, got %q", found.Name)
	}
	if n := focRowCount(t, db); n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}
