package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// caItem exercises the created_at rule on every UPDATE path: a zero created_at
// must never reach the database, while an explicit non-zero value still does.
type caItem struct {
	ID        int       `zorm:"primaryKey"`
	Name      string    `zorm:"column:name"`
	CreatedAt time.Time `zorm:"column:created_at"`
	UpdatedAt time.Time `zorm:"column:updated_at"`
}

func (caItem) TableName() string { return "ca_items" }

// caOnly has no writable column other than created_at, so dropping created_at
// leaves the SET list empty.
type caOnly struct {
	ID        int       `zorm:"primaryKey"`
	CreatedAt time.Time `zorm:"column:created_at"`
}

func (caOnly) TableName() string { return "ca_only" }

// caSeeded is the creation timestamp every subtest seeds and expects to survive.
var caSeeded = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

func setupCreatedAtDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE ca_items (
			id INTEGER PRIMARY KEY,
			name TEXT,
			created_at DATETIME,
			updated_at DATETIME
		);
		CREATE TABLE ca_only (
			id INTEGER PRIMARY KEY,
			created_at DATETIME
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup DB: %v", err)
	}

	seed := &caItem{ID: 1, Name: "orig", CreatedAt: caSeeded, UpdatedAt: caSeeded}
	if err := New[caItem]().SetDB(db).Create(context.Background(), seed); err != nil {
		t.Fatalf("failed to seed row: %v", err)
	}

	return db
}

// storedCreatedAt reads the persisted created_at for row id=1.
func storedCreatedAt(t *testing.T, db *sql.DB) time.Time {
	t.Helper()

	row, err := New[caItem]().SetDB(db).Where("id", 1).First(context.Background())
	if err != nil {
		t.Fatalf("re-fetch failed: %v", err)
	}
	return row.CreatedAt
}

func TestCreatedAtNotOverwrittenByUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("Update with zero CreatedAt leaves the stored value", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		// A partially built entity: the caller only knows the PK and the field
		// they want changed.
		if err := New[caItem]().SetDB(db).Update(ctx, &caItem{ID: 1, Name: "changed"}); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", got, caSeeded)
		}

		row, err := New[caItem]().SetDB(db).Where("id", 1).First(ctx)
		if err != nil {
			t.Fatalf("re-fetch failed: %v", err)
		}
		if row.Name != "changed" {
			t.Errorf("name = %q, want %q", row.Name, "changed")
		}
	})

	t.Run("Update with explicit CreatedAt writes it", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		want := time.Date(2021, 6, 7, 8, 9, 10, 0, time.UTC)
		if err := New[caItem]().SetDB(db).Update(ctx, &caItem{ID: 1, Name: "x", CreatedAt: want}); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(want) {
			t.Errorf("created_at = %v, want %v", got, want)
		}
	})

	t.Run("UpdateOrCreate on an existing row leaves the stored value", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		_, err := New[caItem]().SetDB(db).UpdateOrCreate(ctx,
			map[string]any{"id": 1},
			map[string]any{"name": "upserted"},
		)
		if err != nil {
			t.Fatalf("UpdateOrCreate failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", got, caSeeded)
		}
	})

	t.Run("Save with a zeroed CreatedAt leaves the stored value", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		row, err := New[caItem]().SetDB(db).Where("id", 1).First(ctx)
		if err != nil {
			t.Fatalf("First failed: %v", err)
		}

		row.Name = "saved"
		row.CreatedAt = time.Time{}

		if err := New[caItem]().SetDB(db).Save(ctx, row); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", got, caSeeded)
		}
	})

	t.Run("Save with an explicit CreatedAt writes it", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		row, err := New[caItem]().SetDB(db).Where("id", 1).First(ctx)
		if err != nil {
			t.Fatalf("First failed: %v", err)
		}

		want := time.Date(2022, 2, 2, 2, 2, 2, 0, time.UTC)
		row.CreatedAt = want

		if err := New[caItem]().SetDB(db).Save(ctx, row); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(want) {
			t.Errorf("created_at = %v, want %v", got, want)
		}
	})

	t.Run("UpdateColumns with a zero CreatedAt leaves the stored value", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		entity := &caItem{ID: 1, Name: "cols"}
		if err := New[caItem]().SetDB(db).UpdateColumns(ctx, entity, "name", "created_at"); err != nil {
			t.Fatalf("UpdateColumns failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", got, caSeeded)
		}

		row, err := New[caItem]().SetDB(db).Where("id", 1).First(ctx)
		if err != nil {
			t.Fatalf("re-fetch failed: %v", err)
		}
		if row.Name != "cols" {
			t.Errorf("name = %q, want %q", row.Name, "cols")
		}
	})

	t.Run("UpdateColumns with an explicit CreatedAt writes it", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		want := time.Date(2023, 3, 3, 3, 3, 3, 0, time.UTC)
		entity := &caItem{ID: 1, CreatedAt: want}
		if err := New[caItem]().SetDB(db).UpdateColumns(ctx, entity, "created_at"); err != nil {
			t.Fatalf("UpdateColumns failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(want) {
			t.Errorf("created_at = %v, want %v", got, want)
		}
	})

	t.Run("UpdateMany drops a zero created_at", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		err := New[caItem]().SetDB(db).Where("id", 1).UpdateMany(ctx, map[string]any{
			"name":       "many",
			"created_at": time.Time{},
		})
		if err != nil {
			t.Fatalf("UpdateMany failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", got, caSeeded)
		}

		row, err := New[caItem]().SetDB(db).Where("id", 1).First(ctx)
		if err != nil {
			t.Fatalf("re-fetch failed: %v", err)
		}
		if row.Name != "many" {
			t.Errorf("name = %q, want %q", row.Name, "many")
		}
	})

	t.Run("UpdateMany writes an explicit created_at", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		want := time.Date(2024, 4, 4, 4, 4, 4, 0, time.UTC)
		err := New[caItem]().SetDB(db).Where("id", 1).UpdateMany(ctx, map[string]any{
			"created_at": want,
		})
		if err != nil {
			t.Fatalf("UpdateMany failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(want) {
			t.Errorf("created_at = %v, want %v", got, want)
		}
	})

	t.Run("UpdateManyByKey drops zero created_at entries", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		err := New[caItem]().SetDB(db).UpdateManyByKey(ctx, "id", "created_at", map[int]time.Time{
			1: {},
		})
		if err != nil {
			t.Fatalf("UpdateManyByKey failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", got, caSeeded)
		}
	})

	t.Run("UpdateManyByKey writes explicit created_at entries", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		want := time.Date(2025, 5, 5, 5, 5, 5, 0, time.UTC)
		err := New[caItem]().SetDB(db).UpdateManyByKey(ctx, "id", "created_at", map[int]time.Time{
			1: want,
		})
		if err != nil {
			t.Fatalf("UpdateManyByKey failed: %v", err)
		}

		if got := storedCreatedAt(t, db); !got.Equal(want) {
			t.Errorf("created_at = %v, want %v", got, want)
		}
	})

	t.Run("Update on a created_at-only model emits no statement", func(t *testing.T) {
		db := setupCreatedAtDB(t)

		if err := New[caOnly]().SetDB(db).Create(ctx, &caOnly{ID: 1, CreatedAt: caSeeded}); err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		// Every writable column drops out, so the SET list is empty: the call
		// must be a no-op rather than invalid SQL.
		if err := New[caOnly]().SetDB(db).Update(ctx, &caOnly{ID: 1}); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		row, err := New[caOnly]().SetDB(db).Where("id", 1).First(ctx)
		if err != nil {
			t.Fatalf("re-fetch failed: %v", err)
		}
		if !row.CreatedAt.Equal(caSeeded) {
			t.Errorf("created_at = %v, want unchanged %v", row.CreatedAt, caSeeded)
		}
	})
}
