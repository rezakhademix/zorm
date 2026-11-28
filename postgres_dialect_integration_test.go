package zorm

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

type postgresRegressionRow struct {
	ID         int
	Code       string
	Number     int
	Enabled    bool
	HappenedAt time.Time
	Children   []*postgresRegressionChild
}

func (postgresRegressionRow) TableName() string { return "zorm_postgres_regression_rows" }
func (postgresRegressionRow) ChildrenRelation() HasMany[postgresRegressionChild] {
	return HasMany[postgresRegressionChild]{ForeignKey: "parent_id"}
}

type postgresRegressionChild struct {
	ID       int
	ParentID int
}

func (postgresRegressionChild) TableName() string { return "zorm_postgres_regression_children" }

func TestPostgres_DialectRegressions(t *testing.T) {
	dsn := os.Getenv("ZORM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ZORM_POSTGRES_DSN is not set")
	}
	for _, driverName := range []string{"pgx", "postgres"} {
		t.Run(driverName, func(t *testing.T) {
			db, err := sql.Open(driverName, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err = db.ExecContext(ctx, `CREATE TEMP TABLE zorm_postgres_regression_rows (id INTEGER PRIMARY KEY, code TEXT, number INTEGER, enabled BOOLEAN, happened_at TIMESTAMPTZ);
			CREATE TEMP TABLE zorm_postgres_regression_children (id INTEGER PRIMARY KEY, parent_id INTEGER);
			INSERT INTO zorm_postgres_regression_rows VALUES (1, 'one', 10, false, '2020-01-01'), (2, 'two', 20, false, '2020-01-01');
			INSERT INTO zorm_postgres_regression_children VALUES (1, 1), (2, 2)`)
			if err != nil {
				t.Fatal(err)
			}
			m := New[postgresRegressionRow]().SetDB(db)
			stamp := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)
			for column, updates := range map[string]any{"number": map[string]int{"one": 42}, "enabled": map[string]bool{"one": true}, "happened_at": map[string]time.Time{"one": stamp}} {
				if err := m.UpdateManyByKey(ctx, "code", column, updates); err != nil {
					t.Errorf("%s: %v", column, err)
				}
			}
			rows, err := m.Clone().WhereIn("id", []any{1, 2}).With("Children").OrderBy("id", "ASC").Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 {
				t.Fatalf("rows: %d", len(rows))
			}
			for _, row := range rows {
				defer ClearOriginals(row)
				if len(row.Children) != 1 || row.Children[0].ParentID != row.ID {
					t.Errorf("wrong children: %+v", row.Children)
				}
				for _, child := range row.Children {
					defer ClearOriginals(child)
				}
			}
			if rows[0].Number != 42 || !rows[0].Enabled || !rows[0].HappenedAt.Equal(stamp) {
				t.Errorf("typed updates were not stored: %+v", rows[0])
			}
		})
	}
}

func TestPostgres_ConcurrentSyncReplacesAssociations(t *testing.T) {
	dsn := os.Getenv("ZORM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ZORM_POSTGRES_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, `CREATE TABLE legacy_parents (id INTEGER PRIMARY KEY, legacy_id INTEGER);
	CREATE TABLE legacy_pivot (user_legacy_id INTEGER, role_id INTEGER);
	INSERT INTO legacy_parents VALUES (1, 500)`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE legacy_pivot, legacy_parents")
	first, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	m := New[pivotLocalParent]().SetDB(db)
	parent := &pivotLocalParent{ID: 1, LegacyID: 500}
	if err := m.WithTx(&Tx{Tx: first, ctx: ctx}).Sync(ctx, parent, "Roles", []any{7}, nil); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	finished := make(chan error, 1)
	go func() { finished <- m.WithTx(&Tx{Tx: second, ctx: ctx}).Sync(ctx, parent, "Roles", []any{8}, nil) }()
	// Without the parent lock the second Sync completes against an empty
	// snapshot while the first insert is uncommitted, merging the two sets.
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
		finished <- nil
	case <-time.After(100 * time.Millisecond):
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT role_id FROM legacy_pivot WHERE user_legacy_id = 500 ORDER BY role_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 8 {
		t.Fatalf("concurrent Sync merged sets: %v", ids)
	}
}
