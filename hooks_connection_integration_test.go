package zorm

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

type hookQueryKey struct{}

type hookQueryItem struct {
	ID       int
	ParentID int
	Calls    int `zorm:"-"`
}

func (hookQueryItem) TableName() string { return "hook_query_items" }

func (item *hookQueryItem) AfterFind(ctx context.Context) error {
	q := ctx.Value(hookQueryKey{}).(interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	})
	var n int
	if err := q.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
		return err
	}
	item.Calls++
	return nil
}

type hookQueryParent struct {
	ID       int
	Children []*hookQueryItem
}

func (hookQueryParent) TableName() string { return "hook_query_parents" }
func (hookQueryParent) ChildrenRelation() HasMany[hookQueryItem] {
	return HasMany[hookQueryItem]{ForeignKey: "parent_id"}
}

// Buffered reads must release their rows before hooks issue more database work.
func TestAfterFind_ReleasesBufferedConnection(t *testing.T) {
	for _, loader := range []string{"Get", "Find", "With", "Load"} {
		for _, mode := range []string{"direct", "cached", "transaction"} {
			t.Run(loader+"/"+mode, func(t *testing.T) {
				db, err := sql.Open("sqlite3", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				db.SetMaxOpenConns(1)
				_, err = db.Exec(`CREATE TABLE hook_query_items (id INTEGER PRIMARY KEY, parent_id INTEGER);
					CREATE TABLE hook_query_parents (id INTEGER PRIMARY KEY);
					INSERT INTO hook_query_items VALUES (1, 1), (2, 1);
					INSERT INTO hook_query_parents VALUES (1)`)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				read := func(tx *Tx) error {
					var q any = db
					items := New[hookQueryItem]().SetDB(db)
					parents := New[hookQueryParent]().SetDB(db)
					if tx != nil {
						q = tx.Tx
						items = items.WithTx(tx)
						parents = parents.WithTx(tx)
					}
					if mode == "cached" {
						cache := NewStmtCache(10)
						defer cache.Close()
						items.WithStmtCache(cache)
						parents.WithStmtCache(cache)
					}
					ctx := context.WithValue(ctx, hookQueryKey{}, q)
					var results []*hookQueryItem
					var err error
					switch loader {
					case "Get":
						results, err = items.Get(ctx)
					case "Find":
						var item *hookQueryItem
						item, err = items.Find(ctx, 1)
						results = []*hookQueryItem{item}
					case "With":
						var parent *hookQueryParent
						parent, err = parents.With("Children").Find(ctx, 1)
						if err == nil {
							results = parent.Children
						}
					case "Load":
						parent := &hookQueryParent{ID: 1}
						err = parents.Load(ctx, parent, "Children")
						results = parent.Children
					}
					if err != nil {
						return err
					}
					if len(results) == 0 {
						t.Fatal("expected loaded rows")
					}
					for _, item := range results {
						defer ClearOriginals(item)
						if item.Calls != 1 {
							t.Errorf("AfterFind ran %d times, want 1", item.Calls)
						}
					}
					return nil
				}
				if mode == "transaction" {
					err = New[hookQueryItem]().SetDB(db).Transaction(ctx, read)
				} else {
					err = read(nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
