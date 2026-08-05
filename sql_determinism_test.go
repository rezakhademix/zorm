package zorm

import (
	"context"
	"testing"
	"time"
)

// Go randomizes map iteration order, so anywhere a map decides the ORDER of
// SQL text, two identical calls emit two different statements. That costs a
// prepared-statement cache entry (and a bulkInsertSQLCache entry) per ordering
// and makes Print() unstable for tests and logs.
//
// Two layers: the maps the review named (Where(map) and friends) and
// ModelInfo.Fields, which decides INSERT column order and UPDATE SET order.

// DetItem has enough columns that a random ordering is essentially certain to
// show up within a handful of builds.
type DetItem struct {
	ID        int64     `zorm:"primaryKey"`
	Name      string    `zorm:"column:name"`
	Email     string    `zorm:"column:email"`
	Role      string    `zorm:"column:role"`
	Age       int       `zorm:"column:age"`
	Active    bool      `zorm:"column:active"`
	CreatedAt time.Time `zorm:"column:created_at"`
}

func (DetItem) TableName() string { return "det_items" }

// distinctQueries reports how many different statements the builder produced.
func distinctQueries(qs []string) []string {
	seen := make(map[string]bool, len(qs))
	var out []string
	for _, q := range qs {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	return out
}

const determinismRuns = 50

func assertSingleForm(t *testing.T, what string, queries []string) {
	t.Helper()

	forms := distinctQueries(queries)
	if len(forms) != 1 {
		t.Errorf("%s produced %d different statements across %d builds:\n  %s\n  %s",
			what, len(forms), len(queries), forms[0], forms[1])
	}
}

func TestWhereMap_DeterministicOrder(t *testing.T) {
	queries := make([]string, determinismRuns)
	for i := range queries {
		m := New[DetItem]().Where(map[string]any{
			"name":   "a",
			"email":  "b",
			"role":   "c",
			"age":    1,
			"active": true,
		})
		queries[i], _ = m.Print()
	}
	assertSingleForm(t, "Where(map)", queries)
}

func TestWhereMap_ArgsFollowClauseOrder(t *testing.T) {
	conditions := map[string]any{"name": "a", "email": "b", "role": "c"}

	firstQuery, firstArgs := New[DetItem]().Where(conditions).Print()
	for i := 0; i < determinismRuns; i++ {
		query, args := New[DetItem]().Where(conditions).Print()
		if query != firstQuery {
			t.Fatalf("unstable SQL: %q vs %q", query, firstQuery)
		}
		for j := range args {
			if args[j] != firstArgs[j] {
				t.Fatalf("args diverged from the clause order: %v vs %v", args, firstArgs)
			}
		}
	}
}

func TestScalarWhereMap_DeterministicOrder(t *testing.T) {
	queries := make([]string, determinismRuns)
	for i := range queries {
		q := Query[string]().Table("det_items").Select("name").
			Where(map[string]any{"name": "a", "email": "b", "role": "c", "age": 1})
		queries[i], _ = q.Print()
	}
	assertSingleForm(t, "ScalarQuery.Where(map)", queries)
}

func TestWhereStruct_DeterministicOrder(t *testing.T) {
	queries := make([]string, determinismRuns)
	for i := range queries {
		m := New[DetItem]().Where(&DetItem{Name: "a", Email: "b", Role: "c", Age: 1, Active: true})
		queries[i], _ = m.Print()
	}
	assertSingleForm(t, "Where(struct)", queries)
}

func TestCreate_DeterministicColumnOrder(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		if err := New[DetItem]().SetDB(db).Create(ctx, &DetItem{Name: "a", Email: "b"}); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		queries[i] = log.last()
	}
	assertSingleForm(t, "Create", queries)
}

func TestUpdate_DeterministicSetOrder(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		if err := New[DetItem]().SetDB(db).Update(ctx, &DetItem{ID: 1, Name: "a", Email: "b"}); err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		queries[i] = log.last()
	}
	assertSingleForm(t, "Update", queries)
}

func TestUpdateMany_DeterministicSetOrder(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		err := New[DetItem]().SetDB(db).Where("id", ">", 0).UpdateMany(ctx, map[string]any{
			"name":   "a",
			"email":  "b",
			"role":   "c",
			"age":    1,
			"active": true,
		})
		if err != nil {
			t.Fatalf("UpdateMany failed: %v", err)
		}
		queries[i] = log.last()
	}
	assertSingleForm(t, "UpdateMany", queries)
}

func TestCreateMany_DeterministicColumnOrder(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		rows := []*DetItem{{Name: "a"}, {Name: "b"}}
		if err := New[DetItem]().SetDB(db).CreateMany(ctx, rows); err != nil {
			t.Fatalf("CreateMany failed: %v", err)
		}
		queries[i] = log.last()
	}
	assertSingleForm(t, "CreateMany", queries)
}

func TestBulkInsert_DeterministicColumnOrder(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		rows := []*DetItem{{Name: "a"}, {Name: "b"}}
		if err := New[DetItem]().SetDB(db).BulkInsert(ctx, rows); err != nil {
			t.Fatalf("BulkInsert failed: %v", err)
		}
		queries[i] = log.last()
	}
	assertSingleForm(t, "BulkInsert", queries)
}

// TestFirstOrCreate_DeterministicLookupOrder covers the attributes map, which
// becomes a chain of Where calls.
func TestFirstOrCreate_DeterministicLookupOrder(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		_, err := New[DetItem]().SetDB(db).FirstOrCreate(ctx,
			map[string]any{"name": "a", "email": "b", "role": "c"},
			map[string]any{"age": 1},
		)
		if err != nil {
			t.Fatalf("FirstOrCreate failed: %v", err)
		}
		// The lookup SELECT is the first statement of the call.
		all := log.all()
		queries[i] = all[0]
	}
	assertSingleForm(t, "FirstOrCreate lookup", queries)
}

// TestAttach_DeterministicPivotColumns covers the pivot INSERT, whose extra
// columns came from a map.
func TestAttach_DeterministicPivotColumns(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	pivotData := map[any]map[string]any{
		1: {"note": "n", "weight": 2, "added_by": "me", "source": "s"},
	}

	queries := make([]string, determinismRuns)
	for i := range queries {
		log.reset()
		err := New[SyncUser]().SetDB(db).Attach(ctx, &SyncUser{ID: 1}, "Roles", []any{1}, pivotData)
		if err != nil {
			t.Fatalf("Attach failed: %v", err)
		}
		queries[i] = log.last()
	}
	assertSingleForm(t, "Attach", queries)
}

// TestOrderedFields_MatchesDeclarationOrder pins the ordering rule itself, so a
// future change to schema parsing cannot quietly reintroduce map order.
func TestOrderedFields_MatchesDeclarationOrder(t *testing.T) {
	info := ParseModel[DetItem]()

	want := []string{"id", "name", "email", "role", "age", "active", "created_at"}
	if len(info.OrderedFields) != len(want) {
		t.Fatalf("expected %d ordered fields, got %d", len(want), len(info.OrderedFields))
	}
	for i, col := range want {
		if info.OrderedFields[i].Column != col {
			t.Errorf("field %d: expected %q, got %q", i, col, info.OrderedFields[i].Column)
		}
	}

	// And it must stay a view of the same data the maps hold.
	if len(info.OrderedFields) != len(info.Fields) {
		t.Errorf("OrderedFields (%d) and Fields (%d) disagree", len(info.OrderedFields), len(info.Fields))
	}
	for _, f := range info.OrderedFields {
		if info.Fields[f.Name] != f {
			t.Errorf("field %q in OrderedFields is not the entry in Fields", f.Name)
		}
	}
}

// TestOrderedFields_FlattensEmbedded checks embedded structs land in place.
func TestOrderedFields_FlattensEmbedded(t *testing.T) {
	info := ParseModel[TestModel]()

	if len(info.OrderedFields) != len(info.Fields) {
		t.Fatalf("OrderedFields (%d) and Fields (%d) disagree", len(info.OrderedFields), len(info.Fields))
	}
	seen := make(map[string]bool, len(info.OrderedFields))
	for _, f := range info.OrderedFields {
		if seen[f.Name] {
			t.Errorf("field %q appears twice", f.Name)
		}
		seen[f.Name] = true
	}
}
