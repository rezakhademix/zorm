package zorm

import (
	"context"
	"strings"
	"testing"
)

// Go lets an outer struct field shadow an embedded one. ModelInfo's Fields and
// Columns maps collapse the pair (last write wins), but OrderedFields — added
// so SQL generation stops depending on map iteration order — appended both, so
// every INSERT and UPDATE named the shadowed column twice. SQLite tolerates
// that; PostgreSQL rejects it outright ("column specified more than once" /
// "multiple assignments to same column").

type SfBase struct {
	CreatedAt string `zorm:"column:created_at"`
}

type SfDoc struct {
	SfBase
	ID        int64  `zorm:"primaryKey"`
	CreatedAt string `zorm:"column:created_at"`
	Title     string `zorm:"column:title"`
}

func (SfDoc) TableName() string { return "sf_docs" }

// TestOrderedFields_DedupesShadowedColumn pins the invariant directly.
func TestOrderedFields_DedupesShadowedColumn(t *testing.T) {
	info := ParseModel[SfDoc]()

	if len(info.OrderedFields) != len(info.Columns) {
		var cols []string
		for _, f := range info.OrderedFields {
			cols = append(cols, f.Column)
		}
		t.Fatalf("OrderedFields has %d entries (%v) but Columns has %d",
			len(info.OrderedFields), cols, len(info.Columns))
	}

	seen := make(map[string]bool, len(info.OrderedFields))
	for _, f := range info.OrderedFields {
		if seen[f.Column] {
			t.Errorf("column %q appears twice in OrderedFields", f.Column)
		}
		seen[f.Column] = true
	}

	// The surviving entry must be the one the maps kept — the outer field.
	for _, f := range info.OrderedFields {
		if f.Column == "created_at" && info.Columns["created_at"] != f {
			t.Error("OrderedFields kept a different created_at entry than Columns")
		}
	}
}

// TestShadowedColumn_InsertNamesColumnOnce covers the generated INSERT.
func TestShadowedColumn_InsertNamesColumnOnce(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	if err := New[SfDoc]().SetDB(db).Create(context.Background(), &SfDoc{Title: "x"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got := log.last()
	if n := strings.Count(got, "created_at"); n != 1 {
		t.Errorf("expected created_at once in the INSERT, got %d: %s", n, got)
	}
}

// TestShadowedColumn_UpdateAssignsColumnOnce covers the generated UPDATE.
func TestShadowedColumn_UpdateAssignsColumnOnce(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	if err := New[SfDoc]().SetDB(db).Update(context.Background(), &SfDoc{ID: 1, Title: "x"}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got := log.last()
	if n := strings.Count(got, "created_at = "); n != 1 {
		t.Errorf("expected one created_at assignment, got %d: %s", n, got)
	}
}

// TestShadowedColumn_BulkInsertNamesColumnOnce covers the bulk path.
func TestShadowedColumn_BulkInsertNamesColumnOnce(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	rows := []*SfDoc{{Title: "a"}, {Title: "b"}}
	if err := New[SfDoc]().SetDB(db).CreateMany(context.Background(), rows); err != nil {
		t.Fatalf("CreateMany failed: %v", err)
	}

	got := log.last()
	cols := got[strings.Index(got, "(")+1 : strings.Index(got, ")")]
	if n := strings.Count(cols, "created_at"); n != 1 {
		t.Errorf("expected created_at once in the column list, got %d: %s", n, cols)
	}
}
