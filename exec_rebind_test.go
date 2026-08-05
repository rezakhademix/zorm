package zorm

import (
	"context"
	"strings"
	"testing"
)

// Exec was the only execution path that handed the raw query to the driver
// untouched — Get, First, Print and every write path call rebind() — so the
// same Raw(...) query worked through Get and broke through Exec on PostgreSQL.

type ExecItem struct {
	ID   int64  `zorm:"primaryKey"`
	Name string `zorm:"column:name"`
}

func (ExecItem) TableName() string { return "exec_items" }

// TestExec_RebindsPlaceholders pins that Exec converts ? to $n on PostgreSQL
// like every other execution path.
func TestExec_RebindsPlaceholders(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	SetDialect(DialectPostgres)
	t.Cleanup(func() { SetDialect(DialectAuto) })

	ctx := context.Background()
	_, err := New[ExecItem]().SetDB(db).
		Raw("UPDATE exec_items SET name = ? WHERE id = ?", "a", 1).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	got := log.last()
	if strings.Contains(got, "?") {
		t.Errorf("expected ? placeholders to be rebound, got %q", got)
	}
	if !strings.Contains(got, "$1") || !strings.Contains(got, "$2") {
		t.Errorf("expected $1 and $2 in the executed SQL, got %q", got)
	}
}

// TestExec_MatchesPrintPlaceholders pins the actual invariant: rebind is
// dialect-independent (SQLite accepts $N too), so what Exec sends must be
// exactly what Print reports for the same raw query.
func TestExec_MatchesPrintPlaceholders(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	const raw = "UPDATE exec_items SET name = ? WHERE id = ?"
	ctx := context.Background()

	m := New[ExecItem]().SetDB(db).Raw(raw, "a", 1)
	printed, _ := m.Print()

	if _, err := m.Exec(ctx); err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	if got := log.last(); got != printed {
		t.Errorf("Exec sent %q but Print reports %q", got, printed)
	}
}

// TestExec_PreservesJSONOperators guards the rebind escape conventions on this
// path: a PostgreSQL JSON ?| operator must survive Exec untouched.
func TestExec_PreservesJSONOperators(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()

	ctx := context.Background()
	_, err := New[ExecItem]().SetDB(db).
		Raw(`UPDATE exec_items SET name = ? WHERE tags ?| array['a']`, "x").
		Exec(ctx)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	got := log.last()
	if !strings.Contains(got, "?|") {
		t.Errorf("expected the JSON ?| operator to survive, got %q", got)
	}
	if !strings.Contains(got, "$1") {
		t.Errorf("expected the real placeholder to be rebound, got %q", got)
	}
}
