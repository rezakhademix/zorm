package zorm

import (
	"context"
	"strings"
	"testing"
)

func TestSync_PostgresLocksParentBeforeReadingPivot(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()
	m := New[pivotLocalParent]().SetDB(db)
	if err := m.Sync(context.Background(), &pivotLocalParent{ID: 1, LegacyID: 500}, "Roles", nil, nil); err != nil {
		t.Fatal(err)
	}
	queries := log.all()
	lock, read := -1, -1
	for i, query := range queries {
		if strings.Contains(query, "FOR UPDATE") && strings.Contains(query, "legacy_id") {
			lock = i
		}
		if strings.HasPrefix(query, "SELECT role_id FROM legacy_pivot") {
			read = i
		}
	}
	if lock < 0 || read < 0 || lock >= read {
		t.Fatalf("pivot state read without locking the parent first: %v", queries)
	}
}
