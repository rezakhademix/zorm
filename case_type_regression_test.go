package zorm

import (
	"context"
	"strings"
	"testing"
)

func TestUpdateManyByKey_CaseHasTargetType(t *testing.T) {
	db, log := newRecordingDB()
	defer db.Close()
	if err := New[saveItem]().SetDB(db).UpdateManyByKey(context.Background(), "id", "value", map[int]int{1: 42}); err != nil {
		t.Fatal(err)
	}
	if query := log.last(); !strings.Contains(query, "ELSE value END") {
		t.Fatalf("CASE has no target-column type anchor: %s", query)
	}
}
