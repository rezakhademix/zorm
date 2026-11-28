package zorm

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSyncReadFailure_NoWritesFromIncompletePivotState(t *testing.T) {
	failures := []struct {
		name string
		err  error
	}{
		{"driver", errDatabaseFailure},
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
	}
	targets := []struct {
		name string
		ids  []any
	}{
		{"clear", nil},
		{"keep", []any{1, 2}},
		{"replace", []any{9}},
		{"duplicates", []any{9, 9}},
	}
	for _, failure := range failures {
		for _, yielded := range []int{0, 1, 3} {
			for _, target := range targets {
				for _, callerTx := range []bool{false, true} {
					name := fmt.Sprintf("%s/rows=%d/%s/callerTx=%v", failure.name, yielded, target.name, callerTx)
					t.Run(name, func(t *testing.T) {
						begins, commits, rollbacks, writes, closes := 0, 0, 0, 0, 0
						rows := &failureRows{columns: []string{"child_id"}, err: failure.err, onClose: func() { closes++ }}
						for i := 0; i < yielded; i++ {
							rows.values = append(rows.values, []driver.Value{int64(i + 1)})
						}
						state := &failureDB{
							query:      func(string) (driver.Rows, error) { return rows, nil },
							onBegin:    func() { begins++ },
							onCommit:   func() { commits++ },
							onRollback: func() { rollbacks++ },
							onExec: func(query string) {
								if !strings.HasSuffix(query, "FOR UPDATE") {
									writes++
								}
							},
						}
						db := openFailureDB(t, state)
						ctx := context.Background()
						m := New[failureParent]().SetDB(db)
						var finish func() error
						if callerTx {
							tx, err := db.BeginTx(ctx, nil)
							if err != nil {
								t.Fatal(err)
							}
							defer tx.Rollback()
							m = m.WithTx(&Tx{Tx: tx, ctx: ctx})
							finish = tx.Rollback
						}
						err := m.Sync(ctx, failureParentEntity(), "Peers", target.ids, nil)
						if !errors.Is(err, failure.err) {
							t.Errorf("iteration failure was lost: %v", err)
						}
						if writes != 0 || closes != 1 || begins != 1 || commits != 0 {
							t.Errorf("unsafe side effects or leaked rows: writes=%d closes=%d begins=%d commits=%d", writes, closes, begins, commits)
						}
						wantRollbacks := 1
						if callerTx {
							wantRollbacks = 0
						}
						if rollbacks != wantRollbacks {
							t.Errorf("transaction ownership violated: rollbacks=%d want=%d", rollbacks, wantRollbacks)
						}
						if finish != nil {
							if err := finish(); err != nil {
								t.Errorf("caller transaction was already completed by Sync: %v", err)
							}
						}
					})
				}
			}
		}
	}
}

func TestSyncRegression_ValidResultsRemainIdempotent(t *testing.T) {
	tests := []struct {
		name   string
		ids    []any
		want   []int
		writes int
	}{
		{"unchanged", []any{1, 2}, []int{1, 2}, 0},
		{"clear", nil, nil, 2},
		{"replace", []any{3}, []int{3}, 3},
		{"duplicates", []any{1, 1, 3, 3}, []int{1, 3}, 2},
		{"numeric widths", []any{int64(1), int(1), int64(3), int(3)}, []int{1, 3}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupSyncDB(t)
			_, err := db.Exec(`INSERT INTO sync_role_user VALUES (2, 2);
				CREATE TABLE sync_write_audit (operation TEXT);
				CREATE TRIGGER sync_insert_audit AFTER INSERT ON sync_role_user BEGIN INSERT INTO sync_write_audit VALUES ('insert'); END;
				CREATE TRIGGER sync_delete_audit AFTER DELETE ON sync_role_user BEGIN INSERT INTO sync_write_audit VALUES ('delete'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			m := New[SyncUser]().SetDB(db)
			for attempt := 0; attempt < 2; attempt++ {
				if err := m.Sync(context.Background(), &SyncUser{ID: 1}, "Roles", tt.ids, nil); err != nil {
					t.Fatal(err)
				}
				if got := syncPivotIDs(t, db, 1); !equalInts(got, tt.want) {
					t.Errorf("wrong associations: got %v want %v", got, tt.want)
				}
				var writes int
				if err := db.QueryRow(`SELECT COUNT(*) FROM sync_write_audit`).Scan(&writes); err != nil || writes != tt.writes {
					t.Fatalf("redundant or missing writes: attempt=%d writes=%d want=%d err=%v", attempt, writes, tt.writes, err)
				}
			}
			if got := syncPivotIDs(t, db, 2); !equalInts(got, []int{2}) {
				t.Errorf("Sync changed another parent's associations: %v", got)
			}
		})
	}
}

func TestSyncReadFailure_PreCanceledRequestPreservesAssociations(t *testing.T) {
	db := setupSyncDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := New[SyncUser]().SetDB(db).Sync(ctx, &SyncUser{ID: 1}, "Roles", []any{3}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("request cancellation lost: %v", err)
	}
	if got := syncPivotIDs(t, db, 1); !equalInts(got, []int{1, 2}) {
		t.Errorf("canceled Sync changed associations: %v", got)
	}
}
