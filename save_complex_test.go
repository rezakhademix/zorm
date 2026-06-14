package zorm

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// ---------- shared fixtures ----------

type complexItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Email string
	Age   int
	Score int
	Notes string
}

func (complexItem) TableName() string { return "complex_items" }

type complexVersionedItem struct {
	ID      int `zorm:"primaryKey"`
	Name    string
	Counter int
	Version int64 `zorm:"version"`
}

func (complexVersionedItem) TableName() string { return "complex_versioned" }

type complexU64Versioned struct {
	ID      int `zorm:"primaryKey"`
	Name    string
	Version uint64 `zorm:"version"`
}

func (complexU64Versioned) TableName() string { return "complex_u64_versioned" }

func setupComplexDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	// SQLite in-memory + multiple goroutines: serialize writes via single conn
	// to avoid "database is locked" under -race.
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE complex_items (
			id    INTEGER PRIMARY KEY,
			name  TEXT,
			email TEXT,
			age   INTEGER,
			score INTEGER,
			notes TEXT
		);
		INSERT INTO complex_items (name, email, age, score, notes)
		VALUES ('A', 'a@x', 10, 100, 'n1'),
		       ('B', 'b@x', 20, 200, 'n2'),
		       ('C', 'c@x', 30, 300, 'n3');

		CREATE TABLE complex_versioned (
			id      INTEGER PRIMARY KEY,
			name    TEXT,
			counter INTEGER NOT NULL DEFAULT 0,
			version INTEGER NOT NULL DEFAULT 1
		);
		INSERT INTO complex_versioned (name, counter, version)
		VALUES ('start', 0, 1);

		CREATE TABLE complex_u64_versioned (
			id      INTEGER PRIMARY KEY,
			name    TEXT,
			version INTEGER NOT NULL DEFAULT 1
		);
		INSERT INTO complex_u64_versioned (name, version)
		VALUES ('u64', 1);
	`)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	return db
}

// ======================================================================
// Complex Optimistic Concurrency tests
// ======================================================================

// 1. Many concurrent writers race on the same row. Exactly one Save must win;
// every other must return ErrOptimisticLock. Final DB version == 2.
func TestSave_Versioned_ConcurrentWritersExactlyOneWins(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	const writers = 10
	ctx := context.Background()

	// Each goroutine loads the row independently (so each has its own
	// dirty-tracking baseline), mutates, and races to Save.
	loaded := make([]*complexVersionedItem, writers)
	for i := 0; i < writers; i++ {
		row, err := New[complexVersionedItem]().SetDB(db).Find(ctx, 1)
		if err != nil {
			t.Fatalf("load #%d: %v", i, err)
		}
		loaded[i] = row
	}

	var (
		wg        sync.WaitGroup
		successes atomic.Int32
		conflicts atomic.Int32
		otherErrs atomic.Int32
		startGate = make(chan struct{})
	)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			loaded[i].Counter = i + 1
			err := New[complexVersionedItem]().SetDB(db).Save(ctx, loaded[i])
			switch {
			case err == nil:
				successes.Add(1)
			case IsOptimisticLock(err):
				conflicts.Add(1)
			default:
				otherErrs.Add(1)
			}
		}()
	}
	close(startGate)
	wg.Wait()

	if otherErrs.Load() != 0 {
		t.Fatalf("got %d non-lock errors, want 0", otherErrs.Load())
	}
	if successes.Load() != 1 {
		t.Errorf("successes = %d, want 1", successes.Load())
	}
	if conflicts.Load() != writers-1 {
		t.Errorf("conflicts = %d, want %d", conflicts.Load(), writers-1)
	}

	var dbVersion int64
	if err := db.QueryRow(`SELECT version FROM complex_versioned WHERE id = 1`).Scan(&dbVersion); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if dbVersion != 2 {
		t.Errorf("final db version = %d, want 2", dbVersion)
	}
}

// 2. Retry-loop pattern: on conflict, reload + reapply intent + save. The
// loop must converge and the final DB state must contain both writers'
// changes (one to Name, one to Counter).
func TestSave_Versioned_RetryLoopRecoversFromConflict(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	mk := func() *Model[complexVersionedItem] { return New[complexVersionedItem]().SetDB(db) }

	a, err := mk().Find(ctx, 1)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	b, err := mk().Find(ctx, 1)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}

	// A wins first.
	a.Name = "by-A"
	if err := mk().Save(ctx, a); err != nil {
		t.Fatalf("Save A: %v", err)
	}

	// B mutates a different field and Saves with a stale version => conflict.
	// Retry by reloading, reapplying the intent, and saving again.
	const maxAttempts = 3
	attempt := 0
	for {
		attempt++
		b.Counter = 42
		err := mk().Save(ctx, b)
		if err == nil {
			break
		}
		if !IsOptimisticLock(err) {
			t.Fatalf("Save B attempt %d: unexpected %v", attempt, err)
		}
		if attempt >= maxAttempts {
			t.Fatalf("retry loop did not converge after %d attempts", attempt)
		}
		fresh, ferr := mk().Find(ctx, 1)
		if ferr != nil {
			t.Fatalf("reload B: %v", ferr)
		}
		fresh.Counter = b.Counter // re-apply intent
		b = fresh
	}

	final, err := mk().Find(ctx, 1)
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	if final.Name != "by-A" {
		t.Errorf("Name = %q, want by-A (A's change should survive)", final.Name)
	}
	if final.Counter != 42 {
		t.Errorf("Counter = %d, want 42 (B's retried change)", final.Counter)
	}
	if final.Version != 3 {
		t.Errorf("Version = %d, want 3 (1 initial + 2 successful saves)", final.Version)
	}
}

// 3. Sequential saves on the same entity instance: each round bumps the
// version 1->2->3->4 in DB and in-memory, with no spurious conflicts.
// Validates that the dirty-tracking baseline is refreshed after every Save.
func TestSave_Versioned_SequentialBumpsRefreshBaseline(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexVersionedItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	want := []struct {
		name    string
		counter int
		version int64
	}{
		{"r1", 1, 2},
		{"r2", 2, 3},
		{"r3", 3, 4},
	}

	for i, step := range want {
		row.Name = step.name
		row.Counter = step.counter
		if err := m.Save(ctx, row); err != nil {
			t.Fatalf("round %d Save: %v", i, err)
		}
		if row.Version != step.version {
			t.Errorf("round %d in-memory version = %d, want %d", i, row.Version, step.version)
		}
		var dbVersion int64
		var dbName string
		var dbCounter int
		err := db.QueryRow(`SELECT version, name, counter FROM complex_versioned WHERE id = 1`).
			Scan(&dbVersion, &dbName, &dbCounter)
		if err != nil {
			t.Fatalf("round %d scan: %v", i, err)
		}
		if dbVersion != step.version || dbName != step.name || dbCounter != step.counter {
			t.Errorf("round %d db = (v=%d, name=%q, counter=%d); want (v=%d, name=%q, counter=%d)",
				i, dbVersion, dbName, dbCounter, step.version, step.name, step.counter)
		}
	}
}

// 4. Out-of-band SQL bumps the version (simulating a different service /
// trigger / batch job). The next in-process Save sees the mismatch and
// returns ErrOptimisticLock without overwriting the out-of-band change.
func TestSave_Versioned_OutOfBandUpdateCausesConflict(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexVersionedItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if _, err := db.Exec(`UPDATE complex_versioned SET name = 'oob', version = version + 1 WHERE id = 1`); err != nil {
		t.Fatalf("out-of-band update: %v", err)
	}

	row.Name = "in-process"
	err = m.Save(ctx, row)
	if !errors.Is(err, ErrOptimisticLock) {
		t.Fatalf("expected ErrOptimisticLock, got %v", err)
	}

	var name string
	var version int64
	if err := db.QueryRow(`SELECT name, version FROM complex_versioned WHERE id = 1`).Scan(&name, &version); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if name != "oob" {
		t.Errorf("name = %q, want oob (out-of-band write must not be clobbered)", name)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2 (only out-of-band bump survived)", version)
	}
}

// 5. uint64 version-field kind. Confirms isVersionableKind covers the
// unsigned path end-to-end (find -> save -> increment in DB + in memory).
func TestSave_Versioned_Uint64FieldKind(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexU64Versioned]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if row.Version != 1 {
		t.Fatalf("loaded version = %d, want 1", row.Version)
	}

	row.Name = "changed"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if row.Version != 2 {
		t.Errorf("in-memory version = %d, want 2", row.Version)
	}

	// Second save to confirm baseline + increment work twice.
	row.Name = "changed-again"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	if row.Version != 3 {
		t.Errorf("in-memory version after 2nd save = %d, want 3", row.Version)
	}

	var dbVer int64
	if err := db.QueryRow(`SELECT version FROM complex_u64_versioned WHERE id = 1`).Scan(&dbVer); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if dbVer != 3 {
		t.Errorf("db version = %d, want 3", dbVer)
	}
}

// Companion to #3 above and to the no-op contract: a versioned row with no
// dirty fields must not bump the version. Documents the enterprise
// convention (Hibernate / EF Core skip the round-trip entirely).
func TestSave_Versioned_NoChangesIsNoOp(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexVersionedItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if row.Version != 1 {
		t.Errorf("in-memory version moved to %d on no-op Save (should stay 1)", row.Version)
	}
	var dbVer int64
	if err := db.QueryRow(`SELECT version FROM complex_versioned WHERE id = 1`).Scan(&dbVer); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if dbVer != 1 {
		t.Errorf("db version moved to %d on no-op Save (should stay 1)", dbVer)
	}
}

// ======================================================================
// Complex Save dirty-only tests
// ======================================================================

// 1. Multi-field selective change. Of 5 mutable columns, mutate 3. The
// other 2 must retain out-of-band marker values planted between Find and
// Save, proving the UPDATE set list excluded them.
func TestSave_Dirty_OnlyChangedSubsetOfManyColumns(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Markers on columns we will NOT mutate (email, notes).
	if _, err := db.Exec(`UPDATE complex_items SET email = 'marker@x', notes = 'marker-notes' WHERE id = 1`); err != nil {
		t.Fatalf("marker: %v", err)
	}

	row.Name = "Changed"
	row.Age = 99
	row.Score = 555

	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fresh, err := New[complexItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("re-load: %v", err)
	}
	if fresh.Name != "Changed" || fresh.Age != 99 || fresh.Score != 555 {
		t.Errorf("dirty cols not written: %+v", fresh)
	}
	if fresh.Email != "marker@x" {
		t.Errorf("Email = %q, want marker@x (column should not be in SET)", fresh.Email)
	}
	if fresh.Notes != "marker-notes" {
		t.Errorf("Notes = %q, want marker-notes (column should not be in SET)", fresh.Notes)
	}
}

// 2. Save -> mutate -> Save. After the first Save, the dirty baseline must
// be refreshed so the second Save sees only the new mutation as dirty
// (otherwise the first-round columns would be rewritten every time).
func TestSave_Dirty_BaselineRefreshedBetweenRounds(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexItem]().SetDB(db)

	row, err := m.Find(ctx, 1)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	row.Name = "Round1"
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("round 1: %v", err)
	}

	// Plant marker on a column we touched in round 1 but will NOT touch in
	// round 2. If the baseline wasn't refreshed, round 2 would still treat
	// Name as dirty and overwrite this marker.
	if _, err := db.Exec(`UPDATE complex_items SET name = 'marker-name' WHERE id = 1`); err != nil {
		t.Fatalf("marker: %v", err)
	}

	row.Age = 77 // only Age is dirty for round 2
	if err := m.Save(ctx, row); err != nil {
		t.Fatalf("round 2: %v", err)
	}

	fresh, err := New[complexItem]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("re-load: %v", err)
	}
	if fresh.Name != "marker-name" {
		t.Errorf("Name = %q, want marker-name (baseline must refresh after Save)", fresh.Name)
	}
	if fresh.Age != 77 {
		t.Errorf("Age = %d, want 77", fresh.Age)
	}
}

// 3. Omit() must drop columns from Save even when they are dirty.
func TestSave_Dirty_OmitDropsDirtyColumn(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	m := New[complexItem]().SetDB(db)

	row, err := m.Find(ctx, 2)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Marker on score; we WILL mutate score in memory but Omit it on Save.
	if _, err := db.Exec(`UPDATE complex_items SET score = 1234 WHERE id = 2`); err != nil {
		t.Fatalf("marker: %v", err)
	}

	row.Name = "Kept"
	row.Score = 9999 // dirty in memory but will be omitted

	if err := m.Omit("score").Save(ctx, row); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fresh, err := New[complexItem]().SetDB(db).Find(ctx, 2)
	if err != nil {
		t.Fatalf("re-load: %v", err)
	}
	if fresh.Name != "Kept" {
		t.Errorf("Name = %q, want Kept", fresh.Name)
	}
	if fresh.Score != 1234 {
		t.Errorf("Score = %d, want 1234 (Omit must exclude even dirty columns)", fresh.Score)
	}
}

// 4. Saving multiple entities inside a transaction must persist all on
// commit. Verifies WithTx + Save composition across multiple Saves.
func TestSave_Dirty_MultipleSavesCommitInTransaction(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	mk := func() *Model[complexItem] { return New[complexItem]().SetDB(db) }

	r1, err := mk().Find(ctx, 1)
	if err != nil {
		t.Fatalf("load 1: %v", err)
	}
	r2, err := mk().Find(ctx, 2)
	if err != nil {
		t.Fatalf("load 2: %v", err)
	}
	r3, err := mk().Find(ctx, 3)
	if err != nil {
		t.Fatalf("load 3: %v", err)
	}

	err = mk().Transaction(ctx, func(tx *Tx) error {
		r1.Name = "tx1"
		r2.Name = "tx2"
		r3.Name = "tx3"
		if err := mk().WithTx(tx).Save(ctx, r1); err != nil {
			return err
		}
		if err := mk().WithTx(tx).Save(ctx, r2); err != nil {
			return err
		}
		return mk().WithTx(tx).Save(ctx, r3)
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	for id, want := range map[int]string{1: "tx1", 2: "tx2", 3: "tx3"} {
		var name string
		if err := db.QueryRow(`SELECT name FROM complex_items WHERE id = ?`, id).Scan(&name); err != nil {
			t.Fatalf("scan id=%d: %v", id, err)
		}
		if name != want {
			t.Errorf("id=%d name = %q, want %q", id, name, want)
		}
	}
}

// 5. Statement-cache path. Save with stmtCache enabled across several rounds
// must keep producing correct results (exercising prepareStmtForWrite +
// cache reuse). The same-shape SQL (same dirty column set) should hit the
// cache; a different dirty shape produces a different prepared statement.
func TestSave_Dirty_WithStmtCacheReusesPreparedStmt(t *testing.T) {
	db := setupComplexDB(t)
	defer db.Close()

	ctx := context.Background()
	cache := NewStmtCache(16)
	defer cache.Close()

	mk := func() *Model[complexItem] {
		return New[complexItem]().SetDB(db).WithStmtCache(cache)
	}

	// Two rounds with the same dirty shape (just Name) and one round with a
	// different shape (Name + Age) to exercise both cache hit and miss.
	r1, err := mk().Find(ctx, 1)
	if err != nil {
		t.Fatalf("load 1: %v", err)
	}
	r1.Name = "n1"
	if err := mk().Save(ctx, r1); err != nil {
		t.Fatalf("save 1: %v", err)
	}

	r2, err := mk().Find(ctx, 2)
	if err != nil {
		t.Fatalf("load 2: %v", err)
	}
	r2.Name = "n2"
	if err := mk().Save(ctx, r2); err != nil {
		t.Fatalf("save 2: %v", err)
	}

	r3, err := mk().Find(ctx, 3)
	if err != nil {
		t.Fatalf("load 3: %v", err)
	}
	r3.Name = "n3"
	r3.Age = 88
	if err := mk().Save(ctx, r3); err != nil {
		t.Fatalf("save 3: %v", err)
	}

	for id, wantName := range map[int]string{1: "n1", 2: "n2", 3: "n3"} {
		var name string
		if err := db.QueryRow(`SELECT name FROM complex_items WHERE id = ?`, id).Scan(&name); err != nil {
			t.Fatalf("scan id=%d: %v", id, err)
		}
		if name != wantName {
			t.Errorf("id=%d name = %q, want %q", id, name, wantName)
		}
	}
	var age int
	if err := db.QueryRow(`SELECT age FROM complex_items WHERE id = 3`).Scan(&age); err != nil {
		t.Fatalf("scan age: %v", err)
	}
	if age != 88 {
		t.Errorf("id=3 age = %d, want 88", age)
	}
}
