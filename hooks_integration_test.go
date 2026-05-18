package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// setupHooksDB creates an in-memory SQLite DB for hook tests.
func setupHooksDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE hook_items (
			id    INTEGER PRIMARY KEY,
			name  TEXT,
			value INTEGER
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup hooks DB: %v", err)
	}
	return db
}

// ---- AfterCreate ----

// HookAfterCreateItem implements AfterCreate.
type HookAfterCreateItem struct {
	ID            int `zorm:"primaryKey"`
	Name          string
	Value         int
	AfterCreateOK bool `zorm:"-"` // not stored; set by hook
}

func (h *HookAfterCreateItem) TableName() string { return "hook_items" }

func (h *HookAfterCreateItem) AfterCreate(ctx context.Context) error {
	h.AfterCreateOK = true
	return nil
}

func TestHook_AfterCreate_CalledAfterInsert(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookAfterCreateItem{Name: "test", Value: 42}

	err := New[HookAfterCreateItem]().SetDB(db).Create(ctx, item)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if item.ID == 0 {
		t.Error("expected ID to be set after Create")
	}
	if !item.AfterCreateOK {
		t.Error("expected AfterCreate to be called, but it was not")
	}
}

// HookAfterCreateError implements AfterCreate that returns an error.
type HookAfterCreateError struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterCreateError) TableName() string { return "hook_items" }

func (h *HookAfterCreateError) AfterCreate(ctx context.Context) error {
	return errors.New("after-create intentional error")
}

func TestHook_AfterCreate_ErrorPropagates(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookAfterCreateError{Name: "fail", Value: 1}

	err := New[HookAfterCreateError]().SetDB(db).Create(ctx, item)
	if err == nil {
		t.Fatal("expected AfterCreate error to propagate, got nil")
	}
	if !errors.Is(err, errors.New("after-create intentional error")) && err.Error() != "after-create intentional error" {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---- AfterCreate in BulkInsert ----

type HookBulkItem struct {
	ID             int `zorm:"primaryKey"`
	Name           string
	Value          int
	AfterCreateCnt int `zorm:"-"`
}

func (h *HookBulkItem) TableName() string { return "hook_items" }

func (h *HookBulkItem) AfterCreate(ctx context.Context) error {
	h.AfterCreateCnt++
	return nil
}

func TestHook_AfterCreate_BulkInsert(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	items := []*HookBulkItem{
		{Name: "a", Value: 1},
		{Name: "b", Value: 2},
		{Name: "c", Value: 3},
	}

	err := New[HookBulkItem]().SetDB(db).BulkInsert(ctx, items)
	if err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}
	for i, item := range items {
		if item.AfterCreateCnt != 1 {
			t.Errorf("item[%d]: expected AfterCreate called once, got %d", i, item.AfterCreateCnt)
		}
	}
}

// ---- BeforeDelete ----

var beforeDeleteCalled bool

type HookBeforeDeleteItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeDeleteItem) TableName() string { return "hook_items" }

func (h *HookBeforeDeleteItem) BeforeDelete(ctx context.Context) error {
	beforeDeleteCalled = true
	return nil
}

func TestHook_BeforeDelete_Called(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	// Insert a row directly
	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (99, 'del', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	beforeDeleteCalled = false
	ctx := context.Background()

	err = New[HookBeforeDeleteItem]().SetDB(db).Where("id", 99).Delete(ctx)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !beforeDeleteCalled {
		t.Error("expected BeforeDelete to be called, but it was not")
	}
}

// HookBeforeDeleteAbort causes delete to abort.
type HookBeforeDeleteAbort struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeDeleteAbort) TableName() string { return "hook_items" }

func (h *HookBeforeDeleteAbort) BeforeDelete(ctx context.Context) error {
	return errors.New("delete aborted by hook")
}

func TestHook_BeforeDelete_Aborts(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (100, 'keep', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	err = New[HookBeforeDeleteAbort]().SetDB(db).Where("id", 100).Delete(ctx)
	if err == nil {
		t.Fatal("expected BeforeDelete to abort the delete, got nil")
	}
	if err.Error() != "delete aborted by hook" {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify the row was NOT deleted
	var count int
	db.QueryRow("SELECT COUNT(*) FROM hook_items WHERE id = 100").Scan(&count)
	if count != 1 {
		t.Errorf("expected row to remain after aborted delete, got count=%d", count)
	}
}

// ---- AfterDelete ----

var afterDeleteCalled bool

type HookAfterDeleteItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterDeleteItem) TableName() string { return "hook_items" }

func (h *HookAfterDeleteItem) AfterDelete(ctx context.Context) error {
	afterDeleteCalled = true
	return nil
}

func TestHook_AfterDelete_CalledAfterSuccessfulDelete(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (101, 'bye', 5)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	afterDeleteCalled = false
	ctx := context.Background()

	err = New[HookAfterDeleteItem]().SetDB(db).Where("id", 101).Delete(ctx)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !afterDeleteCalled {
		t.Error("expected AfterDelete to be called, but it was not")
	}
}

// ---- AfterFind ----

type HookAfterFindItem struct {
	ID           int `zorm:"primaryKey"`
	Name         string
	Value        int
	AfterFindCnt int `zorm:"-"`
}

func (h *HookAfterFindItem) TableName() string { return "hook_items" }

func (h *HookAfterFindItem) AfterFind(ctx context.Context) error {
	h.AfterFindCnt++
	return nil
}

func TestHook_AfterFind_CalledForEachRow_Get(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`
		INSERT INTO hook_items (id, name, value) VALUES
		(1, 'x', 1), (2, 'y', 2), (3, 'z', 3);
	`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	results, err := New[HookAfterFindItem]().SetDB(db).Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if r.AfterFindCnt != 1 {
			t.Errorf("result[%d]: expected AfterFind called once, got %d", i, r.AfterFindCnt)
		}
	}
}

func TestHook_AfterFind_CalledForFirst(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (10, 'first', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	result, err := New[HookAfterFindItem]().SetDB(db).Where("id", 10).First(ctx)
	if err != nil {
		t.Fatalf("First failed: %v", err)
	}
	if result.AfterFindCnt != 1 {
		t.Errorf("expected AfterFind called once for First(), got %d", result.AfterFindCnt)
	}
}

// HookAfterFindError makes AfterFind return an error to test propagation.
type HookAfterFindError struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterFindError) TableName() string { return "hook_items" }

func (h *HookAfterFindError) AfterFind(ctx context.Context) error {
	return errors.New("after-find intentional error")
}

func TestHook_AfterFind_ErrorAborts(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (20, 'err', 20)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	_, err = New[HookAfterFindError]().SetDB(db).Get(ctx)
	if err == nil {
		t.Fatal("expected AfterFind error to propagate from Get(), got nil")
	}
}

func TestHook_AfterFind_CalledInCursor(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`
		INSERT INTO hook_items (id, name, value) VALUES
		(30, 'cur1', 1), (31, 'cur2', 2);
	`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	cursor, err := New[HookAfterFindItem]().SetDB(db).OrderBy("id", "ASC").Cursor(ctx)
	if err != nil {
		t.Fatalf("Cursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		item, err := cursor.Scan(ctx)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if item.AfterFindCnt != 1 {
			t.Errorf("cursor item: expected AfterFind called once, got %d", item.AfterFindCnt)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 items via cursor, got %d", count)
	}
}

// ---- H1: AfterCreate NOT called when INSERT fails ----

// HookAfterCreateFailItem has AfterCreate that marks itself; used to verify
// that AfterCreate is skipped when the INSERT fails.
type HookAfterCreateFailItem struct {
	ID            int `zorm:"primaryKey"`
	Name          string
	Value         int
	AfterCreateOK bool `zorm:"-"`
}

func (h *HookAfterCreateFailItem) TableName() string { return "hook_items" }

func (h *HookAfterCreateFailItem) AfterCreate(ctx context.Context) error {
	h.AfterCreateOK = true
	return nil
}

func TestHook_AfterCreate_NotCalledWhenInsertFails(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	// Seed a row with id=500 to trigger a PRIMARY KEY conflict.
	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (500, 'existing', 10)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	// id=500 is non-zero so it is included in the INSERT; conflict → INSERT fails.
	item := &HookAfterCreateFailItem{ID: 500, Name: "duplicate", Value: 20}
	err = New[HookAfterCreateFailItem]().SetDB(db).Create(ctx, item)
	if err == nil {
		t.Fatal("expected INSERT error due to PRIMARY KEY conflict, got nil")
	}
	if item.AfterCreateOK {
		t.Error("expected AfterCreate NOT to be called when INSERT fails, but it was")
	}
}

// ---- H2: BeforeCreate can modify entity fields before INSERT ----

// HookBeforeCreateModify rewrites Name in BeforeCreate; the modified value
// must be visible in both the returned entity and the persisted row.
type HookBeforeCreateModify struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeCreateModify) TableName() string { return "hook_items" }

func (h *HookBeforeCreateModify) BeforeCreate(ctx context.Context) error {
	h.Name = "modified"
	return nil
}

func TestHook_BeforeCreate_CanModifyEntity(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookBeforeCreateModify{Name: "original", Value: 1}

	err := New[HookBeforeCreateModify]().SetDB(db).Create(ctx, item)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// The hook runs before the INSERT query is built, so the mutation is picked up.
	if item.Name != "modified" {
		t.Errorf("expected entity Name='modified' after BeforeCreate, got %q", item.Name)
	}

	// Confirm the modified name was persisted.
	var name string
	if err := db.QueryRow("SELECT name FROM hook_items WHERE id = ?", item.ID).Scan(&name); err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "modified" {
		t.Errorf("expected persisted name='modified', got %q", name)
	}
}

// ---- H3: AfterFind NOT called for an empty result set ----

var afterFindEmptyCalled bool

// HookAfterFindEmptyItem uses a package-level flag so the test can detect
// whether AfterFind fired even though no rows were returned.
type HookAfterFindEmptyItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterFindEmptyItem) TableName() string { return "hook_items" }

func (h *HookAfterFindEmptyItem) AfterFind(ctx context.Context) error {
	afterFindEmptyCalled = true
	return nil
}

func TestHook_AfterFind_EmptyResultSet(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	afterFindEmptyCalled = false
	ctx := context.Background()

	results, err := New[HookAfterFindEmptyItem]().SetDB(db).Where("id", 9999).Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
	if afterFindEmptyCalled {
		t.Error("expected AfterFind NOT to be called for empty result set, but it was")
	}
}

// ---- H4: AfterFind can modify the returned entity ----

// HookAfterFindModify overwrites Value in AfterFind; the caller must see the
// modified value because the hook runs before the entity is appended to results.
type HookAfterFindModify struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookAfterFindModify) TableName() string { return "hook_items" }

func (h *HookAfterFindModify) AfterFind(ctx context.Context) error {
	h.Value = 999
	return nil
}

func TestHook_AfterFind_CanModifyReturnedEntity(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (50, 'modify', 1)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	ctx := context.Background()
	result, err := New[HookAfterFindModify]().SetDB(db).Where("id", 50).First(ctx)
	if err != nil {
		t.Fatalf("First failed: %v", err)
	}
	// AfterFind sets Value=999; the mutation must be visible to the caller.
	if result.Value != 999 {
		t.Errorf("expected AfterFind to set Value=999, got %d", result.Value)
	}
}

// ---- H5: ForceDeleteAll() triggers BeforeDelete and AfterDelete ----

var forceDeleteBeforeCalled bool
var forceDeleteAfterCalled bool

// HookForceDeleteItem tracks hook invocations via package-level flags so the
// test can verify both hooks fire through the ForceDeleteAll code path.
type HookForceDeleteItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookForceDeleteItem) TableName() string { return "hook_items" }

func (h *HookForceDeleteItem) BeforeDelete(ctx context.Context) error {
	forceDeleteBeforeCalled = true
	return nil
}

func (h *HookForceDeleteItem) AfterDelete(ctx context.Context) error {
	forceDeleteAfterCalled = true
	return nil
}

func TestHook_ForceDeleteAll_TriggersHooks(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (200, 'force', 1)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	forceDeleteBeforeCalled = false
	forceDeleteAfterCalled = false
	ctx := context.Background()

	err = New[HookForceDeleteItem]().SetDB(db).ForceDeleteAll(ctx)
	if err != nil {
		t.Fatalf("ForceDeleteAll failed: %v", err)
	}
	if !forceDeleteBeforeCalled {
		t.Error("expected BeforeDelete to be called by ForceDeleteAll, but it was not")
	}
	if !forceDeleteAfterCalled {
		t.Error("expected AfterDelete to be called by ForceDeleteAll, but it was not")
	}
}

// ---- H6: AfterDelete NOT called when BeforeDelete returns error ----

var afterDeleteSkipCalled bool

// HookBeforeDeleteAbortAfterCheck returns an error from BeforeDelete and
// records AfterDelete invocations; AfterDelete must be skipped on abort.
type HookBeforeDeleteAbortAfterCheck struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBeforeDeleteAbortAfterCheck) TableName() string { return "hook_items" }

func (h *HookBeforeDeleteAbortAfterCheck) BeforeDelete(ctx context.Context) error {
	return errors.New("delete aborted by hook")
}

func (h *HookBeforeDeleteAbortAfterCheck) AfterDelete(ctx context.Context) error {
	afterDeleteSkipCalled = true
	return nil
}

func TestHook_AfterDelete_NotCalledWhenBeforeDeleteAborts(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (300, 'skip', 1)`)
	if err != nil {
		t.Fatalf("setup insert failed: %v", err)
	}

	afterDeleteSkipCalled = false
	ctx := context.Background()

	err = New[HookBeforeDeleteAbortAfterCheck]().SetDB(db).Where("id", 300).Delete(ctx)
	if err == nil {
		t.Fatal("expected BeforeDelete to abort delete, got nil")
	}
	if afterDeleteSkipCalled {
		t.Error("expected AfterDelete NOT to be called when BeforeDelete aborts, but it was")
	}
}

// ---- H7: Hooks work inside WithTx() transactions ----

var hookTxAfterCreateCalled bool
var hookTxBeforeDeleteCalled bool
var hookTxAfterDeleteCalled bool

// HookTxItem implements AfterCreate (on the entity) and BeforeDelete/AfterDelete
// (on a zero-value instance per execDelete semantics); all three hooks set
// package-level flags so the test can verify they fired inside a transaction.
type HookTxItem struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookTxItem) TableName() string { return "hook_items" }

func (h *HookTxItem) AfterCreate(ctx context.Context) error {
	hookTxAfterCreateCalled = true
	return nil
}

func (h *HookTxItem) BeforeDelete(ctx context.Context) error {
	hookTxBeforeDeleteCalled = true
	return nil
}

func (h *HookTxItem) AfterDelete(ctx context.Context) error {
	hookTxAfterDeleteCalled = true
	return nil
}

func TestHook_Hooks_WorkInTransaction(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	hookTxAfterCreateCalled = false
	hookTxBeforeDeleteCalled = false
	hookTxAfterDeleteCalled = false

	ctx := context.Background()
	model := New[HookTxItem]().SetDB(db)

	err := model.Transaction(ctx, func(tx *Tx) error {
		item := &HookTxItem{Name: "tx-item", Value: 42}
		if err := model.WithTx(tx).Create(ctx, item); err != nil {
			return err
		}
		// Delete the just-created row within the same transaction.
		return model.WithTx(tx).Where("id", item.ID).Delete(ctx)
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	if !hookTxAfterCreateCalled {
		t.Error("expected AfterCreate to be called within transaction, but it was not")
	}
	if !hookTxBeforeDeleteCalled {
		t.Error("expected BeforeDelete to be called within transaction, but it was not")
	}
	if !hookTxAfterDeleteCalled {
		t.Error("expected AfterDelete to be called within transaction, but it was not")
	}

	// After commit, the insert+delete are permanent; the table must be empty.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM hook_items").Scan(&count); err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after committed transaction (create+delete), got %d", count)
	}
}

// ============================================================================
// *Tx hook variants
// ============================================================================

// setupHooksTxDB extends setupHooksDB with an `audit` table used to prove that
// hook-side DB writes done via the passed *Tx roll back atomically.
//
// SQLite :memory: behaves correctly here because *sql.DB pins a single
// connection for sequential operations and the same DSN sees the same DB.
func setupHooksTxDB(t *testing.T) *sql.DB {
	t.Helper()
	db := setupHooksDB(t)
	if _, err := db.Exec(`CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)`); err != nil {
		t.Fatalf("failed to create audit table: %v", err)
	}
	// Pin to one connection so all tx work and post-tx assertions hit the same
	// in-memory database.
	db.SetMaxOpenConns(1)
	return db
}

// ---- BeforeCreateTx: auto-opened tx ----

// HookCreateTxItem captures the *sql.Tx that BeforeCreateTx received so the
// test can assert it is non-nil (i.e., an auto-tx was opened).
type HookCreateTxItem struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Value  int
	HookTx *sql.Tx `zorm:"-"`
}

func (h *HookCreateTxItem) TableName() string { return "hook_items" }

func (h *HookCreateTxItem) BeforeCreateTx(ctx context.Context, tx *Tx) error {
	h.HookTx = tx.Tx
	return nil
}

func TestHook_BeforeCreateTx_ReceivesAutoOpenedTx(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookCreateTxItem{Name: "auto", Value: 1}

	if err := New[HookCreateTxItem]().SetDB(db).Create(ctx, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if item.HookTx == nil {
		t.Fatal("expected BeforeCreateTx to receive a non-nil *sql.Tx (auto-tx)")
	}
	if item.ID == 0 {
		t.Error("expected ID to be set after Create")
	}
}

// ---- BeforeCreateTx: existing tx is reused ----

func TestHook_BeforeCreateTx_ReceivesActiveTx(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookCreateTxItem{Name: "active", Value: 2}

	model := New[HookCreateTxItem]().SetDB(db)
	var outerTx *sql.Tx
	err := model.Transaction(ctx, func(tx *Tx) error {
		outerTx = tx.Tx
		return model.WithTx(tx).Create(ctx, item)
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
	if item.HookTx == nil {
		t.Fatal("expected BeforeCreateTx to receive a non-nil *sql.Tx")
	}
	if item.HookTx != outerTx {
		t.Errorf("expected hook to receive the outer transaction (same *sql.Tx); got different pointer")
	}
}

// ---- AfterCreateTx error rolls back the INSERT (auto-tx path) ----

// HookCreateTxRollback writes an audit row in BeforeCreateTx via the passed *Tx
// and returns an error from AfterCreateTx. Both the hook_items row and the
// audit row must be rolled back.
type HookCreateTxRollback struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookCreateTxRollback) TableName() string { return "hook_items" }

func (h *HookCreateTxRollback) BeforeCreateTx(ctx context.Context, tx *Tx) error {
	_, err := tx.Tx.ExecContext(ctx, `INSERT INTO audit (note) VALUES (?)`, "creating "+h.Name)
	return err
}

func (h *HookCreateTxRollback) AfterCreateTx(ctx context.Context, tx *Tx) error {
	return errors.New("after-create-tx rejection")
}

func TestHook_AfterCreateTx_RollsBackInsertAndSideEffect(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookCreateTxRollback{Name: "rollback-me", Value: 7}

	err := New[HookCreateTxRollback]().SetDB(db).Create(ctx, item)
	if err == nil {
		t.Fatal("expected error from AfterCreateTx, got nil")
	}

	var itemCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hook_items WHERE name = ?`, "rollback-me").Scan(&itemCount); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if itemCount != 0 {
		t.Errorf("expected INSERT to roll back (0 rows), got %d", itemCount)
	}

	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&auditCount); err != nil {
		t.Fatalf("audit count failed: %v", err)
	}
	if auditCount != 0 {
		t.Errorf("expected hook-side audit row to roll back (0 rows), got %d", auditCount)
	}
}

// ---- Tx variant takes precedence over plain hook ----

// HookCreateTxPrecedence implements both BeforeCreate and BeforeCreateTx; only
// the Tx variant must fire.
type HookCreateTxPrecedence struct {
	ID         int `zorm:"primaryKey"`
	Name       string
	Value      int
	PlainFired bool `zorm:"-"`
	TxFired    bool `zorm:"-"`
}

func (h *HookCreateTxPrecedence) TableName() string { return "hook_items" }

func (h *HookCreateTxPrecedence) BeforeCreate(ctx context.Context) error {
	h.PlainFired = true
	return nil
}

func (h *HookCreateTxPrecedence) BeforeCreateTx(ctx context.Context, tx *Tx) error {
	h.TxFired = true
	return nil
}

func TestHook_BeforeCreateTx_TakesPrecedenceOverPlain(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookCreateTxPrecedence{Name: "precedence", Value: 1}

	if err := New[HookCreateTxPrecedence]().SetDB(db).Create(ctx, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !item.TxFired {
		t.Error("expected BeforeCreateTx to fire")
	}
	if item.PlainFired {
		t.Error("expected plain BeforeCreate NOT to fire when *Tx variant exists")
	}
}

// ---- BeforeUpdateTx rolls back UPDATE + side effect ----

// HookUpdateTxRollback writes audit in BeforeUpdateTx, then AfterUpdateTx
// errors so the whole transaction rolls back.
type HookUpdateTxRollback struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookUpdateTxRollback) TableName() string { return "hook_items" }

func (h *HookUpdateTxRollback) BeforeUpdateTx(ctx context.Context, tx *Tx) error {
	_, err := tx.Tx.ExecContext(ctx, `INSERT INTO audit (note) VALUES (?)`, "updating "+h.Name)
	return err
}

func (h *HookUpdateTxRollback) AfterUpdateTx(ctx context.Context, tx *Tx) error {
	return errors.New("after-update-tx rejection")
}

func TestHook_AfterUpdateTx_RollsBackUpdateAndSideEffect(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (700, 'before', 1)`); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}

	ctx := context.Background()
	err := New[HookUpdateTxRollback]().SetDB(db).Update(ctx, &HookUpdateTxRollback{ID: 700, Name: "after", Value: 2})
	if err == nil {
		t.Fatal("expected error from AfterUpdateTx, got nil")
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM hook_items WHERE id = 700`).Scan(&name); err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if name != "before" {
		t.Errorf("expected UPDATE to roll back (name=before), got %q", name)
	}

	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&auditCount); err != nil {
		t.Fatalf("audit count failed: %v", err)
	}
	if auditCount != 0 {
		t.Errorf("expected audit row to roll back (0 rows), got %d", auditCount)
	}
}

// ---- BeforeDeleteTx rolls back DELETE + side effect ----

// HookDeleteTxRollback inserts audit in BeforeDeleteTx, then AfterDeleteTx
// errors. Delete hooks fire on a zero-value *T, so this type has no per-row
// state besides identity.
type HookDeleteTxRollback struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookDeleteTxRollback) TableName() string { return "hook_items" }

func (h *HookDeleteTxRollback) BeforeDeleteTx(ctx context.Context, tx *Tx) error {
	_, err := tx.Tx.ExecContext(ctx, `INSERT INTO audit (note) VALUES (?)`, "deleting")
	return err
}

func (h *HookDeleteTxRollback) AfterDeleteTx(ctx context.Context, tx *Tx) error {
	return errors.New("after-delete-tx rejection")
}

func TestHook_AfterDeleteTx_RollsBackDeleteAndSideEffect(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (800, 'keepme', 1)`); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}

	ctx := context.Background()
	err := New[HookDeleteTxRollback]().SetDB(db).Where("id", 800).Delete(ctx)
	if err == nil {
		t.Fatal("expected error from AfterDeleteTx, got nil")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hook_items WHERE id = 800`).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected DELETE to roll back (row still present), got count=%d", count)
	}

	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&auditCount); err != nil {
		t.Fatalf("audit count failed: %v", err)
	}
	if auditCount != 0 {
		t.Errorf("expected audit row to roll back (0 rows), got %d", auditCount)
	}
}

// ---- BulkInsert: per-row AfterCreateTx receives the same *Tx ----

// HookBulkTxItem captures the *sql.Tx for each entity; all entities in a single
// BulkInsert must see the same tx pointer.
type HookBulkTxItem struct {
	ID     int `zorm:"primaryKey"`
	Name   string
	Value  int
	HookTx *sql.Tx `zorm:"-"`
}

func (h *HookBulkTxItem) TableName() string { return "hook_items" }

func (h *HookBulkTxItem) AfterCreateTx(ctx context.Context, tx *Tx) error {
	h.HookTx = tx.Tx
	return nil
}

func TestHook_AfterCreateTx_BulkInsert_SharesTx(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	items := []*HookBulkTxItem{
		{Name: "a", Value: 1},
		{Name: "b", Value: 2},
		{Name: "c", Value: 3},
	}

	if err := New[HookBulkTxItem]().SetDB(db).BulkInsert(ctx, items); err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}

	first := items[0].HookTx
	if first == nil {
		t.Fatal("expected AfterCreateTx to receive non-nil *sql.Tx for the first row")
	}
	for i, item := range items {
		if item.HookTx != first {
			t.Errorf("item[%d]: expected the same *sql.Tx across all rows, got different pointer", i)
		}
	}
}

// ---- Regression: plain BeforeCreate still works after Tx-variant changes ----

// HookPlainOnly intentionally implements ONLY the plain BeforeCreate to prove
// existing hook signatures keep firing and do NOT trigger auto-tx.
type HookPlainOnly struct {
	ID         int `zorm:"primaryKey"`
	Name       string
	Value      int
	PlainFired bool `zorm:"-"`
}

func (h *HookPlainOnly) TableName() string { return "hook_items" }

func (h *HookPlainOnly) BeforeCreate(ctx context.Context) error {
	h.PlainFired = true
	return nil
}

func TestHook_PlainBeforeCreate_StillWorks(t *testing.T) {
	db := setupHooksDB(t)
	defer db.Close()

	ctx := context.Background()
	item := &HookPlainOnly{Name: "plain", Value: 1}

	if err := New[HookPlainOnly]().SetDB(db).Create(ctx, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !item.PlainFired {
		t.Error("expected plain BeforeCreate to fire")
	}
	if item.ID == 0 {
		t.Error("expected ID to be set after Create")
	}
}

// ============================================================================
// Complex *Tx hook scenarios
// ============================================================================

// ---- C1: BulkInsert mid-batch hook error rolls back ALL rows + ALL audit writes ----

// HookBulkTxFail audits every per-row INSERT via the passed *Tx and rejects the
// row whose Value == 3. The atomicity contract: when the third hook returns an
// error, the first two INSERTs AND their hook-side audit writes must be rolled
// back together with the row that triggered the error.
type HookBulkTxFail struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookBulkTxFail) TableName() string { return "hook_items" }

func (h *HookBulkTxFail) AfterCreateTx(ctx context.Context, tx *Tx) error {
	if _, err := tx.Tx.ExecContext(ctx, `INSERT INTO audit (note) VALUES (?)`, "bulk:"+h.Name); err != nil {
		return err
	}
	if h.Value == 3 {
		return errors.New("bulk hook rejected row 3")
	}
	return nil
}

func TestHook_AfterCreateTx_BulkInsert_MidBatchRollback(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	items := []*HookBulkTxFail{
		{Name: "a", Value: 1},
		{Name: "b", Value: 2},
		{Name: "c", Value: 3}, // triggers hook error
		{Name: "d", Value: 4}, // never reached
	}

	err := New[HookBulkTxFail]().SetDB(db).BulkInsert(ctx, items)
	if err == nil {
		t.Fatal("expected mid-batch hook error, got nil")
	}

	var itemCount, auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hook_items`).Scan(&itemCount); err != nil {
		t.Fatalf("hook_items count failed: %v", err)
	}
	if itemCount != 0 {
		t.Errorf("expected 0 hook_items after mid-batch rollback, got %d", itemCount)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&auditCount); err != nil {
		t.Fatalf("audit count failed: %v", err)
	}
	if auditCount != 0 {
		t.Errorf("expected 0 audit rows (all per-row hook writes rolled back), got %d", auditCount)
	}
}

// ---- C2: User-managed Transaction spans Create + Update; downstream hook error rolls back everything ----

// HookSpanCreate writes an audit row from AfterCreateTx so a later rollback
// must wipe it.
type HookSpanCreate struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookSpanCreate) TableName() string { return "hook_items" }

func (h *HookSpanCreate) AfterCreateTx(ctx context.Context, tx *Tx) error {
	_, err := tx.Tx.ExecContext(ctx, `INSERT INTO audit (note) VALUES (?)`, "create:"+h.Name)
	return err
}

// HookSpanUpdate rejects all updates from BeforeUpdateTx so the outer tx fails
// after HookSpanCreate has already done its INSERT + audit write.
type HookSpanUpdate struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookSpanUpdate) TableName() string { return "hook_items" }

func (h *HookSpanUpdate) BeforeUpdateTx(ctx context.Context, tx *Tx) error {
	return errors.New("update rejected by BeforeUpdateTx")
}

func TestHook_TxVariants_AtomicAcrossMultipleOps(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO hook_items (id, name, value) VALUES (900, 'preexisting', 10)`); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	ctx := context.Background()
	createModel := New[HookSpanCreate]().SetDB(db)
	updateModel := New[HookSpanUpdate]().SetDB(db)

	err := createModel.Transaction(ctx, func(tx *Tx) error {
		if err := createModel.WithTx(tx).Create(ctx, &HookSpanCreate{Name: "new", Value: 1}); err != nil {
			return err
		}
		return updateModel.WithTx(tx).Update(ctx, &HookSpanUpdate{ID: 900, Name: "modified", Value: 11})
	})
	if err == nil {
		t.Fatal("expected BeforeUpdateTx error to propagate, got nil")
	}

	// 1. Create-side INSERT must roll back.
	var newCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hook_items WHERE name = 'new'`).Scan(&newCount); err != nil {
		t.Fatalf("new-row count failed: %v", err)
	}
	if newCount != 0 {
		t.Errorf("expected Create to roll back (0 'new' rows), got %d", newCount)
	}

	// 2. Pre-existing row must be unchanged (Update was rejected).
	var preName string
	if err := db.QueryRow(`SELECT name FROM hook_items WHERE id = 900`).Scan(&preName); err != nil {
		t.Fatalf("pre-existing row select failed: %v", err)
	}
	if preName != "preexisting" {
		t.Errorf("expected pre-existing row untouched, got name=%q", preName)
	}

	// 3. AfterCreateTx's audit write must roll back too (same tx).
	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&auditCount); err != nil {
		t.Fatalf("audit count failed: %v", err)
	}
	if auditCount != 0 {
		t.Errorf("expected audit row to roll back (0 rows), got %d", auditCount)
	}
}

// ---- C3: AfterCreateTx uses zorm itself (model.WithTx) to insert a related row atomically ----

// HookParentChild inserts a child row via zorm in AfterCreateTx using the same
// transaction. The recursive insert is guarded by Value >= 100 sentinel so the
// child hook short-circuits instead of recursing infinitely.
type HookParentChild struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Value int
}

func (h *HookParentChild) TableName() string { return "hook_items" }

func (h *HookParentChild) AfterCreateTx(ctx context.Context, tx *Tx) error {
	if h.Value >= 100 {
		return nil // child sentinel — stop recursing
	}
	child := &HookParentChild{Name: h.Name + "-child", Value: 100}
	return New[HookParentChild]().WithTx(tx).Create(ctx, child)
}

func TestHook_AfterCreateTx_NestedZormCallSharesTx(t *testing.T) {
	db := setupHooksTxDB(t)
	defer db.Close()

	ctx := context.Background()
	parent := &HookParentChild{Name: "parent", Value: 1}
	if err := New[HookParentChild]().SetDB(db).Create(ctx, parent); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Both rows must be present — they were inserted under the same auto-opened
	// transaction that wrapped the outer Create.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hook_items`).Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected parent + child rows persisted (2), got %d", count)
	}

	var childName string
	if err := db.QueryRow(`SELECT name FROM hook_items WHERE id != ?`, parent.ID).Scan(&childName); err != nil {
		t.Fatalf("child select failed: %v", err)
	}
	if childName != "parent-child" {
		t.Errorf("expected child name 'parent-child', got %q", childName)
	}
}
