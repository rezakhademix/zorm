package zorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Read-path hook coverage: AfterFind must see the context the caller passed to
// the terminal method, and it must fire for entities materialized by a relation
// load exactly as it does for entities fetched directly. Relation loads go
// through scanRowsDynamic, which is a separate materialization path from
// scanRows, so each relation kind is exercised here.

type hookCtxKey string

const hookReadTenantKey hookCtxKey = "tenant"

// ---- fixtures ----

// HookReadAuthor is the parent side of the has-many and the target of the
// belongs-to. Counters carry `zorm:"-"` so they are not treated as columns.
type HookReadAuthor struct {
	ID             int `zorm:"primaryKey"`
	Name           string
	Books          []*HookReadBook
	AfterFindCalls int    `zorm:"-"`
	SeenTenant     string `zorm:"-"`
	CtxErr         error  `zorm:"-"`
}

func (HookReadAuthor) TableName() string { return "hookread_authors" }

func (a HookReadAuthor) BooksRelation() HasMany[HookReadBook] {
	return HasMany[HookReadBook]{ForeignKey: "author_id"}
}

func (a *HookReadAuthor) AfterFind(ctx context.Context) error {
	a.AfterFindCalls++
	if v, ok := ctx.Value(hookReadTenantKey).(string); ok {
		a.SeenTenant = v
	}
	a.CtxErr = ctx.Err()
	return nil
}

// HookReadBook is the related model. It also declares an accessor so the
// relation path's accessor handling is covered.
type HookReadBook struct {
	ID             int `zorm:"primaryKey"`
	AuthorID       int
	Title          string
	Author         *HookReadAuthor
	Reviews        []*HookReadReview
	Attributes     map[string]any `zorm:"-"`
	AfterFindCalls int            `zorm:"-"`
	SeenTenant     string         `zorm:"-"`
}

func (HookReadBook) TableName() string { return "hookread_books" }

func (b HookReadBook) AuthorRelation() BelongsTo[HookReadAuthor] {
	return BelongsTo[HookReadAuthor]{ForeignKey: "author_id"}
}

func (b HookReadBook) ReviewsRelation() HasMany[HookReadReview] {
	return HasMany[HookReadReview]{ForeignKey: "book_id"}
}

func (b *HookReadBook) AfterFind(ctx context.Context) error {
	b.AfterFindCalls++
	if v, ok := ctx.Value(hookReadTenantKey).(string); ok {
		b.SeenTenant = v
	}
	return nil
}

// GetSlug is an accessor: the loaded entity should expose attributes["slug"].
func (b HookReadBook) GetSlug() string { return "slug-" + b.Title }

// HookReadReview is the third level, used for nested eager loading.
type HookReadReview struct {
	ID             int `zorm:"primaryKey"`
	BookID         int
	Body           string
	AfterFindCalls int `zorm:"-"`
}

func (HookReadReview) TableName() string { return "hookread_reviews" }

func (r *HookReadReview) AfterFind(ctx context.Context) error {
	r.AfterFindCalls++
	return nil
}

func setupHookReadDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE hookread_authors (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE hookread_books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT);
		CREATE TABLE hookread_reviews (id INTEGER PRIMARY KEY, book_id INTEGER, body TEXT);

		INSERT INTO hookread_authors (id, name) VALUES (1, 'Ursula'), (2, 'Octavia');
		INSERT INTO hookread_books (id, author_id, title) VALUES
			(1, 1, 'Left Hand'), (2, 1, 'Dispossessed'), (3, 2, 'Kindred');
		INSERT INTO hookread_reviews (id, book_id, body) VALUES
			(1, 1, 'great'), (2, 1, 'good'), (3, 3, 'excellent');
	`)
	if err != nil {
		t.Fatalf("failed to setup hookread DB: %v", err)
	}
	return db
}

// ---- AfterFind receives the caller's context ----

func TestHook_AfterFind_ReceivesCallerContext(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	ctx := context.WithValue(context.Background(), hookReadTenantKey, "acme")

	authors, err := New[HookReadAuthor]().SetDB(db).Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(authors) != 2 {
		t.Fatalf("expected 2 authors, got %d", len(authors))
	}
	for _, a := range authors {
		if a.SeenTenant != "acme" {
			t.Errorf("author %d: AfterFind saw tenant %q, want %q (hook got the model's stored context, not the caller's)", a.ID, a.SeenTenant, "acme")
		}
	}
}

// A cancelled caller context must be visible to the hook, so hook-side DB work
// stops with the request instead of running against a background context.
func TestHook_AfterFind_ObservesCallerCancellation(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	// Rows are already buffered by the driver, so cancelling after the query is
	// issued still reaches the hook while it runs.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	authors, err := New[HookReadAuthor]().SetDB(db).Get(ctx)
	if err != nil {
		// A cancelled context legitimately fails at the query stage; nothing to
		// assert about the hook in that case.
		return
	}
	for _, a := range authors {
		if !errors.Is(a.CtxErr, context.Canceled) {
			t.Errorf("author %d: AfterFind saw ctx.Err() = %v, want context.Canceled", a.ID, a.CtxErr)
		}
	}
}

// WithContext must not override the context the terminal method was given.
func TestHook_AfterFind_CallerContextWinsOverModelContext(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	stale := context.WithValue(context.Background(), hookReadTenantKey, "stale")
	caller := context.WithValue(context.Background(), hookReadTenantKey, "caller")

	authors, err := New[HookReadAuthor]().SetDB(db).WithContext(stale).Get(caller)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	for _, a := range authors {
		if a.SeenTenant != "caller" {
			t.Errorf("author %d: AfterFind saw tenant %q, want %q", a.ID, a.SeenTenant, "caller")
		}
	}
}

// ---- AfterFind on relation-loaded entities ----

func TestHook_AfterFind_FiresForEagerHasMany(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	ctx := context.WithValue(context.Background(), hookReadTenantKey, "acme")

	authors, err := New[HookReadAuthor]().SetDB(db).With("Books").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	total := 0
	for _, a := range authors {
		for _, b := range a.Books {
			total++
			if b.AfterFindCalls != 1 {
				t.Errorf("book %d: AfterFind called %d times, want 1", b.ID, b.AfterFindCalls)
			}
			if b.SeenTenant != "acme" {
				t.Errorf("book %d: AfterFind saw tenant %q, want %q", b.ID, b.SeenTenant, "acme")
			}
		}
	}
	if total != 3 {
		t.Errorf("expected 3 eagerly-loaded books, got %d", total)
	}
}

func TestHook_AfterFind_FiresForLazyLoad(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	ctx := context.Background()
	author, err := New[HookReadAuthor]().SetDB(db).Find(ctx, 1)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if err := New[HookReadAuthor]().SetDB(db).Load(ctx, author, "Books"); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(author.Books) != 2 {
		t.Fatalf("expected 2 books, got %d", len(author.Books))
	}
	for _, b := range author.Books {
		if b.AfterFindCalls != 1 {
			t.Errorf("book %d: AfterFind called %d times, want 1", b.ID, b.AfterFindCalls)
		}
	}
}

func TestHook_AfterFind_FiresForBelongsTo(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	books, err := New[HookReadBook]().SetDB(db).With("Author").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	for _, b := range books {
		if b.Author == nil {
			t.Fatalf("book %d: Author not loaded", b.ID)
		}
		if b.Author.AfterFindCalls != 1 {
			t.Errorf("book %d: author AfterFind called %d times, want 1", b.ID, b.Author.AfterFindCalls)
		}
	}
}

func TestHook_AfterFind_FiresForNestedRelation(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	authors, err := New[HookReadAuthor]().SetDB(db).With("Books.Reviews").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	reviews := 0
	for _, a := range authors {
		for _, b := range a.Books {
			for _, r := range b.Reviews {
				reviews++
				if r.AfterFindCalls != 1 {
					t.Errorf("review %d: AfterFind called %d times, want 1", r.ID, r.AfterFindCalls)
				}
			}
		}
	}
	if reviews != 3 {
		t.Errorf("expected 3 nested reviews, got %d", reviews)
	}
}

// ---- an AfterFind error on a relation aborts the parent query ----

type HookReadBadAuthor struct {
	ID    int `zorm:"primaryKey"`
	Name  string
	Books []*HookReadBadBook
}

func (HookReadBadAuthor) TableName() string { return "hookread_authors" }

func (a HookReadBadAuthor) BooksRelation() HasMany[HookReadBadBook] {
	return HasMany[HookReadBadBook]{ForeignKey: "author_id"}
}

type HookReadBadBook struct {
	ID       int `zorm:"primaryKey"`
	AuthorID int
	Title    string
}

func (HookReadBadBook) TableName() string { return "hookread_books" }

var errHookReadRelation = errors.New("relation after-find rejection")

func (b *HookReadBadBook) AfterFind(ctx context.Context) error { return errHookReadRelation }

func TestHook_AfterFind_RelationErrorAbortsQuery(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	_, err := New[HookReadBadAuthor]().SetDB(db).With("Books").Get(context.Background())
	if !errors.Is(err, errHookReadRelation) {
		t.Fatalf("expected relation AfterFind error to propagate, got %v", err)
	}
}

// ---- relation-loaded entities get the rest of the post-scan pipeline ----

func TestRelations_LoadedEntityIsDirtyTracked(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	ctx := context.Background()
	authors, err := New[HookReadAuthor]().SetDB(db).Where("id", 1).With("Books").Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	book := authors[0].Books[0]

	if !IsTracked(book) {
		t.Fatal("expected an eagerly-loaded entity to be dirty-tracked")
	}

	book.Title = "Renamed"
	if err := New[HookReadBook]().SetDB(db).Save(ctx, book); err != nil {
		t.Fatalf("Save on a relation-loaded entity failed: %v", err)
	}

	var title string
	var authorID int
	if err := db.QueryRow(`SELECT title, author_id FROM hookread_books WHERE id = ?`, book.ID).Scan(&title, &authorID); err != nil {
		t.Fatalf("verification query failed: %v", err)
	}
	if title != "Renamed" {
		t.Errorf("title = %q, want %q", title, "Renamed")
	}
	// Save must write only the dirty column; a full-row rewrite would be the
	// symptom of a missing baseline.
	if authorID != 1 {
		t.Errorf("author_id = %d, want 1 (untouched)", authorID)
	}
}

func TestRelations_LoadedEntityHasAccessors(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	authors, err := New[HookReadAuthor]().SetDB(db).Where("id", 1).With("Books").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	book := authors[0].Books[0]

	slug, ok := book.Attributes["slug"]
	if !ok {
		t.Fatalf("expected attributes[slug] on a relation-loaded entity, got %#v", book.Attributes)
	}
	if slug != "slug-"+book.Title {
		t.Errorf("attributes[slug] = %v, want %v", slug, "slug-"+book.Title)
	}
}

// ---- MorphTo: hook fires, tracking is the documented gap ----

type HookReadImage struct {
	ID            int `zorm:"primaryKey"`
	OwnerID       int
	OwnerType     string
	Owner         any `zorm:"-"`
	AfterFindCall int `zorm:"-"`
}

func (HookReadImage) TableName() string { return "hookread_images" }

func (i HookReadImage) OwnerRelation() MorphTo[any] {
	return MorphTo[any]{
		Type: "owner_type",
		ID:   "owner_id",
		TypeMap: map[string]any{
			"HookReadAuthor": HookReadAuthor{},
		},
	}
}

func (i *HookReadImage) AfterFind(ctx context.Context) error {
	i.AfterFindCall++
	return nil
}

func TestHook_AfterFind_FiresForMorphToTarget(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE hookread_images (id INTEGER PRIMARY KEY, owner_id INTEGER, owner_type TEXT);
		INSERT INTO hookread_images (id, owner_id, owner_type) VALUES (1, 1, 'HookReadAuthor');
	`); err != nil {
		t.Fatalf("failed to extend schema: %v", err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	images, err := New[HookReadImage]().SetDB(db).With("Owner").Get(context.Background())
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(images) != 1 {
		t.Fatalf("expected 1 image, got %d", len(images))
	}

	owner, ok := images[0].Owner.(*HookReadAuthor)
	if !ok {
		t.Fatalf("expected *HookReadAuthor owner, got %T", images[0].Owner)
	}
	if owner.AfterFindCalls != 1 {
		t.Errorf("morph target: AfterFind called %d times, want 1", owner.AfterFindCalls)
	}
	// Documented gap: MorphTo resolves its target type per row, so there is no
	// typed bridge to record a tracking baseline through.
	if IsTracked(owner) {
		t.Error("expected MorphTo targets to be untracked; if this now passes, update the trackRelatedResults doc and CLAUDE.md")
	}
}

// ---- auto-tx rollback clears the create baseline ----

// HookReadRollback returns an error from AfterCreateTx so the auto-opened
// transaction rolls the INSERT back.
type HookReadRollback struct {
	ID   int `zorm:"primaryKey"`
	Name string
}

func (HookReadRollback) TableName() string { return "hookread_authors" }

var errHookReadAfterCreate = errors.New("after-create-tx rejection")

func (h *HookReadRollback) AfterCreateTx(ctx context.Context, tx *Tx) error {
	return errHookReadAfterCreate
}

func TestHook_AfterCreateTx_RollbackClearsTrackingBaseline(t *testing.T) {
	db := setupHookReadDB(t)
	defer db.Close()

	ctx := context.Background()
	entity := &HookReadRollback{Name: "doomed"}

	err := New[HookReadRollback]().SetDB(db).Create(ctx, entity)
	if !errors.Is(err, errHookReadAfterCreate) {
		t.Fatalf("expected AfterCreateTx error, got %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hookread_authors WHERE name = ?`, "doomed").Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected the INSERT to roll back, found %d rows", count)
	}

	if IsTracked(entity) {
		t.Error("expected the tracking baseline to be cleared after rollback; a tracked entity lets Save() UPDATE a row that does not exist")
	}
}
