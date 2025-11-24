package zorm

import (
	"context"
	"database/sql"
	"time"
)

// DB is the global database connection pool.
// In a real app, this might be managed differently, but for this ORM style,
// we often have a global or a passed-in DB.
// For now, we'll allow setting it globally or per-instance.
var GlobalDB *sql.DB

// Model[T] is the main struct for the ORM.
type Model[T any] struct {
	ctx       context.Context
	db        *sql.DB
	tx        *sql.Tx
	modelInfo *ModelInfo

	// Query Builder State
	columns           []string
	wheres            []string
	args              []any
	orderBys          []string
	groupBys          []string
	havings           []string
	distinct          bool
	distinctOn        []string
	limit             int
	offset            int
	relations         []string
	relationCallbacks map[string]any                 // Map of relation name to callback function
	morphRelations    map[string]map[string][]string // Map of relation -> type -> []relations

	// Raw Query State
	rawQuery string
	rawArgs  []any
}

// New creates a new Model instance for type T.
func New[T any]() *Model[T] {
	return &Model[T]{
		ctx:       context.Background(),
		db:        GlobalDB,
		modelInfo: ParseModel[T](),
	}
}

// WithContext sets the context for the query.
func (m *Model[T]) WithContext(ctx context.Context) *Model[T] {
	m.ctx = ctx
	return m
}

// TableName returns the table name for the model.
func (m *Model[T]) TableName() string {
	return m.modelInfo.TableName
}

// SetDB sets a custom database connection for this model instance.
func (m *Model[T]) SetDB(db *sql.DB) *Model[T] {
	m.db = db
	return m
}

// ConfigureConnectionPoolSeconds accepts durations in seconds.
// Pass 0 to leave duration unlimited / not set.
func ConfigureConnectionPoolSeconds(db *sql.DB, maxOpen, maxIdle int, maxLifetimeSec, idleTimeoutSec int64) {
	if db == nil {
		return
	}
	if maxOpen > 0 {
		db.SetMaxOpenConns(maxOpen)
	}
	if maxIdle >= 0 {
		db.SetMaxIdleConns(maxIdle)
	}
	if maxLifetimeSec >= 0 {
		db.SetConnMaxLifetime(time.Duration(maxLifetimeSec) * time.Second)
	}
	if idleTimeoutSec >= 0 {
		db.SetConnMaxIdleTime(time.Duration(idleTimeoutSec) * time.Second)
	}
}
