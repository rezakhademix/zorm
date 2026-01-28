package zorm

import (
	"context"
	"database/sql"
	"maps"
	"reflect"
	"sync"
	"time"
)

// modelPools stores sync.Pool instances for each model type.
// Key is the reflect.Type of the model struct.
var modelPools sync.Map

// GlobalDB is the global database connection pool.
// In a real app, this might be managed differently, but for this ORM style,
// we often have a global or a passed-in DB.
// For now, we'll allow setting it globally or per-instance.
var GlobalDB *sql.DB

// GlobalResolver is the global database resolver for primary/replica setup.
// If configured, it will automatically route read queries to replicas
// and write queries to the primary database.
var GlobalResolver *DBResolver

// Model provides a strongly typed ORM interface for working with the entity
// type T. It stores the active query state—including selected columns, filters,
// ordering, grouping, relation loading rules, and raw SQL segments—allowing the
// builder to compose complex queries in a structured and chainable manner.
//
// The Model also tracks the execution context, database handle or transaction,
// and metadata derived from T that is used for mapping database rows into
// entities.
type Model[T any] struct {
	ctx       context.Context
	db        *sql.DB
	tx        *sql.Tx
	modelInfo *ModelInfo

	// Custom Table Name
	tableName string

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
	lockMode          string                         // Lock mode for SELECT ... FOR UPDATE/SHARE

	// Resolver State (for primary/replica routing)
	forcePrimary bool // Force use of primary database
	forceReplica int  // Force specific replica (-1 = auto, 0+ = replica index)

	// Raw Query State
	rawQuery string
	rawArgs  []any

	// CTE State
	ctes []CTE

	// Statement Cache (optional)
	stmtCache *StmtCache

	// Omit columns for Update/Save operations
	omitColumns map[string]bool
}

// CTE represents a Common Table Expression.
type CTE struct {
	Name  string
	Query any // string or *Model[T]
	Args  []any
}

// New creates a new Model instance for type T.
func New[T any]() *Model[T] {
	return &Model[T]{
		ctx:               context.Background(),
		db:                GlobalDB,
		modelInfo:         ParseModel[T](),
		relationCallbacks: make(map[string]any),
		morphRelations:    make(map[string]map[string][]string),
		forceReplica:      -1, // -1 means auto-select
		wheres:            make([]string, 0, 4),
		args:              make([]any, 0, 4),
	}
}

// getModelPool returns the sync.Pool for the given model type T.
func getModelPool[T any]() *sync.Pool {
	var t T
	typ := reflect.TypeOf(t)

	if pool, ok := modelPools.Load(typ); ok {
		return pool.(*sync.Pool)
	}

	// Create new pool
	pool := &sync.Pool{
		New: func() any {
			return &Model[T]{
				relationCallbacks: make(map[string]any),
				morphRelations:    make(map[string]map[string][]string),
				wheres:            make([]string, 0, 4),
				args:              make([]any, 0, 4),
			}
		},
	}
	actual, _ := modelPools.LoadOrStore(typ, pool)
	return actual.(*sync.Pool)
}

// Acquire retrieves a Model[T] from the pool for high-throughput scenarios.
// The returned model is pre-configured with default values and ready for use.
// Call Release() when done to return the model to the pool.
//
// Example:
//
//	m := Acquire[User]()
//	defer m.Release()
//	users, err := m.Where("active", true).Get(ctx)
func Acquire[T any]() *Model[T] {
	pool := getModelPool[T]()
	m := pool.Get().(*Model[T])
	m.reset()
	m.ctx = context.Background()
	m.db = GlobalDB
	m.modelInfo = ParseModel[T]()
	m.forceReplica = -1
	return m
}

// Release returns the Model to the pool for reuse.
// After calling Release, the Model should not be used again.
func (m *Model[T]) Release() {
	m.reset()
	pool := getModelPool[T]()
	pool.Put(m)
}

// reset clears all query state from the model for reuse.
func (m *Model[T]) reset() {
	m.ctx = nil
	m.db = nil
	m.tx = nil
	m.tableName = ""
	m.columns = m.columns[:0]
	m.wheres = m.wheres[:0]
	m.args = m.args[:0]
	m.orderBys = nil
	m.groupBys = nil
	m.havings = nil
	m.distinct = false
	m.distinctOn = nil
	m.limit = 0
	m.offset = 0
	m.relations = nil
	m.lockMode = ""
	m.forcePrimary = false
	m.forceReplica = -1
	m.rawQuery = ""
	m.rawArgs = nil
	m.ctes = nil
	m.stmtCache = nil

	// Recreate maps instead of deleting keys (faster for pooling)
	m.relationCallbacks = make(map[string]any)
	m.morphRelations = make(map[string]map[string][]string)
	m.omitColumns = nil
}

// Clone creates a deep copy of the Model.
// This is useful for creating new queries based on an existing one without modifying it.
//
// IMPORTANT: Clone is NOT safe for concurrent use. Do not call Clone() on a Model
// instance that is being modified concurrently by another goroutine. This will
// cause data races. Each goroutine should create its own Model instance.
//
// Safe usage:
//
//	base := New[User]().Where("active", true)
//	// Each request handler gets its own clone
//	handler1 := base.Clone().Where("age >", 18).Get(ctx)
//	handler2 := base.Clone().Where("verified", true).Get(ctx)
//
// Unsafe usage (will cause race):
//
//	base := New[User]().Where("active", true)
//	go func() {
//	    q1 := base.Clone().Get(ctx1) // RACE if base is being modified
//	}()
//	go func() {
//	    base.Where("new_condition", true) // Modifies base while Clone() reads
//	}()
func (m *Model[T]) Clone() *Model[T] {
	newModel := &Model[T]{
		ctx:       m.ctx,
		db:        m.db,
		tx:        m.tx,
		modelInfo: m.modelInfo,
		tableName: m.tableName,
		distinct:  m.distinct,
		limit:     m.limit,
		offset:    m.offset,
		rawQuery:  m.rawQuery,
		stmtCache: m.stmtCache, // Preserve statement cache reference
	}

	// Copy slices
	if len(m.columns) > 0 {
		newModel.columns = make([]string, len(m.columns))
		copy(newModel.columns, m.columns)
	}
	if len(m.wheres) > 0 {
		newModel.wheres = make([]string, len(m.wheres))
		copy(newModel.wheres, m.wheres)
	}
	if len(m.args) > 0 {
		newModel.args = make([]any, len(m.args))
		copy(newModel.args, m.args)
	}
	if len(m.orderBys) > 0 {
		newModel.orderBys = make([]string, len(m.orderBys))
		copy(newModel.orderBys, m.orderBys)
	}
	if len(m.groupBys) > 0 {
		newModel.groupBys = make([]string, len(m.groupBys))
		copy(newModel.groupBys, m.groupBys)
	}
	if len(m.havings) > 0 {
		newModel.havings = make([]string, len(m.havings))
		copy(newModel.havings, m.havings)
	}
	if len(m.distinctOn) > 0 {
		newModel.distinctOn = make([]string, len(m.distinctOn))
		copy(newModel.distinctOn, m.distinctOn)
	}
	if len(m.relations) > 0 {
		newModel.relations = make([]string, len(m.relations))
		copy(newModel.relations, m.relations)
	}
	if len(m.rawArgs) > 0 {
		newModel.rawArgs = make([]any, len(m.rawArgs))
		copy(newModel.rawArgs, m.rawArgs)
	}
	if len(m.ctes) > 0 {
		newModel.ctes = make([]CTE, len(m.ctes))
		copy(newModel.ctes, m.ctes)
	}

	// Copy maps
	newModel.relationCallbacks = make(map[string]any)
	if m.relationCallbacks != nil {
		maps.Copy(newModel.relationCallbacks, m.relationCallbacks)
	}

	newModel.morphRelations = make(map[string]map[string][]string)
	if m.morphRelations != nil {
		for k, v := range m.morphRelations {
			newMap := make(map[string][]string, len(v))
			for mk, mv := range v {
				// Deep copy the slice
				newSlice := make([]string, len(mv))
				copy(newSlice, mv)
				newMap[mk] = newSlice
			}
			newModel.morphRelations[k] = newMap
		}
	}

	// Copy omitColumns
	if m.omitColumns != nil {
		newModel.omitColumns = make(map[string]bool, len(m.omitColumns))
		for k, v := range m.omitColumns {
			newModel.omitColumns[k] = v
		}
	}

	return newModel
}

// WithContext sets the context for the query.
func (m *Model[T]) WithContext(ctx context.Context) *Model[T] {
	m.ctx = ctx
	return m
}

// Table sets a custom table name for the query.
// This overrides the table name derived from the struct type.
func (m *Model[T]) Table(name string) *Model[T] {
	m.tableName = name
	return m
}

// TableName returns the table name for the model.
// If a custom table name is set via Table(), it returns that.
// Otherwise, it returns the table name from the model info.
func (m *Model[T]) TableName() string {
	if m.tableName != "" {
		return m.tableName
	}

	return m.modelInfo.TableName
}

// SetDB sets a custom database connection for this model instance.
func (m *Model[T]) SetDB(db *sql.DB) *Model[T] {
	m.db = db
	return m
}

// WithStmtCache enables statement caching for this model instance.
// The cache will be used to store and reuse prepared statements,
// improving performance by avoiding re-preparation of frequently used queries.
//
// Example:
//
//	cache := NewStmtCache(100)
//	defer cache.Close()
//	model := New[User]().WithStmtCache(cache)
func (m *Model[T]) WithStmtCache(cache *StmtCache) *Model[T] {
	m.stmtCache = cache
	return m
}

// ConfigureConnectionPool configures the database connection pool.
func ConfigureConnectionPool(db *sql.DB, maxOpen, maxIdle int, maxLifetime, idleTimeout time.Duration) {
	if db == nil {
		return
	}
	if maxOpen > 0 {
		db.SetMaxOpenConns(maxOpen)
	}
	if maxIdle >= 0 {
		db.SetMaxIdleConns(maxIdle)
	}
	if maxLifetime > 0 {
		db.SetConnMaxLifetime(maxLifetime)
	}
	if idleTimeout > 0 {
		db.SetConnMaxIdleTime(idleTimeout)
	}
}

// ConfigureDBResolver configures the global database resolver for primary/replica setup.
// Example:
//
//	ConfigureDBResolver(
//	    WithPrimary(primaryDB),
//	    WithReplicas(replica1, replica2),
//	    WithLoadBalancer(RoundRobinLB),
//	)
func ConfigureDBResolver(opts ...ResolverOption) {
	GlobalResolver = &DBResolver{
		lb: &RoundRobinLoadBalancer{}, // Default load balancer
	}
	for _, opt := range opts {
		opt(GlobalResolver)
	}
}
