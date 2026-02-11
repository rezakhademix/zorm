package zorm

import (
	"context"
	"database/sql"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// modelPools stores sync.Pool instances for each model type.
// Key is the reflect.Type of the model struct.
var modelPools sync.Map

// globalDB is the atomic pointer to the global database connection pool.
// Use GetGlobalDB() and SetGlobalDB() for thread-safe access.
var globalDB atomic.Pointer[sql.DB]

// GlobalDB is the global database connection pool.
// For thread-safe access in concurrent code, prefer SetGlobalDB() for writes.
// Reads via GetGlobalDB() will check both this variable and the atomic pointer
// for backwards compatibility with code that directly assigns to GlobalDB.
var GlobalDB *sql.DB

// GetGlobalDB returns the global database connection in a thread-safe manner.
// For backwards compatibility, it checks both the atomic pointer and the
// deprecated GlobalDB variable, preferring the atomic if set.
func GetGlobalDB() *sql.DB {
	// First check atomic pointer (thread-safe path)
	if db := globalDB.Load(); db != nil {
		return db
	}
	// Fall back to deprecated variable for backwards compatibility
	// This allows existing code that does "GlobalDB = db" to keep working
	return GlobalDB
}

// SetGlobalDB sets the global database connection in a thread-safe manner.
// This also updates the GlobalDB variable for backwards compatibility.
func SetGlobalDB(db *sql.DB) {
	globalDB.Store(db)
	GlobalDB = db // Keep in sync for backwards compatibility
}

// globalResolver is the atomic pointer to the database resolver for primary/replica setup.
// Using atomic.Pointer ensures thread-safe read/write access.
var globalResolver atomic.Pointer[DBResolver]

// GetGlobalResolver returns the current global database resolver.
// Returns nil if no resolver is configured.
func GetGlobalResolver() *DBResolver {
	return globalResolver.Load()
}

// Model provides a strongly typed ORM interface for working with the entity
// type T. It stores the active query state—including selected columns, filters,
// ordering, grouping, relation loading rules, and raw SQL segments—allowing the
// builder to compose complex queries in a structured and chainable manner.
//
// The Model also tracks the execution context, database handle or transaction,
// and metadata derived from T that is used for mapping database rows into
// entities.
//
// Thread Safety: Model instances are NOT safe for concurrent modification.
// Query builder methods (Where, Select, OrderBy, etc.) mutate internal state
// without locking and must not be called concurrently on the same Model instance.
//
// Safe patterns for concurrent use:
//  1. Clone before branching: Call Clone() to create independent copies before
//     modifying in different goroutines. Clone() uses RWMutex internally.
//  2. Create per goroutine: Create new Model instances via New[T]() in each goroutine.
//
// Example:
//
//	base := New[User]().Where("active", true)
//	// SAFE: Clone before concurrent use
//	go func() { base.Clone().Where("role", "admin").Get(ctx) }()
//	go func() { base.Clone().Where("role", "user").Get(ctx) }()
//
//	// UNSAFE: Concurrent mutation of same Model
//	go func() { base.Where("role", "admin").Get(ctx) }() // DATA RACE
//	go func() { base.Where("role", "user").Get(ctx) }()  // DATA RACE
type Model[T any] struct {
	mu sync.RWMutex // Protects query state for Clone() operations

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

	// Tracking scope for batch operations with automatic cleanup
	trackingScope *TrackingScope
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
		db:                GetGlobalDB(),
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

	// Parse model info once for this type - will be reused by all pooled instances
	modelInfo := ParseModel[T]()

	// Create new pool
	pool := &sync.Pool{
		New: func() any {
			return &Model[T]{
				modelInfo:         modelInfo,
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
	// Save modelInfo before reset since it's set by pool.New and should be reused
	modelInfo := m.modelInfo
	m.reset()
	m.ctx = context.Background()
	m.db = GetGlobalDB()
	m.modelInfo = modelInfo
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

// maxPooledSliceCap is the maximum capacity for slices retained in pooled models.
// Slices larger than this will be replaced to prevent memory bloat.
const maxPooledSliceCap = 64

// reset clears all query state from the model for reuse.
// It carefully balances memory reuse (keeping small allocations) with
// preventing memory bloat (replacing overly large allocations).
func (m *Model[T]) reset() {
	m.ctx = nil
	m.db = nil
	m.tx = nil
	m.tableName = ""

	// Reuse slices if they have reasonable capacity, otherwise replace
	// This prevents memory bloat from queries with many conditions
	if cap(m.columns) <= maxPooledSliceCap {
		m.columns = m.columns[:0]
	} else {
		m.columns = nil
	}
	if cap(m.wheres) <= maxPooledSliceCap {
		m.wheres = m.wheres[:0]
	} else {
		m.wheres = nil
	}
	if cap(m.args) <= maxPooledSliceCap {
		m.args = m.args[:0]
	} else {
		m.args = nil
	}

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

	// Clear maps by deleting keys to reuse capacity, or recreate if too large
	// This provides better pooling efficiency than always recreating
	if m.relationCallbacks != nil && len(m.relationCallbacks) <= maxPooledSliceCap {
		clear(m.relationCallbacks)
	} else {
		m.relationCallbacks = make(map[string]any)
	}
	if m.morphRelations != nil && len(m.morphRelations) <= maxPooledSliceCap {
		clear(m.morphRelations)
	} else {
		m.morphRelations = make(map[string]map[string][]string)
	}

	m.omitColumns = nil
	m.trackingScope = nil
}

// Clone creates a deep copy of the Model.
// This is useful for creating new queries based on an existing one without modifying it.
//
// Thread Safety: Clone() acquires a read lock to safely copy state. Multiple goroutines
// can call Clone() concurrently on the same model. However, calling Clone() while another
// goroutine is modifying the model (via Where, Select, etc.) requires the modification
// methods to also acquire locks, which they do not for performance reasons.
//
// Recommended usage patterns:
//
//	// Pattern 1: Create base query once, then clone for each request
//	base := New[User]().Where("active", true)  // Setup phase, single goroutine
//	// ... later, in request handlers (multiple goroutines)
//	handler1 := base.Clone().Where("age >", 18).Get(ctx)
//	handler2 := base.Clone().Where("verified", true).Get(ctx)
//
//	// Pattern 2: Create new model per goroutine
//	go func() {
//	    m := New[User]().Where("active", true).Get(ctx)
//	}()
func (m *Model[T]) Clone() *Model[T] {
	m.mu.RLock()
	defer m.mu.RUnlock()

	newModel := &Model[T]{
		ctx:          m.ctx,
		db:           m.db,
		tx:           m.tx,
		modelInfo:    m.modelInfo,
		tableName:    m.tableName,
		distinct:     m.distinct,
		limit:        m.limit,
		offset:       m.offset,
		rawQuery:     m.rawQuery,
		stmtCache:    m.stmtCache, // Preserve statement cache reference
		lockMode:     m.lockMode,
		forcePrimary: m.forcePrimary,
		forceReplica: m.forceReplica,
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

	// Copy maps - only allocate if source has content
	if len(m.relationCallbacks) > 0 {
		newModel.relationCallbacks = make(map[string]any, len(m.relationCallbacks))
		maps.Copy(newModel.relationCallbacks, m.relationCallbacks)
	}

	if len(m.morphRelations) > 0 {
		newModel.morphRelations = make(map[string]map[string][]string, len(m.morphRelations))
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

	// Copy omitColumns - only allocate if source has content
	if len(m.omitColumns) > 0 {
		newModel.omitColumns = make(map[string]bool, len(m.omitColumns))
		maps.Copy(newModel.omitColumns, m.omitColumns)
	}

	// Copy tracking scope reference (scopes can be shared)
	newModel.trackingScope = m.trackingScope

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

// WithTrackingScope sets a tracking scope for this model instance.
// All entities loaded through this model will be registered with the scope,
// and their tracking data will be automatically cleared when the scope is closed.
//
// This is useful for batch operations where you want automatic cleanup
// of tracking data without memory leaks.
//
// Example:
//
//	scope := zorm.NewTrackingScope()
//	defer scope.Close()
//	model := zorm.New[User]().WithTrackingScope(scope)
//	users, _ := model.Get(ctx) // All users are tracked in scope
//	// When scope.Close() is called, all tracking data is cleared
func (m *Model[T]) WithTrackingScope(scope *TrackingScope) *Model[T] {
	m.trackingScope = scope
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
// This function is thread-safe and can be called at any time.
// Example:
//
//	ConfigureDBResolver(
//	    WithPrimary(primaryDB),
//	    WithReplicas(replica1, replica2),
//	    WithLoadBalancer(RoundRobinLB),
//	)
func ConfigureDBResolver(opts ...ResolverOption) {
	resolver := &DBResolver{
		lb: &RoundRobinLoadBalancer{}, // Default load balancer
	}
	for _, opt := range opts {
		opt(resolver)
	}
	globalResolver.Store(resolver)
}

// ClearDBResolver removes the global database resolver.
// This function is thread-safe.
func ClearDBResolver() {
	globalResolver.Store(nil)
}
