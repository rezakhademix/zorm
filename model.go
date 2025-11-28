package zorm

import (
	"context"
	"database/sql"
	"maps"
	"time"
)

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
	}
}

// Clone creates a deep copy of the Model.
// This is useful for creating new queries based on an existing one without modifying it.
func (m *Model[T]) Clone() *Model[T] {
	newModel := &Model[T]{
		ctx:       m.ctx,
		db:        m.db,
		tx:        m.tx,
		modelInfo: m.modelInfo,
		distinct:  m.distinct,
		limit:     m.limit,
		offset:    m.offset,
		rawQuery:  m.rawQuery,
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
			newMap := make(map[string][]string)
			maps.Copy(newMap, v)
			newModel.morphRelations[k] = newMap
		}
	}

	return newModel
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
