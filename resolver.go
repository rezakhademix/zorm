package zorm

import (
	"database/sql"
	"sync/atomic"
)

// DBResolver manages primary and replica database connections.
// It automatically routes write operations to the primary and read operations to replicas.
type DBResolver struct {
	primary  *sql.DB
	replicas []*sql.DB
	lb       LoadBalancer
}

// LoadBalancer is an interface for selecting a replica from a pool.
type LoadBalancer interface {
	Next(replicas []*sql.DB) *sql.DB
}

// RoundRobinLoadBalancer distributes load across replicas using round-robin.
type RoundRobinLoadBalancer struct {
	counter uint64
}

// Next returns the next replica in round-robin order.
func (r *RoundRobinLoadBalancer) Next(replicas []*sql.DB) *sql.DB {
	if len(replicas) == 0 {
		return nil
	}
	if len(replicas) == 1 {
		return replicas[0]
	}

	// Atomically increment and get next index
	idx := atomic.AddUint64(&r.counter, 1) - 1
	return replicas[idx%uint64(len(replicas))]
}

// RandomLoadBalancer is a placeholder for future random load balancing.
// Currently not implemented, defaults to round-robin.
type RandomLoadBalancer struct{}

func (r *RandomLoadBalancer) Next(replicas []*sql.DB) *sql.DB {
	// TODO: Implement random selection
	return (&RoundRobinLoadBalancer{}).Next(replicas)
}

// ResolverOption is a functional option for configuring DBResolver.
type ResolverOption func(*DBResolver)

// WithPrimary sets the primary database connection.
func WithPrimary(db *sql.DB) ResolverOption {
	return func(r *DBResolver) {
		r.primary = db
	}
}

// WithReplicas sets the replica database connections.
func WithReplicas(dbs ...*sql.DB) ResolverOption {
	return func(r *DBResolver) {
		r.replicas = dbs
	}
}

// WithLoadBalancer sets the load balancer strategy.
// Default is RoundRobinLoadBalancer.
func WithLoadBalancer(lb LoadBalancer) ResolverOption {
	return func(r *DBResolver) {
		r.lb = lb
	}
}

// RoundRobinLB is a convenience variable for round-robin load balancing.
var RoundRobinLB LoadBalancer = &RoundRobinLoadBalancer{}

// RandomLB is a convenience variable for random load balancing.
var RandomLB LoadBalancer = &RandomLoadBalancer{}

// Primary returns the primary database connection.
func (r *DBResolver) Primary() *sql.DB {
	return r.primary
}

// Replica returns a replica based on the load balancer strategy.
func (r *DBResolver) Replica() *sql.DB {
	if len(r.replicas) == 0 {
		// Fallback to primary if no replicas configured
		return r.primary
	}
	return r.lb.Next(r.replicas)
}

// ReplicaAt returns a specific replica by index.
// Returns nil if index is out of bounds.
func (r *DBResolver) ReplicaAt(index int) *sql.DB {
	if index < 0 || index >= len(r.replicas) {
		return nil
	}
	return r.replicas[index]
}

// HasReplicas returns true if replicas are configured.
func (r *DBResolver) HasReplicas() bool {
	return len(r.replicas) > 0
}
