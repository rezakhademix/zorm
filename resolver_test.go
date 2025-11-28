package zorm

import (
	"database/sql"
	"strings"
	"testing"
)

// TestRoundRobinLoadBalancer tests the round-robin load balancing
func TestRoundRobinLoadBalancer(t *testing.T) {
	lb := &RoundRobinLoadBalancer{}

	// Create mock replicas
	replicas := []*sql.DB{
		&sql.DB{}, // replica 0
		&sql.DB{}, // replica 1
		&sql.DB{}, // replica 2
	}

	// Test round-robin distribution
	selected := make(map[*sql.DB]int)
	iterations := 9 // 3 full rounds

	for i := 0; i < iterations; i++ {
		db := lb.Next(replicas)
		selected[db]++
	}

	// Each replica should be selected exactly 3 times
	for _, db := range replicas {
		if selected[db] != 3 {
			t.Errorf("Expected replica to be selected 3 times, got %d", selected[db])
		}
	}
}

// TestRoundRobinLoadBalancer_SingleReplica tests with single replica
func TestRoundRobinLoadBalancer_SingleReplica(t *testing.T) {
	lb := &RoundRobinLoadBalancer{}
	replicas := []*sql.DB{&sql.DB{}}

	// Should always return the same replica
	for i := 0; i < 10; i++ {
		db := lb.Next(replicas)
		if db != replicas[0] {
			t.Error("Expected same replica on every call")
		}
	}
}

// TestRoundRobinLoadBalancer_EmptyReplicas tests with no replicas
func TestRoundRobinLoadBalancer_EmptyReplicas(t *testing.T) {
	lb := &RoundRobinLoadBalancer{}
	replicas := []*sql.DB{}

	db := lb.Next(replicas)
	if db != nil {
		t.Error("Expected nil for empty replicas")
	}
}

// TestDBResolver_Primary tests primary database access
func TestDBResolver_Primary(t *testing.T) {
	primary := &sql.DB{}
	resolver := &DBResolver{
		primary: primary,
		lb:      &RoundRobinLoadBalancer{},
	}

	if resolver.Primary() != primary {
		t.Error("Expected Primary() to return primary database")
	}
}

// TestDBResolver_Replica tests replica selection
func TestDBResolver_Replica(t *testing.T) {
	primary := &sql.DB{}
	replica1 := &sql.DB{}
	replica2 := &sql.DB{}

	resolver := &DBResolver{
		primary:  primary,
		replicas: []*sql.DB{replica1, replica2},
		lb:       &RoundRobinLoadBalancer{},
	}

	// Should return one of the replicas (not primary)
	db := resolver.Replica()
	if db != replica1 && db != replica2 {
		t.Error("Expected Replica() to return a replica database")
	}
}

// TestDBResolver_Replica_FallbackToPrimary tests fallback when no replicas
func TestDBResolver_Replica_FallbackToPrimary(t *testing.T) {
	primary := &sql.DB{}
	resolver := &DBResolver{
		primary:  primary,
		replicas: []*sql.DB{},
		lb:       &RoundRobinLoadBalancer{},
	}

	// Should fallback to primary when no replicas configured
	if resolver.Replica() != primary {
		t.Error("Expected Replica() to fallback to primary when no replicas")
	}
}

// TestDBResolver_ReplicaAt tests accessing specific replica by index
func TestDBResolver_ReplicaAt(t *testing.T) {
	replica1 := &sql.DB{}
	replica2 := &sql.DB{}

	resolver := &DBResolver{
		replicas: []*sql.DB{replica1, replica2},
	}

	if resolver.ReplicaAt(0) != replica1 {
		t.Error("Expected ReplicaAt(0) to return first replica")
	}
	if resolver.ReplicaAt(1) != replica2 {
		t.Error("Expected ReplicaAt(1) to return second replica")
	}
	if resolver.ReplicaAt(2) != nil {
		t.Error("Expected ReplicaAt(2) to return nil for out of bounds")
	}
	if resolver.ReplicaAt(-1) != nil {
		t.Error("Expected ReplicaAt(-1) to return nil for negative index")
	}
}

// TestConfigureDBResolver tests the configuration function
func TestConfigureDBResolver(t *testing.T) {
	primary := &sql.DB{}
	replica1 := &sql.DB{}
	replica2 := &sql.DB{}

	ConfigureDBResolver(
		WithPrimary(primary),
		WithReplicas(replica1, replica2),
		WithLoadBalancer(RoundRobinLB),
	)

	if GlobalResolver == nil {
		t.Fatal("Expected GlobalResolver to be configured")
	}
	if GlobalResolver.Primary() != primary {
		t.Error("Expected primary to be configured")
	}
	if len(GlobalResolver.replicas) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(GlobalResolver.replicas))
	}

	// Clean up
	GlobalResolver = nil
}

// TestModel_UsePrimary tests forcing primary database
func TestModel_UsePrimary(t *testing.T) {
	m := New[TestModel]()
	m.UsePrimary()

	if !m.forcePrimary {
		t.Error("Expected forcePrimary to be true")
	}
	if m.forceReplica != -1 {
		t.Error("Expected forceReplica to be reset to -1")
	}
}

// TestModel_UseReplica tests forcing specific replica
func TestModel_UseReplica(t *testing.T) {
	m := New[TestModel]()
	m.UseReplica(1)

	if m.forcePrimary {
		t.Error("Expected forcePrimary to be false")
	}
	if m.forceReplica != 1 {
		t.Errorf("Expected forceReplica to be 1, got %d", m.forceReplica)
	}
}

// TestModel_QueryWithResolver tests query building with resolver methods
func TestModel_QueryWithResolver(t *testing.T) {
	m := New[TestModel]()

	// Test chaining with UsePrimary
	m.Where("id", 1).UsePrimary().Limit(10)
	query, args := m.buildSelectQuery()

	expected := "SELECT * FROM test_models WHERE 1=1  AND (id = ?) LIMIT 10"
	if strings.TrimSpace(query) != expected {
		t.Errorf("expected query %q, got %q", expected, query)
	}
	if !m.forcePrimary {
		t.Error("Expected forcePrimary to be set")
	}
	if len(args) != 1 {
		t.Errorf("Expected 1 arg, got %d", len(args))
	}
}
