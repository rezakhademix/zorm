package zorm

import "testing"

type alignedTrackedRecord struct {
	ID      int64
	Padding [248]byte `zorm:"-"`
}

func TestDirtyTracking_AlignedEntitiesUseAvailableCapacity(t *testing.T) {
	old := globalTracker.Load()
	ConfigureDirtyTracking(50000)
	defer globalTracker.Store(old)
	entities := make([]alignedTrackedRecord, 300)
	info := ParseModel[alignedTrackedRecord]()
	for i := range entities {
		entities[i].ID = int64(i + 1)
		trackOriginals(&entities[i], info)
	}
	for i := range entities {
		if !IsTracked(&entities[i]) {
			t.Fatalf("entity %d evicted after only %d entries with capacity 50000", i, TrackedEntityCount())
		}
	}
}
