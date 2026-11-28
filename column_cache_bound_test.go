package zorm

import (
	"fmt"
	"testing"
)

func TestColumnValidationCache_Bounded(t *testing.T) {
	for i := 0; i < 3000; i++ {
		if err := ValidateColumnName(fmt.Sprintf("request_column_%d", i)); err != nil {
			t.Fatal(err)
		}
		_ = ValidateColumnName(fmt.Sprintf("rejected_column_%d;", i))
	}
	columnValidationCache.mu.Lock()
	count := len(columnValidationCache.items)
	columnValidationCache.mu.Unlock()
	if count > 1000 {
		t.Fatalf("validation cache retained %d entries, want at most 1000", count)
	}
}
