package delivery

import (
	"context"
	"testing"
	"time"
)

func TestDatabaseContextUsesBoundedTimeoutAndParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := databaseContext(parent)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("database operation context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > databaseOperationTimeout {
		t.Fatalf("database operation timeout remaining=%s, want at most %s", remaining, databaseOperationTimeout)
	}
	cancelParent()
	if ctx.Err() != context.Canceled {
		t.Fatalf("parent cancellation was not propagated: %v", ctx.Err())
	}
}
