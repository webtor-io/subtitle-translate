package services

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForCalls blocks until the fake translator has been entered n times.
// Batch k+1 starts only after batch k was stored, so the call count is the
// synchronisation point the background job actually offers — a sleep would
// be a guess about its speed.
func waitForCalls(t *testing.T, ft *fakeTranslator, n int32) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if atomic.LoadInt32(&ft.calls) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d calls, got %d", n, atomic.LoadInt32(&ft.calls))
}

// releaser closes c exactly once, so a blocked fake translator can be
// released both mid-test and from a deferred cleanup after a failure.
func releaser(c chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(c) }) }
}
