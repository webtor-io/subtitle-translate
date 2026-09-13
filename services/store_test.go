package services

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreContract(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if p, err := s.GetProgress(ctx, "k"); err != nil || p != nil {
		t.Fatalf("empty progress: %v %v", p, err)
	}
	if err := s.PutProgress(ctx, "k", &Progress{Total: 3, Lines: []string{"a", "", ""}}); err != nil {
		t.Fatal(err)
	}
	p, _ := s.GetProgress(ctx, "k")
	if p == nil || p.Total != 3 || p.Lines[0] != "a" {
		t.Fatalf("progress=%+v", p)
	}
	ok, _ := s.TryLock(ctx, "k", time.Minute)
	ok2, _ := s.TryLock(ctx, "k", time.Minute)
	if !ok || ok2 {
		t.Fatalf("lock: first=%v second=%v", ok, ok2)
	}
	_ = s.Unlock(ctx, "k")
	if ok3, _ := s.TryLock(ctx, "k", time.Minute); !ok3 {
		t.Fatal("lock must be free after Unlock")
	}
	if _, found, _ := s.GetFinal(ctx, "k"); found {
		t.Fatal("no final yet")
	}
	_ = s.PutFinal(ctx, "k", []byte("WEBVTT\n"))
	if b, found, _ := s.GetFinal(ctx, "k"); !found || string(b) != "WEBVTT\n" {
		t.Fatalf("final=%q found=%v", b, found)
	}
	_ = s.DropProgress(ctx, "k")
	if p, _ := s.GetProgress(ctx, "k"); p != nil {
		t.Fatal("progress must be dropped")
	}
}

func TestMemoryStoreGetFinalCopies(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	original := []byte("WEBVTT\noriginal data")
	_ = s.PutFinal(ctx, "k", original)

	// Get the bytes and mutate them
	b1, found, _ := s.GetFinal(ctx, "k")
	if !found {
		t.Fatal("should have found final")
	}
	// Mutate the returned slice
	if len(b1) > 0 {
		b1[0] = 'X'
	}

	// Verify the stored value is unchanged
	b2, _, _ := s.GetFinal(ctx, "k")
	if string(b2) != string(original) {
		t.Fatalf("stored value was corrupted: got %q expected %q", b2, original)
	}
}

func TestMemoryStoreRefreshLockChecksExpiry(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// RefreshLock on a never-acquired key should not create a lock
	_ = s.RefreshLock(ctx, "k", time.Minute)

	// TryLock on "k" must succeed (lock was not created)
	ok, _ := s.TryLock(ctx, "k", time.Minute)
	if !ok {
		t.Fatal("TryLock must succeed on a key where RefreshLock was never called")
	}

	// Unlock the lock we just acquired
	_ = s.Unlock(ctx, "k")

	// Create an expired lock
	expiredTime := time.Now().Add(-time.Minute)
	s.locks["expired"] = expiredTime

	// RefreshLock on the expired key should not refresh it
	_ = s.RefreshLock(ctx, "expired", time.Minute)

	// TryLock on "expired" must succeed (lock was expired and not refreshed)
	ok2, _ := s.TryLock(ctx, "expired", time.Minute)
	if !ok2 {
		t.Fatal("TryLock must succeed on an expired lock")
	}
}
