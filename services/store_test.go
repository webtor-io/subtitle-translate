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
	token, ok, _ := s.TryLock(ctx, "k", time.Minute)
	_, ok2, _ := s.TryLock(ctx, "k", time.Minute)
	if !ok || ok2 || token == "" {
		t.Fatalf("lock: first=%v second=%v token=%q", ok, ok2, token)
	}
	if refreshed, err := s.RefreshLock(ctx, "k", token, time.Minute); err != nil || !refreshed {
		t.Fatalf("the owner must be able to refresh: %v %v", refreshed, err)
	}
	_ = s.Unlock(ctx, "k", token)
	if _, ok3, _ := s.TryLock(ctx, "k", time.Minute); !ok3 {
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

	// RefreshLock on a never-acquired key must not create a lock.
	if ok, err := s.RefreshLock(ctx, "k", "whatever", time.Minute); ok || err != nil {
		t.Fatalf("refresh of a missing lock: ok=%v err=%v", ok, err)
	}
	token, ok, _ := s.TryLock(ctx, "k", time.Minute)
	if !ok {
		t.Fatal("TryLock must succeed on a key where RefreshLock was never called")
	}
	_ = s.Unlock(ctx, "k", token)

	// An expired lock is not refreshable and does not block a new holder.
	s.locks["expired"] = memLock{token: "t", until: time.Now().Add(-time.Minute)}
	if ok, _ := s.RefreshLock(ctx, "expired", "t", time.Minute); ok {
		t.Fatal("an expired lock must not be refreshable")
	}
	if _, ok2, _ := s.TryLock(ctx, "expired", time.Minute); !ok2 {
		t.Fatal("TryLock must succeed on an expired lock")
	}
}

// TestMemoryStoreLockIsOwned is the negative control for lock ownership:
// a foreign token must neither extend nor release someone else's lock.
func TestMemoryStoreLockIsOwned(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	token, ok, _ := s.TryLock(ctx, "k", time.Minute)
	if !ok {
		t.Fatal("first TryLock must succeed")
	}
	if refreshed, err := s.RefreshLock(ctx, "k", "not-my-token", time.Minute); refreshed || err != nil {
		t.Fatalf("foreign refresh: ok=%v err=%v", refreshed, err)
	}
	if err := s.Unlock(ctx, "k", "not-my-token"); err != nil {
		t.Fatalf("foreign unlock must be a silent no-op, got %v", err)
	}
	if _, stolen, _ := s.TryLock(ctx, "k", time.Minute); stolen {
		t.Fatal("a foreign unlock must not release the lock")
	}
	if refreshed, _ := s.RefreshLock(ctx, "k", token, time.Minute); !refreshed {
		t.Fatal("the owner still holds the lock")
	}
}
