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
