package services

import (
	"context"
	"sync"
	"time"
)

type Progress struct {
	Total int
	Lines []string
}

type Store interface {
	GetFinal(ctx context.Context, key string) ([]byte, bool, error)
	PutFinal(ctx context.Context, key string, vtt []byte) error
	GetProgress(ctx context.Context, key string) (*Progress, error)
	PutProgress(ctx context.Context, key string, p *Progress) error
	DropProgress(ctx context.Context, key string) error
	TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	RefreshLock(ctx context.Context, key string, ttl time.Duration) error
	Unlock(ctx context.Context, key string) error
}

type MemoryStore struct {
	mu       sync.Mutex
	final    map[string][]byte
	progress map[string]*Progress
	locks    map[string]time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{final: map[string][]byte{}, progress: map[string]*Progress{}, locks: map[string]time.Time{}}
}

func (m *MemoryStore) GetFinal(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.final[key]
	return b, ok, nil
}

func (m *MemoryStore) PutFinal(_ context.Context, key string, vtt []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.final[key] = append([]byte(nil), vtt...)
	return nil
}

func (m *MemoryStore) GetProgress(_ context.Context, key string) (*Progress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.progress[key]
	if !ok {
		return nil, nil
	}
	cp := &Progress{Total: p.Total, Lines: append([]string(nil), p.Lines...)}
	return cp, nil
}

func (m *MemoryStore) PutProgress(_ context.Context, key string, p *Progress) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progress[key] = &Progress{Total: p.Total, Lines: append([]string(nil), p.Lines...)}
	return nil
}

func (m *MemoryStore) DropProgress(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.progress, key)
	return nil
}

func (m *MemoryStore) TryLock(_ context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if until, ok := m.locks[key]; ok && time.Now().Before(until) {
		return false, nil
	}
	m.locks[key] = time.Now().Add(ttl)
	return true, nil
}

func (m *MemoryStore) RefreshLock(_ context.Context, key string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locks[key] = time.Now().Add(ttl)
	return nil
}

func (m *MemoryStore) Unlock(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, key)
	return nil
}
