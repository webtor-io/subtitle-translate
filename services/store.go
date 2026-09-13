package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	// TryLock returns an ownership token when the lock was taken. Refresh
	// and Unlock act only for the holder of that token, so a job whose lock
	// expired under it cannot extend or release the one its successor took.
	TryLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
	RefreshLock(ctx context.Context, key string, token string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key string, token string) error
}

// newLockToken is the ownership proof stored under the lock key.
func newLockToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", errors.Wrap(err, "failed to generate lock token")
	}
	return hex.EncodeToString(b[:]), nil
}

type memLock struct {
	token string
	until time.Time
}

type MemoryStore struct {
	mu       sync.Mutex
	final    map[string][]byte
	progress map[string]*Progress
	locks    map[string]memLock
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{final: map[string][]byte{}, progress: map[string]*Progress{}, locks: map[string]memLock{}}
}

func (m *MemoryStore) GetFinal(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.final[key]
	return append([]byte(nil), b...), ok, nil
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

func (m *MemoryStore) TryLock(_ context.Context, key string, ttl time.Duration) (string, bool, error) {
	token, err := newLockToken()
	if err != nil {
		return "", false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.locks[key]; ok && time.Now().Before(l.until) {
		return "", false, nil
	}
	m.locks[key] = memLock{token: token, until: time.Now().Add(ttl)}
	return token, true, nil
}

func (m *MemoryStore) RefreshLock(_ context.Context, key string, token string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.locks[key]
	if !ok || l.token != token || time.Now().After(l.until) {
		return false, nil
	}
	m.locks[key] = memLock{token: token, until: time.Now().Add(ttl)}
	return true, nil
}

func (m *MemoryStore) Unlock(_ context.Context, key string, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.locks[key]; ok && l.token == token {
		delete(m.locks, key)
	}
	return nil
}
