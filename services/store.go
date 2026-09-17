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
	// CueKeys is the CueKey of every cue, indexed like Lines. Live jobs only:
	// a restarted job matches the cues it rediscovers against these and
	// reuses the translations instead of paying for them twice.
	CueKeys []string
	// Live marks a progress record written while the playlist was still
	// growing. It is cleared on the last write of a live job that produced
	// no final artifact, which is how a reader tells "still coming" from
	// "this is all there will be".
	Live bool
	// Status is the terminal state of a live run that stopped: "done" (an
	// ENDLIST was reached and everything pending was translated, whether or
	// not a final artifact came out of it) or "stopped" (the source went
	// away, or outgrew its caps, before that). Empty means neither — the
	// job is running, idle-paused, or this record predates the field. Set
	// only alongside Live=false (see stopLive), and cleared back to empty
	// the moment a new run re-arms the job, so a stale terminal value from
	// the previous run is never read as this one's.
	//
	// A gob-compatible addition: appended at the end, so a record encoded
	// before this field existed decodes with Status == "".
	Status string
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
	// PutPos and GetPos carry the viewer's playhead (movie time) for a key.
	// It is what makes a file-source job position-aware the way a live one
	// is: a live job reads the run's offset off the transcoder playlist,
	// a file job has no playlist, so the player's polls leave the position
	// here and the job orders its batches by it (pendingByTime, the same
	// rule the live loop uses). Best effort on both ends: no position means
	// file order, exactly the behaviour before positions existed.
	PutPos(ctx context.Context, key string, pos time.Duration) error
	GetPos(ctx context.Context, key string) (time.Duration, bool, error)
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
	pos      map[string]time.Duration
}

func (m *MemoryStore) PutPos(_ context.Context, key string, pos time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pos[key] = pos
	return nil
}

func (m *MemoryStore) GetPos(_ context.Context, key string) (time.Duration, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pos[key]
	return p, ok, nil
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{final: map[string][]byte{}, progress: map[string]*Progress{}, locks: map[string]memLock{}, pos: map[string]time.Duration{}}
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
	cp := &Progress{Total: p.Total, Lines: append([]string(nil), p.Lines...),
		CueKeys: append([]string(nil), p.CueKeys...), Live: p.Live, Status: p.Status}
	return cp, nil
}

func (m *MemoryStore) PutProgress(_ context.Context, key string, p *Progress) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progress[key] = &Progress{Total: p.Total, Lines: append([]string(nil), p.Lines...),
		CueKeys: append([]string(nil), p.CueKeys...), Live: p.Live, Status: p.Status}
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
