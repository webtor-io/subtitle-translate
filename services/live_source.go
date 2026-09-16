package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrSourceGone     = errors.New("source gone")
	ErrSourceTooLarge = errors.New("source too large")
)

type Refresh struct {
	Added int
	Ended bool
}

// LiveSource follows one subtitle media playlist of a transcoder session.
type LiveSource struct {
	url      string
	client   *http.Client
	doc      *LiveDoc
	maxBytes int64
	maxCues  int

	mu         sync.Mutex
	seen       map[string]bool
	ended      bool
	contiguous bool
}

func NewLiveSource(playlistURL string, client *http.Client, maxBytes int64, maxCues int) *LiveSource {
	return &LiveSource{url: playlistURL, client: client, doc: NewLiveDoc(), maxBytes: maxBytes, maxCues: maxCues, seen: map[string]bool{}, contiguous: true}
}

func (s *LiveSource) Doc() *LiveDoc { return s.doc }

func (s *LiveSource) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

func (s *LiveSource) Contiguous() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contiguous
}

func (s *LiveSource) get(ctx context.Context, u string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, sourceFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	res, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusServiceUnavailable:
		// 404: the session is gone (viewer left). 503: the transcoder hit
		// its restart budget for this session. Neither comes back.
		return nil, errors.Wrapf(ErrSourceGone, "status %d", res.StatusCode)
	default:
		return nil, errors.Errorf("source returned %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, s.maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > s.maxBytes {
		return nil, ErrSourceTooLarge
	}
	return data, nil
}

// Refresh reads the playlist once and fetches every segment not seen
// before (in playlist order). It returns ErrSourceGone on a 404/503 for
// the playlist, ErrSourceTooLarge past the caps; other fetch errors are
// returned as-is (transient: the caller retries next tick).
func (s *LiveSource) Refresh(ctx context.Context) (Refresh, error) {
	data, err := s.get(ctx, s.url)
	if err != nil {
		return Refresh{}, err
	}
	pl, err := ParseMediaPlaylist(s.url, data)
	if err != nil {
		return Refresh{}, err
	}
	var out Refresh
	for _, seg := range pl.Segments {
		key := fmt.Sprintf("%d|%s", pl.Offset/time.Millisecond, seg.Name)
		s.mu.Lock()
		dup := s.seen[key]
		s.mu.Unlock()
		if dup {
			continue
		}
		body, err := s.get(ctx, seg.URI)
		if err != nil {
			return out, err
		}
		if s.doc.Bytes()+int64(len(body)) > s.maxBytes {
			return out, ErrSourceTooLarge
		}
		n, err := s.doc.AddSegment(pl.Offset, body)
		if err != nil {
			return out, err
		}
		if s.doc.Len() > s.maxCues {
			return out, ErrSourceTooLarge
		}
		s.mu.Lock()
		s.seen[key] = true
		s.mu.Unlock()
		out.Added += n
	}
	s.mu.Lock()
	if pl.Offset != 0 {
		s.contiguous = false
	}
	if pl.Ended {
		s.ended = true
	}
	out.Ended = s.ended
	s.mu.Unlock()
	return out, nil
}
