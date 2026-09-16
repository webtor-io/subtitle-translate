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

	// refreshMu serializes whole Refresh calls: the handler refreshes once
	// on the first GET while the job's loop refreshes on its own ticker, and
	// two concurrent passes over the same playlist would fetch every new
	// segment twice (seen is only marked after the fetch).
	refreshMu sync.Mutex

	mu         sync.Mutex
	seen       map[string]bool
	ended      bool
	contiguous bool
	retired    bool
	// gone records that the playlist itself answered 404/503 — the session
	// is over as far as the transcoder is concerned. Retire does not set it:
	// a source this process replaced in its cache is not evidence about the
	// session, only about our own bookkeeping.
	gone bool
	// lastRefresh is when Refresh last completed successfully. It is what
	// RefreshIfStale reads, so a replica that does not own the job can keep
	// the source it serves growing without doubling the poll rate of the
	// one that does.
	lastRefresh time.Time
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

// Gone reports whether the playlist answered 404/503: the transcoder
// session this source follows is over. Distinct from Retire, which is this
// process deciding to stop using a source that may well still be alive —
// the handler tells the two apart when it has to choose between a source
// it retired and the one it is serving now.
func (s *LiveSource) Gone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gone
}

// Retire marks the source as gone for good, without touching the network:
// the handler calls this on the LiveSource it is about to replace in its
// cache (a new transcoder session reusing the same artifact key), so the
// job still polling the old URL sees ErrSourceGone on its very next tick
// instead of waiting out its own poll interval — and possibly an in-flight
// batch — before the transcoder itself 404s it. A Refresh already past
// this check when Retire runs completes normally; only the next call is
// turned away.
func (s *LiveSource) Retire() {
	s.mu.Lock()
	s.retired = true
	s.mu.Unlock()
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
	if res.StatusCode != http.StatusOK {
		// The status is carried, not interpreted: only the playlist's
		// 404/503 means the session is over (see Refresh). A segment's is a
		// hiccup of whatever sits in between — torrent-http-proxy answers
		// 503 under load and while rate-limiting — and the next tick asks
		// for it again.
		return nil, &sourceStatusError{status: res.StatusCode}
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

// sourceStatusError is a non-200 from the transcoder, carrying the status
// so the caller can decide what it means for the URL it asked for.
type sourceStatusError struct{ status int }

func (e *sourceStatusError) Error() string { return fmt.Sprintf("source returned %d", e.status) }

// endsSession reports whether a status on the playlist means the transcoder
// session is over for good. 404: the session was reaped (viewer left).
// 503: the transcoder hit its restart budget for this session.
func endsSession(err error) (int, bool) {
	var se *sourceStatusError
	if !errors.As(err, &se) {
		return 0, false
	}
	return se.status, se.status == http.StatusNotFound || se.status == http.StatusServiceUnavailable
}

// Refresh reads the playlist once and fetches every segment not seen
// before (in playlist order). It returns ErrSourceGone on a 404/503 for
// the playlist or when Retire was called, ErrSourceTooLarge past the caps;
// other fetch errors — a segment's 404/503 included — are returned as-is
// (transient: the caller retries next tick, and the segment is not in seen,
// so nothing is lost).
func (s *LiveSource) Refresh(ctx context.Context) (Refresh, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return s.refresh(ctx)
}

// RefreshIfStale runs Refresh only when the last successful one is older
// than maxAge (or there has not been one). It exists for the handler: the
// job refreshes on its own ticker, but only on the replica that won the
// store lock, and every other replica serves a document that nothing else
// would ever advance. Calling this on every poll costs at most one playlist
// read per maxAge per key per replica, and is a no-op on the replica whose
// loop has just refreshed the same object.
func (s *LiveSource) RefreshIfStale(ctx context.Context, maxAge time.Duration) (Refresh, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	s.mu.Lock()
	last := s.lastRefresh
	ended := s.ended
	s.mu.Unlock()
	if !last.IsZero() && time.Since(last) < maxAge {
		return Refresh{Ended: ended}, nil
	}
	return s.refresh(ctx)
}

// refresh is Refresh with the refresh lock already held.
func (s *LiveSource) refresh(ctx context.Context) (Refresh, error) {
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired {
		return Refresh{}, ErrSourceGone
	}
	data, err := s.get(ctx, s.url)
	if err != nil {
		if status, ends := endsSession(err); ends {
			s.mu.Lock()
			s.gone = true
			s.mu.Unlock()
			return Refresh{}, errors.Wrapf(ErrSourceGone, "status %d", status)
		}
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
	s.lastRefresh = time.Now()
	s.mu.Unlock()
	return out, nil
}
