package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	ErrSourceGone     = errors.New("source gone")
	ErrSourceTooLarge = errors.New("source too large")
)

// segmentMaxStrikes is how many consecutive failures one segment gets before
// the source gives up on it. A segment is retried at the head of the playlist
// (order matters: the segments behind it wait), so without a terminal state
// one segment the transcoder has GC'd while still listing it — or one behind
// a path the proxy keeps rate-limiting — freezes the whole document while the
// playlist keeps answering 200. Three covers the hiccup the transient path
// exists for and still gives up within a few poll intervals.
const segmentMaxStrikes = 3

type Refresh struct {
	Added int
	Ended bool
}

// LiveSource follows one subtitle media playlist of a transcoder session.
type LiveSource struct {
	// url is the URL fetched from, query included. It is mutable: the same
	// session arrives with a different query every ~15 s (the player's ?rev=
	// bump) and can be handed a renewed token, and the newest query is the
	// one upstream will still accept. Identity — whether two URLs are the
	// same session at all — is the handler's business (sourceIdentity), and
	// is decided without the query.
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

	mu   sync.Mutex
	seen map[string]bool
	// fails counts consecutive failures per segment key; a success clears
	// the entry, so only a segment failing every time reaches the strike
	// limit.
	fails      map[string]int
	ended      bool
	contiguous bool
	retired    bool
	// runEnded records that the run this source followed is finished and
	// produced no final artifact: the playlist ended, everything it carried
	// was translated, and the run was not contiguous so nothing may be
	// written as final. There is no work left against this source, which is
	// what keeps a poll from starting a job that would immediately conclude
	// the same thing.
	runEnded bool
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
	// lastAttempt is when a refresh last finished, successfully or not.
	// RefreshIfStale gates on it rather than on lastRefresh alone, so an
	// upstream that keeps failing costs one playlist read per maxAge per
	// replica instead of one per poll of every viewer — a 5xx, a
	// half-written playlist and a transcoder that accepts and stalls all
	// last longer than a poll interval.
	lastAttempt time.Time
}

func NewLiveSource(playlistURL string, client *http.Client, maxBytes int64, maxCues int) *LiveSource {
	return &LiveSource{url: playlistURL, client: client, doc: NewLiveDoc(), maxBytes: maxBytes, maxCues: maxCues, seen: map[string]bool{}, fails: map[string]int{}, contiguous: true}
}

func (s *LiveSource) Doc() *LiveDoc { return s.doc }

// URL is the URL this source fetches from right now.
func (s *LiveSource) URL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.url
}

// SetURL points the source at a fresher URL for the same session, keeping
// everything else: the document, the segments already seen, the strikes. The
// handler calls it when a poll arrives for this session with a different
// query — a ?rev= bump, or a renewed token.
func (s *LiveSource) SetURL(u string) {
	s.mu.Lock()
	s.url = u
	s.mu.Unlock()
}

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

// MarkRunEnded says this source's run is over with no final artifact to
// come. See the runEnded field.
func (s *LiveSource) MarkRunEnded() {
	s.mu.Lock()
	s.runEnded = true
	s.mu.Unlock()
}

// RunEnded reports whether there is any point starting a job against this
// source.
func (s *LiveSource) RunEnded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runEnded
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

// RefreshIfStale runs Refresh only when the last attempt — successful or
// not — is older than maxAge (or there has not been one). It exists for the
// handler: the job refreshes on its own ticker, but only on the replica that
// won the store lock, and every other replica serves a document that nothing
// else would ever advance. Calling this on every poll costs at most one
// playlist read per maxAge per key per replica, and is a no-op on the replica
// whose loop has just refreshed the same object.
//
// It never waits for a refresh already in flight. The handler runs this on a
// context detached from the request, so blocking here would queue every
// poller of the key behind one stalled playlist read — request k waiting k
// fetch timeouts, and a client that hung up not freeing its place. What the
// handler's refresh buys is liveness of the document, not freshness of this
// one response, so a poll that finds the lock taken serves what is known and
// lets the in-flight refresh deliver it to the next one.
func (s *LiveSource) RefreshIfStale(ctx context.Context, maxAge time.Duration) (Refresh, error) {
	if !s.refreshMu.TryLock() {
		return Refresh{Ended: s.Ended()}, nil
	}
	defer s.refreshMu.Unlock()
	s.mu.Lock()
	last := s.lastRefresh
	if s.lastAttempt.After(last) {
		last = s.lastAttempt
	}
	ended := s.ended
	s.mu.Unlock()
	if !last.IsZero() && time.Since(last) < maxAge {
		return Refresh{Ended: ended}, nil
	}
	return s.refresh(ctx)
}

// strike records one more consecutive failure for a segment and returns the
// running count.
func (s *LiveSource) strike(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails[key]++
	return s.fails[key]
}

// skipSegment gives up on a segment: it is marked seen so this pass and every
// later one move on to the segments behind it.
//
// The document is left with a hole no later reader could detect, so the run
// stops being contiguous — a final artifact is served forever and without a
// source, and only a run that saw the whole movie may write one. That is the
// same rule a run joining after a seek falls under.
//
// Only the segment's name is logged: its URI carries the session token, and a
// status is all the operator needs to tell a GC'd segment from a rate limit.
func (s *LiveSource) skipSegment(key, name string, cause error) {
	s.mu.Lock()
	s.seen[key] = true
	delete(s.fails, key)
	s.contiguous = false
	s.mu.Unlock()
	LiveSegmentsSkipped.Inc()
	fields := log.Fields{"segment": name, "strikes": segmentMaxStrikes}
	var se *sourceStatusError
	if errors.As(cause, &se) {
		fields["status"] = se.status
	}
	log.WithFields(fields).Warn("giving up on a live segment after repeated failures")
}

// refresh is Refresh with the refresh lock already held.
func (s *LiveSource) refresh(ctx context.Context) (Refresh, error) {
	s.mu.Lock()
	retired := s.retired
	// Read once, under the lock, and used for both the playlist fetch and
	// the base the segment URIs are resolved against: a SetURL landing
	// mid-pass must not split one refresh across two URLs.
	u := s.url
	s.mu.Unlock()
	if retired {
		return Refresh{}, ErrSourceGone
	}
	// Stamped on the way out whatever happened, and after the work rather
	// than before it: a read that took the whole fetch timeout is not stale
	// the instant it returns. This is what throttles RefreshIfStale against
	// an upstream that is failing.
	defer func() {
		s.mu.Lock()
		s.lastAttempt = time.Now()
		s.mu.Unlock()
	}()
	data, err := s.get(ctx, u)
	if err != nil {
		if status, ends := endsSession(err); ends {
			s.mu.Lock()
			s.gone = true
			s.mu.Unlock()
			return Refresh{}, errors.Wrapf(ErrSourceGone, "status %d", status)
		}
		return Refresh{}, err
	}
	pl, err := ParseMediaPlaylist(u, data)
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
			if s.strike(key) < segmentMaxStrikes {
				return out, err
			}
			s.skipSegment(key, seg.Name, err)
			continue
		}
		if s.doc.Bytes()+int64(len(body)) > s.maxBytes {
			return out, ErrSourceTooLarge
		}
		n, err := s.doc.AddSegment(pl.Offset, body)
		if err != nil {
			// Same terminal state, for the same reason: a segment that
			// cannot be parsed will not parse on the next tick either, and
			// it sits in front of everything after it.
			if s.strike(key) < segmentMaxStrikes {
				return out, err
			}
			s.skipSegment(key, seg.Name, err)
			continue
		}
		if s.doc.Len() > s.maxCues {
			return out, ErrSourceTooLarge
		}
		s.mu.Lock()
		s.seen[key] = true
		// A success clears the segment's strikes. Not observable through
		// Refresh — a segment that succeeded is also marked seen, so it is
		// never fetched again — but it keeps fails from accumulating an
		// entry per segment of the film.
		delete(s.fails, key)
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
	// A playlist that is live again (no ENDLIST) and carries new cues is a
	// new run, not a continuation of whatever the previous run concluded:
	// clear the sticky mark so a HEAD/GET in between reports live again,
	// and so the job that picks the source back up follows this run to its
	// own ENDLIST instead of treating the first batch as final because the
	// mark was still set from before.
	if !pl.Ended && out.Added > 0 {
		s.ended = false
	}
	if out.Added > 0 {
		// New cues are new work, whatever the source concluded before: the
		// runEnded mark only ever said the PREVIOUS run finished with
		// nothing left to do, not that nothing can ever happen against this
		// source again. A seek keeps the same session and playlist URL, so
		// without this a run that ends and is later reused by a second seek
		// stays marked forever and Ensure refuses to start a job for it
		// (job.go), leaving the viewer stuck.
		s.runEnded = false
	}
	out.Ended = s.ended
	s.lastRefresh = time.Now()
	s.mu.Unlock()
	return out, nil
}
