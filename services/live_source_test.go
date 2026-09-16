package services

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// livePlaylistServer serves a playlist whose body the test swaps at will,
// and segments from a map. Requests are counted per path.
type livePlaylistServer struct {
	mu        sync.Mutex
	playlist  string
	status    int
	segments  map[string]string
	segFail   map[string]int
	segAlways map[string]int
	hits      map[string]int
	srv       *httptest.Server
}

func newLivePlaylistServer(t *testing.T) *livePlaylistServer {
	t.Helper()
	s := &livePlaylistServer{status: 200, segments: map[string]string{}, segFail: map[string]int{}, segAlways: map[string]int{}, hits: map[string]int{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.hits[r.URL.Path]++
		if r.URL.Path == "/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8" {
			if s.status != 200 {
				w.WriteHeader(s.status)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte(s.playlist))
			return
		}
		if st, ok := s.segAlways[r.URL.Path]; ok {
			w.WriteHeader(st)
			return
		}
		if st, ok := s.segFail[r.URL.Path]; ok {
			delete(s.segFail, r.URL.Path)
			w.WriteHeader(st)
			return
		}
		if body, ok := s.segments[r.URL.Path]; ok {
			w.Header().Set("Content-Type", "text/vtt")
			_, _ = w.Write([]byte(body))
			return
		}
		w.WriteHeader(404)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *livePlaylistServer) set(playlist string, segs map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.playlist = playlist
	for k, v := range segs {
		s.segments["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/"+k] = v
	}
}

// failSegmentOnce makes the next request for one segment answer status,
// once. The playlist keeps answering 200: a hiccup on a segment is what the
// proxy in front of the transcoder does under load, and says nothing about
// the session.
func (s *livePlaylistServer) failSegmentOnce(name string, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segFail["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/"+name] = status
}

// failSegmentAlways makes every request for one segment answer status: the
// segment the transcoder has GC'd while still listing it, or a path the
// proxy in front of it keeps rate-limiting.
func (s *livePlaylistServer) failSegmentAlways(name string, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segAlways["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/"+name] = status
}

// counterValue reads a counter without prometheus/testutil, which would pull
// a new module in for one number.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func (s *livePlaylistServer) url() string {
	return s.srv.URL + "/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8?token=T"
}

const pl1 = "#EXTM3U\n#EXT-X-SESSION-OFFSET:0\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:EVENT\n#EXT-X-TARGETDURATION:64\n#EXTINF:63.7,\ns0-0.vtt?token=T\n"
const pl2 = pl1 + "#EXTINF:1.3,\ns0-1.vtt?token=T\n"
const pl2end = pl2 + "#EXT-X-ENDLIST\n"

func TestLiveSourceFetchesOnlyNewSegments(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r, err := ls.Refresh(context.Background())
	if err != nil || r.Added != 1 || r.Ended {
		t.Fatalf("r=%+v err=%v", r, err)
	}
	srv.set(pl2, nil)
	r, err = ls.Refresh(context.Background())
	if err != nil || r.Added != 2 || r.Ended || ls.Doc().Len() != 3 {
		t.Fatalf("r=%+v len=%d err=%v", r, ls.Doc().Len(), err)
	}
	srv.set(pl2end, nil)
	r, err = ls.Refresh(context.Background())
	if err != nil || r.Added != 0 || !r.Ended || !ls.Ended() {
		t.Fatalf("r=%+v err=%v", r, err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.hits["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0-0.vtt"] != 1 {
		t.Fatalf("segment 0 fetched %d times", srv.hits["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0-0.vtt"])
	}
	if !ls.Contiguous() {
		t.Fatal("offset-0-only run must be contiguous")
	}
}

func TestLiveSourceSeekIsANewRun(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// After a seek the transcoder restarts numbering at s0-0 with a new
	// offset and new content: the same Name must be fetched again.
	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\ns0-0.vtt?token=T\n"
	srv.set(seek, map[string]string{"s0-0.vtt": "WEBVTT\n\n00:01.000 --> 00:02.000\nПозже.\n"})
	r, err := ls.Refresh(context.Background())
	if err != nil || r.Added != 1 {
		t.Fatalf("r=%+v err=%v", r, err)
	}
	snap := ls.Doc().Snapshot()
	if snap.Cues[len(snap.Cues)-1].Start != 601*time.Second {
		t.Fatalf("seeked cue at %v", snap.Cues[len(snap.Cues)-1].Start)
	}
	if ls.Contiguous() {
		t.Fatal("a run with offset 600 breaks contiguity")
	}
}

func TestLiveSourceGoneAndTooLarge(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 10, 5000) // 10 bytes cap
	if _, err := ls.Refresh(context.Background()); !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("want too large, got %v", err)
	}
	srv.mu.Lock()
	srv.status = 404
	srv.mu.Unlock()
	ls2 := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls2.Refresh(context.Background()); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("want gone, got %v", err)
	}
}

// TestLiveSourceCumulativeBytesCap exercises the cumulative-bytes check in
// Refresh's segment loop (s.doc.Bytes()+len(body) > s.maxBytes), not the
// per-fetch limit in get(). The cap must sit above the playlist body and
// above either segment alone, but below their combined size, so the first
// segment is accepted and the second is refused only once its bytes would
// push the running total over the cap.
//
// This cannot use the package-level pl2 fixture: pl2 is 169 bytes, more
// than len(seg0)+len(seg1) (126 bytes), so no cap value can sit below the
// combined segment size and still above the playlist body — the playlist
// fetch itself would trip get()'s per-fetch limit first. miniPl carries
// the same two segments with a leaner body so the combined-segments cap
// is reachable.
func TestLiveSourceCumulativeBytesCap(t *testing.T) {
	srv := newLivePlaylistServer(t)
	miniPl := "#EXTM3U\n#EXT-X-SESSION-OFFSET:0\n#EXTINF:1,\ns0-0.vtt\n#EXTINF:1,\ns0-1.vtt\n"
	srv.set(miniPl, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})

	maxBytes := int64(len(miniPl))
	if int64(len(seg0)) > maxBytes {
		maxBytes = int64(len(seg0))
	}
	if int64(len(seg1)) > maxBytes {
		maxBytes = int64(len(seg1))
	}
	maxBytes++
	if maxBytes >= int64(len(seg0)+len(seg1)) {
		t.Fatalf("fixture invalid: maxBytes=%d must stay below len(seg0)+len(seg1)=%d", maxBytes, len(seg0)+len(seg1))
	}

	ls := NewLiveSource(srv.url(), srv.srv.Client(), maxBytes, 5000)
	_, err := ls.Refresh(context.Background())
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("want too large, got %v", err)
	}

	// The first segment (s0-0, in playlist order) fit under the cap and was
	// accepted; the second (s0-1) was refused by the cumulative-bytes check
	// before ever reaching AddSegment. Only the first segment's cues should
	// be in the doc.
	want := NewLiveDoc()
	if _, werr := want.AddSegment(0, []byte(seg0)); werr != nil {
		t.Fatal(werr)
	}
	if got := ls.Doc().Len(); got != want.Len() {
		t.Fatalf("doc len=%d, want %d (only the first segment's cues)", got, want.Len())
	}
}

// TestLiveSourceCueCap exercises the cue-count check in Refresh's segment
// loop (s.doc.Len() > s.maxCues), which runs after AddSegment: the segment
// that pushes the count over the cap is already merged into the doc by the
// time Refresh returns the error. This documents that ruled behavior.
func TestLiveSourceCueCap(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})

	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 1) // cap at 1 cue
	_, err := ls.Refresh(context.Background())
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("want too large, got %v", err)
	}
	if got := ls.Doc().Len(); got != 3 {
		t.Fatalf("doc len=%d, want 3 (cap checked after AddSegment already applied it)", got)
	}
}

// TestLiveSourceRetireEndsFutureRefreshes pins Retire's contract: once
// called, every subsequent Refresh returns ErrSourceGone without touching
// the network, so a job still polling a source the handler has replaced in
// its cache exits on its very next tick instead of waiting for the
// transcoder's own 404.
func TestLiveSourceRetireEndsFutureRefreshes(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	ls.Retire()
	if _, err := ls.Refresh(context.Background()); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("want gone after retire, got %v", err)
	}

	srv.mu.Lock()
	hits := srv.hits["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8"]
	srv.mu.Unlock()
	if hits != 1 {
		t.Fatalf("refresh after retire must not touch the network: playlist hits=%d, want 1", hits)
	}
}

// TestLiveSourceSegmentFailureIsTransient: only the playlist's 404/503 says
// the session is over. A segment's is a hiccup of whatever sits between us
// and the transcoder — torrent-http-proxy answers 503 under load and while
// rate-limiting — and ending the translation of a whole film on one of them
// reports a source_gone that is not true.
func TestLiveSourceSegmentFailureIsTransient(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	srv.failSegmentOnce("s0-1.vtt", 503)
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)

	_, err := ls.Refresh(context.Background())
	if err == nil {
		t.Fatal("the failing segment must surface as an error")
	}
	if errors.Is(err, ErrSourceGone) {
		t.Fatalf("a segment's 503 must be transient, got %v", err)
	}
	if ls.Gone() {
		t.Fatal("a segment's 503 must not mark the session gone")
	}

	// The segment was never marked seen, so the next tick picks it up.
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatalf("retry after a transient segment failure: %v", err)
	}
	if got := ls.Doc().Len(); got != 3 {
		t.Fatalf("doc len=%d, want 3 (both segments merged)", got)
	}
}

// TestLiveSourcePlaylistGoneMarksGone is the other side of the same split,
// and pins what Gone means: the transcoder's verdict on the playlist, never
// Retire's local bookkeeping.
func TestLiveSourcePlaylistGoneMarksGone(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	ls.Retire()
	if _, err := ls.Refresh(context.Background()); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("want gone after retire, got %v", err)
	}
	if ls.Gone() {
		t.Fatal("Retire is not evidence about the session")
	}

	srv.mu.Lock()
	srv.status = 503
	srv.mu.Unlock()
	ls2 := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls2.Refresh(context.Background()); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("want gone on a playlist 503, got %v", err)
	}
	if !ls2.Gone() {
		t.Fatal("a 503 on the playlist ends the session")
	}
}

// TestLiveSourceRefreshIfStaleSkipsAFreshRead: the handler calls this on
// every poll, so a source the loop just refreshed must not cost a second
// playlist read.
func TestLiveSourceRefreshIfStaleSkipsAFreshRead(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	playlist := "/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8"

	// Never refreshed: it reads, whatever the age.
	if _, err := ls.RefreshIfStale(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	first := srv.hits[playlist]
	srv.mu.Unlock()
	if first != 1 {
		t.Fatalf("playlist hits=%d, want 1", first)
	}

	if _, err := ls.RefreshIfStale(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	second := srv.hits[playlist]
	srv.mu.Unlock()
	if second != 1 {
		t.Fatalf("a fresh source must not be re-read: playlist hits=%d", second)
	}

	if _, err := ls.RefreshIfStale(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	third := srv.hits[playlist]
	srv.mu.Unlock()
	if third != 2 {
		t.Fatalf("a stale source must be re-read: playlist hits=%d", third)
	}
}

// TestLiveSourceRefreshIfStaleThrottlesAFailingUpstream: staleness keyed on
// the last *success* means an upstream that is failing is re-read on every
// single poll, from every viewer, on every replica — one playlist read per
// poll instead of the documented one per interval, and each of them can burn
// the full fetch timeout. The attempt is what the throttle is about, not its
// outcome.
func TestLiveSourceRefreshIfStaleThrottlesAFailingUpstream(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	srv.mu.Lock()
	srv.status = 500
	srv.mu.Unlock()
	playlist := "/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8"
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)

	if _, err := ls.RefreshIfStale(context.Background(), time.Hour); err == nil {
		t.Fatal("a 500 on the playlist must surface as an error")
	}
	// The second poll lands inside maxAge: it serves what is known instead of
	// asking the sick upstream again.
	if _, err := ls.RefreshIfStale(context.Background(), time.Hour); err != nil {
		t.Fatalf("a throttled poll must serve what is known, got %v", err)
	}
	srv.mu.Lock()
	hits := srv.hits[playlist]
	srv.mu.Unlock()
	if hits != 1 {
		t.Fatalf("a failing upstream must be re-read at most once per maxAge: playlist hits=%d, want 1", hits)
	}

	// Past maxAge it is asked again — the throttle is a rate limit, not a
	// memory of failure.
	if _, err := ls.RefreshIfStale(context.Background(), 0); err == nil {
		t.Fatal("past maxAge the upstream must be asked again")
	}
	srv.mu.Lock()
	hits = srv.hits[playlist]
	srv.mu.Unlock()
	if hits != 2 {
		t.Fatalf("playlist hits=%d, want 2", hits)
	}
}

// TestLiveSourceRefreshIfStaleDoesNotQueueBehindAnInFlightRefresh: the
// handler calls this on every poll of every viewer of the key, on a context
// detached from the request. Blocking on refreshMu would pile those polls up
// behind one stalled playlist read (the transcoder accepting and then not
// answering is the scenario the poll loop's own comment cites), each waiting
// up to the fetch timeout, and a disconnected client would not free its slot.
// Liveness is the point of the handler's refresh, not the freshness of this
// one response.
func TestLiveSourceRefreshIfStaleDoesNotQueueBehindAnInFlightRefresh(t *testing.T) {
	release := make(chan struct{})
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		<-release
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-SESSION-OFFSET:0\n#EXT-X-TARGETDURATION:64\n"))
	}))
	defer srv.Close()
	// Releasing before Close runs on every exit path, a failing assertion
	// included: Close waits out the in-flight request, so a test that dies
	// while the handler is parked would hang the suite instead of failing it.
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	defer releaseAll()

	ls := NewLiveSource(srv.URL+"/s0.m3u8", srv.Client(), 1<<20, 5000)
	stuck := make(chan struct{})
	go func() {
		defer close(stuck)
		_, _ = ls.RefreshIfStale(context.Background(), time.Hour)
	}()
	for atomic.LoadInt64(&hits) == 0 {
		time.Sleep(time.Millisecond)
	}

	second := make(chan struct{})
	go func() {
		defer close(second)
		_, _ = ls.RefreshIfStale(context.Background(), time.Hour)
	}()
	select {
	case <-second:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("a poll queued behind an in-flight refresh instead of serving what is known")
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("the second poll read the playlist anyway: hits=%d", got)
	}

	releaseAll()
	<-stuck
	<-second
}

// TestLiveSourceSkipsASegmentAfterThreeStrikes: a segment that keeps failing
// is retried at the head of the playlist forever, so one dead segment freezes
// the whole document — playlist 200, X-Subtitle-Live: 1, progress stuck, and
// nothing but a repeating warn to say so. A segment the transcoder has GC'd
// while still listing it, or one behind a path the proxy keeps rate-limiting,
// does exactly that. After three strikes it is given up on, the pass carries
// on to the segments behind it, and the skip leaves a trace.
func TestLiveSourceSkipsASegmentAfterThreeStrikes(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	srv.failSegmentAlways("s0-0.vtt", 503)
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	before := counterValue(t, LiveSegmentsSkipped)

	for i := 0; i < 2; i++ {
		if _, err := ls.Refresh(context.Background()); err == nil {
			t.Fatalf("pass %d: a failing segment must still surface as an error", i)
		}
		if got := ls.Doc().Len(); got != 0 {
			t.Fatalf("pass %d: playlist order must hold while the segment has strikes left, doc len=%d", i, got)
		}
	}

	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatalf("the third strike must skip the segment and finish the pass: %v", err)
	}
	if got := ls.Doc().Len(); got != 2 {
		t.Fatalf("doc len=%d, want 2 (the cues of the segment behind the dead one)", got)
	}
	if got := counterValue(t, LiveSegmentsSkipped) - before; got != 1 {
		t.Fatalf("skipped counter moved by %v, want 1", got)
	}
	if ls.Contiguous() {
		t.Fatal("a skipped segment is a hole: the run must not be able to write a final artifact")
	}

	// And it is not paid for again on every later pass.
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatalf("a skipped segment must not come back: %v", err)
	}
	if got := counterValue(t, LiveSegmentsSkipped) - before; got != 1 {
		t.Fatalf("skipped counter moved by %v after a fourth pass, want 1", got)
	}
}

// TestLiveSourceRetriesASegmentBeforeGivingUp is the other side: the strikes
// are consecutive, so the hiccup the transient path exists for (a 503 under
// load) still costs nothing. Two failures then a success must leave the
// document whole and nothing skipped.
func TestLiveSourceRetriesASegmentBeforeGivingUp(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	before := counterValue(t, LiveSegmentsSkipped)

	for i := 0; i < 2; i++ {
		srv.failSegmentOnce("s0-0.vtt", 503)
		if _, err := ls.Refresh(context.Background()); err == nil {
			t.Fatalf("pass %d: the failing segment must surface as an error", i)
		}
	}
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatalf("the third pass succeeds: %v", err)
	}
	if got := ls.Doc().Len(); got != 3 {
		t.Fatalf("doc len=%d, want 3 (both segments merged)", got)
	}
	if got := counterValue(t, LiveSegmentsSkipped) - before; got != 0 {
		t.Fatalf("nothing was given up on, counter moved by %v", got)
	}
	if !ls.Contiguous() {
		t.Fatal("no segment was skipped, so the run stays contiguous")
	}
}
