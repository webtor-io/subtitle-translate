package services

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// livePlaylistServer serves a playlist whose body the test swaps at will,
// and segments from a map. Requests are counted per path.
type livePlaylistServer struct {
	mu       sync.Mutex
	playlist string
	status   int
	segments map[string]string
	hits     map[string]int
	srv      *httptest.Server
}

func newLivePlaylistServer(t *testing.T) *livePlaylistServer {
	t.Helper()
	s := &livePlaylistServer{status: 200, segments: map[string]string{}, hits: map[string]int{}}
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
