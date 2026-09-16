package services

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func newHandlerForTest(t *testing.T, tr Translator, source string) (*Handler, *httptest.Server) {
	t.Helper()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if source == "" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(source))
	}))
	t.Cleanup(src.Close)
	h := &Handler{Runner: NewRunner(NewMemoryStore(), tr, 3, 4, time.Minute), Model: "m", Client: src.Client(), MaxSourceBytes: 1 << 20, MaxCues: 5000}
	return h, src
}

func do(h http.Handler, method, path, sourceURL string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-Info-Hash", "abc")
	req.Header.Set("X-Path", "/movie.srt~vtt/movie.vtt")
	if sourceURL != "" {
		req.Header.Set("X-Source-Url", sourceURL)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTargetLangPrefersModExtraHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/Sintel.en.vtt", nil)
	if _, ok := TargetLang(req); ok {
		t.Fatal("no header and no ~tr segment must be rejected")
	}
	req.Header.Set("X-Mod-Extra", "pt")
	if got, ok := TargetLang(req); !ok || got != "pt" {
		t.Fatalf("header lang: got %q ok=%v", got, ok)
	}
	req.Header.Set("X-Mod-Extra", "xx")
	if _, ok := TargetLang(req); ok {
		t.Fatal("unknown header lang must be rejected, not fall back to the path")
	}
	full := httptest.NewRequest("GET", "/abc/movie.vtt~tr:ru/movie.vtt", nil)
	if got, ok := TargetLang(full); !ok || got != "ru" {
		t.Fatalf("path fallback: got %q ok=%v", got, ok)
	}
}

func TestParseLang(t *testing.T) {
	for p, want := range map[string]string{
		"/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt":      "pt",
		"/abc/movie.mkv~vi/opensubtitles/1.vtt~tr:ru/1.vtt": "ru",
	} {
		if got, ok := ParseLang(p); !ok || got != want {
			t.Errorf("%s: got %q ok=%v", p, got, ok)
		}
	}
	for _, p := range []string{"/abc/movie.vtt", "/abc/movie.vtt~tr:xx/movie.vtt", "/abc/movie.vtt~tr:pt/movie.srt", "/abc/movie.vtt~tr:PT/movie.vtt"} {
		if _, ok := ParseLang(p); ok {
			t.Errorf("%s must be rejected", p)
		}
	}
}

func TestHandlerRejectsBadRequests(t *testing.T) {
	h, src := newHandlerForTest(t, &fakeTranslator{}, vttWith(2))
	if rec := do(h, "GET", "/abc/movie.vtt", src.URL); rec.Code != 400 {
		t.Fatalf("no lang: %d", rec.Code)
	}
	if rec := do(h, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", ""); rec.Code != 400 {
		t.Fatalf("no source: %d", rec.Code)
	}
	h2, src2 := newHandlerForTest(t, &fakeTranslator{}, "")
	if rec := do(h2, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", src2.URL); rec.Code != 404 {
		t.Fatalf("source 404 must map to 404: %d", rec.Code)
	}
	h.MaxCues = 1
	if rec := do(h, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", src.URL); rec.Code != 413 {
		t.Fatalf("too many cues: %d", rec.Code)
	}
}

func TestHandlerProgressiveGetAndHead(t *testing.T) {
	ft := &fakeTranslator{block: make(chan struct{})}
	h, src := newHandlerForTest(t, ft, vttWith(5))
	path := "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt"
	rec := do(h, "GET", path, src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "0/5" || rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Content-Type") != "text/vtt; charset=utf-8" {
		t.Fatalf("first get: code=%d headers=%v", rec.Code, rec.Header())
	}
	if !strings.HasPrefix(rec.Body.String(), "WEBVTT") {
		t.Fatalf("body=%q", rec.Body.String())
	}
	ft.block <- struct{}{}
	// Batch 2 starts only after batch 1 was stored.
	waitForCalls(t, ft, 2)
	rec = do(h, "HEAD", path, src.URL)
	if rec.Header().Get("Access-Control-Expose-Headers") != "X-Subtitle-Progress" {
		t.Fatalf("progress header must be exposed to cross-origin readers: %q", rec.Header().Get("Access-Control-Expose-Headers"))
	}
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "3/5" || rec.Body.Len() != 0 {
		t.Fatalf("head: code=%d progress=%s body=%d", rec.Code, rec.Header().Get("X-Subtitle-Progress"), rec.Body.Len())
	}
	close(ft.block)
	key := ArtifactKey("abc", "/movie.srt~vtt/movie.vtt", "pt", "m", PromptVersion)
	h.Runner.Wait(key)
	rec = do(h, "GET", path, src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "100/100" || rec.Header().Get("Cache-Control") != "public, max-age=86400" || !strings.Contains(rec.Body.String(), "PT:line 5") {
		t.Fatalf("final get: code=%d headers=%v body=%q", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestHeadDoesNotStartJob(t *testing.T) {
	ft := &fakeTranslator{}
	h, src := newHandlerForTest(t, ft, vttWith(2))
	rec := do(h, "HEAD", "/abc/movie.vtt~tr:pt/movie.vtt", src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "0/0" {
		t.Fatalf("head before any get: %d %s", rec.Code, rec.Header().Get("X-Subtitle-Progress"))
	}
	time.Sleep(30 * time.Millisecond)
	if ft.calls != 0 {
		t.Fatal("HEAD must not translate")
	}
}

// vttWithMusicAt builds n cues "line 1" … "line n", except cue index
// musicIdx (0-based) which is a music-only line that Normalize strips to
// zero lines — a "structurally empty" cue that is never sent for
// translation (see pendingInBatch in job.go).
func vttWithMusicAt(n, musicIdx int) string {
	var b strings.Builder
	b.WriteString("WEBVTT\n\n")
	for i := 0; i < n; i++ {
		text := fmt.Sprintf("line %d", i+1)
		if i == musicIdx {
			text = "♪ ♪"
		}
		fmt.Fprintf(&b, "%d\n00:00:%02d.000 --> 00:00:%02d.500\n%s\n\n", i+1, i, i, text)
	}
	return b.String()
}

// TestHeadProgressPastStructurallyEmptyCue is a negative control for the
// break-on-first-empty-line bug: with a music-only cue at index 2 (never
// translated, its p.Lines entry stays "" forever), HEAD's progress must
// keep growing as later cues finish instead of freezing at the empty gap.
func TestHeadProgressPastStructurallyEmptyCue(t *testing.T) {
	ft := &fakeTranslator{block: make(chan struct{})}
	h, src := newHandlerForTest(t, ft, vttWithMusicAt(5, 2))
	h.Runner = NewRunner(NewMemoryStore(), ft, 1, 4, time.Minute) // batch size 1: one cue per batch
	path := "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt"

	do(h, "GET", path, src.URL) // starts the background job, cue 0's batch (1st call) blocks

	waitForCalls(t, ft, 1)
	ft.block <- struct{}{} // cue 0 done; cue 1's batch (2nd call) blocks next
	waitForCalls(t, ft, 2)
	ft.block <- struct{}{} // cue 1 done; cue 2 is skipped (normalized to zero lines,
	// never sent for translation), so cue 3's batch (3rd call) blocks next
	waitForCalls(t, ft, 3)
	ft.block <- struct{}{} // cue 3 done; cue 4's batch (4th call) blocks next
	waitForCalls(t, ft, 4)
	// At this point p.Lines = [filled, filled, "", filled, ""]: cue 2 is a
	// permanent gap but cue 3 finished after it, so done must be 3, not
	// frozen at 2 (the old break-on-first-empty behavior).
	rec := do(h, "HEAD", path, src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "3/5" {
		t.Fatalf("progress must count past the empty gap: code=%d progress=%s", rec.Code, rec.Header().Get("X-Subtitle-Progress"))
	}

	// Drain the rest so the job finishes cleanly.
	ft.block <- struct{}{}
	key := ArtifactKey("abc", "/movie.srt~vtt/movie.vtt", "pt", "m", PromptVersion)
	h.Runner.Wait(key)
	rec = do(h, "GET", path, src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "100/100" {
		t.Fatalf("final get: code=%d progress=%s", rec.Code, rec.Header().Get("X-Subtitle-Progress"))
	}
}

func TestParseNamesTruncatesByRunesAndCapsCount(t *testing.T) {
	long := strings.Repeat("Ж", 45) // 45-rune Cyrillic name, over the 40-rune cap
	got := ParseNames(long)
	if len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	if n := utf8.RuneCountInString(got[0]); n != 40 {
		t.Fatalf("rune count=%d want 40", n)
	}
	if !utf8.ValidString(got[0]) {
		t.Fatalf("truncated name is not valid utf-8: %q", got[0])
	}
	if got[0] != strings.Repeat("Ж", 40) {
		t.Fatalf("got %q", got[0])
	}

	var names []string
	for i := 0; i < 35; i++ {
		names = append(names, fmt.Sprintf("n%d", i))
	}
	got = ParseNames(strings.Join(names, ","))
	if len(got) != 30 {
		t.Fatalf("cap: got %d entries", len(got))
	}
}

func TestHandlerMaxSourceBytesReturns413(t *testing.T) {
	h, src := newHandlerForTest(t, &fakeTranslator{}, vttWith(2))
	h.MaxSourceBytes = 5
	if rec := do(h, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", src.URL); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("source over MaxSourceBytes: %d", rec.Code)
	}
}

func TestHeadAgainstFinalArtifact(t *testing.T) {
	ft := &fakeTranslator{}
	h, src := newHandlerForTest(t, ft, vttWith(2))
	path := "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt"
	do(h, "GET", path, src.URL)
	key := ArtifactKey("abc", "/movie.srt~vtt/movie.vtt", "pt", "m", PromptVersion)
	h.Runner.Wait(key)
	rec := do(h, "HEAD", path, src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "100/100" || rec.Header().Get("Cache-Control") != "public, max-age=86400" || rec.Body.Len() != 0 {
		t.Fatalf("head against final: code=%d headers=%v body=%d", rec.Code, rec.Header(), rec.Body.Len())
	}
}

// TestHeadReportsTotalBeforeFirstBatch pins the contract that "0/0" means
// "unknown, the job has not registered yet": once a GET has started the
// job, HEAD must report the real cue count even though no batch has come
// back yet.
func TestHeadReportsTotalBeforeFirstBatch(t *testing.T) {
	ft := &fakeTranslator{block: make(chan struct{})}
	h, src := newHandlerForTest(t, ft, vttWith(5))
	path := "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt"
	do(h, "GET", path, src.URL)
	// The first Translate call means the job is past registering progress.
	waitForCalls(t, ft, 1)
	rec := do(h, "HEAD", path, src.URL)
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Progress") != "0/5" {
		t.Fatalf("head before the first batch: code=%d progress=%s", rec.Code, rec.Header().Get("X-Subtitle-Progress"))
	}
	close(ft.block)
	h.Runner.Wait(ArtifactKey("abc", "/movie.srt~vtt/movie.vtt", "pt", "m", PromptVersion))
}

// TestHandlerHidesSourceErrorFromClient pins that a failed source fetch
// tells the client nothing about the source: no URL, no dial text.
func TestHandlerHidesSourceErrorFromClient(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // nothing is listening on deadURL any more
	h, _ := newHandlerForTest(t, &fakeTranslator{}, vttWith(2))
	rec := do(h, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", deadURL)
	if rec.Code != 404 {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := strings.TrimRight(rec.Body.String(), "\n"); got != "source unavailable" {
		t.Fatalf("body=%q must be the fixed message only", got)
	}
}

func TestHandlerRejectsNonHTTPSourceScheme(t *testing.T) {
	h, _ := newHandlerForTest(t, &fakeTranslator{}, vttWith(2))
	for _, u := range []string{"file:///etc/passwd", "ftp://example.org/a.vtt", "/relative/a.vtt"} {
		rec := do(h, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", u)
		if rec.Code != 400 {
			t.Errorf("%s: code=%d want 400", u, rec.Code)
		}
		if got := strings.TrimRight(rec.Body.String(), "\n"); got != "bad request" {
			t.Errorf("%s: body=%q", u, got)
		}
	}
}

// errFinalStore fails GetFinal, standing in for an unreachable progress
// store.
type errFinalStore struct {
	*MemoryStore
}

func (s *errFinalStore) GetFinal(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("store is down")
}

func TestHandlerFailsFastWhenFinalLookupFails(t *testing.T) {
	var fetched int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fetched, 1)
		_, _ = w.Write([]byte(vttWith(2)))
	}))
	t.Cleanup(src.Close)
	h := &Handler{
		Runner:         NewRunner(&errFinalStore{MemoryStore: NewMemoryStore()}, &fakeTranslator{}, 3, 4, time.Minute),
		Model:          "m",
		Client:         src.Client(),
		MaxSourceBytes: 1 << 20,
		MaxCues:        5000,
	}
	rec := do(h, "GET", "/abc/movie.vtt~tr:pt/movie.vtt", src.URL)
	if rec.Code != 502 {
		t.Fatalf("code=%d want 502", rec.Code)
	}
	if got := strings.TrimRight(rec.Body.String(), "\n"); got != "upstream state unavailable" {
		t.Fatalf("body=%q", got)
	}
	if n := atomic.LoadInt32(&fetched); n != 0 {
		t.Fatalf("source fetched %d times: a failed store lookup must not reach the source", n)
	}
}

func TestHandlerCachesParsedSource(t *testing.T) {
	var fetched int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fetched, 1)
		_, _ = w.Write([]byte(vttWith(4)))
	}))
	t.Cleanup(src.Close)
	ft := &fakeTranslator{block: make(chan struct{})}
	h := &Handler{
		Runner:         NewRunner(NewMemoryStore(), ft, 2, 4, time.Minute),
		Model:          "m",
		Client:         src.Client(),
		MaxSourceBytes: 1 << 20,
		MaxCues:        5000,
	}
	release := releaser(ft.block)
	defer func() { release(); h.Runner.Close() }()
	path := "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt"
	// A client polls the same URL while the job runs; the source is the
	// same bytes every time and must be fetched once.
	for i := 0; i < 3; i++ {
		if rec := do(h, "GET", path, src.URL); rec.Code != 200 {
			t.Fatalf("poll %d: code=%d", i, rec.Code)
		}
	}
	if n := atomic.LoadInt32(&fetched); n != 1 {
		t.Fatalf("source fetched %d times, want 1", n)
	}
}

func newLiveHandlerForTest(t *testing.T, tr Translator) (*Handler, *livePlaylistServer) {
	t.Helper()
	srv := newLivePlaylistServer(t)
	r := NewRunner(NewMemoryStore(), tr, 3, 4, time.Minute)
	r.SetLive(LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: 20 * time.Millisecond, Idle: time.Minute})
	t.Cleanup(r.Close)
	h := &Handler{Runner: r, Model: "m", Client: srv.srv.Client(), MaxSourceBytes: 1 << 20, MaxCues: 5000}
	return h, srv
}

func doLive(h http.Handler, method, sourceURL string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/s0.vtt", nil)
	req.Header.Set("X-Mod-Extra", "pt")
	req.Header.Set("X-Info-Hash", "abc")
	req.Header.Set("X-Path", "/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8")
	req.Header.Set("X-Source-Url", sourceURL)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHandlerLivePlaylistSource(t *testing.T) {
	h, srv := newLiveHandlerForTest(t, &fakeTranslator{})
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	rec := doLive(h, "GET", srv.url())
	if rec.Code != 200 || rec.Header().Get("X-Subtitle-Live") != "1" || rec.Header().Get("X-Subtitle-Progress") != "0/1" {
		t.Fatalf("code=%d live=%q progress=%q", rec.Code, rec.Header().Get("X-Subtitle-Live"), rec.Header().Get("X-Subtitle-Progress"))
	}
	if !strings.Contains(rec.Header().Get("Access-Control-Expose-Headers"), "X-Subtitle-Live") {
		t.Fatalf("expose: %q", rec.Header().Get("Access-Control-Expose-Headers"))
	}
	if strings.Contains(rec.Body.String(), "Макс") {
		t.Fatal("untranslated cue must not be in the live body")
	}
	time.Sleep(80 * time.Millisecond)
	rec = doLive(h, "HEAD", srv.url())
	if rec.Header().Get("X-Subtitle-Progress") != "1/1" || rec.Header().Get("X-Subtitle-Live") != "1" {
		t.Fatalf("after timer batch: %q live=%q", rec.Header().Get("X-Subtitle-Progress"), rec.Header().Get("X-Subtitle-Live"))
	}
	srv.set(pl2end, nil)
	h.Runner.Wait(ArtifactKey("abc", "/a.mkv~hls/s0.m3u8", "pt", "m", PromptVersion))
	rec = doLive(h, "GET", srv.url())
	if rec.Header().Get("X-Subtitle-Live") != "" || rec.Header().Get("Cache-Control") != "public, max-age=86400" || !strings.Contains(rec.Body.String(), "PT:Привет.") {
		t.Fatalf("final: live=%q cc=%q body=%s", rec.Header().Get("X-Subtitle-Live"), rec.Header().Get("Cache-Control"), rec.Body.String())
	}
}

func TestHandlerLiveKeyIgnoresSessionID(t *testing.T) {
	h, srv := newLiveHandlerForTest(t, &fakeTranslator{})
	srv.set(pl1+"#EXT-X-ENDLIST\n", map[string]string{"s0-0.vtt": seg0})
	doLive(h, "GET", srv.url())
	h.Runner.Wait(ArtifactKey("abc", "/a.mkv~hls/s0.m3u8", "pt", "m", PromptVersion))
	req := httptest.NewRequest("GET", "/s0.vtt", nil)
	req.Header.Set("X-Mod-Extra", "pt")
	req.Header.Set("X-Info-Hash", "abc")
	req.Header.Set("X-Path", "/a.mkv~hls/session/ffffffffffffffffffffffffffffffff/s0.m3u8")
	req.Header.Set("X-Source-Url", "http://127.0.0.1:1/dead~hls/session/ffffffffffffffffffffffffffffffff/s0.m3u8")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Header().Get("Cache-Control") != "public, max-age=86400" {
		t.Fatalf("second session must hit the final of the first: code=%d cc=%q", rec.Code, rec.Header().Get("Cache-Control"))
	}
}

// waitRunnerKey blocks until the in-process job for key exits, or fails
// the test after d: a regression that leaves the stale job polling forever
// (e.g. the retire-before-drop call removed) must fail fast, not hang the
// suite.
func waitRunnerKey(t *testing.T, r *Runner, key string, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { r.Wait(key); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("job for key did not exit within %s", d)
	}
}

func TestHandlerLiveNewSessionReplacesStaleSource(t *testing.T) {
	h, srv1 := newLiveHandlerForTest(t, &fakeTranslator{})
	srv1.set(pl1, map[string]string{"s0-0.vtt": seg0})
	doLive(h, "GET", srv1.url())

	// A second transcoder session for the same key (X-Path carries the same
	// session id — doLive always sends it — since KeyPath strips it either
	// way): a genuinely different source URL, a separate server rather than
	// just a different query parameter, ending in its own distinct cue so
	// the eventual final artifact can only have come from this source.
	srv2 := newLivePlaylistServer(t)
	const seg2 = "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nВторая сессия.\n"
	srv2.set(pl1+"#EXT-X-ENDLIST\n", map[string]string{"s0-0.vtt": seg2})

	key := ArtifactKey("abc", "/a.mkv~hls/s0.m3u8", "pt", "m", PromptVersion)

	rec := doLive(h, "GET", srv2.url())
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	// The cache must already hold the new source: liveFor replaces a
	// mismatched URL synchronously, within this same request.
	src, _ := h.lives.Get(key, func() (*LiveSource, error) { t.Fatal("must already be cached"); return nil, nil })
	if src.url != srv2.url() {
		t.Fatalf("stale source kept: %s", src.url)
	}

	// The first request's Ensure(Live: src2) was silently dropped — the old
	// job (against srv1) still owned the key. That old job must retire on
	// its own, via the source this GET just retired, rather than the test
	// waiting out srv1's eventual (here: never, since srv1 stays up and
	// never 404s) source_gone.
	waitRunnerKey(t, h.Runner, key, 2*time.Second)

	// Now that the key is free, the next GET's Ensure actually starts a job
	// against the new source.
	if rec := doLive(h, "GET", srv2.url()); rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	waitRunnerKey(t, h.Runner, key, 2*time.Second)

	rec = doLive(h, "GET", srv2.url())
	if !strings.Contains(rec.Body.String(), "PT:Вторая сессия.") {
		t.Fatalf("final body must come from the new session: %s", rec.Body.String())
	}
}

func TestHandlerLiveGoneSourceIs404(t *testing.T) {
	h, srv := newLiveHandlerForTest(t, &fakeTranslator{})
	srv.mu.Lock()
	srv.status = 404
	srv.mu.Unlock()
	rec := doLive(h, "GET", srv.url())
	if rec.Code != 404 || rec.Body.String() != msgSourceUnavail+"\n" {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}
