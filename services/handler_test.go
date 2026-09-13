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
	h := &Handler{Runner: NewRunner(NewMemoryStore(), tr, "m", 3, time.Minute), Model: "m", Client: src.Client(), MaxSourceBytes: 1 << 20, MaxCues: 5000}
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
	time.Sleep(50 * time.Millisecond)
	rec = do(h, "HEAD", path, src.URL)
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

func waitForCalls(t *testing.T, ft *fakeTranslator, n int32) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if atomic.LoadInt32(&ft.calls) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d calls, got %d", n, atomic.LoadInt32(&ft.calls))
}

// TestHeadProgressPastStructurallyEmptyCue is a negative control for the
// break-on-first-empty-line bug: with a music-only cue at index 2 (never
// translated, its p.Lines entry stays "" forever), HEAD's progress must
// keep growing as later cues finish instead of freezing at the empty gap.
func TestHeadProgressPastStructurallyEmptyCue(t *testing.T) {
	ft := &fakeTranslator{block: make(chan struct{})}
	h, src := newHandlerForTest(t, ft, vttWithMusicAt(5, 2))
	h.Runner = NewRunner(NewMemoryStore(), ft, "m", 1, time.Minute) // batch size 1: one cue per batch
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
		Runner:         NewRunner(&errFinalStore{MemoryStore: NewMemoryStore()}, &fakeTranslator{}, "m", 3, time.Minute),
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
