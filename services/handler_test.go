package services

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
