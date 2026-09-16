package services

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNotConfiguredHandlerReturns501(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt", nil)
	NotConfiguredHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := rec.Body.String(); got != "translation is not configured\n" {
		t.Fatalf("body=%q", got)
	}
}

// TestWebServerLimits pins the server's shape. The header cap was 50 MB —
// 50× the Go default — against a 512Mi pod, with nothing in the request
// shape that needs it; and with no read timeouts a slow reader held a
// connection (and its header buffer) for as long as it liked.
func TestWebServerLimits(t *testing.T) {
	srv := newHTTPServer(http.NotFoundHandler())
	if srv.MaxHeaderBytes != 1<<20 {
		t.Fatalf("MaxHeaderBytes=%d want %d", srv.MaxHeaderBytes, 1<<20)
	}
	if srv.ReadHeaderTimeout != 10*time.Second {
		t.Fatalf("ReadHeaderTimeout=%s", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout != 30*time.Second {
		t.Fatalf("ReadTimeout=%s", srv.ReadTimeout)
	}
	if srv.IdleTimeout != 120*time.Second {
		t.Fatalf("IdleTimeout=%s", srv.IdleTimeout)
	}
	// Deliberately unset: a response is written for as long as the render
	// takes, and a write deadline would cut a long one.
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout=%s want unset", srv.WriteTimeout)
	}
}

// TestWebCloseWaitsForInFlightRequests: Close closed the listener, which
// does nothing to connections already accepted — in-flight requests were cut
// at process exit instead of finishing.
func TestWebCloseWaitsForInFlightRequests(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	w := &Web{host: "127.0.0.1", h: http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = rw.Write([]byte("done"))
	})}
	served := make(chan error, 1)
	go func() { served <- w.Serve() }()
	var addr string
	for i := 0; i < 200 && addr == ""; i++ {
		addr = w.addr()
		time.Sleep(5 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("the server never started listening")
	}
	body := make(chan string, 1)
	go func() {
		res, err := http.Get("http://" + addr + "/")
		if err != nil {
			body <- "error: " + err.Error()
			return
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		body <- string(b)
	}()
	<-started
	closed := make(chan struct{})
	go func() { w.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a request was still in flight: no graceful shutdown")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-body:
		if got != "done" {
			t.Fatalf("in-flight request was cut: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the in-flight request never completed")
	}
	<-closed
	if err := <-served; err != nil {
		t.Fatalf("Serve must return cleanly after a shutdown: %v", err)
	}
}
