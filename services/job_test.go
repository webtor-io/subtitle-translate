package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeTranslator struct {
	calls int32
	delay time.Duration
	fail  error
	block chan struct{}
}

func (f *fakeTranslator) Translate(_ context.Context, req BatchRequest) (BatchResult, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.block != nil {
		<-f.block
	}
	time.Sleep(f.delay)
	if f.fail != nil {
		return BatchResult{}, f.fail
	}
	out := make([]string, len(req.Lines))
	for i, l := range req.Lines {
		out[i] = "PT:" + l
	}
	return BatchResult{Lines: out, InputTokens: 1, OutputTokens: 1}, nil
}

// vttWith builds n cues with increasing timings: "line 1" … "line n".
func vttWith(n int) string {
	var b strings.Builder
	b.WriteString("WEBVTT\n\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%d\n00:00:%02d.000 --> 00:00:%02d.500\nline %d\n\n", i+1, i, i, i+1)
	}
	return b.String()
}

func TestArtifactKeyStable(t *testing.T) {
	a := ArtifactKey("h", "/a.srt~vtt/a.vtt", "pt", "m", "v1")
	b := ArtifactKey("h", "/a.srt~vtt/a.vtt", "pt", "m", "v1")
	c := ArtifactKey("h", "/a.srt~vtt/a.vtt", "es", "m", "v1")
	if a != b || a == c || len(a) != 64 {
		t.Fatalf("a=%s b=%s c=%s", a, b, c)
	}
}

func TestRunnerProgressiveThenFinal(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(7)))
	doc.Normalize()
	ft := &fakeTranslator{block: make(chan struct{})}
	r := NewRunner(NewMemoryStore(), ft, "m", 3, time.Minute)
	key := "k1"
	snap, _ := r.Snapshot(context.Background(), key, doc)
	if snap.Done != 0 || snap.Final || !strings.HasPrefix(string(snap.Body), "WEBVTT") {
		t.Fatalf("initial snapshot=%+v", snap)
	}
	r.Ensure(context.Background(), key, &Job{Lang: "pt", Doc: doc})
	r.Ensure(context.Background(), key, &Job{Lang: "pt", Doc: doc}) // second call must not start a second job
	ft.block <- struct{}{}                                          // release batch 1 only
	time.Sleep(50 * time.Millisecond)
	snap, _ = r.Snapshot(context.Background(), key, doc)
	if snap.Done != 3 || snap.Final || !strings.Contains(string(snap.Body), "PT:line 3") || strings.Contains(string(snap.Body), "line 4") {
		t.Fatalf("after batch 1: done=%d final=%v body=%q", snap.Done, snap.Final, snap.Body)
	}
	close(ft.block)
	r.Wait(key)
	snap, _ = r.Snapshot(context.Background(), key, doc)
	if !snap.Final || snap.Done != 100 || snap.Total != 100 || !strings.Contains(string(snap.Body), "PT:line 7") {
		t.Fatalf("final: %+v", snap)
	}
	if atomic.LoadInt32(&ft.calls) != 3 {
		t.Fatalf("calls=%d want 3 batches", ft.calls)
	}
}

func TestRunnerResumesFromStoredProgress(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(6)))
	doc.Normalize()
	st := NewMemoryStore()
	_ = st.PutProgress(context.Background(), "k2", &Progress{Total: 6, Lines: []string{"PT:line 1", "PT:line 2", "PT:line 3", "", "", ""}})
	ft := &fakeTranslator{}
	r := NewRunner(st, ft, "m", 3, time.Minute)
	r.Ensure(context.Background(), "k2", &Job{Lang: "pt", Doc: doc})
	r.Wait("k2")
	if atomic.LoadInt32(&ft.calls) != 1 {
		t.Fatalf("calls=%d: must translate only the missing batch", ft.calls)
	}
	snap, _ := r.Snapshot(context.Background(), "k2", doc)
	if !snap.Final || !strings.Contains(string(snap.Body), "PT:line 1") || !strings.Contains(string(snap.Body), "PT:line 6") {
		t.Fatalf("final=%+v", snap)
	}
}

func TestRunnerKeepsOriginalOnLineMismatch(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(2)))
	doc.Normalize()
	ft := &fakeTranslator{fail: ErrLineMismatch}
	r := NewRunner(NewMemoryStore(), ft, "m", 50, time.Minute)
	r.Ensure(context.Background(), "k3", &Job{Lang: "pt", Doc: doc})
	r.Wait("k3")
	snap, _ := r.Snapshot(context.Background(), "k3", doc)
	if !snap.Final || !strings.Contains(string(snap.Body), "line 1") {
		t.Fatalf("mismatch must fall back to originals and still finish: %+v", snap)
	}
}

func TestRunnerStopsOnUpstreamErrorKeepingProgress(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(2)))
	doc.Normalize()
	ft := &fakeTranslator{fail: errors.New("boom")}
	st := NewMemoryStore()
	r := NewRunner(st, ft, "m", 50, time.Minute)
	r.Ensure(context.Background(), "k4", &Job{Lang: "pt", Doc: doc})
	r.Wait("k4")
	snap, _ := r.Snapshot(context.Background(), "k4", doc)
	if snap.Final || snap.Done != 0 {
		t.Fatalf("must not finish on upstream error: %+v", snap)
	}
	if ok, _ := st.TryLock(context.Background(), "k4", time.Minute); !ok {
		t.Fatal("lock must be released after a failed run")
	}
}
