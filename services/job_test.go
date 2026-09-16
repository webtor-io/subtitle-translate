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
	r := NewRunner(NewMemoryStore(), ft, 3, 4, time.Minute)
	key := "k1"
	snap, _ := r.Snapshot(context.Background(), key, doc)
	if snap.Done != 0 || snap.Final || !strings.HasPrefix(string(snap.Body), "WEBVTT") {
		t.Fatalf("initial snapshot=%+v", snap)
	}
	r.Ensure(context.Background(), key, &Job{Lang: "pt", Doc: doc})
	r.Ensure(context.Background(), key, &Job{Lang: "pt", Doc: doc}) // second call must not start a second job
	ft.block <- struct{}{}                                          // release batch 1 only
	// The second call starts only after batch 1 was stored, so waiting for
	// it is the synchronisation point.
	waitForCalls(t, ft, 2)
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
	r := NewRunner(st, ft, 3, 4, time.Minute)
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
	r := NewRunner(NewMemoryStore(), ft, 50, 4, time.Minute)
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
	r := NewRunner(st, ft, 50, 4, time.Minute)
	r.Ensure(context.Background(), "k4", &Job{Lang: "pt", Doc: doc})
	r.Wait("k4")
	snap, _ := r.Snapshot(context.Background(), "k4", doc)
	if snap.Final || snap.Done != 0 {
		t.Fatalf("must not finish on upstream error: %+v", snap)
	}
	if _, ok, _ := st.TryLock(context.Background(), "k4", time.Minute); !ok {
		t.Fatal("lock must be released after a failed run")
	}
}

// stopReasonTranslator fails every batch longer than max lines with fail
// (a truncation or a refusal), and translates anything shorter.
type stopReasonTranslator struct {
	max   int
	fail  error
	calls int32
}

func (f *stopReasonTranslator) Translate(_ context.Context, req BatchRequest) (BatchResult, error) {
	atomic.AddInt32(&f.calls, 1)
	if len(req.Lines) > f.max {
		return BatchResult{}, f.fail
	}
	out := make([]string, len(req.Lines))
	for i, l := range req.Lines {
		out[i] = "PT:" + l
	}
	return BatchResult{Lines: out}, nil
}

func TestRunnerSplitsTruncatedBatches(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(7)))
	doc.Normalize()
	// One batch of 7; anything over 2 lines truncates, so the runner must
	// halve its way down (7 → 3+4 → …) until every cue is translated.
	ft := &stopReasonTranslator{max: 2, fail: ErrTruncated}
	r := NewRunner(NewMemoryStore(), ft, 50, 4, time.Minute)
	r.Ensure(context.Background(), "k5", &Job{Lang: "pt", Doc: doc})
	r.Wait("k5")
	snap, _ := r.Snapshot(context.Background(), "k5", doc)
	if !snap.Final {
		t.Fatalf("job must finish: %+v", snap)
	}
	for i := 1; i <= 7; i++ {
		if !strings.Contains(string(snap.Body), fmt.Sprintf("PT:line %d", i)) {
			t.Fatalf("cue %d not translated:\n%s", i, snap.Body)
		}
	}
}

func TestRunnerKeepsSourceForSingleCueTruncation(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(2)))
	doc.Normalize()
	// max 0: even a single cue truncates, so the source text is kept.
	ft := &stopReasonTranslator{max: 0, fail: ErrTruncated}
	r := NewRunner(NewMemoryStore(), ft, 50, 4, time.Minute)
	r.Ensure(context.Background(), "k6", &Job{Lang: "pt", Doc: doc})
	r.Wait("k6")
	snap, _ := r.Snapshot(context.Background(), "k6", doc)
	if !snap.Final || !strings.Contains(string(snap.Body), "line 1") || strings.Contains(string(snap.Body), "PT:") {
		t.Fatalf("single-cue truncation must keep originals and finish: %+v", snap)
	}
}

func TestRunnerFillsSourceOnRefusal(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(4)))
	doc.Normalize()
	ft := &stopReasonTranslator{max: 0, fail: ErrRefused}
	r := NewRunner(NewMemoryStore(), ft, 2, 4, time.Minute)
	r.Ensure(context.Background(), "k7", &Job{Lang: "pt", Doc: doc})
	r.Wait("k7")
	snap, _ := r.Snapshot(context.Background(), "k7", doc)
	if !snap.Final || !strings.Contains(string(snap.Body), "line 4") || strings.Contains(string(snap.Body), "PT:") {
		t.Fatalf("refusal must keep originals and finish: %+v", snap)
	}
	// A refusal is not retried and never split: one call per batch.
	if got := atomic.LoadInt32(&ft.calls); got != 2 {
		t.Fatalf("calls=%d want 2 (one per batch, no splitting)", got)
	}
}

// stealingStore hands every key to a foreign holder the moment it is
// locked, so the lock-lost branch can be driven without timing.
type stealingStore struct {
	*MemoryStore
}

func (s *stealingStore) TryLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	token, ok, err := s.MemoryStore.TryLock(ctx, key, ttl)
	if ok {
		s.MemoryStore.mu.Lock()
		s.MemoryStore.locks[key] = memLock{token: "stolen", until: time.Now().Add(ttl)}
		s.MemoryStore.mu.Unlock()
	}
	return token, ok, err
}

func TestRunnerAbortsWhenLockIsLost(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(6)))
	doc.Normalize()
	// Steal the key between the job taking it and the first refresh: the
	// job must stop rather than keep writing over someone else's work.
	st := &stealingStore{MemoryStore: NewMemoryStore()}
	ft := &fakeTranslator{}
	r := NewRunner(st, ft, 3, 4, time.Minute)
	r.Ensure(context.Background(), "k8", &Job{Lang: "pt", Doc: doc})
	r.Wait("k8")
	if got := atomic.LoadInt32(&ft.calls); got != 1 {
		t.Fatalf("calls=%d: the job must stop at the first lost refresh", got)
	}
	snap, _ := r.Snapshot(context.Background(), "k8", doc)
	if snap.Final {
		t.Fatal("a job that lost its lock must not publish a final artifact")
	}
	// Progress from the batch that did run stays for the new holder.
	if p, _ := st.GetProgress(context.Background(), "k8"); p == nil || p.Lines[0] == "" {
		t.Fatalf("progress must be left in place: %+v", p)
	}
}

func TestRunnerCloseStopsRunningJobs(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(6)))
	doc.Normalize()
	st := NewMemoryStore()
	ft := &fakeTranslator{block: make(chan struct{})}
	r := NewRunner(st, ft, 3, 4, time.Minute)
	r.Ensure(context.Background(), "k9", &Job{Lang: "pt", Doc: doc})
	waitForCalls(t, ft, 1)
	close(ft.block)
	r.Close()
	// Close waits for the job, and the job releases its lock on the way out
	// even though its context is already canceled.
	if _, ok, _ := st.TryLock(context.Background(), "k9", time.Minute); !ok {
		t.Fatal("the lock must be released by the time Close returns")
	}
	// A closed runner starts nothing new.
	r.Ensure(context.Background(), "k10", &Job{Lang: "pt", Doc: doc})
	if p, _ := st.GetProgress(context.Background(), "k10"); p != nil {
		t.Fatal("a closed runner must not start jobs")
	}
}

func TestRunnerLimitsConcurrentJobs(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(3)))
	doc.Normalize()
	st := NewMemoryStore()
	ft := &fakeTranslator{block: make(chan struct{})}
	r := NewRunner(st, ft, 3, 1, time.Minute) // one job at a time
	// Close waits for the jobs, so the blocked translator has to be released
	// even when the test fails early.
	release := releaser(ft.block)
	defer func() { release(); r.Close() }()
	r.Ensure(context.Background(), "a", &Job{Lang: "pt", Doc: doc})
	waitForCalls(t, ft, 1) // job "a" holds the only slot and is blocked
	r.Ensure(context.Background(), "b", &Job{Lang: "pt", Doc: doc})
	// While "a" holds the slot, "b" cannot have reached the store at all:
	// the wait is before TryLock, so no lock and no progress exist for it.
	if p, _ := st.GetProgress(context.Background(), "b"); p != nil {
		t.Fatalf("job b ran without a free slot: %+v", p)
	}
	probe, ok, _ := st.TryLock(context.Background(), "b", time.Minute)
	if !ok {
		t.Fatal("job b must not have taken the lock yet")
	}
	_ = st.Unlock(context.Background(), "b", probe)
	release()
	r.Wait("a")
	// "a" freed the slot, so "b" gets to run.
	waitForCalls(t, ft, 2)
	r.Wait("b")
}

func newLiveRunner(t *testing.T, tr Translator, batch int, cfg LiveConfig) (*Runner, *MemoryStore) {
	t.Helper()
	st := NewMemoryStore()
	r := NewRunner(st, tr, batch, 4, time.Minute)
	r.SetLive(cfg)
	t.Cleanup(r.Close)
	return r, st
}

func TestLiveRunnerTranslatesByTimerThenFinal(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	tr := &fakeTranslator{}
	r, st := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: 30 * time.Millisecond, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	time.Sleep(80 * time.Millisecond) // one cue pending for > BatchWait → translated alone
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || len(p.Lines) != 1 || p.Lines[0] != "PT:Макс." || !p.Live || p.CueKeys[0] == "" {
		t.Fatalf("progress after timer batch: %+v", p)
	}
	srv.set(pl2end, nil)
	r.Wait("k")
	body, ok, _ := st.GetFinal(context.Background(), "k")
	if !ok || !strings.Contains(string(body), "PT:Привет.") {
		t.Fatalf("final missing: ok=%v body=%s", ok, body)
	}
	if p, _ := st.GetProgress(context.Background(), "k"); p != nil {
		t.Fatal("progress must be dropped after the final")
	}
}

func TestLiveRunnerBatchBySize(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": vttWith(4)})
	tr := &fakeTranslator{}
	r, st := newLiveRunner(t, tr, 2, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	time.Sleep(80 * time.Millisecond)
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || countFilled(p.Lines) != 4 || atomic.LoadInt32(&tr.calls) != 2 {
		t.Fatalf("size batches: filled=%d calls=%d", countFilled(p.Lines), atomic.LoadInt32(&tr.calls))
	}
}

func countFilled(lines []string) int {
	n := 0
	for _, l := range lines {
		if l != "" {
			n++
		}
	}
	return n
}

func TestLiveRunnerNoFinalAfterSeek(t *testing.T) {
	srv := newLivePlaylistServer(t)
	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXTINF:2.0,\ns0-0.vtt?token=T\n#EXT-X-ENDLIST\n"
	srv.set(seek, map[string]string{"s0-0.vtt": seg0})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	r.Wait("k")
	if _, ok, _ := st.GetFinal(context.Background(), "k"); ok {
		t.Fatal("a run that did not start at 0 must not produce a final artifact")
	}
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || p.Live || p.Lines[0] != "PT:Макс." {
		t.Fatalf("partial progress kept without Live: %+v", p)
	}
}

func TestLiveRunnerStopsWhenViewerGone(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: 50 * time.Millisecond})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	done := make(chan struct{})
	go func() { r.Wait("k"); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not stop after the idle window")
	}
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || !p.Live {
		t.Fatalf("progress must stay Live (source not ended): %+v", p)
	}
	srv.mu.Lock()
	hits := srv.hits["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8"]
	srv.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.hits["/h/a.mkv~hls/session/0123456789abcdef0123456789abcdef/s0.m3u8"] != hits {
		t.Fatal("playlist still polled after the job stopped")
	}
}

func TestLiveRunnerStopsOnSourceGone(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	time.Sleep(30 * time.Millisecond)
	srv.mu.Lock()
	srv.status = 404
	srv.mu.Unlock()
	r.Wait("k")
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || p.Live {
		t.Fatalf("gone source must clear Live and keep progress: %+v", p)
	}
}

func TestLiveRunnerReusesTranslationsByCueKey(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	tr := &fakeTranslator{}
	r, st := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: 20 * time.Millisecond, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	time.Sleep(80 * time.Millisecond)
	srv.mu.Lock()
	srv.status = 404
	srv.mu.Unlock()
	r.Wait("k")
	calls := atomic.LoadInt32(&tr.calls)
	// A new session: fresh LiveSource, same key, same cue → no new upstream call.
	srv.mu.Lock()
	srv.status = 200
	srv.mu.Unlock()
	srv.set(pl1+"#EXT-X-ENDLIST\n", nil)
	ls2 := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls2})
	r.Wait("k")
	if atomic.LoadInt32(&tr.calls) != calls {
		t.Fatalf("translated again: %d → %d", calls, atomic.LoadInt32(&tr.calls))
	}
	if _, ok, _ := st.GetFinal(context.Background(), "k"); !ok {
		t.Fatal("second contiguous run reaching ENDLIST must write the final")
	}
}

func TestLiveSnapshotReportsLive(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r, _ := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, err := r.LiveSnapshot(context.Background(), "k", ls)
	if err != nil || !s.Live || s.Final || s.Total != 1 || s.Done != 0 || strings.Contains(string(s.Body), "Макс") {
		t.Fatalf("snap=%+v body=%s err=%v", s, s.Body, err)
	}
}

// TestLiveRunnerReusesTranslationsAfterTheDocumentShrinks is the case
// positional reuse gets wrong: a restarted session whose playlist no longer
// carries the first segment renumbers every cue, so the cue that was index
// 1 is now index 0. Nothing may be translated twice, and no cue may be
// rendered with another cue's text.
func TestLiveRunnerReusesTranslationsAfterTheDocumentShrinks(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	tr := &fakeTranslator{}
	r, st := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: 20 * time.Millisecond, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	time.Sleep(80 * time.Millisecond)
	srv.mu.Lock()
	srv.status = 404
	srv.mu.Unlock()
	r.Wait("k")
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || countFilled(p.Lines) != 2 {
		t.Fatalf("the first run must translate both spoken cues: %+v", p)
	}
	calls := atomic.LoadInt32(&tr.calls)
	// The new session serves the tail of the playlist only: "Привет." keeps
	// its movie time (and so its cue key) but lands at index 0.
	srv.mu.Lock()
	srv.status = 200
	srv.mu.Unlock()
	only1 := "#EXTM3U\n#EXT-X-SESSION-OFFSET:0\n#EXTINF:1.3,\ns0-1.vtt?token=T\n#EXT-X-ENDLIST\n"
	srv.set(only1, nil)
	ls2 := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls2})
	r.Wait("k")
	if got := atomic.LoadInt32(&tr.calls); got != calls {
		t.Fatalf("translated again: %d → %d", calls, got)
	}
	body, ok, _ := st.GetFinal(context.Background(), "k")
	if !ok || !strings.Contains(string(body), "PT:Привет.") {
		t.Fatalf("final missing the reused translation: ok=%v body=%s", ok, body)
	}
	if strings.Contains(string(body), "PT:Макс.") {
		t.Fatalf("the first run's translation landed under the wrong cue: %s", body)
	}
}

// TestLiveSnapshotAlignsStoredLinesByCueKey is a negative control for
// LiveSnapshot going back to trusting p.Lines positionally: the stored
// record here carries its two lines in the opposite order from the
// document's cue order, and only key-based alignment (alignLines) puts
// each translation under its own cue.
func TestLiveSnapshotAlignsStoredLinesByCueKey(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// doc = Макс.[0], Привет.[1], empty[2] ("[door slams]" strips to zero
	// lines under Normalize).
	keys := ls.Doc().Keys()
	p := &Progress{
		Total:   2,
		Live:    true,
		CueKeys: []string{keys[1], keys[0]}, // Привет., then Макс. — reversed
		Lines:   []string{"PT:Привет.", "PT:Макс."},
	}
	if err := st.PutProgress(context.Background(), "k", p); err != nil {
		t.Fatal(err)
	}
	s, err := r.LiveSnapshot(context.Background(), "k", ls)
	if err != nil {
		t.Fatal(err)
	}
	body := string(s.Body)
	iMaxTime := strings.Index(body, "00:00:45.107") // cue 1 start
	iMax := strings.Index(body, "PT:Макс.")
	iHiTime := strings.Index(body, "00:01:03.699 -->") // cue 2 start (unique: cue 1's end time reads "--> 00:01:03.699")
	iHi := strings.Index(body, "PT:Привет.")
	if iMaxTime < 0 || iMax < 0 || iHiTime < 0 || iHi < 0 || !(iMaxTime < iMax && iMax < iHiTime && iHiTime < iHi) {
		t.Fatalf("translations not aligned by cue key: body=%s", body)
	}
}

// A job that stopped because its source went away is not live any more,
// even though the playlist it was reading never said ENDLIST. Reading
// liveness off the source would leave the client polling a track nobody is
// translating.
func TestLiveSnapshotNotLiveAfterTheSourceIsGone(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r, _ := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	time.Sleep(30 * time.Millisecond)
	srv.mu.Lock()
	srv.status = 404
	srv.mu.Unlock()
	r.Wait("k")
	if ls.Ended() {
		t.Fatal("the fixture must leave the playlist unfinished")
	}
	s, err := r.LiveSnapshot(context.Background(), "k", ls)
	if err != nil || s.Live || s.Final {
		t.Fatalf("a stopped job must not report live: snap=%+v err=%v", s, err)
	}
}

// Ensure seeds the idle mark itself, so the idle window does not depend on
// a Touch surviving the exit of the previous job for the same key.
func TestLiveRunnerIdlesOutWithoutATouch(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r, _ := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: 50 * time.Millisecond})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls}) // nobody touched this key
	done := make(chan struct{})
	go func() { r.Wait("k"); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a job nobody touched must still stop after the idle window")
	}
}
