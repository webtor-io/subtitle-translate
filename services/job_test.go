package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

type fakeTranslator struct {
	calls int32
	delay time.Duration
	fail  error
	block chan struct{}

	// mu guards reqs: Translate runs on the job goroutine, requests() is
	// read from the test goroutine after Wait, but a mutex costs nothing
	// and keeps this safe if that ever changes.
	mu   sync.Mutex
	reqs [][]string
}

func (f *fakeTranslator) Translate(_ context.Context, req BatchRequest) (BatchResult, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.block != nil {
		<-f.block
	}
	time.Sleep(f.delay)
	f.mu.Lock()
	f.reqs = append(f.reqs, append([]string(nil), req.Lines...))
	f.mu.Unlock()
	if f.fail != nil {
		return BatchResult{}, f.fail
	}
	out := make([]string, len(req.Lines))
	for i, l := range req.Lines {
		out[i] = "PT:" + l
	}
	return BatchResult{Lines: out, InputTokens: 1, OutputTokens: 1}, nil
}

// requests is every batch of source lines Translate has been called with,
// in call order — what pendingByTime handed to translateChunk as texts.
func (f *fakeTranslator) requests() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.reqs))
	copy(out, f.reqs)
	return out
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
	snap, _ := r.Snapshot(context.Background(), key, doc, 0, false)
	if snap.Done != 0 || snap.Final || !strings.HasPrefix(string(snap.Body), "WEBVTT") {
		t.Fatalf("initial snapshot=%+v", snap)
	}
	r.Ensure(context.Background(), key, &Job{Lang: "pt", Doc: doc})
	r.Ensure(context.Background(), key, &Job{Lang: "pt", Doc: doc}) // second call must not start a second job
	ft.block <- struct{}{}                                          // release batch 1 only
	// The second call starts only after batch 1 was stored, so waiting for
	// it is the synchronisation point.
	waitForCalls(t, ft, 2)
	snap, _ = r.Snapshot(context.Background(), key, doc, 0, false)
	if snap.Done != 3 || snap.Final || !strings.Contains(string(snap.Body), "PT:line 3") || strings.Contains(string(snap.Body), "line 4") {
		t.Fatalf("after batch 1: done=%d final=%v body=%q", snap.Done, snap.Final, snap.Body)
	}
	close(ft.block)
	r.Wait(key)
	snap, _ = r.Snapshot(context.Background(), key, doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k2", doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k3", doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k4", doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k5", doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k6", doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k7", doc, 0, false)
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
	snap, _ := r.Snapshot(context.Background(), "k8", doc, 0, false)
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

// TestPendingByTimeAheadOfPlayheadFirst pins pendingByTime's ordering
// directly: given a document mixing an untranslated backlog at 0-100 s with
// cues at 600 s and later, current=600 s must return every cue at or after
// 600 s before any backlog cue, regardless of ingest run. Reverting to plain
// document-time order (dropping the ahead/behind split) puts the backlog
// first and reddens this test.
func TestPendingByTimeAheadOfPlayheadFirst(t *testing.T) {
	cue := func(i, sec int, run time.Duration, text string) Cue {
		return Cue{Index: i, Start: time.Duration(sec) * time.Second, End: time.Duration(sec+1) * time.Second, Lines: []string{text}, Run: run}
	}
	// Already in movie-time order, as LiveDoc.Snapshot produces: the 0-100 s
	// backlog sorts ahead of the 600 s+ cues by time alone.
	doc := &Doc{Cues: []Cue{
		cue(0, 0, 0, "a"),
		cue(1, 50, 0, "b"),
		cue(2, 100, 0, "c"),
		cue(3, 600, 600*time.Second, "x"),
		cue(4, 650, 600*time.Second, "y"),
	}}
	lines := make([]string, len(doc.Cues))
	idx := pendingByTime(doc, lines, 600*time.Second, 0)
	texts := textsFor(doc, idx)
	if len(idx) != 5 {
		t.Fatalf("idx=%v, want all 5 cues pending", idx)
	}
	if idx[0] != 3 || idx[1] != 4 {
		t.Fatalf("cues at/after the current position must lead: idx=%v", idx)
	}
	if idx[2] != 0 || idx[3] != 1 || idx[4] != 2 {
		t.Fatalf("the backlog must keep its own time order behind the current position: idx=%v", idx)
	}
	if texts[0] != "x" || texts[1] != "y" {
		t.Fatalf("texts must track idx: %v", texts)
	}
}

// TestPendingByTimeAheadOfPlayheadWinsAcrossIngestRuns is the guard on why
// pendingByTime compares Cue.Start against current rather than Cue.Run:
// #EXT-X-SESSION-OFFSET is quantized to 30 s, so a seek to 200 s starts a
// run at offset 180 while cues covering 180-240 s can already sit in the
// document tagged with an EARLIER run's offset (150) — ingested before the
// seek, while the viewer was still watching that run play forward. Matching
// by ingest run would deprioritize exactly the cues at the new position;
// matching by Start does not.
func TestPendingByTimeAheadOfPlayheadWinsAcrossIngestRuns(t *testing.T) {
	cue := func(i, sec int, run time.Duration, text string) Cue {
		return Cue{Index: i, Start: time.Duration(sec) * time.Second, End: time.Duration(sec+1) * time.Second, Lines: []string{text}, Run: run}
	}
	doc := &Doc{Cues: []Cue{
		cue(0, 50, 0, "old"),               // behind the seek target: trails
		cue(1, 180, 150*time.Second, "p1"), // at the new playhead, ingested under run 150
		cue(2, 240, 150*time.Second, "p2"), // ahead of it, same (earlier) ingest run
	}}
	lines := make([]string, len(doc.Cues))
	idx := pendingByTime(doc, lines, 180*time.Second, 0)
	texts := textsFor(doc, idx)
	if len(idx) != 3 {
		t.Fatalf("idx=%v, want all 3 cues pending", idx)
	}
	if idx[0] != 1 || idx[1] != 2 {
		t.Fatalf("cues at/after the playhead must lead despite their ingest run: idx=%v", idx)
	}
	if idx[2] != 0 {
		t.Fatalf("the cue behind the playhead must trail: idx=%v", idx)
	}
	if texts[0] != "p1" || texts[1] != "p2" {
		t.Fatalf("texts must track idx: %v", texts)
	}
}

// TestPendingFrom pins pendingFrom's filter and selection: the same pending
// predicate pendingByTime uses (structurally-empty and out-of-range cues
// never count), restricted to cues at or after the current offset by their
// End rather than their Start — a cue straddling the playhead is on screen
// right now and must count, a cue that ended before it must not.
func TestPendingFrom(t *testing.T) {
	cue := func(i, sec int, text string) Cue {
		return Cue{Index: i, Start: time.Duration(sec) * time.Second, End: time.Duration(sec+1) * time.Second, Lines: []string{text}}
	}

	t.Run("earliest of several pending cues ahead, one translated between them", func(t *testing.T) {
		doc := &Doc{Cues: []Cue{
			cue(0, 100, "a"),
			cue(1, 150, "b"),
			cue(2, 200, "c"),
		}}
		lines := []string{"", "PT:b", ""}
		from, ok := pendingFrom(doc, lines, 0)
		if !ok || from != 100*time.Second {
			t.Fatalf("from=%s ok=%v, want 100s/true", from, ok)
		}
	})

	t.Run("a cue entirely before current is ignored, a straddling cue counts", func(t *testing.T) {
		doc := &Doc{Cues: []Cue{
			// End (11s) < current (50s): the viewer already passed it.
			{Index: 0, Start: 10 * time.Second, End: 11 * time.Second, Lines: []string{"before"}},
			// Start (45s) < current (50s) <= End (55s): on screen right now.
			{Index: 1, Start: 45 * time.Second, End: 55 * time.Second, Lines: []string{"straddle"}},
		}}
		lines := []string{"", ""}
		from, ok := pendingFrom(doc, lines, 50*time.Second)
		if !ok || from != 45*time.Second {
			t.Fatalf("from=%s ok=%v, want 45s/true (the passed-by cue must not win)", from, ok)
		}
	})

	t.Run("all translated: ok is false", func(t *testing.T) {
		doc := &Doc{Cues: []Cue{cue(0, 10, "a"), cue(1, 20, "b")}}
		lines := []string{"PT:a", "PT:b"}
		if _, ok := pendingFrom(doc, lines, 0); ok {
			t.Fatal("everything translated: ok must be false")
		}
	})

	t.Run("a structurally empty cue is never pending", func(t *testing.T) {
		doc := &Doc{Cues: []Cue{
			{Index: 0, Start: 0, End: 1 * time.Second, Lines: nil},
			cue(1, 100, "b"),
		}}
		lines := []string{"", ""}
		from, ok := pendingFrom(doc, lines, 0)
		if !ok || from != 100*time.Second {
			t.Fatalf("from=%s ok=%v, want 100s/true (the empty-lines cue must not count)", from, ok)
		}
	})

	t.Run("lines shorter than the cue index: that cue is not pending", func(t *testing.T) {
		doc := &Doc{Cues: []Cue{
			cue(0, 100, "a"),
			{Index: 5, Start: 10 * time.Second, End: 11 * time.Second, Lines: []string{"out of range"}},
		}}
		lines := []string{""} // len 1: index 0 is in range, index 5 is not
		from, ok := pendingFrom(doc, lines, 0)
		if !ok || from != 100*time.Second {
			t.Fatalf("from=%s ok=%v, want 100s/true (the out-of-range cue must not count)", from, ok)
		}
	})
}

// TestLiveRunnerTranslatesAheadOfPlayheadFirstAfterSeek is the runner-level
// reproduction of the reported bug: after a seek, hundreds of untranslated
// cues can be left behind by the abandoned run, and ordering pending work by
// document time alone spent every batch on that backlog before the cue
// playing now was even queued — at 50 cues/batch and several seconds/batch,
// minutes of wait at the new position. Here the run-0 backlog is 4 cues,
// kept under batchSize and behind an hour-long BatchWait so nothing is
// translated before the seek; the seek playlist ends the session
// (#EXT-X-ENDLIST), which flushes everything pending, and it is the order of
// the batches — recorded via fakeTranslator.requests — that pins the fix.
func TestLiveRunnerTranslatesAheadOfPlayheadFirstAfterSeek(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": vttWith(4)})
	tr := &fakeTranslator{}
	// FreshRun off: this test is about the order inside one batch, and needs
	// the run-0 backlog to still be waiting when the seek lands.
	r, st := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute, FreshRun: -1})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})

	// The 4-cue backlog sits well under batchSize(50), and BatchWait(1h)
	// never expires here, so nothing fires on its own.
	time.Sleep(80 * time.Millisecond)
	if calls := atomic.LoadInt32(&tr.calls); calls != 0 {
		t.Fatalf("the run-0 backlog must wait for the seek, got %d upstream calls already", calls)
	}

	// The transcoder restarted at a seek: new offset, new segment content,
	// playlist ends immediately.
	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXTINF:2.0,\ns0-0.vtt?token=T\n#EXT-X-ENDLIST\n"
	srv.set(seek, map[string]string{"s0-0.vtt": "WEBVTT\n\n00:00.000 --> 00:01.000\nseeked line\n"})
	r.Wait("k")

	// What the viewer can still meet goes first and goes ALONE (2026-09-19):
	// the first batch after a seek used to be topped up with the backlog,
	// and the viewer waited for an upstream call fifty cues long that owed
	// them three. The backlog follows in a batch of its own.
	reqs := tr.requests()
	if len(reqs) != 2 {
		t.Fatalf("want the cue ahead of the viewer, then the backlog: got %d batches: %v", len(reqs), reqs)
	}
	if len(reqs[0]) != 1 || reqs[0][0] != "seeked line" {
		t.Fatalf("the first batch is the cue ahead of the playhead and nothing else, got %v", reqs[0])
	}
	if len(reqs[1]) != 4 {
		t.Fatalf("the backlog follows, whole: got %v", reqs[1])
	}
	if p, _ := st.GetProgress(context.Background(), "k"); p == nil || p.Live || p.Status != statusDone {
		t.Fatalf("seeked run reaching ENDLIST must record Status=done: %+v", p)
	}
}

// TestLiveRunnerFreshRunDoesNotWaitForCompany: right after a run starts
// (a seek, or the first read) the viewer is standing on the untranslated
// cues, so a partial batch goes out at once. BatchWait used to come on top
// of the poll interval and the upstream call there: the first line at a new
// position arrived 15-20 s after the seek.
func TestLiveRunnerFreshRunDoesNotWaitForCompany(t *testing.T) {
	run := func(t *testing.T, cfg LiveConfig) int32 {
		srv := newLivePlaylistServer(t)
		srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
		tr := &fakeTranslator{}
		r, _ := newLiveRunner(t, tr, 50, cfg)
		ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
		r.Touch("k")
		r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
		time.Sleep(150 * time.Millisecond)
		return atomic.LoadInt32(&tr.calls)
	}
	base := LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute}
	if calls := run(t, base); calls != 1 {
		t.Fatalf("a fresh run's lone cue must be translated at once, got %d upstream calls", calls)
	}
	off := base
	off.FreshRun = -1
	if calls := run(t, off); calls != 0 {
		t.Fatalf("control: without FreshRun the lone cue waits for BatchWait, got %d upstream calls", calls)
	}
	stale := base
	stale.FreshRun = time.Nanosecond
	if calls := run(t, stale); calls != 0 {
		t.Fatalf("a run older than FreshRun waits for company again, got %d upstream calls", calls)
	}
}

// TestLiveRunnerWakesOnNewRun: the job's ticker is its own schedule, but a
// seek seen by a viewer's poll (the handler refreshes the source) must not
// wait for it — the job wakes on the new run and translates it.
func TestLiveRunnerWakesOnNewRun(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	tr := &fakeTranslator{}
	// A ticker that never fires inside this test: only the wake-up can move
	// the job.
	r, _ := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	// The handler primes the source before it starts the job.
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	waitForCalls(t, tr, 1)

	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXTINF:2.0,\ns0-0.vtt?token=T\n"
	srv.set(seek, map[string]string{"s0-0.vtt": "WEBVTT\n\n00:00.000 --> 00:01.000\nseeked line\n"})
	// A viewer's poll after the seek: the handler's refresh sees the new run.
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, tr, 2)
	reqs := tr.requests()
	if last := reqs[len(reqs)-1]; len(last) != 1 || last[0] != "seeked line" {
		t.Fatalf("the wake-up must translate the new run's cue, got %v", reqs)
	}
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
	if p.Status != statusStopped {
		t.Fatalf("gone source must record Status=stopped, got %q", p.Status)
	}
}

// TestLiveRunnerNoFinalAfterSeekRecordsStatusDone is the other stopLive
// caller in finishLive: a run that reaches ENDLIST with everything pending
// translated, but cannot write a final artifact because it joined after a
// seek, is still "done" — not "stopped" — because nothing was cut short.
func TestLiveRunnerNoFinalAfterSeekRecordsStatusDone(t *testing.T) {
	srv := newLivePlaylistServer(t)
	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXTINF:2.0,\ns0-0.vtt?token=T\n#EXT-X-ENDLIST\n"
	srv.set(seek, map[string]string{"s0-0.vtt": seg0})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	r.Wait("k")
	p, _ := st.GetProgress(context.Background(), "k")
	if p == nil || p.Live || p.Status != statusDone {
		t.Fatalf("seeked run that reached ENDLIST must record Status=done: %+v", p)
	}
}

// TestLiveRunnerClearsStatusOnRearm is the guard on runLive's own doc
// comment: a record a re-armed job picks up must not go on reporting the
// previous run's terminal Status once this run is, once again, undecided.
func TestLiveRunnerClearsStatusOnRearm(t *testing.T) {
	st := NewMemoryStore()
	if err := st.PutProgress(context.Background(), "k", &Progress{Total: 1, Lines: []string{"PT:x"}, Live: false, Status: statusDone}); err != nil {
		t.Fatal(err)
	}
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r := NewRunner(st, &fakeTranslator{}, 50, 4, time.Minute)
	r.SetLive(LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	t.Cleanup(r.Close)
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	// runLive sets Live=true/Status="" on the record it loaded before its
	// first tick, but only persists on that tick (the store write above
	// primed the on-disk record, not the goroutine's local copy) — so this
	// polls for the write rather than sleeping past one PollInterval.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p, _ := st.GetProgress(context.Background(), "k")
		if p != nil && p.Live {
			if p.Status != "" {
				t.Fatalf("a re-armed run must clear the previous run's Status, got %q", p.Status)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("runLive never wrote its first record")
		}
		time.Sleep(2 * time.Millisecond)
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

// TestRunnerLiveJobsDoNotWaitOnTheOfflineSemaphore: an offline job holds
// its slot for seconds, a live one for the length of a film. Sharing one
// semaphore means the (default: 4) first live translations park every later
// key behind a whole movie, and its viewer is told "0/N, live" — the same
// thing a job that is merely starting up says — for an hour.
func TestRunnerLiveJobsDoNotWaitOnTheOfflineSemaphore(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	doc, _ := ParseVTT(strings.NewReader(vttWith(3)))
	doc.Normalize()
	ft := &fakeTranslator{block: make(chan struct{})}
	release := releaser(ft.block)
	st := NewMemoryStore()
	r := NewRunner(st, ft, 3, 1, time.Minute) // one offline slot, taken below
	// FreshRun off: the translator is blocked for the offline job, and a
	// fresh run's first act would be a batch into that same block.
	r.SetLive(LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute, FreshRun: -1})
	defer func() { release(); r.Close() }()

	r.Ensure(context.Background(), "offline", &Job{Lang: "pt", Doc: doc})
	waitForCalls(t, ft, 1) // the offline job holds the only offline slot

	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("live")
	r.Ensure(context.Background(), "live", &Job{Lang: "pt", Live: ls})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if p, _ := st.GetProgress(context.Background(), "live"); p != nil && p.Total > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the live job never ran: it is queued behind the offline semaphore")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLiveProgressMatchesSnapshotWithoutABody: HEAD asks for the counts,
// not the track. LiveSnapshot renders the whole document (a sort plus a
// WebVTT serialization of every cue) before the handler drops the body, on
// every HEAD of every viewer — so the HEAD path reads LiveProgress, which
// must agree with LiveSnapshot cue for cue.
func TestLiveProgressMatchesSnapshotWithoutABody(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	keys := ls.Doc().Keys()

	check := func(what string) {
		t.Helper()
		snap, err := r.LiveSnapshot(context.Background(), "k", ls)
		if err != nil {
			t.Fatal(err)
		}
		head, err := r.LiveProgress(context.Background(), "k", ls)
		if err != nil {
			t.Fatal(err)
		}
		if head.Done != snap.Done || head.Total != snap.Total || head.Live != snap.Live || head.Status != snap.Status ||
			head.HasPending != snap.HasPending || head.PendingFrom != snap.PendingFrom {
			t.Fatalf("%s: progress=%d/%d live=%v status=%q pending=%v/%s, snapshot=%d/%d live=%v status=%q pending=%v/%s",
				what, head.Done, head.Total, head.Live, head.Status, head.HasPending, head.PendingFrom,
				snap.Done, snap.Total, snap.Live, snap.Status, snap.HasPending, snap.PendingFrom)
		}
	}

	check("no record yet")

	// One cue translated, stored under its own key but at another index:
	// the counts have to come from the same alignment the body does.
	p := &Progress{Total: 2, Live: true, CueKeys: []string{keys[1], keys[0]}, Lines: []string{"PT:Привет.", ""}}
	if err := st.PutProgress(context.Background(), "k", p); err != nil {
		t.Fatal(err)
	}
	check("one cue aligned by key")

	p.Live = false
	if err := st.PutProgress(context.Background(), "k", p); err != nil {
		t.Fatal(err)
	}
	check("job stopped")

	if err := st.PutFinal(context.Background(), "k", []byte("WEBVTT\n")); err != nil {
		t.Fatal(err)
	}
	check("final artifact")
}

// TestLiveRunnerReusesTranslationsAcrossRunShift: the other half of the
// tolerant cue identity. A session's stored translations carry the cue keys
// of the run that produced them; the next run's timeline is shifted by up
// to one GOP (measured: 1.657 s), so exact-key reuse misses every one of
// them and the whole film is paid for again.
func TestLiveRunnerReusesTranslationsAcrossRunShift(t *testing.T) {
	const ms = time.Millisecond
	srv := newLivePlaylistServer(t)
	segA := "WEBVTT\n\n00:00.000 --> 00:02.628\nЯ такой.\n\n00:02.628 --> 00:04.588\nСтой.\n"
	plA := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-TARGETDURATION:5\n#EXTINF:4.588,\ns0-0.vtt?token=T\n"
	srv.set(plA, map[string]string{"s0-0.vtt": segA})
	tr := &fakeTranslator{}
	r, st := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: 20 * time.Millisecond, Idle: time.Minute})

	// What the previous session stored, on its own run's timeline (+1.657 s).
	p := &Progress{
		Total: 2,
		Live:  true,
		CueKeys: []string{
			CueKey(601657*ms, 604285*ms, []string{"Я такой."}),
			CueKey(604285*ms, 606245*ms, []string{"Стой."}),
		},
		Lines: []string{"PT:Я такой.", "PT:Стой."},
	}
	if err := st.PutProgress(context.Background(), "k", p); err != nil {
		t.Fatal(err)
	}

	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})

	deadline := time.Now().Add(2 * time.Second)
	for {
		got, _ := st.GetProgress(context.Background(), "k")
		if got != nil && got.Total == 2 && countFilled(got.Lines) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job never picked the document up: %+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Past BatchWait: if the stored lines had not been reused, the pending
	// cues would have gone upstream by now.
	time.Sleep(60 * time.Millisecond)
	if n := atomic.LoadInt32(&tr.calls); n != 0 {
		t.Fatalf("translated %d batch(es) that were already paid for on the previous run", n)
	}
}

// emptyReplyTranslator answers every line with an empty string: a reply that
// parses, numbered correctly, and says nothing.
type emptyReplyTranslator struct{}

func (emptyReplyTranslator) Translate(_ context.Context, req BatchRequest) (BatchResult, error) {
	return BatchResult{Lines: make([]string, len(req.Lines))}, nil
}

// TestTranslateChunkEmptyReplyKeepsSource: an empty reply line for a cue with
// text used to be stored as "", which is what "pending" means — so the cue
// was sent again with every later batch and, on a live source, pinned
// X-Subtitle-Pending-From to itself for the rest of the run.
func TestTranslateChunkEmptyReplyKeepsSource(t *testing.T) {
	r := NewRunner(NewMemoryStore(), emptyReplyTranslator{}, 50, 4, time.Minute)
	t.Cleanup(r.Close)
	p := &Progress{Lines: make([]string, 2)}
	logger := log.NewEntry(log.StandardLogger())
	if err := r.translateChunk(context.Background(), logger, &Job{Lang: "pt"}, "Portuguese", p, []int{0, 1}, []string{"first", "second"}); err != nil {
		t.Fatal(err)
	}
	if p.Lines[0] != "first" || p.Lines[1] != "second" {
		t.Fatalf("empty replies must keep the source text: %q", p.Lines)
	}
}

// TestLiveRunnerWakeUpsAreSpaced: every read that sees a new run wakes the
// job, and a playlist whose offset flaps would otherwise have the job reading
// back to back.
func TestLiveRunnerWakeUpsAreSpaced(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1, map[string]string{"s0-0.vtt": seg0})
	r, _ := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute, FreshRun: -1})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	playlist := "/h/a.mkv~hls/session/" + liveSessionID + "/s0.m3u8"
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	deadline := time.Now().Add(time.Second)
	for srv.hitCount(playlist) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	start := srv.hitCount(playlist)
	testReads := 0
	for i := 0; i < 10; i++ {
		off := "600"
		if i%2 == 1 {
			off = "0"
		}
		srv.set("#EXTM3U\n#EXT-X-SESSION-OFFSET:"+off+"\n#EXTINF:2.0,\ns0-0.vtt?token=T\n", map[string]string{"s0-0.vtt": seg0})
		if _, err := ls.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		testReads++
		time.Sleep(20 * time.Millisecond)
	}
	if jobReads := srv.hitCount(playlist) - start - testReads; jobReads > 2 {
		t.Fatalf("ten new runs in 200 ms made the job read %d times; wake-ups must be spaced", jobReads)
	}
}

// TestLiveRunnerRetriesAFailedFreshRead: a new run's first segment failing
// once must not cost the viewer a whole poll interval; the job reads again
// after liveWakeMinGap.
func TestLiveRunnerRetriesAFailedFreshRead(t *testing.T) {
	srv := newLivePlaylistServer(t)
	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:600\n#EXTINF:2.0,\ns0-9.vtt?token=T\n"
	srv.set(seek, map[string]string{"s0-9.vtt": "WEBVTT\n\n00:00.000 --> 00:01.000\nseeked line\n"})
	tr := &fakeTranslator{}
	r, _ := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	// A viewer's poll sees the new run first (its wake-up token is queued)
	// and its segment read fails.
	srv.failSegmentOnce("s0-9.vtt", 503)
	if _, err := ls.Refresh(context.Background()); err == nil {
		t.Fatal("setup: the first segment read must fail")
	}
	// The job spends that token on its own read, which fails as well; the
	// run is no longer new, so that read queues no token of its own.
	srv.failSegmentOnce("s0-9.vtt", 503)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})
	// The ticker never fires inside this test: only the nudge after the
	// failed read can bring the job back to translate.
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&tr.calls) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if atomic.LoadInt32(&tr.calls) == 0 {
		t.Fatal("the failed fresh read was not retried within a few seconds")
	}
}

// TestRunnerBatchTranslatesAheadOfPositionFirst: a file job orders its
// batches the way a live one does — the viewer's position first (the player
// leaves it with every poll), then the backlog — and re-reads the position
// at every batch boundary, so a seek moves the job within one batch.
func TestRunnerBatchTranslatesAheadOfPositionFirst(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(4))) // cues at 0,1,2,3 s
	doc.Normalize()
	ft := &fakeTranslator{block: make(chan struct{})}
	st := NewMemoryStore()
	r := NewRunner(st, ft, 1, 4, time.Minute) // one cue per batch
	// The cues sit a second apart, so any lead-in would pull the ones behind
	// the viewer into "ahead" and hide the rule under test: the position
	// decides, and is re-read per batch. TestPendingByTimeLeadIn has the
	// lead-in.
	r.SetLeadIn(0)
	t.Cleanup(r.Close)

	// The viewer is at 2 s when the job starts.
	if err := st.PutPos(context.Background(), "k", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Doc: doc})
	waitForCalls(t, ft, 1)
	// They seek back to the start while the first batch is upstream.
	if err := st.PutPos(context.Background(), "k", 0); err != nil {
		t.Fatal(err)
	}
	close(ft.block)
	r.Wait("k")

	want := [][]string{{"line 3"}, {"line 1"}, {"line 2"}, {"line 4"}}
	got := ft.requests()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("batch order: got %v, want %v (position first, then re-read per batch)", got, want)
	}
	if _, ok, _ := st.GetFinal(context.Background(), "k"); !ok {
		t.Fatal("the job must still finish the whole file")
	}
}

// TestSnapshotIsSparseAndCarriesTheFrontier: a partial file answer uses the
// live path's semantics — translated cues wherever they sit, pending ones
// left out — and, when the poll says where the viewer is, the same
// X-Subtitle-Pending-From fields a live answer carries.
func TestSnapshotIsSparseAndCarriesTheFrontier(t *testing.T) {
	doc, _ := ParseVTT(strings.NewReader(vttWith(3))) // cues at 0,1,2 s
	doc.Normalize()
	st := NewMemoryStore()
	r := NewRunner(st, &fakeTranslator{}, 3, 4, time.Minute)
	t.Cleanup(r.Close)
	if err := st.PutProgress(context.Background(), "k", &Progress{Total: 3, Lines: []string{"", "PT:line 2", ""}}); err != nil {
		t.Fatal(err)
	}
	snap, err := r.Snapshot(context.Background(), "k", doc, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	body := string(snap.Body)
	if !strings.Contains(body, "PT:line 2") || strings.Contains(body, "line 1") || strings.Contains(body, "line 3") {
		t.Fatalf("sparse body: %q", body)
	}
	if snap.Done != 1 || snap.Total != 3 {
		t.Fatalf("done=%d total=%d", snap.Done, snap.Total)
	}
	// At 1 s the cue at 0-0.5 s is behind the viewer; the earliest pending
	// cue they can still meet starts at 2 s.
	if !snap.HasPending || snap.PendingFrom != 2*time.Second {
		t.Fatalf("frontier: %v %v", snap.HasPending, snap.PendingFrom)
	}
	// No position, no frontier: the pre-position answer.
	snap, err = r.Snapshot(context.Background(), "k", doc, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if snap.HasPending {
		t.Fatal("a poll that does not say where the viewer is gets no frontier")
	}
}

// TestPendingByTimeLeadIn: the line between "ahead" and "behind" sits
// leadIn before the position and is tested on a cue's End, so the cue on
// screen right now and the exchange before it go out in the first batch
// (owner, 2026-09-18). Anything that ended before that stays behind.
func TestPendingByTimeLeadIn(t *testing.T) {
	at := func(i int, start, end float64) Cue {
		return Cue{Index: i, Start: time.Duration(start * float64(time.Second)), End: time.Duration(end * float64(time.Second)), Lines: []string{"x"}}
	}
	doc := &Doc{Cues: []Cue{
		at(0, 500, 503), // long gone
		at(1, 565, 569), // ended 31 s ago: just outside
		at(2, 572, 575), // ended 25 s ago: inside the lead-in
		at(3, 598, 603), // on screen right now
		at(4, 610, 612), // next
	}}
	lines := make([]string, len(doc.Cues))
	if got := fmt.Sprint(pendingByTime(doc, lines, 600*time.Second, 30*time.Second)); got != "[2 3 4 0 1]" {
		t.Fatalf("lead-in 30 s: got %s, want [2 3 4 0 1]", got)
	}
	// No lead-in still takes the cue on screen: End, not Start.
	if got := fmt.Sprint(pendingByTime(doc, lines, 600*time.Second, 0)); got != "[3 4 0 1 2]" {
		t.Fatalf("no lead-in: got %s, want [3 4 0 1 2]", got)
	}
	// A position inside the first half minute does not go negative.
	if got := fmt.Sprint(pendingByTime(doc, lines, 10*time.Second, 30*time.Second)); got != "[0 1 2 3 4]" {
		t.Fatalf("near the start: got %s", got)
	}
}

// stubAt is the transcoder's subtitle playlist while a run has written none
// yet: no segments, no ENDLIST, the run's offset (content-transcoder tags it
// since 2026-09-19).
func stubAt(offset string) string {
	return "#EXTM3U\n#EXT-X-SESSION-OFFSET:" + offset + "\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n"
}

// TestLiveProgressReportsAnUnreadRunAsPending: right after a seek the
// document has nothing for the new position -- the transcoder writes the
// subtitle playlist when the first segment closes, minutes on a source that
// is still downloading. "No untranslated cue ahead" used to go out as
// "nothing pending", and the player let the film go without subtitles under
// a pill saying "caught up" (owner, 2026-09-19).
func TestLiveProgressReportsAnUnreadRunAsPending(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(stubAt("1800.000"), nil)
	r, _ := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	head, err := r.LiveProgress(context.Background(), "k", ls)
	if err != nil {
		t.Fatal(err)
	}
	if !head.HasPending || head.PendingFrom != 1800*time.Second {
		t.Fatalf("an unread run must be pending from where the viewer is: pending=%v from=%s", head.HasPending, head.PendingFrom)
	}
	if head.SessionOffset != 1800*time.Second {
		t.Fatalf("and it must be about the new run: offset=%s", head.SessionOffset)
	}
	// The snapshot (GET) agrees with the head.
	snap, err := r.LiveSnapshot(context.Background(), "k", ls)
	if err != nil {
		t.Fatal(err)
	}
	if snap.HasPending != head.HasPending || snap.PendingFrom != head.PendingFrom {
		t.Fatalf("GET and HEAD disagree: %v/%s vs %v/%s", snap.HasPending, snap.PendingFrom, head.HasPending, head.PendingFrom)
	}
}

// The special case is narrow on purpose: one cue past the position, an
// ended playlist, or a run older than liveUnreadHold each end it.
func TestLiveFrontierUnreadRunIsBounded(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(stubAt("1800.000"), nil)
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	current := 1800 * time.Second
	empty := &Doc{}
	if from, ok := liveFrontier(empty, nil, current, ls, now); !ok || from != current {
		t.Fatalf("fixture: unread run is pending, got %v/%s", ok, from)
	}
	// A run that has been unread for too long says nothing any more: the
	// end credits never produce a subtitle playlist at all.
	if _, ok := liveFrontier(empty, nil, current, ls, now.Add(liveUnreadHold+time.Second)); ok {
		t.Fatal("past liveUnreadHold the unread run must stop holding the viewer")
	}
	// One cue at or after the position, already translated: the document
	// speaks for itself, and it says nothing is pending.
	read := &Doc{Cues: []Cue{{Index: 0, Start: 1805 * time.Second, End: 1808 * time.Second, Lines: []string{"x"}}}}
	if _, ok := liveFrontier(read, []string{"y"}, current, ls, now); ok {
		t.Fatal("a translated cue ahead of the viewer: nothing pending")
	}
	// Cues only BEHIND the viewer (the run before the seek) do not count as
	// read for this position.
	behind := &Doc{Cues: []Cue{{Index: 0, Start: 30 * time.Second, End: 33 * time.Second, Lines: []string{"x"}}}}
	if from, ok := liveFrontier(behind, []string{"y"}, current, ls, now); !ok || from != current {
		t.Fatalf("cues behind the viewer say nothing about where they are: %v/%s", ok, from)
	}
	// A run that has shown its first segment is a working run: a viewer
	// past its last cue has nothing ahead, as before.
	working := newLivePlaylistServer(t)
	working.set(pl1, map[string]string{"s0-0.vtt": seg0})
	wls := NewLiveSource(working.url(), working.srv.Client(), 1<<20, 5000)
	if _, err := wls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := liveFrontier(wls.Doc().Snapshot(), nil, 100*time.Second, wls, now); ok {
		t.Fatal("a run that is being read, viewer past its last cue: nothing pending")
	}
	// An ended playlist has nothing more coming.
	srv.set(stubAt("1800.000")+"#EXT-X-ENDLIST\n", nil)
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := liveFrontier(empty, nil, current, ls, now); ok {
		t.Fatal("an ended playlist with nothing ahead: nothing pending")
	}
}

// The batch log says which stretch of film a batch covers and who told the
// job where the viewer is; both are read off these two helpers.
func TestCueSpanAndWhereTheViewerIs(t *testing.T) {
	doc := &Doc{Cues: []Cue{
		{Index: 0, Start: 10 * time.Second, End: 12 * time.Second},
		{Index: 1, Start: 1500 * time.Second, End: 1503 * time.Second},
		{Index: 2, Start: 1490 * time.Second, End: 1509 * time.Second},
	}}
	first, last := cueSpan(doc, []int{1, 2})
	if first != 1490*time.Second || last != 1509*time.Second {
		t.Fatalf("span of cues 1,2 = %s..%s", first, last)
	}
	if f, l := cueSpan(doc, nil); f != 0 || l != 0 {
		t.Fatalf("an empty batch spans nothing, got %s..%s", f, l)
	}

	srv := newLivePlaylistServer(t)
	srv.set(pl2, map[string]string{"s0-0.vtt": seg0, "s0-1.vtt": seg1})
	r, st := newLiveRunner(t, &fakeTranslator{}, 50, LiveConfig{PollInterval: time.Hour, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	ref, err := ls.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ref.Segments != 2 {
		t.Fatalf("the read fetched two segments, reported %d", ref.Segments)
	}
	if ref, _ := ls.Refresh(context.Background()); ref.Segments != 0 {
		t.Fatalf("a read with nothing new fetches nothing, reported %d", ref.Segments)
	}
	if _, from := r.liveCurrentFrom(context.Background(), "k", ls); from != "run" {
		t.Fatalf("no position reported: the run start stands in, got %q", from)
	}
	if err := st.PutPos(context.Background(), LivePosKey("k", ls.CurrentOffset()), 50*time.Second); err != nil {
		t.Fatal(err)
	}
	if cur, from := r.liveCurrentFrom(context.Background(), "k", ls); from != "poll" || cur != 50*time.Second {
		t.Fatalf("a reported position: got %s from %q", cur, from)
	}
}

// slowTranslator holds every call open until it is released or its context
// is cancelled, the way an upstream call is: it records which it was.
type slowTranslator struct {
	mu        sync.Mutex
	started   [][]string
	cancelled [][]string
	release   chan struct{}
}

func (s *slowTranslator) Translate(ctx context.Context, req BatchRequest) (BatchResult, error) {
	lines := append([]string(nil), req.Lines...)
	s.mu.Lock()
	s.started = append(s.started, lines)
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.cancelled = append(s.cancelled, lines)
		s.mu.Unlock()
		return BatchResult{}, ctx.Err()
	case <-s.release:
	}
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = "PT:" + l
	}
	return BatchResult{Lines: out, InputTokens: 1, OutputTokens: 1}, nil
}

func (s *slowTranslator) snapshot() (started, cancelled [][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.started...), append([][]string(nil), s.cancelled...)
}

// TestLiveRunnerDropsABatchOrderedForTheOldRun: a seek while an upstream call
// is out. The batch in flight was ordered for where the viewer WAS, and they
// now wait behind it -- measured in production 2026-09-19: 9 s of a 27.7 s
// call, before the batch that mattered could even start. A viewer's poll
// makes the handler read the playlist, the new run is seen, and the batch is
// dropped; the job goes on with the new run instead of stopping.
func TestLiveRunnerDropsABatchOrderedForTheOldRun(t *testing.T) {
	srv := newLivePlaylistServer(t)
	srv.set(pl1+"#EXT-X-ENDLIST\n", map[string]string{"s0-0.vtt": vttWith(4)})
	tr := &slowTranslator{release: make(chan struct{})}
	r, _ := newLiveRunner(t, tr, 50, LiveConfig{PollInterval: 10 * time.Millisecond, BatchWait: time.Hour, Idle: time.Minute})
	ls := NewLiveSource(srv.url(), srv.srv.Client(), 1<<20, 5000)
	r.Touch("k")
	r.Ensure(context.Background(), "k", &Job{Lang: "pt", Live: ls})

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				started, cancelled := tr.snapshot()
				t.Fatalf("%s: started=%v cancelled=%v", what, started, cancelled)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor("the run-0 batch never went out", func() bool { s, _ := tr.snapshot(); return len(s) == 1 })

	// The seek: a new run far from anything in that batch. The handler's
	// read (a viewer's poll naming the new run) is what sees it.
	seek := "#EXTM3U\n#EXT-X-SESSION-OFFSET:1500\n#EXTINF:2.0,\ns0-0.vtt?token=T\n"
	srv.set(seek, map[string]string{"s0-0.vtt": "WEBVTT\n\n00:00.000 --> 00:01.000\nseeked line\n"})
	if _, err := ls.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	waitFor("the batch ordered for the old run was not dropped", func() bool { _, c := tr.snapshot(); return len(c) == 1 })
	// ...and the job is still alive: the next call is the new run's cue.
	waitFor("the job did not go on with the new run", func() bool {
		s, _ := tr.snapshot()
		return len(s) >= 2 && len(s[1]) == 1 && s[1][0] == "seeked line"
	})
	close(tr.release)
}

// A batch that already serves the new run is left alone: dropping it would
// throw away exactly the call the viewer is waiting for.
func TestBatchServesTheNewRun(t *testing.T) {
	doc := &Doc{Cues: []Cue{
		{Index: 0, Start: 400 * time.Second, End: 403 * time.Second},
		{Index: 1, Start: 1490 * time.Second, End: 1493 * time.Second},
		{Index: 2, Start: 2400 * time.Second, End: 2403 * time.Second},
	}}
	lead := 30 * time.Second
	if batchServes(doc, []int{0}, 1500*time.Second, lead) {
		t.Fatal("a cue eighteen minutes behind the new run serves nobody there")
	}
	if !batchServes(doc, []int{0, 1}, 1500*time.Second, lead) {
		t.Fatal("a cue inside the lead-in of the new run is what the viewer waits for")
	}
	if batchServes(doc, []int{2}, 1500*time.Second, lead) {
		t.Fatal("a cue fifteen minutes ahead is not what they are waiting for either")
	}
}
