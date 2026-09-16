package services

import (
	"strings"
	"testing"
	"time"
)

const seg0 = "WEBVTT\n\n00:45.107 --> 01:03.699\nМакс.\n"
const seg1 = "WEBVTT\n\n01:03.699 --> 01:05.000\nПривет.\n\n01:05.000 --> 01:06.000\n[door slams]\n"

func TestLiveDocAppendsAndDedups(t *testing.T) {
	d := NewLiveDoc()
	n, err := d.AddSegment(0, []byte(seg0))
	if err != nil || n != 1 || d.Len() != 1 {
		t.Fatalf("n=%d len=%d err=%v", n, d.Len(), err)
	}
	// The same segment again (a re-read playlist) adds nothing.
	if n, _ := d.AddSegment(0, []byte(seg0)); n != 0 || d.Len() != 1 {
		t.Fatalf("dup: n=%d len=%d", n, d.Len())
	}
	// A cue that normalizes to nothing still counts (keeps its slot).
	if n, _ := d.AddSegment(0, []byte(seg1)); n != 2 || d.Len() != 3 {
		t.Fatalf("seg1: n=%d len=%d", n, d.Len())
	}
	if got := d.Bytes(); got != int64(2*len(seg0)+len(seg1)) {
		t.Fatalf("bytes=%d", got)
	}
	keys := d.Keys()
	if len(keys) != 3 || keys[0] == keys[1] {
		t.Fatalf("keys=%v", keys)
	}
}

func TestLiveDocShiftsByOffset(t *testing.T) {
	d := NewLiveDoc()
	if _, err := d.AddSegment(30*time.Second, []byte(seg0)); err != nil {
		t.Fatal(err)
	}
	snap := d.Snapshot()
	if snap.Cues[0].Start != 75107*time.Millisecond {
		t.Fatalf("start=%v", snap.Cues[0].Start)
	}
	// The same text at the same movie time from a different run (offset 0,
	// cue at 1:15.107) is the same cue.
	same := "WEBVTT\n\n01:15.107 --> 01:33.699\nМакс.\n"
	if n, _ := d.AddSegment(0, []byte(same)); n != 0 {
		t.Fatalf("cross-run dup added: %d", n)
	}
}

func TestLiveDocSnapshotSortsButKeepsIndex(t *testing.T) {
	d := NewLiveDoc()
	_, _ = d.AddSegment(60*time.Second, []byte(seg0)) // added first, later in time
	_, _ = d.AddSegment(0, []byte(seg1))              // added second, earlier in time
	snap := d.Snapshot()
	if snap.Cues[0].Index != 1 || snap.Cues[len(snap.Cues)-1].Index != 0 {
		t.Fatalf("order: %+v", snap.Cues)
	}
	body, err := snap.RenderByIndex([]string{"PT:Макс.", "PT:Привет.", ""})
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	if strings.Index(s, "PT:Привет.") > strings.Index(s, "PT:Макс.") {
		t.Fatalf("render not time-ordered:\n%s", s)
	}
}

func TestRenderByIndexSkipsUntranslated(t *testing.T) {
	d := NewLiveDoc()
	_, _ = d.AddSegment(0, []byte(seg1)) // "Привет." then a structurally empty cue
	snap := d.Snapshot()
	body, err := snap.RenderByIndex([]string{"", ""})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "Привет") {
		t.Fatalf("untranslated cue leaked into the live render:\n%s", body)
	}
	// seg1 has one untranslated cue ("Привет.", must be omitted) and one
	// structurally empty cue ("[door slams]", keeps its slot): exactly one
	// cue should survive.
	if n := strings.Count(string(body), "-->"); n != 1 {
		t.Fatalf("want 1 cue (the empty slot), got %d:\n%s", n, body)
	}
	body, _ = snap.RenderByIndex([]string{"PT:Привет.", ""})
	if !strings.Contains(string(body), "PT:Привет.") {
		t.Fatalf("translated cue missing:\n%s", body)
	}
}

// Measured on the stand (2026-09-16, same MKV, two sessions): seek 600 →
// run A, seek 570 → run B. The same line lands at A 600.000 / B 601.657,
// A 602.628 / B 604.285, A 604.588 / B 606.245 — a constant 1.657 s between
// runs, and A's first cue clipped to 00:00.000. The transcoder reports the
// requested, 30 s-quantized seek as #EXT-X-SESSION-OFFSET but starts FFmpeg
// at the keyframe at or before it (input -ss with -noaccurate_seek), so
// cue + offset is short by up to one GOP (48 frames ≈ 2 s at 24 fps) per
// run. Exact-millisecond identity therefore misses across runs, and the
// same cue is translated and rendered twice.
func TestLiveDocDedupsTheSameCueAcrossRunShift(t *testing.T) {
	const runA = "WEBVTT\n\n00:00.000 --> 00:02.628\nЯ такой.\n"  // clipped first cue of the seek-600 run
	const runB = "WEBVTT\n\n00:31.657 --> 00:34.285\nЯ такой.\n"  // same line in the seek-570 run
	const later = "WEBVTT\n\n00:10.000 --> 00:12.000\nЯ такой.\n" // the line genuinely said again
	d := NewLiveDoc()
	if n, err := d.AddSegment(600*time.Second, []byte(runA)); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if n, _ := d.AddSegment(570*time.Second, []byte(runB)); n != 0 || d.Len() != 1 {
		t.Fatalf("a 1.657 s run shift is the same cue: n=%d len=%d", n, d.Len())
	}
	if n, _ := d.AddSegment(600*time.Second, []byte(later)); n != 1 || d.Len() != 2 {
		t.Fatalf("the same line 10 s later is a different cue: n=%d len=%d", n, d.Len())
	}
}

// A cue that straddles the seek point is clipped to the run's start, so its
// start carries the whole clip while its end keeps the run shift. Matching
// on start alone misses it; the end is what identifies it.
func TestLiveDocDedupsAClippedCueByItsEnd(t *testing.T) {
	const unclipped = "WEBVTT\n\n00:26.657 --> 00:36.245\nСтой.\n" // run B (offset 570): 596.657 → 606.245
	const clipped = "WEBVTT\n\n00:00.000 --> 00:04.588\nСтой.\n"   // run A (offset 600): 600.000 → 604.588
	d := NewLiveDoc()
	if n, err := d.AddSegment(570*time.Second, []byte(unclipped)); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// |Δstart| = 3.343 s is outside the window; |Δend| = 1.657 s is inside.
	if n, _ := d.AddSegment(600*time.Second, []byte(clipped)); n != 0 || d.Len() != 1 {
		t.Fatalf("a clipped twin is the same cue: n=%d len=%d", n, d.Len())
	}
}
