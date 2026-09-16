package services

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asticode/go-astisub"
	"github.com/pkg/errors"
)

// cueMatchTolerance is how far apart in movie time two cues with the same
// text may sit and still be the same cue.
//
// The transcoder reports the requested, 30 s-quantized seek as
// #EXT-X-SESSION-OFFSET, but invokes FFmpeg with input-level -ss plus
// -noaccurate_seek in copy mode, so the run actually starts at the keyframe
// at or before that point: cue + offset is short by up to one GOP (48
// frames, ~2 s at 24 fps) and by a different amount in every run. Measured
// on the stand (2026-09-16, same MKV, seek 600 vs seek 570): a constant
// 1.657 s between the two runs' timelines on every shared line. Exact
// millisecond identity therefore fails across runs, and the cost of that is
// paid twice — translated twice, then rendered twice as two overlapping
// near-identical cues.
//
// 3 s is above the measured shift and above one GOP at every frame rate we
// transcode, and well below the gap at which a line repeated in dialogue is
// a different cue.
const cueMatchTolerance = 3 * time.Second

// LiveDoc accumulates cues from a stream of WebVTT segments. Segments of
// one FFmpeg run carry times from the run's start; offset is that run's
// #EXT-X-SESSION-OFFSET, so every cue is stored in movie time. The same
// cue reached twice (a re-read playlist, or a seek that replays a range)
// is stored once: within one run identity is exact, and across two runs it
// is normalized text plus movie time within cueMatchTolerance — the window
// measures the shift between runs, so it is only asked about two of them.
// The cue that is kept is the first one seen, with the
// timing of the run that produced it — so its CueKey, the identity every
// stored translation is filed under, never moves under a reader.
type LiveDoc struct {
	mu    sync.Mutex
	cues  []Cue
	items []*astisub.Item
	// offsets is the #EXT-X-SESSION-OFFSET of the run each cue came from,
	// indexed like cues. The tolerance answers a question about two runs, so
	// the run a cue belongs to is part of its identity.
	offsets []time.Duration
	keys    map[string]int
	// byText indexes cue positions by their joined normalized text, which
	// is what the tolerant match needs to look up before comparing times.
	byText map[string][]int
	bytes  int64
}

func NewLiveDoc() *LiveDoc { return &LiveDoc{keys: map[string]int{}, byText: map[string][]int{}} }

// cueText is the text half of CueKey: the joined normalized lines.
func cueText(lines []string) string { return strings.Join(lines, "\n") }

// sameCue reports whether two cues with the same text are the same cue.
// Either end of the interval is enough: a cue that straddles the seek point
// is clipped to the run's start, so its start carries the whole clip while
// its end keeps only the run shift (and vice versa at the tail).
func sameCue(aStart, aEnd, bStart, bEnd time.Duration) bool {
	return absDuration(aStart-bStart) <= cueMatchTolerance || absDuration(aEnd-bEnd) <= cueMatchTolerance
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// CueKey is the identity of a cue across runs and re-reads.
func CueKey(start, end time.Duration, lines []string) string {
	return strconv.FormatInt(int64(start/time.Millisecond), 10) + "|" +
		strconv.FormatInt(int64(end/time.Millisecond), 10) + "|" + strings.Join(lines, "\n")
}

// AddSegment parses one WebVTT segment, shifts every cue by offset,
// normalizes, and appends the cues not seen before. Bytes accumulates
// len(vtt) for every segment, dup or not, since it reports how much source
// was read.
func (d *LiveDoc) AddSegment(offset time.Duration, vtt []byte) (int, error) {
	doc, err := ParseVTT(bytes.NewReader(vtt))
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse webvtt segment")
	}
	doc.Normalize()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bytes += int64(len(vtt))
	added := 0
	for i, c := range doc.Cues {
		c.Start += offset
		c.End += offset
		key := CueKey(c.Start, c.End, c.Lines)
		if _, seen := d.keys[key]; seen {
			continue
		}
		text := cueText(c.Lines)
		// Structurally empty cues are matched exactly and never by
		// tolerance: they all share the empty text, they carry no
		// translation to reuse, and collapsing two that happen to fall
		// within the window would drop a slot for nothing.
		if len(c.Lines) > 0 && d.dupWithin(text, offset, c.Start, c.End) {
			continue
		}
		it := doc.Items.Items[i]
		item := &astisub.Item{StartAt: c.Start, EndAt: c.End, InlineStyle: it.InlineStyle, Region: it.Region, Style: it.Style, Lines: it.Lines}
		c.Index = len(d.cues)
		d.keys[key] = c.Index
		if len(c.Lines) > 0 {
			d.byText[text] = append(d.byText[text], c.Index)
		}
		d.cues = append(d.cues, c)
		d.items = append(d.items, item)
		d.offsets = append(d.offsets, offset)
		added++
	}
	return added, nil
}

// dupWithin reports whether a cue with this text, from a different run,
// already sits within the tolerance window. Caller holds d.mu.
//
// Only across runs: the window measures the keyframe shift between two runs
// of the same file, and two cues of one run cannot be that — inside a run the
// transcoder's timestamps are the film's own, so a line repeated 1.5 s later
// is a line repeated 1.5 s later. Within a run identity stays exact, which
// d.keys already decides before this is reached.
func (d *LiveDoc) dupWithin(text string, offset, start, end time.Duration) bool {
	for _, i := range d.byText[text] {
		if d.offsets[i] == offset {
			continue
		}
		if sameCue(d.cues[i].Start, d.cues[i].End, start, end) {
			return true
		}
	}
	return false
}

func (d *LiveDoc) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.cues)
}

func (d *LiveDoc) Bytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bytes
}

// Keys returns CueKey for each cue, indexed by append order.
func (d *LiveDoc) Keys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.cues))
	for _, c := range d.cues {
		out[c.Index] = CueKey(c.Start, c.End, c.Lines)
	}
	return out
}

// Snapshot is the document as it stands, cues in movie-time order. Each
// Cue keeps Index = its position in the append order, which is the index
// into Progress.Lines / the translated slice passed to RenderByIndex.
func (d *LiveDoc) Snapshot() *Doc {
	d.mu.Lock()
	defer d.mu.Unlock()
	idx := make([]int, len(d.cues))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return d.cues[idx[a]].Start < d.cues[idx[b]].Start })
	out := &Doc{Items: &astisub.Subtitles{}}
	for _, i := range idx {
		c := d.cues[i]
		c.Run = d.offsets[i]
		out.Cues = append(out.Cues, c)
		out.Items.Items = append(out.Items.Items, d.items[i])
	}
	return out
}
