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

// LiveDoc accumulates cues from a stream of WebVTT segments. Segments of
// one FFmpeg run carry times from the run's start; offset is that run's
// #EXT-X-SESSION-OFFSET, so every cue is stored in movie time. The same
// cue reached twice (a re-read playlist, or a seek that replays a range)
// is stored once: identity is movie time plus normalized text.
type LiveDoc struct {
	mu    sync.Mutex
	cues  []Cue
	items []*astisub.Item
	keys  map[string]int
	bytes int64
}

func NewLiveDoc() *LiveDoc { return &LiveDoc{keys: map[string]int{}} }

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
		it := doc.Items.Items[i]
		item := &astisub.Item{StartAt: c.Start, EndAt: c.End, InlineStyle: it.InlineStyle, Region: it.Region, Style: it.Style, Lines: it.Lines}
		c.Index = len(d.cues)
		d.keys[key] = c.Index
		d.cues = append(d.cues, c)
		d.items = append(d.items, item)
		added++
	}
	return added, nil
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
		out.Cues = append(out.Cues, d.cues[i])
		out.Items.Items = append(out.Items.Items, d.items[i])
	}
	return out
}
