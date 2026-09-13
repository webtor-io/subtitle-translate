package services

import (
	"bytes"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/asticode/go-astisub"
	"github.com/pkg/errors"
)

const lineBreakToken = " ⏎ "

type Cue struct {
	Index int
	Start time.Duration
	End   time.Duration
	Lines []string
}

type Doc struct {
	Items *astisub.Subtitles
	Cues  []Cue
}

func ParseVTT(r io.Reader) (*Doc, error) {
	subs, err := astisub.ReadFromWebVTT(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse webvtt")
	}
	d := &Doc{Items: subs}
	for i, it := range subs.Items {
		c := Cue{Index: i, Start: it.StartAt, End: it.EndAt}
		for _, l := range it.Lines {
			c.Lines = append(c.Lines, l.String())
		}
		d.Cues = append(d.Cues, c)
	}
	return d, nil
}

var (
	hiTagRe     = regexp.MustCompile(`\[[^\]]*\]|\([^)]*\)`)
	musicOnlyRe = regexp.MustCompile(`^[\s♪#♫♬]*$`)
	spacesRe    = regexp.MustCompile(`\s{2,}`)
)

// Normalize strips hearing-impaired markup and music-only lines so the
// model translates dialogue only. Timings and cue count are untouched: a
// cue that becomes empty stays as an empty cue.
func (d *Doc) Normalize() {
	for ci := range d.Cues {
		var kept []string
		for _, l := range d.Cues[ci].Lines {
			l = hiTagRe.ReplaceAllString(l, "")
			l = strings.TrimSpace(spacesRe.ReplaceAllString(l, " "))
			if l == "" || musicOnlyRe.MatchString(l) {
				continue
			}
			kept = append(kept, l)
		}
		d.Cues[ci].Lines = kept
	}
}

func Batches(n, size int) [][2]int {
	if size <= 0 {
		size = 50
	}
	var out [][2]int
	for from := 0; from < n; from += size {
		to := from + size
		if to > n {
			to = n
		}
		out = append(out, [2]int{from, to})
	}
	return out
}

func JoinLines(c Cue) string { return strings.Join(c.Lines, lineBreakToken) }

func SplitLines(s string) []string {
	var out []string
	for _, p := range strings.Split(s, strings.TrimSpace(lineBreakToken)) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Render writes a WebVTT with the first upTo cues. translated[i] replaces
// cue i's text when non-empty; otherwise the original lines are kept.
func (d *Doc) Render(translated []string, upTo int) ([]byte, error) {
	if upTo > len(d.Cues) {
		upTo = len(d.Cues)
	}
	// Styles and regions are document-level: a cue referencing one by name
	// renders wrong without its definition.
	out := &astisub.Subtitles{Metadata: d.Items.Metadata, Styles: d.Items.Styles, Regions: d.Items.Regions}
	for i := 0; i < upTo; i++ {
		src := d.Items.Items[i]
		var lines []astisub.Line
		if i < len(translated) && strings.TrimSpace(translated[i]) != "" {
			for _, t := range SplitLines(translated[i]) {
				lines = append(lines, astisub.Line{Items: []astisub.LineItem{{Text: t}}})
			}
		} else {
			// Use normalized lines from d.Cues
			for _, line := range d.Cues[i].Lines {
				lines = append(lines, astisub.Line{Items: []astisub.LineItem{{Text: line}}})
			}
		}
		// Cue settings (align/line/position/size, style and region) belong to
		// the timing, not to the text, so they carry over verbatim: a
		// translated cue must stay where the source put it.
		out.Items = append(out.Items, &astisub.Item{
			StartAt:     src.StartAt,
			EndAt:       src.EndAt,
			InlineStyle: src.InlineStyle,
			Region:      src.Region,
			Style:       src.Style,
			Lines:       lines,
		})
	}
	buf := &bytes.Buffer{}
	if len(out.Items) == 0 {
		buf.WriteString("WEBVTT\n\n")
		return buf.Bytes(), nil
	}
	if err := out.WriteToWebVTT(buf); err != nil {
		return nil, errors.Wrap(err, "failed to write webvtt")
	}
	return buf.Bytes(), nil
}
