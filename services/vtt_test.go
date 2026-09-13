package services

import (
	"strings"
	"testing"
)

const sampleVTT = `WEBVTT

1
00:00:01.000 --> 00:00:02.000
[DOOR SLAMS]

2
00:00:03.000 --> 00:00:04.000
Hello there,
General Kenobi.

3
00:00:05.000 --> 00:00:06.000
♪ ♪

4
00:00:07.000 --> 00:00:08.000
(sighs) Fine. [laughs] Let's go.
`

func TestParseAndNormalize(t *testing.T) {
	d, err := ParseVTT(strings.NewReader(sampleVTT))
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Cues) != 4 {
		t.Fatalf("cues=%d", len(d.Cues))
	}
	d.Normalize()
	got := []string{JoinLines(d.Cues[0]), JoinLines(d.Cues[1]), JoinLines(d.Cues[2]), JoinLines(d.Cues[3])}
	want := []string{"", "Hello there, ⏎ General Kenobi.", "", "Fine. Let's go."}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("cue %d: got %q want %q", i, got[i], want[i])
		}
	}
	if d.Cues[1].Start.Seconds() != 3 || d.Cues[1].End.Seconds() != 4 {
		t.Errorf("timings changed: %v-%v", d.Cues[1].Start, d.Cues[1].End)
	}
}

func TestBatches(t *testing.T) {
	got := Batches(7, 3)
	want := [][2]int{{0, 3}, {3, 6}, {6, 7}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if len(Batches(0, 3)) != 0 {
		t.Fatal("empty input must give no batches")
	}
}

func TestRenderPartialKeepsTimingsAndOrder(t *testing.T) {
	d, _ := ParseVTT(strings.NewReader(sampleVTT))
	d.Normalize()
	tr := []string{"", "Olá, ⏎ General Kenobi.", "", ""}
	out, err := d.Render(tr, 2)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.HasPrefix(s, "WEBVTT") {
		t.Fatalf("no header: %q", s[:20])
	}
	if !strings.Contains(s, "00:00:03.000 --> 00:00:04.000\nOlá,\nGeneral Kenobi.") {
		t.Fatalf("translated cue with line break missing:\n%s", s)
	}
	if strings.Contains(s, "00:00:05.000") {
		t.Fatalf("cue beyond upTo rendered:\n%s", s)
	}
	full, _ := d.Render(tr, 4)
	if !strings.Contains(string(full), "Fine. Let's go.") {
		t.Fatalf("untranslated cue must fall back to the original:\n%s", full)
	}
}

func TestSplitJoinRoundTrip(t *testing.T) {
	c := Cue{Lines: []string{"a", "b"}}
	if got := SplitLines(JoinLines(c)); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}

const positionedVTT = `WEBVTT

STYLE
::cue { color: yellow; }

REGION
id:top
width:40%

1
00:00:01.000 --> 00:00:02.000 align:start line:5%
Hello there.

2
00:00:03.000 --> 00:00:04.000 position:10% size:35%
General Kenobi.
`

func TestRenderKeepsCuePositioning(t *testing.T) {
	d, err := ParseVTT(strings.NewReader(positionedVTT))
	if err != nil {
		t.Fatal(err)
	}
	d.Normalize()
	out, err := d.Render([]string{"Olá.", ""}, 2)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{"align:start", "line:5%", "position:10%", "size:35%"} {
		if !strings.Contains(s, want) {
			t.Errorf("rendered cue lost %q:\n%s", want, s)
		}
	}
	// The positioning must survive on the translated cue, not only on the
	// untouched one.
	if !strings.Contains(s, "align:start line:5%") || !strings.Contains(s, "Olá.") {
		t.Errorf("translated cue lost its settings:\n%s", s)
	}
}

func TestNormalizeStripsBeamedMusicNote(t *testing.T) {
	d, _ := ParseVTT(strings.NewReader("WEBVTT\n\n1\n00:00:01.000 --> 00:00:02.000\n♬ ♬\n"))
	d.Normalize()
	if len(d.Cues[0].Lines) != 0 {
		t.Fatalf("beamed-note cue must normalize to empty, got %v", d.Cues[0].Lines)
	}
}
