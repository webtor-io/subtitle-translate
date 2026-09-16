package services

import (
	"testing"
	"time"
)

const stagePlaylist = `#EXTM3U
#EXT-X-SESSION-OFFSET:30
#EXT-X-START:TIME-OFFSET=0
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-TARGETDURATION:64
#EXTINF:63.699000,
s0-0.vtt?api-key=K&token=T
#EXTINF:1.375000,
s0-1.vtt?api-key=K&token=T
`

func TestParseMediaPlaylistStageShape(t *testing.T) {
	p, err := ParseMediaPlaylist("https://edge.example/h/a.mkv~hls/session/abc/s0.m3u8?api-key=K&token=T", []byte(stagePlaylist))
	if err != nil {
		t.Fatal(err)
	}
	if p.Offset != 30*time.Second || p.Ended {
		t.Fatalf("offset=%v ended=%v", p.Offset, p.Ended)
	}
	if len(p.Segments) != 2 {
		t.Fatalf("segments: %d", len(p.Segments))
	}
	if p.Segments[0].URI != "https://edge.example/h/a.mkv~hls/session/abc/s0-0.vtt?api-key=K&token=T" {
		t.Fatalf("uri: %s", p.Segments[0].URI)
	}
	if p.Segments[0].Name != "s0-0.vtt" || p.Segments[1].Name != "s0-1.vtt" {
		t.Fatalf("names: %q %q", p.Segments[0].Name, p.Segments[1].Name)
	}
	if d := p.Segments[0].Duration; d < 63*time.Second || d > 64*time.Second {
		t.Fatalf("duration: %v", d)
	}
}

func TestParseMediaPlaylistEnded(t *testing.T) {
	p, err := ParseMediaPlaylist("https://e/x/s0.m3u8", []byte("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4.0,\ns0-0.vtt\n#EXT-X-ENDLIST\n"))
	if err != nil || !p.Ended || p.Offset != 0 {
		t.Fatalf("p=%+v err=%v", p, err)
	}
}

func TestParseMediaPlaylistRejectsMasterAndGarbage(t *testing.T) {
	if _, err := ParseMediaPlaylist("https://e/x/index.m3u8", []byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nv0.m3u8\n")); err == nil {
		t.Fatal("master playlist must be rejected")
	}
	if _, err := ParseMediaPlaylist("https://e/x/s0.m3u8", []byte("WEBVTT\n")); err == nil {
		t.Fatal("non-m3u8 must be rejected")
	}
}

func TestKeyPathStripsSession(t *testing.T) {
	cases := map[string]string{
		"/a.mkv~hls/session/54269b3aaa44b7a7419a01301cd046dc/s0.m3u8": "/a.mkv~hls/s0.m3u8",
		"/a.mkv~hls/s0.m3u8":                 "/a.mkv~hls/s0.m3u8",
		"/movie.srt~vtt/movie.vtt":           "/movie.srt~vtt/movie.vtt",
		"/dir/session/notahex/s0.m3u8~hls/x": "/dir/session/notahex/s0.m3u8~hls/x",
	}
	for in, want := range cases {
		if got := KeyPath(in); got != want {
			t.Fatalf("KeyPath(%q)=%q want %q", in, got, want)
		}
	}
}
