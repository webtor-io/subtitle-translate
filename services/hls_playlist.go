package services

import (
	"bufio"
	"bytes"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// SegmentRef is one entry of a media playlist. Name is the identity of the
// segment inside one FFmpeg run (the transcoder numbers segments from 0 on
// every run, so Name alone is not unique across seeks — pair it with the
// playlist's Offset).
type SegmentRef struct {
	URI      string
	Name     string
	Duration time.Duration
}

// MediaPlaylist is the part of an HLS media playlist the live source needs.
type MediaPlaylist struct {
	Offset   time.Duration
	Ended    bool
	Segments []SegmentRef
}

// ParseMediaPlaylist reads the transcoder's subtitle playlist. It refuses a
// master playlist: the caller must point at the s<N>.m3u8 variant itself.
func ParseMediaPlaylist(playlistURL string, data []byte) (*MediaPlaylist, error) {
	if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("#EXTM3U")) {
		return nil, errors.New("not an m3u8 playlist")
	}
	base, err := url.Parse(playlistURL)
	if err != nil {
		return nil, errors.Wrap(err, "bad playlist url")
	}
	p := &MediaPlaylist{}
	var pending time.Duration
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "#EXT-X-STREAM-INF"):
			return nil, errors.New("master playlist given where a media playlist was expected")
		case strings.HasPrefix(line, "#EXT-X-SESSION-OFFSET:"):
			sec, perr := strconv.ParseFloat(strings.TrimPrefix(line, "#EXT-X-SESSION-OFFSET:"), 64)
			if perr != nil {
				return nil, errors.Wrap(perr, "bad session offset")
			}
			p.Offset = time.Duration(sec * float64(time.Second))
		case line == "#EXT-X-ENDLIST":
			p.Ended = true
		case strings.HasPrefix(line, "#EXTINF:"):
			v := strings.TrimPrefix(line, "#EXTINF:")
			if i := strings.IndexByte(v, ','); i >= 0 {
				v = v[:i]
			}
			sec, perr := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if perr != nil {
				return nil, errors.Wrap(perr, "bad EXTINF")
			}
			pending = time.Duration(sec * float64(time.Second))
		case strings.HasPrefix(line, "#"):
			continue
		default:
			ref, perr := url.Parse(line)
			if perr != nil {
				return nil, errors.Wrap(perr, "bad segment uri")
			}
			abs := base.ResolveReference(ref)
			p.Segments = append(p.Segments, SegmentRef{URI: abs.String(), Name: path.Base(abs.Path), Duration: pending})
			pending = 0
		}
	}
	if err := sc.Err(); err != nil {
		return nil, errors.Wrap(err, "read playlist")
	}
	return p, nil
}

var sessionPathRe = regexp.MustCompile(`~hls/session/[0-9a-f]{32}/`)

// KeyPath is X-Path with the transcoder session id removed, so every
// session of the same file and stream shares one artifact key. Paths
// without a session segment are returned unchanged.
func KeyPath(p string) string {
	return sessionPathRe.ReplaceAllString(p, "~hls/")
}
