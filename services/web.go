package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	logrusmiddleware "github.com/bakins/logrus-middleware"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"github.com/webtor-io/lazymap"
)

const (
	webHostFlag = "host"
	webPortFlag = "port"
)

func RegisterWebFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{Name: webHostFlag, Usage: "listening host", Value: "", EnvVar: "WEB_HOST"},
		cli.IntFlag{Name: webPortFlag, Usage: "http listening port", Value: 8080, EnvVar: "WEB_PORT"},
	)
}

// NotConfiguredHandler is served when no API key is configured: the
// capability is absent and says so instead of failing silently.
func NotConfiguredHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "translation is not configured", http.StatusNotImplemented)
	})
}

type Web struct {
	host string
	port int
	h    http.Handler
	ln   net.Listener
}

func NewWeb(c *cli.Context, h http.Handler) *Web {
	return &Web{host: c.String(webHostFlag), port: c.Int(webPortFlag), h: h}
}

func (s *Web) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "failed to listen to tcp connection")
	}
	s.ln = ln
	logger := log.New()
	m := logrusmiddleware.Middleware{Logger: logger}
	srv := &http.Server{Handler: m.Handler(s.h, ""), MaxHeaderBytes: 50 << 20}
	log.Infof("serving web at %v", addr)
	return srv.Serve(ln)
}

func (s *Web) Close() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

var trPathRe = regexp.MustCompile(`~tr:([a-z]{2})/[^/]*\.vtt$`)

// TargetLang resolves the target language. torrent-http-proxy strips the
// mod segment from the path it forwards and sends its argument as
// X-Mod-Extra (~tr:pt → "pt"); a direct call keeps the full path, so
// /…~tr:pt/name.vtt is parsed as a fallback.
func TargetLang(r *http.Request) (string, bool) {
	if extra := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Mod-Extra"))); extra != "" {
		if _, ok := LangName(extra); ok {
			return extra, true
		}
		return "", false
	}
	return ParseLang(r.URL.Path)
}

// ParseLang takes the target language from a full ~tr:<lang>/<name>.vtt path.
func ParseLang(p string) (string, bool) {
	m := trPathRe.FindStringSubmatch(p)
	if m == nil {
		return "", false
	}
	if _, ok := LangName(m[1]); !ok {
		return "", false
	}
	return m[1], true
}

// ParseNames reads the "names" query parameter (comma-separated glossary
// entries), trims whitespace, drops empties, and caps the result at 30
// entries of at most 40 runes each.
func ParseNames(q string) []string {
	var out []string
	for _, n := range strings.Split(q, ",") {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		if r := []rune(n); len(r) > 40 {
			n = string(r[:40])
		}
		out = append(out, n)
		if len(out) == 30 {
			break
		}
	}
	return out
}

// sourceCacheTTL is how long a parsed source track is reused. A client
// polls the same URL every few seconds while the job runs, and the source
// does not change between polls.
//
// liveSourceCacheTTL is longer: a live job can run for the length of a
// whole movie, and dropping the cached LiveSource mid-playback would
// restart its accumulated document (and re-fetch every segment) on the
// next poll instead of reusing what the background job already built.
const (
	sourceCacheTTL      = 10 * time.Minute
	liveSourceCacheTTL  = 30 * time.Minute
	sourceCacheCapacity = 64
	sourceFetchTimeout  = 30 * time.Second
)

type Handler struct {
	Runner         *Runner
	Model          string
	Client         *http.Client
	MaxSourceBytes int64
	MaxCues        int

	once sync.Once
	docs *lazymap.LazyMap[*Doc]

	liveOnce sync.Once
	lives    *lazymap.LazyMap[*LiveSource]
}

// docCache holds the parsed source per artifact key. Failures are not
// stored (lazymap drops a failed entry), so a source that recovers is
// picked up on the next poll instead of being remembered as broken.
func (h *Handler) docCache() *lazymap.LazyMap[*Doc] {
	h.once.Do(func() {
		// Capacity bounds resident memory: a parsed 1 MiB source is ~11 MB,
		// so this is the ceiling on distinct tracks kept warm per replica.
		h.docs = lazymap.New[*Doc](&lazymap.Config{Expire: sourceCacheTTL, Capacity: sourceCacheCapacity})
	})
	return h.docs
}

// docFor is fetchDoc behind the cache: concurrent pollers of the same key
// share one fetch, and a hit skips the source entirely.
func (h *Handler) docFor(ctx context.Context, key, sourceURL string) (*Doc, *clientError) {
	doc, err := h.docCache().Get(key, func() (*Doc, error) {
		// The fetch is shared by every poller of this key, so it must not
		// die with the first requester's connection.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sourceFetchTimeout)
		defer cancel()
		d, cerr := h.fetchDoc(fctx, sourceURL)
		if cerr != nil {
			return nil, cerr
		}
		return d, nil
	})
	if err != nil {
		var cerr *clientError
		if errors.As(err, &cerr) {
			return nil, cerr
		}
		return nil, &clientError{http.StatusBadGateway, msgUpstreamUnavail, err}
	}
	return doc, nil
}

// isPlaylistSource reports whether u is the transcoder's subtitle media
// playlist (…~hls/session/<id>/s<N>.m3u8) rather than a single VTT/SRT
// track: a live job follows the playlist instead of reading one file.
func isPlaylistSource(u *url.URL) bool {
	return strings.HasSuffix(strings.ToLower(u.Path), ".m3u8")
}

// livesCache is docCache's counterpart for live sources.
func (h *Handler) livesCache() *lazymap.LazyMap[*LiveSource] {
	h.liveOnce.Do(func() {
		h.lives = lazymap.New[*LiveSource](&lazymap.Config{Expire: liveSourceCacheTTL, Capacity: sourceCacheCapacity})
	})
	return h.lives
}

// liveFor returns the cached LiveSource for key, creating one lazily.
// NewLiveSource makes no network call, so a HEAD before any GET populates
// the cache without fetching anything.
//
// key survives across transcoder sessions (KeyPath strips the session id),
// but each new session serves its playlist at a new URL. Reusing a cached
// LiveSource built for the old URL would poll a URL that now 404s, ending
// the job with source_gone while the viewer is mid-session — so a URL
// mismatch drops the stale entry and starts a fresh source under the same
// key instead of trusting the cache.
func (h *Handler) liveFor(key, sourceURL string) *LiveSource {
	cache := h.livesCache()
	newSource := func() (*LiveSource, error) {
		return NewLiveSource(sourceURL, h.Client, h.MaxSourceBytes, h.MaxCues), nil
	}
	src, _ := cache.Get(key, newSource)
	if src.url != sourceURL {
		// Retire before dropping: a job may still be mid-poll against the
		// stale source (Ensure is a no-op while a job for key is already
		// running, so this request's own Ensure below will not replace it),
		// and without this it would keep polling the dead URL for up to one
		// more PollInterval — plus any in-flight batch — before noticing on
		// its own.
		src.Retire()
		cache.Drop(key)
		src, _ = cache.Get(key, newSource)
	}
	return src
}

// Client-facing bodies. The real cause is logged and never written to the
// response: the source URL and the dial error belong to the operator, not
// to whoever is polling the track.
const (
	msgBadRequest      = "bad request"
	msgSourceUnavail   = "source unavailable"
	msgSourceTooLarge  = "source too large"
	msgTooManyCues     = "too many cues"
	msgUpstreamUnavail = "upstream state unavailable"
)

// clientError pairs what the client is told with the real cause, which
// only reaches the log.
type clientError struct {
	status int
	msg    string
	err    error
}

func (e *clientError) Error() string { return e.err.Error() }

func (e *clientError) Unwrap() error { return e.err }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	lang, ok := TargetLang(r)
	if !ok {
		log.WithFields(log.Fields{"path": r.URL.Path, "extra": r.Header.Get("X-Mod-Extra")}).Warn("unsupported or missing target language")
		http.Error(w, msgBadRequest, http.StatusBadRequest)
		return
	}
	sourceURL := r.Header.Get("X-Source-Url")
	if sourceURL == "" {
		log.WithField("path", r.URL.Path).Warn("missing X-Source-Url")
		http.Error(w, msgBadRequest, http.StatusBadRequest)
		return
	}
	// KeyPath drops the transcoder session id, so every session of the same
	// stream shares one artifact key; it is the identity function on any
	// path without a session segment, so a non-HLS track's key is exactly
	// what it was before.
	path := KeyPath(r.Header.Get("X-Path"))
	key := ArtifactKey(r.Header.Get("X-Info-Hash"), path, lang, h.Model, PromptVersion)
	logger := log.WithFields(log.Fields{"key": key[:12], "lang": lang, "infoHash": r.Header.Get("X-Info-Hash"), "path": path})
	ctx := r.Context()

	body, found, err := h.Runner.store.GetFinal(ctx, key)
	if err != nil {
		// The store is the only thing that can tell a finished artifact from
		// an unstarted one; without it, fetching the source would be work
		// spent on an answer we cannot give.
		logger.WithError(err).Error("failed to read the final artifact")
		http.Error(w, msgUpstreamUnavail, http.StatusBadGateway)
		return
	}
	if found {
		writeVTT(w, r, body, 100, 100, true, false)
		return
	}
	// Touch keeps a live job's idle timer from expiring: polling the
	// playlist (GET or HEAD) is what keeps the transcoder session alive.
	// For a regular source there is no idle job to keep, so this is a
	// harmless no-op.
	h.Runner.Touch(key)

	if u, perr := url.Parse(sourceURL); perr == nil && isPlaylistSource(u) {
		src := h.liveFor(key, sourceURL)
		if r.Method == http.MethodHead {
			// HEAD never starts the job (same contract as the regular
			// source below): it reports what is known without triggering
			// or waiting on a translation.
			snap, err := h.Runner.LiveSnapshot(ctx, key, src)
			if err != nil {
				logger.WithError(err).Error("live snapshot failed")
				http.Error(w, msgUpstreamUnavail, http.StatusBadGateway)
				return
			}
			writeVTTLive(w, r, nil, snap)
			return
		}
		// A fresh source has nothing yet: one synchronous read here means
		// the first response already carries whatever cues exist, and a
		// gone/oversize source is reported to this request the same way a
		// regular source's fetch failure is, instead of surfacing only on
		// the background job's next tick.
		if src.Doc().Len() == 0 {
			// The source is cached and shared with whoever polls this key
			// next, so — like docFor's fetch — this priming read must not
			// die with this request's own connection.
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sourceFetchTimeout)
			_, rerr := src.Refresh(rctx)
			cancel()
			if rerr != nil {
				switch {
				case errors.Is(rerr, ErrSourceGone):
					logger.WithError(rerr).Warn("live source unavailable")
					http.Error(w, msgSourceUnavail, http.StatusNotFound)
				case errors.Is(rerr, ErrSourceTooLarge):
					logger.WithError(rerr).Warn("live source outgrew its caps")
					http.Error(w, msgSourceTooLarge, http.StatusRequestEntityTooLarge)
				default:
					logger.WithError(rerr).Warn("live source unavailable")
					http.Error(w, msgSourceUnavail, http.StatusNotFound)
				}
				return
			}
		}
		h.Runner.Ensure(ctx, key, &Job{Lang: lang, SourceLang: r.URL.Query().Get("srclang"), Glossary: ParseNames(r.URL.Query().Get("names")), Live: src})
		snap, err := h.Runner.LiveSnapshot(ctx, key, src)
		if err != nil {
			logger.WithError(err).Error("live snapshot failed")
			http.Error(w, msgUpstreamUnavail, http.StatusBadGateway)
			return
		}
		writeVTTLive(w, r, snap.Body, snap)
		return
	}

	if r.Method == http.MethodHead {
		p, _ := h.Runner.store.GetProgress(ctx, key)
		done, total := 0, 0
		if p != nil {
			total = p.Total
			// HEAD has no Doc, so it cannot tell a still-pending cue from one
			// that normalized to empty and was never sent for translation
			// (see countDone in job.go); count every filled-in line instead
			// of stopping at the first gap, so progress keeps advancing past
			// structurally-empty cues instead of freezing there.
			for _, l := range p.Lines {
				if l != "" {
					done++
				}
			}
		}
		writeVTT(w, r, nil, done, total, false, false)
		return
	}
	doc, cerr := h.docFor(ctx, key, sourceURL)
	if cerr != nil {
		logger.WithError(cerr.err).Warn("source unavailable")
		http.Error(w, cerr.msg, cerr.status)
		return
	}
	h.Runner.Ensure(ctx, key, &Job{Lang: lang, SourceLang: r.URL.Query().Get("srclang"), Glossary: ParseNames(r.URL.Query().Get("names")), Doc: doc})
	snap, err := h.Runner.Snapshot(ctx, key, doc)
	if err != nil {
		logger.WithError(err).Error("snapshot failed")
		http.Error(w, msgUpstreamUnavail, http.StatusBadGateway)
		return
	}
	writeVTT(w, r, snap.Body, snap.Done, snap.Total, snap.Final, false)
}

func (h *Handler) fetchDoc(ctx context.Context, sourceURL string) (*Doc, *clientError) {
	// Scheme guard before dialing: the source URL arrives in a header, and
	// only http(s) is a subtitle track. file:// and friends would be read by
	// the transport as local or intranet resources.
	u, err := url.Parse(sourceURL)
	if err != nil {
		return nil, &clientError{http.StatusBadRequest, msgBadRequest, errors.Wrap(err, "bad source url")}
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, &clientError{http.StatusBadRequest, msgBadRequest, errors.Errorf("unsupported source scheme %q", u.Scheme)}
	}
	ctx, cancel := context.WithTimeout(ctx, sourceFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, &clientError{http.StatusBadRequest, msgBadRequest, errors.Wrap(err, "bad source url")}
	}
	res, err := h.Client.Do(req)
	if err != nil {
		return nil, &clientError{http.StatusNotFound, msgSourceUnavail, errors.Wrap(err, "source fetch failed")}
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, &clientError{http.StatusNotFound, msgSourceUnavail, errors.Errorf("source returned %d", res.StatusCode)}
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, h.MaxSourceBytes+1))
	if err != nil {
		return nil, &clientError{http.StatusNotFound, msgSourceUnavail, errors.Wrap(err, "source read failed")}
	}
	if int64(len(data)) > h.MaxSourceBytes {
		return nil, &clientError{http.StatusRequestEntityTooLarge, msgSourceTooLarge, errors.Errorf("source over %d bytes", h.MaxSourceBytes)}
	}
	doc, err := ParseVTT(bytes.NewReader(data))
	if err != nil {
		return nil, &clientError{http.StatusNotFound, msgSourceUnavail, err}
	}
	if len(doc.Cues) > h.MaxCues {
		return nil, &clientError{http.StatusRequestEntityTooLarge, msgTooManyCues, errors.Errorf("too many cues: %d", len(doc.Cues))}
	}
	doc.Normalize()
	return doc, nil
}

// writeVTT renders a response. live sets X-Subtitle-Live and adds it to the
// exposed header list; a non-live response keeps the exact header set it
// had before live sources existed, since it is polled the same way whether
// or not this build knows about live sources at all.
func writeVTT(w http.ResponseWriter, r *http.Request, body []byte, done, total int, final, live bool) {
	w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	w.Header().Set("X-Subtitle-Progress", strconv.Itoa(done)+"/"+strconv.Itoa(total))
	// The player polls these headers cross-origin (the proxy adds
	// Access-Control-Allow-Origin itself and passes these through).
	expose := "X-Subtitle-Progress"
	if live {
		w.Header().Set("X-Subtitle-Live", "1")
		expose = "X-Subtitle-Progress, X-Subtitle-Live"
	}
	w.Header().Set("Access-Control-Expose-Headers", expose)
	if final {
		w.Header().Set("Cache-Control", "public, max-age=86400")
	} else {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead || body == nil {
		return
	}
	_, _ = w.Write(body)
}

// writeVTTLive renders a Snapshot from a live source: Live decides whether
// X-Subtitle-Live is set, on top of the same done/total/final contract a
// regular track's snapshot uses.
func writeVTTLive(w http.ResponseWriter, r *http.Request, body []byte, snap *Snapshot) {
	writeVTT(w, r, body, snap.Done, snap.Total, snap.Final, snap.Live)
}
