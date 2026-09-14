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
const (
	sourceCacheTTL      = 10 * time.Minute
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
	key := ArtifactKey(r.Header.Get("X-Info-Hash"), r.Header.Get("X-Path"), lang, h.Model, PromptVersion)
	logger := log.WithFields(log.Fields{"key": key[:12], "lang": lang, "infoHash": r.Header.Get("X-Info-Hash"), "path": r.Header.Get("X-Path")})
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
		writeVTT(w, r, body, 100, 100, true)
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
		writeVTT(w, r, nil, done, total, false)
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
	writeVTT(w, r, snap.Body, snap.Done, snap.Total, snap.Final)
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

func writeVTT(w http.ResponseWriter, r *http.Request, body []byte, done, total int, final bool) {
	w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	w.Header().Set("X-Subtitle-Progress", strconv.Itoa(done)+"/"+strconv.Itoa(total))
	// The player polls this header cross-origin (the proxy adds
	// Access-Control-Allow-Origin itself and passes this one through).
	w.Header().Set("Access-Control-Expose-Headers", "X-Subtitle-Progress")
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
