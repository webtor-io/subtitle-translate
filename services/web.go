package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	logrusmiddleware "github.com/bakins/logrus-middleware"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
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

// ParseLang takes the target language from the request path: THP does
// not forward the mod extra, but the reverse proxy keeps the original
// path, so /…~tr:pt/name.vtt is visible here.
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
		if len(n) > 40 {
			n = n[:40]
		}
		out = append(out, n)
		if len(out) == 30 {
			break
		}
	}
	return out
}

type Handler struct {
	Runner         *Runner
	Model          string
	Client         *http.Client
	MaxSourceBytes int64
	MaxCues        int
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	lang, ok := ParseLang(r.URL.Path)
	if !ok {
		http.Error(w, "unsupported or missing target language", http.StatusBadRequest)
		return
	}
	sourceURL := r.Header.Get("X-Source-Url")
	if sourceURL == "" {
		http.Error(w, "missing X-Source-Url", http.StatusBadRequest)
		return
	}
	key := ArtifactKey(r.Header.Get("X-Info-Hash"), r.Header.Get("X-Path"), lang, h.Model, PromptVersion)
	logger := log.WithFields(log.Fields{"key": key[:12], "lang": lang, "infoHash": r.Header.Get("X-Info-Hash"), "path": r.Header.Get("X-Path")})
	ctx := r.Context()

	if body, ok, err := h.Runner.store.GetFinal(ctx, key); err == nil && ok {
		writeVTT(w, r, body, 100, 100, true)
		return
	}
	if r.Method == http.MethodHead {
		p, _ := h.Runner.store.GetProgress(ctx, key)
		done, total := 0, 0
		if p != nil {
			total = p.Total
			for _, l := range p.Lines {
				if l == "" {
					break
				}
				done++
			}
		}
		writeVTT(w, r, nil, done, total, false)
		return
	}
	doc, status, err := h.fetchDoc(ctx, sourceURL)
	if err != nil {
		logger.WithError(err).Warn("source unavailable")
		http.Error(w, err.Error(), status)
		return
	}
	h.Runner.Ensure(ctx, key, &Job{Lang: lang, SourceLang: r.URL.Query().Get("srclang"), Glossary: ParseNames(r.URL.Query().Get("names")), Doc: doc})
	snap, err := h.Runner.Snapshot(ctx, key, doc)
	if err != nil {
		logger.WithError(err).Error("snapshot failed")
		http.Error(w, "upstream state unavailable", http.StatusBadGateway)
		return
	}
	writeVTT(w, r, snap.Body, snap.Done, snap.Total, snap.Final)
}

func (h *Handler) fetchDoc(ctx context.Context, sourceURL string) (*Doc, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrap(err, "bad source url")
	}
	res, err := h.Client.Do(req)
	if err != nil {
		return nil, http.StatusNotFound, errors.Wrap(err, "source fetch failed")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, http.StatusNotFound, errors.Errorf("source returned %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, h.MaxSourceBytes+1))
	if err != nil {
		return nil, http.StatusNotFound, errors.Wrap(err, "source read failed")
	}
	if int64(len(data)) > h.MaxSourceBytes {
		return nil, http.StatusRequestEntityTooLarge, errors.New("source too large")
	}
	doc, err := ParseVTT(bytes.NewReader(data))
	if err != nil {
		return nil, http.StatusNotFound, err
	}
	if len(doc.Cues) > h.MaxCues {
		return nil, http.StatusRequestEntityTooLarge, errors.Errorf("too many cues: %d", len(doc.Cues))
	}
	doc.Normalize()
	return doc, 0, nil
}

func writeVTT(w http.ResponseWriter, r *http.Request, body []byte, done, total int, final bool) {
	w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	w.Header().Set("X-Subtitle-Progress", strconv.Itoa(done)+"/"+strconv.Itoa(total))
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
