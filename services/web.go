package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

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

// webShutdownGrace is how long Close waits for in-flight requests before
// the listener goes away regardless.
const webShutdownGrace = 5 * time.Second

type Web struct {
	host string
	port int
	h    http.Handler

	// mu guards ln and srv: Serve runs in its own goroutine (cs.Serve
	// starts every Servable that way) while Close runs on the main one.
	mu  sync.Mutex
	ln  net.Listener
	srv *http.Server
}

// newHTTPServer builds the server. The limits are the point of the function
// existing: they are part of the service's contract with whatever can open a
// connection to it, and that is the whole cluster network (there is no
// NetworkPolicy — see the README).
//
// MaxHeaderBytes is the Go default. Nothing legitimately sends 50 MB of
// headers, and against the pod's 512Mi limit a handful of connections that
// did would end the process. ReadHeaderTimeout and ReadTimeout bound what a
// slow reader can hold; IdleTimeout bounds a keep-alive connection nobody is
// using. WriteTimeout is deliberately left unset: a response is written for
// as long as its render takes, and there is no upper bound worth guessing.
func newHTTPServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
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
	logger := log.New()
	m := logrusmiddleware.Middleware{Logger: logger}
	srv := newHTTPServer(m.Handler(s.h, ""))
	s.mu.Lock()
	s.ln = ln
	s.srv = srv
	s.mu.Unlock()
	log.Infof("serving web at %v", ln.Addr())
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		// Close asked for this; it is not a serve error, and reporting it as
		// one would turn an orderly shutdown into a failed exit code.
		return nil
	}
	return err
}

// addr is the address actually being listened on. With port 0 that is only
// known after the listen, which is why it is read rather than formatted.
func (s *Web) addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Close drains the server: requests in flight get webShutdownGrace to
// finish, and only then does the listener go away. Closing the listener on
// its own — what this did before — does nothing to connections already
// accepted, so a poll being answered at the moment the pod was told to stop
// was cut mid-response.
func (s *Web) Close() {
	s.mu.Lock()
	srv, ln := s.srv, s.ln
	s.mu.Unlock()
	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), webShutdownGrace)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.WithError(err).Warn("web server did not drain within its grace period")
		}
		// Shutdown closed the listener itself, successfully or not.
		return
	}
	if ln != nil {
		_ = ln.Close()
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

// ParseSourceLang reads the "srclang" query parameter: a hint about the
// language the track is already in, formatted into the system prompt.
//
// It is a language code or it is nothing. The hint is optional and the
// translation is fine without it, so an unknown value is dropped rather than
// refused — but it must not reach the prompt as free text: the artifact it
// helps produce is shared by everyone watching that file and language, and
// served from cache for 24 h (indefinitely from S3), so whatever the first
// requester wrote would be baked into what every later viewer reads.
func ParseSourceLang(q string) string {
	code := strings.ToLower(strings.TrimSpace(q))
	if _, ok := LangName(code); !ok {
		return ""
	}
	return code
}

// flattenName reduces one glossary entry to a single line: control
// characters (a newline among them) and any other whitespace become single
// spaces, and the ends are trimmed. Entries are joined into one line of the
// prompt — "Glossary (character names): a, b, c" — so a newline inside one
// is not a formatting nuisance but a way to write an instruction of one's
// own into a prompt whose artifact is then served to every viewer of the
// track. The same sharing argument as ParseSourceLang, and the same answer.
func flattenName(n string) string {
	var b strings.Builder
	pending := false
	for _, r := range n {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			pending = b.Len() > 0
			continue
		}
		if pending {
			b.WriteRune(' ')
			pending = false
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ParseNames reads the "names" query parameter (comma-separated glossary
// entries), flattens each to one line (see flattenName), drops the ones that
// come out empty, and caps the result at 30 entries of at most 40 runes each.
func ParseNames(q string) []string {
	var out []string
	for _, n := range strings.Split(q, ",") {
		n = flattenName(n)
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
// liveSourceCacheTTL is an idle timeout, not a session budget: liveFor
// Touches the entry on every poll, so the clock only runs once nobody is
// asking for the key any more. A live job can run for the length of a whole
// movie, and dropping the cached LiveSource mid-playback would restart its
// accumulated document (and re-fetch every segment) on the next poll
// instead of reusing what the background job already built — so the value
// bounds how long an abandoned session's document stays resident, and does
// not have to exceed the longest film.
//
// sourceCacheCapacity and liveCacheCapacity are counted against the pod's
// 512Mi limit, not against how many tracks one would like to keep warm: a
// parsed 1 MiB source is ~11 MB in memory, so 16 of them is ~180 MB, and a
// live entry holds a LiveDoc that may grow to --max-cues. 64 (the previous
// value, for both) put the ceiling at ~700 MB — a limit that cannot be
// reached without the pod being killed is not a limit. Live entries are also
// bounded by --live-max-jobs (16) on the replica that owns the jobs; the
// cache holds one per key this replica serves, job or no job.
const (
	sourceCacheTTL      = 10 * time.Minute
	liveSourceCacheTTL  = 30 * time.Minute
	sourceCacheCapacity = 16
	liveCacheCapacity   = 16
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

	// LiveCacheTTL overrides liveSourceCacheTTL (tests use a short one).
	LiveCacheTTL time.Duration

	liveOnce sync.Once
	lives    *lazymap.LazyMap[*liveEntry]
}

// docCache holds the parsed source per artifact key. Failures are not
// stored (lazymap drops a failed entry), so a source that recovers is
// picked up on the next poll instead of being remembered as broken.
func (h *Handler) docCache() *lazymap.LazyMap[*Doc] {
	h.once.Do(func() {
		// Capacity bounds resident memory: a parsed 1 MiB source is ~11 MB,
		// so this is the ceiling on distinct tracks kept warm per replica
		// (see the constant for the arithmetic against the pod's limit).
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

// sourceIdentity is what makes two live source URLs the same transcoder
// session: scheme, host and path, with the query deliberately left out.
//
// The query is not stable for the length of a session. The player reloads
// the <track> as ?…&rev=<done> every ~15 s while cues are arriving, and THP
// copies the raw query into X-Source-Url; the session token can also be
// renewed mid-session. Comparing full URLs therefore read every reload as a
// new session — retiring the source, killing the job that was growing it,
// re-downloading every segment so far, and writing a Live=false the player
// takes to mean the track is finished. The session id lives in the path
// (…~hls/session/<id>/s<N>.m3u8), so a real session change is still caught.
//
// A URL that does not parse is its own identity: it will fail the scheme
// guard before anything is fetched, and two of them are only equal to each
// other.
func sourceIdentity(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Scheme + "://" + u.Host + u.Path
}

// liveEntry is what one artifact key holds: the source being followed now,
// and the identities this key has already given up on.
//
// The key deliberately survives across transcoder sessions (KeyPath strips
// the session id), so two viewers of the same file and language share it
// while holding different session URLs — and a poll carrying a URL this key
// has retired is a straggler (a reload, a second tab, an in-flight request,
// another viewer), not news about a new session.
type liveEntry struct {
	mu  sync.Mutex
	cur *LiveSource
	// retired is keyed by sourceIdentity, like every other comparison here.
	retired map[string]bool
}

// livesCache is docCache's counterpart for live sources.
func (h *Handler) livesCache() *lazymap.LazyMap[*liveEntry] {
	h.liveOnce.Do(func() {
		ttl := h.LiveCacheTTL
		if ttl <= 0 {
			ttl = liveSourceCacheTTL
		}
		h.lives = lazymap.New[*liveEntry](&lazymap.Config{Expire: ttl, Capacity: liveCacheCapacity})
	})
	return h.lives
}

// liveFor returns the LiveSource this key is being served from, creating
// one lazily. NewLiveSource makes no network call, so a HEAD before any GET
// populates the cache without fetching anything.
//
// A URL that does not match the current source is either a new transcoder
// session (the old one's playlist now 404s, and a job still polling it has
// to be told) or a straggler from a session this key already retired. The
// two are told apart by the retired set: a retired URL is served the
// current source unchanged, which for a second viewer of the same file is
// the right content — same film, same cues, same movie time — not a
// fallback. Resurrecting it instead would retire the live session's source
// on every poll, killing its job and re-downloading every segment so far.
func (h *Handler) liveFor(key, sourceURL string) *LiveSource {
	cache := h.livesCache()
	e, _ := cache.Get(key, func() (*liveEntry, error) {
		// A miss is not always a key this replica has not seen: the cache is
		// bounded (capacity) and expiring (TTL), and an eviction says nothing
		// about the job that is still polling the source the entry held.
		// Building a fresh LiveSource here would put two pollers on one
		// transcoder session and re-download every segment of the film so
		// far, so a running job's source is what the key is still on. The
		// switch below decides whether it is the session being asked for.
		cur := h.Runner.LiveSource(key)
		if cur == nil {
			cur = NewLiveSource(sourceURL, h.Client, h.MaxSourceBytes, h.MaxCues)
		}
		return &liveEntry{cur: cur, retired: map[string]bool{}}, nil
	})
	// lazymap arms the expiry timer once, when the entry is created; Get
	// does not reset it. Touch does, and that is the difference between a
	// TTL that bounds a session and one that bounds idleness.
	cache.Touch(key)
	id := sourceIdentity(sourceURL)
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case sourceIdentity(e.cur.URL()) == id:
		// The same session, possibly with a fresher query: a ?rev= bump, or
		// a renewed token. Nothing is retired and nothing is rebuilt — only
		// the URL the source fetches from is updated, because the newest
		// query is the one upstream will still accept.
		e.cur.SetURL(sourceURL)
	case e.retired[id] && !e.cur.Gone():
		// A straggler for a session this key gave up on, while the session
		// it moved to is still alive: serve the current source. Gone is the
		// transcoder's verdict on the playlist, not our own bookkeeping —
		// a source we merely retired never counts as evidence here, or the
		// first straggler after a swap would swap straight back.
	default:
		// Retire before replacing: a job may still be mid-poll against the
		// old source (Ensure is a no-op while a job for key is already
		// running, so this request's own Ensure will not replace it), and
		// without this it would keep polling a dead URL for up to one more
		// PollInterval — plus any in-flight batch — before noticing.
		e.cur.Retire()
		e.retired[sourceIdentity(e.cur.URL())] = true
		e.cur = NewLiveSource(sourceURL, h.Client, h.MaxSourceBytes, h.MaxCues)
	}
	return e.cur
}

// liveHintMinAge bounds the re-reads a `sof` hint can force: at most one
// playlist read per key per replica per this interval, however many polls
// carry a hint that disagrees with the source.
const liveHintMinAge = 500 * time.Millisecond

// parseSessionOffsetHint reads the `sof` query parameter: decimal seconds,
// finite and not negative. Anything else is no hint.
func parseSessionOffsetHint(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > 1e7 {
		return 0, false
	}
	return time.Duration(f * float64(time.Second)), true
}

// liveRefresh refreshes the source this request is serving and returns the
// source the rest of the request must use — which is not always the one it
// came in with.
//
// ErrSourceGone on a source that is no longer this key's current one is not
// news about the track: either another poll retired it between liveFor
// returning and this refresh (every session swap can catch a concurrent poll,
// and one viewer seeking is enough to cause one), or this is a straggler
// being served a current source that has just 404'd. Both are answered by
// asking liveFor again — it either hands back the session the key moved to,
// or, if that one is the source that just went gone, installs this request's
// own URL — and refreshing what it hands back. Exactly once: a client is
// entitled to treat a 404 as final, so only a gone source that IS the current
// one may become one.
func (h *Handler) liveRefresh(ctx context.Context, key, sourceURL string, src *LiveSource, maxAge time.Duration) (*LiveSource, error) {
	_, err := src.RefreshIfStale(ctx, maxAge)
	if err == nil || !errors.Is(err, ErrSourceGone) {
		return src, err
	}
	next := h.liveFor(key, sourceURL)
	if next == src {
		// The gone source is the one this key is on: a real answer.
		return src, err
	}
	_, nerr := next.RefreshIfStale(ctx, maxAge)
	return next, nerr
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
		writeVTT(w, r, body, 100, 100, true, vttMeta{})
		return
	}
	if u, perr := url.Parse(sourceURL); perr == nil && isPlaylistSource(u) {
		// Same scheme guard as fetchDoc, and for the same reason: the
		// source URL arrives in a header, and file:// and friends would be
		// read by the transport as local or intranet resources. Before the
		// cache, so a rejected URL never becomes the source a key follows.
		if u.Scheme != "http" && u.Scheme != "https" {
			logger.WithField("scheme", u.Scheme).Warn("unsupported live source scheme")
			http.Error(w, msgBadRequest, http.StatusBadRequest)
			return
		}
		// Touch keeps a live job's idle timer from expiring: polling the
		// playlist (GET or HEAD) is what keeps the transcoder session alive.
		// Only the live path leaves a mark — the offline path has no idle
		// job to keep, and its marks were never dropped: HEAD never reaches
		// Ensure, so nothing ran the forget that pairs with it, and the map
		// grew one permanent 64-char entry per key polled.
		h.Runner.Touch(key)
		src := h.liveFor(key, sourceURL)
		// Every poll, GET and HEAD alike, refreshes a source nobody has
		// read for a poll interval. The background job only runs on the
		// replica that won the store lock; on every other one this is the
		// only thing that advances the document being served, and on the
		// owning one it is a no-op because the loop has just refreshed the
		// same object. It also subsumes the old "prime while the document
		// is empty" read: the first response still carries whatever cues
		// exist.
		//
		// The source is cached and shared with whoever polls this key next,
		// so — like docFor's fetch — this read must not die with this
		// request's own connection.
		//
		// A poll that says which run it is watching (`sof`, the player's own
		// session offset, sent after a seek) and names a different one than
		// the source last read is not served the stale document for a poll
		// interval: the playlist is re-read as soon as liveHintMinAge allows.
		// Measured on a real session: the transcoder lists the new run
		// ~200 ms after the seek POST, the player's first poll after a seek
		// lands ~100 ms after it, so the poll-interval gate answered the
		// question "is the new position translated" about the old run.
		maxAge := h.Runner.LivePollInterval()
		if want, ok := parseSessionOffsetHint(r.URL.Query().Get("sof")); ok && absDuration(src.CurrentOffset()-want) >= time.Second {
			maxAge = liveHintMinAge
		}
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sourceFetchTimeout)
		src, rerr := h.liveRefresh(rctx, key, sourceURL, src, maxAge)
		cancel()
		if rerr != nil {
			switch {
			case errors.Is(rerr, ErrSourceGone):
				logger.WithError(rerr).Warn("live source unavailable")
				http.Error(w, msgSourceUnavail, http.StatusNotFound)
				return
			case errors.Is(rerr, ErrSourceTooLarge):
				logger.WithError(rerr).Warn("live source outgrew its caps")
				http.Error(w, msgSourceTooLarge, http.StatusRequestEntityTooLarge)
				return
			default:
				// Transient: a timeout on a cold catch-up, a 502 from the
				// proxy, a half-written playlist. Not an answer about the
				// track, so it must not be reported as one — the job retries
				// on its own tick and the client keeps polling what is known.
				logger.WithError(rerr).Warn("failed to refresh the live source, serving what is known")
			}
		}
		if r.Method == http.MethodHead {
			// HEAD never starts the job (same contract as the regular
			// source below): it reports what is known without triggering
			// or waiting on a translation. It also never renders — the body
			// would be built and dropped on every poll of every viewer.
			head, err := h.Runner.LiveProgress(ctx, key, src)
			if err != nil {
				logger.WithError(err).Error("live progress failed")
				http.Error(w, msgUpstreamUnavail, http.StatusBadGateway)
				return
			}
			writeVTT(w, r, nil, head.Done, head.Total, false, vttMeta{
				live:             head.Live,
				status:           head.Status,
				hasPending:       head.HasPending,
				pendingFrom:      head.PendingFrom,
				hasSessionOffset: head.HasSessionOffset,
				sessionOffset:    head.SessionOffset,
			})
			return
		}
		h.Runner.Ensure(ctx, key, &Job{Lang: lang, SourceLang: ParseSourceLang(r.URL.Query().Get("srclang")), Glossary: ParseNames(r.URL.Query().Get("names")), Live: src})
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
			// A live record is never truncated when the document shrinks
			// (see syncLive), so len(p.Lines) can exceed p.Total; clamp so
			// done never reads as "more than total" (which the player takes
			// to mean the track is finished).
			if done > total {
				done = total
			}
		}
		writeVTT(w, r, nil, done, total, false, vttMeta{})
		return
	}
	doc, cerr := h.docFor(ctx, key, sourceURL)
	if cerr != nil {
		logger.WithError(cerr.err).Warn("source unavailable")
		http.Error(w, cerr.msg, cerr.status)
		return
	}
	h.Runner.Ensure(ctx, key, &Job{Lang: lang, SourceLang: ParseSourceLang(r.URL.Query().Get("srclang")), Glossary: ParseNames(r.URL.Query().Get("names")), Doc: doc})
	snap, err := h.Runner.Snapshot(ctx, key, doc)
	if err != nil {
		logger.WithError(err).Error("snapshot failed")
		http.Error(w, msgUpstreamUnavail, http.StatusBadGateway)
		return
	}
	writeVTT(w, r, snap.Body, snap.Done, snap.Total, snap.Final, vttMeta{})
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

// vttMeta is the live-only metadata writeVTT may add on top of the
// done/total/final contract every response carries. The zero value adds
// none of it, which is what every non-live call site passes: a non-live
// response keeps the exact header set it had before live sources existed,
// since it is polled the same way whether or not this build knows about
// live sources at all.
type vttMeta struct {
	// live sets X-Subtitle-Live.
	live bool
	// status is independent of live: a live source's job can conclude
	// (Live goes false) while the response is still, and only ever, about
	// that live source — "done" or "stopped" — so it sets X-Subtitle-Status
	// whenever non-empty, not only while live is true. An offline/file-source
	// call site simply never has a status to pass, so it stays empty there.
	status string
	// hasPending sets X-Subtitle-Pending-From to pendingFrom, formatted as
	// decimal seconds to 3 places. false leaves the header off: no pending
	// cue in/after the current run window, a final artifact, or a file
	// source — see pendingFrom's doc comment for what "pending" means.
	hasPending  bool
	pendingFrom time.Duration
	// hasSessionOffset sets X-Subtitle-Session-Offset to sessionOffset, the
	// run the pending-from answer describes.
	hasSessionOffset bool
	sessionOffset    time.Duration
}

// writeVTT renders a response, adding whatever meta carries to the header
// set every response gets.
func writeVTT(w http.ResponseWriter, r *http.Request, body []byte, done, total int, final bool, meta vttMeta) {
	w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	w.Header().Set("X-Subtitle-Progress", strconv.Itoa(done)+"/"+strconv.Itoa(total))
	// The player polls these headers cross-origin (the proxy adds
	// Access-Control-Allow-Origin itself and passes these through).
	expose := []string{"X-Subtitle-Progress"}
	if meta.live {
		w.Header().Set("X-Subtitle-Live", "1")
		expose = append(expose, "X-Subtitle-Live")
	}
	if meta.status != "" {
		w.Header().Set("X-Subtitle-Status", meta.status)
		expose = append(expose, "X-Subtitle-Status")
	}
	if meta.hasPending {
		w.Header().Set("X-Subtitle-Pending-From", strconv.FormatFloat(meta.pendingFrom.Seconds(), 'f', 3, 64))
		expose = append(expose, "X-Subtitle-Pending-From")
	}
	if meta.hasSessionOffset {
		w.Header().Set("X-Subtitle-Session-Offset", strconv.FormatFloat(meta.sessionOffset.Seconds(), 'f', 3, 64))
		expose = append(expose, "X-Subtitle-Session-Offset")
	}
	w.Header().Set("Access-Control-Expose-Headers", strings.Join(expose, ", "))
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
// X-Subtitle-Live is set, Status whether X-Subtitle-Status is, HasPending
// whether X-Subtitle-Pending-From is, on top of the same done/total/final
// contract a regular track's snapshot uses.
func writeVTTLive(w http.ResponseWriter, r *http.Request, body []byte, snap *Snapshot) {
	writeVTT(w, r, body, snap.Done, snap.Total, snap.Final, vttMeta{
		live:             snap.Live,
		status:           snap.Status,
		hasPending:       snap.HasPending,
		pendingFrom:      snap.PendingFrom,
		hasSessionOffset: snap.HasSessionOffset,
		sessionOffset:    snap.SessionOffset,
	})
}
