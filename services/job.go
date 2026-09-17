package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func ArtifactKey(infoHash, path, lang, model, promptVersion string) string {
	sum := sha256.Sum256([]byte(infoHash + "\x00" + path + "\x00" + lang + "\x00" + model + "\x00" + promptVersion))
	return hex.EncodeToString(sum[:])
}

type Job struct {
	Lang       string
	SourceLang string
	Glossary   []string
	// Doc is the whole source document, known up front. Exactly one of Doc
	// and Live is set: a live job has no document to start from, it grows
	// one as the transcoder writes segments.
	Doc  *Doc
	Live *LiveSource
}

type Snapshot struct {
	Body  []byte
	Done  int
	Total int
	Final bool
	// Live says more cues are still coming, so Done/Total describe the
	// document so far rather than the whole track. Only live sources set it.
	Live bool
	// Status carries Progress.Status through to the response for a live
	// source: "done", "stopped", or "" (running, idle-paused, or an
	// offline snapshot, which never sets this). Empty means the header
	// stays off; only writeVTT decides that.
	Status string
	// PendingFrom is the Start of the earliest cue still untranslated among
	// those ending at or after the live source's current offset (see
	// pendingFrom in live_job.go) — the movie-time position the viewer will
	// next hit a translation gap. HasPending is false when there is none: no
	// such cue exists, this is a final artifact, or this is not a live
	// snapshot at all. Only LiveSnapshot and LiveProgress set these.
	PendingFrom time.Duration
	HasPending  bool
	// SessionOffset is the #EXT-X-SESSION-OFFSET the pending-from answer was
	// computed against, sent as X-Subtitle-Session-Offset. A client that has
	// just seeked compares it with its own session offset to tell an answer
	// about its run from one read before the transcoder listed that run.
	// HasSessionOffset is false until the source has read a playlist, and
	// on every non-live snapshot.
	SessionOffset    time.Duration
	HasSessionOffset bool
}

type Runner struct {
	store     Store
	tr        Translator
	batchSize int
	lockTTL   time.Duration
	// ctx is the lifetime of every background job: Close cancels it, and
	// jobs drop out at the next context check instead of outliving the
	// process's other components.
	ctx    context.Context
	cancel context.CancelFunc
	// sem bounds how many jobs translate at once: each job holds an upstream
	// connection and a whole parsed document for its lifetime.
	sem chan struct{}
	// liveSem is sem's counterpart for live jobs, sized by LiveConfig
	// (see SetLive): the two kinds of job hold a slot for wildly different
	// lengths of time, and one film must not queue every later track.
	liveSem chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool
	running map[string]chan struct{}
	// liveSrc is the source the running live job for a key is following.
	// The handler's cache is bounded and expiring, and an eviction says
	// nothing about the job: without this, the next poll of an evicted key
	// would build a second LiveSource for a session that already has one.
	liveSrc map[string]*LiveSource

	live     LiveConfig
	seenMu   sync.Mutex
	lastSeen map[string]time.Time
}

func NewRunner(store Store, tr Translator, batchSize, maxJobs int, lockTTL time.Duration) *Runner {
	if maxJobs < 1 {
		maxJobs = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runner{store: store, tr: tr, batchSize: batchSize, lockTTL: lockTTL,
		ctx: ctx, cancel: cancel, sem: make(chan struct{}, maxJobs), running: map[string]chan struct{}{},
		liveSrc: map[string]*LiveSource{}, lastSeen: map[string]time.Time{}}
	r.SetLive(LiveConfig{})
	return r
}

// Close stops accepting new jobs, cancels the running ones and waits for
// them to exit. Each job still releases its lock on the way out: the
// deferred unlock runs on a fresh context, not on the canceled one.
func (r *Runner) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	r.cancel()
	r.wg.Wait()
}

// Snapshot renders what is known for key without starting anything.
func (r *Runner) Snapshot(ctx context.Context, key string, doc *Doc) (*Snapshot, error) {
	// A finished artifact is served without re-reading the source, so the
	// cue count is unknown here: progress is reported as 100/100 (the
	// player treats done == total as complete).
	if b, ok, err := r.store.GetFinal(ctx, key); err != nil {
		return nil, err
	} else if ok {
		return &Snapshot{Body: b, Done: 100, Total: 100, Final: true}, nil
	}
	p, err := r.store.GetProgress(ctx, key)
	if err != nil {
		return nil, err
	}
	total := len(doc.Cues)
	var lines []string
	if p != nil {
		lines = p.Lines
	}
	done := countDone(lines, doc)
	body, err := doc.Render(lines, done)
	if err != nil {
		return nil, err
	}
	return &Snapshot{Body: body, Done: done, Total: total}, nil
}

// countDone is the length of the translated prefix: a cue counts as done
// when it has a translation or was empty after normalization.
func countDone(lines []string, doc *Doc) int {
	n := 0
	for i := range doc.Cues {
		if len(doc.Cues[i].Lines) == 0 || (i < len(lines) && lines[i] != "") {
			n++
			continue
		}
		break
	}
	return n
}

// Ensure starts the background job once per key per process. Safe to call
// concurrently: only the first caller for a given key spawns a goroutine,
// cross-replica exclusion is left to the store lock acquired inside run.
func (r *Runner) Ensure(_ context.Context, key string, job *Job) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	if _, ok := r.running[key]; ok {
		r.mu.Unlock()
		return
	}
	if job.Live != nil && job.Live.RunEnded() {
		// Nothing to do against this source and nothing that will change
		// that: the run ended and wrote what it could. A new transcoder
		// session arrives as a different LiveSource, and that one is work.
		r.mu.Unlock()
		return
	}
	done := make(chan struct{})
	r.running[key] = done
	if job.Live != nil {
		r.liveSrc[key] = job.Live
	}
	// The idle mark belongs to the running entry and is seeded here, under
	// the same lock: a live job that nobody Touched still has a window to
	// idle out of, and the mark cannot be seeded before the entry that owns
	// it exists.
	r.Touch(key)
	r.wg.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		defer func() {
			if rec := recover(); rec != nil {
				JobErrors.WithLabelValues("panic").Inc()
				log.WithFields(log.Fields{"key": key, "lang": job.Lang, "panic": rec}).Error("translation job panicked")
			}
			r.mu.Lock()
			delete(r.running, key)
			delete(r.liveSrc, key)
			// Dropped in the same critical section as the running entry:
			// otherwise a Touch+Ensure that got in right after the delete has
			// its fresh mark erased here, and the job it started would read
			// "never seen" and poll the transcoder until the process dies.
			r.forgetSeen(key)
			r.mu.Unlock()
			close(done)
		}()
		// The key is registered before the slot is taken, so a burst of
		// requests for the same track still collapses into one job; what
		// waits here is the work, not the deduplication.
		sem := r.sem
		if job.Live != nil {
			sem = r.liveSem
		}
		select {
		case sem <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		defer func() { <-sem }()
		JobsRunning.Inc()
		defer JobsRunning.Dec()
		// The background job outlives the request that triggered it, so it
		// runs on the runner's own context rather than the request's.
		r.run(r.ctx, key, job)
	}()
}

// LiveSource is the source the live job for key is following, or nil when
// no live job for it is running in this process. The handler asks when its
// own cache does not have the key: the cache is bounded and expiring, and
// an entry going away is not a reason to start following the session twice.
func (r *Runner) LiveSource(key string) *LiveSource {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.liveSrc[key]
}

// Wait blocks until the in-process goroutine for key exits. No-op when
// none is running (already finished, or never started).
func (r *Runner) Wait(key string) {
	r.mu.Lock()
	ch, ok := r.running[key]
	r.mu.Unlock()
	if ok {
		<-ch
	}
}

func (r *Runner) run(ctx context.Context, key string, job *Job) {
	start := time.Now()
	logger := log.WithFields(log.Fields{"key": key, "lang": job.Lang})
	if _, ok, err := r.store.GetFinal(ctx, key); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to check final artifact")
		return
	} else if ok {
		return
	}
	token, locked, err := r.store.TryLock(ctx, key, r.lockTTL)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to acquire lock")
		return
	}
	if !locked {
		logger.Debug("another worker holds the lock")
		return
	}
	defer func() {
		// A fresh context: the job may be exiting because ctx was canceled,
		// and the lock still has to come off.
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.store.Unlock(uctx, key, token); err != nil {
			JobErrors.WithLabelValues("store").Inc()
			logger.WithError(err).Error("failed to release lock")
		}
	}()

	// A live job has no document yet, so everything below (which reads
	// job.Doc) belongs to the other branch.
	if job.Live != nil {
		r.runLive(ctx, key, token, logger, job, start)
		return
	}

	p, err := r.store.GetProgress(ctx, key)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to load progress")
		return
	}
	if p == nil || len(p.Lines) != len(job.Doc.Cues) {
		p = &Progress{Total: len(job.Doc.Cues), Lines: make([]string, len(job.Doc.Cues))}
	}
	// Publish the cue count before the first batch: until this lands, HEAD
	// has no progress record to read and reports 0/0, which the client is
	// told to read as "unknown, keep polling" rather than "nothing to do".
	if err := r.store.PutProgress(ctx, key, p); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to register progress")
		return
	}
	targetName, _ := LangName(job.Lang)
	for _, b := range Batches(len(job.Doc.Cues), r.batchSize) {
		idx, lines := pendingInBatch(job.Doc, p.Lines, b[0], b[1])
		if len(idx) == 0 {
			continue
		}
		if !r.runBatch(ctx, key, token, logger.WithField("batch", b), job, targetName, p, idx, lines) {
			return
		}
	}
	body, err := job.Doc.Render(p.Lines, len(job.Doc.Cues))
	if err != nil {
		JobErrors.WithLabelValues("render").Inc()
		logger.WithError(err).Error("failed to render final")
		return
	}
	r.writeFinal(ctx, key, logger, body, start)
}

// writeFinal retires a finished job: the artifact replaces the progress
// record, which is no longer anyone's starting point. Only the rendering
// differs between the two job kinds, so the caller hands over the body.
func (r *Runner) writeFinal(ctx context.Context, key string, logger *log.Entry, body []byte, start time.Time) {
	if err := r.store.PutFinal(ctx, key, body); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to store final")
		return
	}
	_ = r.store.DropProgress(ctx, key)
	JobDuration.Observe(time.Since(start).Seconds())
	logger.WithField("seconds", time.Since(start).Seconds()).Info("translation finished")
}

// runBatch translates one batch and persists it, all under a deadline of
// one lock TTL: translate, store and refresh have to fit inside the lease
// this job holds, otherwise the lock can expire mid-batch and a second
// worker start on the same key. It returns false when the job must stop.
func (r *Runner) runBatch(ctx context.Context, key, token string, logger *log.Entry, job *Job, targetName string, p *Progress, idx []int, texts []string) bool {
	bctx, cancel := context.WithTimeout(ctx, r.lockTTL)
	defer cancel()
	if err := r.translateChunk(bctx, logger, job, targetName, p, idx, texts); err != nil {
		JobErrors.WithLabelValues("upstream").Inc()
		logger.WithError(err).Error("upstream failed, stopping")
		// Keep whatever was translated so a later run resumes from here,
		// even when the job context is already cancelled (shutdown).
		sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer scancel()
		_ = r.store.PutProgress(sctx, key, p)
		return false
	}
	if err := r.store.PutProgress(bctx, key, p); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to store progress")
		return false
	}
	ok, err := r.store.RefreshLock(bctx, key, token, r.lockTTL)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to refresh lock")
		return false
	}
	if !ok {
		// Someone else owns the key now. Progress stays where it is: it is
		// the new holder's starting point, not ours to drop.
		JobErrors.WithLabelValues("lock_lost").Inc()
		logger.Warn("lock lost, another worker owns this key")
		return false
	}
	return true
}

// translateChunk translates the cues at idx (whose source text is texts)
// into p.Lines. Recoverable upstream verdicts are absorbed here and the
// job continues; only an error worth stopping the whole job is returned.
//
// A truncated reply is retried as two halves rather than as the same
// prompt, down to a single cue: the reply was cut off by the output token
// limit, so the only useful change is asking for less at a time.
func (r *Runner) translateChunk(ctx context.Context, logger *log.Entry, job *Job, targetName string, p *Progress, idx []int, texts []string) error {
	res, err := r.tr.Translate(ctx, BatchRequest{
		TargetLang: job.Lang,
		TargetName: targetName,
		SourceLang: job.SourceLang,
		Glossary:   job.Glossary,
		Context:    lastTranslated(p.Lines, idx[0], 5),
		Lines:      texts,
	})
	switch {
	case err == nil:
		for i, li := range idx {
			// An empty reply line for a cue that has text would leave the
			// cue pending forever: re-sent with every later batch and, on a
			// live source, pinning X-Subtitle-Pending-From to it for the rest
			// of the run. The original text is what the viewer gets instead,
			// as for every other reply that cannot be used.
			if strings.TrimSpace(res.Lines[i]) == "" {
				p.Lines[li] = texts[i]
				continue
			}
			p.Lines[li] = res.Lines[i]
		}
		BatchesTotal.Inc()
		return nil
	case errors.Is(err, ErrLineMismatch):
		logger.WithField("cues", len(idx)).Warn("line mismatch, keeping originals")
		keepSource(p, idx, texts)
		BatchesFallback.WithLabelValues("mismatch").Inc()
		return nil
	case errors.Is(err, ErrTruncated):
		if len(idx) == 1 {
			logger.WithField("cue", idx[0]).Warn("single cue truncated, keeping the original")
			JobErrors.WithLabelValues("truncated").Inc()
			keepSource(p, idx, texts)
			BatchesFallback.WithLabelValues("truncated").Inc()
			return nil
		}
		half := len(idx) / 2
		logger.WithField("cues", len(idx)).Warn("reply truncated, splitting the batch")
		if err := r.translateChunk(ctx, logger, job, targetName, p, idx[:half], texts[:half]); err != nil {
			return err
		}
		return r.translateChunk(ctx, logger, job, targetName, p, idx[half:], texts[half:])
	case errors.Is(err, ErrRefused):
		logger.WithField("cues", len(idx)).Warn("upstream refused the batch, keeping originals")
		JobErrors.WithLabelValues("refusal").Inc()
		keepSource(p, idx, texts)
		BatchesFallback.WithLabelValues("refusal").Inc()
		return nil
	default:
		return err
	}
}

// keepSource writes the untranslated source text into the progress, so the
// cue counts as done and the viewer sees the original instead of a gap.
func keepSource(p *Progress, idx []int, texts []string) {
	for i, li := range idx {
		p.Lines[li] = texts[i]
	}
}

// pendingInBatch returns cue indexes in [from,to) that still need a
// translation: non-empty after normalization and not yet translated. It
// also returns their source text, joined per cue.
func pendingInBatch(doc *Doc, lines []string, from, to int) ([]int, []string) {
	var idx []int
	var texts []string
	for i := from; i < to; i++ {
		if len(doc.Cues[i].Lines) == 0 || lines[i] != "" {
			continue
		}
		idx = append(idx, i)
		texts = append(texts, JoinLines(doc.Cues[i]))
	}
	return idx, texts
}

// lastTranslated returns up to n non-empty entries of lines preceding
// index before, in cue order.
func lastTranslated(lines []string, before, n int) []string {
	var out []string
	for i := before - 1; i >= 0 && len(out) < n; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			out = append([]string{lines[i]}, out...)
		}
	}
	return out
}
