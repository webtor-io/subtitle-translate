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
}

// LiveConfig tunes the live loop: how often the playlist is re-read, how
// long a lone pending cue waits for company before it is translated alone,
// and how long the job keeps going after the last request for its key.
type LiveConfig struct{ PollInterval, BatchWait, Idle time.Duration }

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
	sem     chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool
	running map[string]chan struct{}

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
		lastSeen: map[string]time.Time{}}
	r.SetLive(LiveConfig{})
	return r
}

// SetLive replaces the live loop's timings. A zero field keeps the default.
func (r *Runner) SetLive(cfg LiveConfig) {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 4 * time.Second
	}
	if cfg.BatchWait <= 0 {
		cfg.BatchWait = 10 * time.Second
	}
	if cfg.Idle <= 0 {
		cfg.Idle = 90 * time.Second
	}
	r.live = cfg
}

// Touch records that someone asked for key just now. The live loop stops
// when nobody has for r.live.Idle: polling the playlist keeps the
// transcoder session (and its FFmpeg) alive, and a translation nobody is
// watching would otherwise transcode the whole file for no one.
func (r *Runner) Touch(key string) {
	r.seenMu.Lock()
	if r.lastSeen == nil {
		r.lastSeen = map[string]time.Time{}
	}
	r.lastSeen[key] = time.Now()
	r.seenMu.Unlock()
}

// sinceSeen is how long ago key was last asked for. A key nobody ever
// touched reads as "just now": the idle window is a reason to stop a job
// whose viewer left, not a reason to refuse one nobody registered.
func (r *Runner) sinceSeen(key string) time.Duration {
	r.seenMu.Lock()
	defer r.seenMu.Unlock()
	t, ok := r.lastSeen[key]
	if !ok {
		return 0
	}
	return time.Since(t)
}

func (r *Runner) forgetSeen(key string) {
	r.seenMu.Lock()
	delete(r.lastSeen, key)
	r.seenMu.Unlock()
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

// LiveSnapshot renders what is known for a live key: every cue the source
// has fetched so far, with the translations stored for it. Nothing is
// started here, and unlike Snapshot the counts describe the document as it
// stands, which is why Live tells the reader whether to expect more.
func (r *Runner) LiveSnapshot(ctx context.Context, key string, src *LiveSource) (*Snapshot, error) {
	if b, ok, err := r.store.GetFinal(ctx, key); err != nil {
		return nil, err
	} else if ok {
		return &Snapshot{Body: b, Done: 100, Total: 100, Final: true}, nil
	}
	p, err := r.store.GetProgress(ctx, key)
	if err != nil {
		return nil, err
	}
	var lines []string
	if p != nil {
		lines = p.Lines
	}
	doc := src.Doc().Snapshot()
	body, err := doc.RenderByIndex(lines)
	if err != nil {
		return nil, err
	}
	// A source that ended while the job is still translating is not done:
	// the job says so by keeping Live set until its last write. When it
	// stopped without a final (a seek, a gone source), Live is cleared and
	// done == total, which the client reads as "this is all there is".
	return &Snapshot{
		Body:  body,
		Done:  countDoneByIndex(lines, doc),
		Total: len(doc.Cues),
		Live:  !src.Ended() || (p != nil && p.Live),
	}, nil
}

// countDoneByIndex counts translated cues in a live document. Unlike
// countDone there is no prefix rule: cues arrive in playlist order but are
// translated earliest-first, and a gap in the middle is normal.
func countDoneByIndex(lines []string, doc *Doc) int {
	n := 0
	for _, c := range doc.Cues {
		if len(c.Lines) == 0 || (c.Index < len(lines) && lines[c.Index] != "") {
			n++
		}
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
	done := make(chan struct{})
	r.running[key] = done
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
			r.mu.Unlock()
			// The job is gone, so its last-seen mark is dead weight: without
			// this the map grows by one entry per translated track forever.
			r.forgetSeen(key)
			close(done)
		}()
		// The key is registered before the slot is taken, so a burst of
		// requests for the same track still collapses into one job; what
		// waits here is the work, not the deduplication.
		select {
		case r.sem <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		defer func() { <-r.sem }()
		JobsRunning.Inc()
		defer JobsRunning.Dec()
		// The background job outlives the request that triggered it, so it
		// runs on the runner's own context rather than the request's.
		r.run(r.ctx, key, job)
	}()
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
	if err := r.store.PutFinal(ctx, key, body); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to store final")
		return
	}
	_ = r.store.DropProgress(ctx, key)
	JobDuration.Observe(time.Since(start).Seconds())
	logger.WithField("seconds", time.Since(start).Seconds()).Info("translation finished")
}

// runLive translates a playlist that is still being written. It holds the
// same lock as run and lives until the playlist ends, the source goes away
// or nobody is watching any more; the document it translates grows under
// it, so every tick re-reads it rather than trusting the previous pass.
func (r *Runner) runLive(ctx context.Context, key, token string, logger *log.Entry, job *Job, start time.Time) {
	p, err := r.store.GetProgress(ctx, key)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to load progress")
		return
	}
	if p == nil {
		p = &Progress{}
	}
	// What an earlier run of this key already paid for. Cue positions are not
	// stable across runs (a seek restarts the document from a new offset),
	// cue identity is, so the carry-over is keyed by CueKey.
	known := map[string]string{}
	for i, k := range p.CueKeys {
		if k == "" || i >= len(p.Lines) || p.Lines[i] == "" {
			continue
		}
		known[k] = p.Lines[i]
	}
	p.Live = true
	targetName, _ := LangName(job.Lang)
	ticker := time.NewTicker(r.live.PollInterval)
	defer ticker.Stop()
	var firstPendingAt time.Time
	// published is the cue count the store has heard about, so a tick that
	// changed nothing does not rewrite the same record.
	published := -1
	lockedAt := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ref, err := job.Live.Refresh(ctx)
		switch {
		case err == nil:
		case errors.Is(err, ErrSourceGone):
			logger.Info("source gone, stopping")
			JobErrors.WithLabelValues("source_gone").Inc()
			r.stopLive(ctx, key, logger, p)
			return
		case errors.Is(err, ErrSourceTooLarge):
			logger.Warn("source outgrew its caps, stopping")
			JobErrors.WithLabelValues("too_large").Inc()
			r.stopLive(ctx, key, logger, p)
			return
		default:
			if ctx.Err() != nil {
				return
			}
			// Transient: a timeout, a 5xx, a half-written playlist. The next
			// tick reads the whole playlist again, so nothing is lost.
			logger.WithError(err).Warn("failed to refresh the live source")
			continue
		}
		// One snapshot per tick: the cue keys and the cues have to describe
		// the same document, and the handler refreshes the source too.
		doc := job.Live.Doc().Snapshot()
		syncLive(p, doc, known)
		idx, texts := pendingByTime(doc, p.Lines)
		pending := len(idx)
		now := time.Now()
		switch {
		case pending == 0:
			firstPendingAt = time.Time{}
		case firstPendingAt.IsZero():
			firstPendingAt = now
		}
		// A full batch is translated at once; a partial one waits for company
		// until BatchWait, which bounds how long the viewer stares at a gap.
		// An ended playlist has no company coming, so it never waits.
		batched := false
		if pending > 0 && (pending >= r.batchSize || now.Sub(firstPendingAt) >= r.live.BatchWait || ref.Ended) {
			if r.batchSize > 0 && pending > r.batchSize {
				idx, texts = idx[:r.batchSize], texts[:r.batchSize]
			}
			if !r.runBatch(ctx, key, token, logger, job, targetName, p, idx, texts) {
				return
			}
			firstPendingAt = time.Time{}
			batched = true
			published = p.Total
			lockedAt = time.Now()
		}
		if !batched {
			// New cues have to reach the store even when nothing was
			// translated: a reader polling HEAD learns the document grew.
			if p.Total != published {
				if err := r.store.PutProgress(ctx, key, p); err != nil {
					JobErrors.WithLabelValues("store").Inc()
					logger.WithError(err).Error("failed to store progress")
					return
				}
				published = p.Total
			}
			// Ticks are seconds and the lease is minutes, so the lock is
			// refreshed on its own schedule instead of once per tick.
			if !r.holdLock(ctx, key, token, logger, &lockedAt) {
				return
			}
		}
		if ref.Ended && pending == 0 {
			r.finishLive(ctx, key, logger, job, p, doc, start)
			return
		}
		if r.sinceSeen(key) > r.live.Idle {
			logger.Info("viewer gone, stopping")
			JobErrors.WithLabelValues("viewer_gone").Inc()
			// Live stays set: the source has not ended, the translation is
			// only paused until someone asks for this track again.
			r.putProgress(ctx, key, logger, p)
			return
		}
	}
}

// holdLock refreshes the lease once per lockTTL/3 and reports whether this
// job may keep going.
func (r *Runner) holdLock(ctx context.Context, key, token string, logger *log.Entry, lockedAt *time.Time) bool {
	if time.Since(*lockedAt) < r.lockTTL/3 {
		return true
	}
	ok, err := r.store.RefreshLock(ctx, key, token, r.lockTTL)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to refresh lock")
		return false
	}
	if !ok {
		JobErrors.WithLabelValues("lock_lost").Inc()
		logger.Warn("lock lost, another worker owns this key")
		return false
	}
	*lockedAt = time.Now()
	return true
}

// syncLive lines p up with the document as it stands. Lines and CueKeys are
// indexed by Cue.Index (the append order, which is also what RenderByIndex
// expects); a cue whose key this key already paid for starts out translated,
// and a slot whose key changed under us (a seek rebuilt the document) is
// rewritten rather than left pointing at another cue's text.
func syncLive(p *Progress, doc *Doc, known map[string]string) {
	n := len(doc.Cues)
	keys := make([]string, n)
	for _, c := range doc.Cues {
		if c.Index >= 0 && c.Index < n {
			keys[c.Index] = CueKey(c.Start, c.End, c.Lines)
		}
	}
	if len(p.Lines) > n {
		p.Lines = p.Lines[:n]
	}
	if len(p.CueKeys) > n {
		p.CueKeys = p.CueKeys[:n]
	}
	for len(p.Lines) < n {
		p.Lines = append(p.Lines, "")
	}
	for len(p.CueKeys) < n {
		p.CueKeys = append(p.CueKeys, "")
	}
	for i, k := range keys {
		if p.CueKeys[i] == k {
			continue
		}
		p.CueKeys[i] = k
		p.Lines[i] = known[k]
	}
	p.Total = n
}

// pendingByTime returns the cues still needing a translation, earliest
// first: the viewer is watching the front of the document, so the cue that
// plays next is the one worth spending a batch on. Indexes are Cue.Index,
// texts are the joined source lines.
func pendingByTime(doc *Doc, lines []string) ([]int, []string) {
	var idx []int
	var texts []string
	for _, c := range doc.Cues {
		if len(c.Lines) == 0 || c.Index < 0 || c.Index >= len(lines) || lines[c.Index] != "" {
			continue
		}
		idx = append(idx, c.Index)
		texts = append(texts, JoinLines(c))
	}
	return idx, texts
}

// finishLive is the end of a playlist that said ENDLIST with everything
// translated. A final artifact is served forever and without a source, so
// only a run that saw the whole movie may write one.
func (r *Runner) finishLive(ctx context.Context, key string, logger *log.Entry, job *Job, p *Progress, doc *Doc, start time.Time) {
	if !job.Live.Contiguous() {
		// This run joined after a seek, so the document has holes no later
		// reader could detect. The partial stays, the artifact is not written.
		logger.Info("live source ended on a run that skipped ahead, keeping the partial")
		r.stopLive(ctx, key, logger, p)
		return
	}
	body, err := doc.RenderByIndex(p.Lines)
	if err != nil {
		JobErrors.WithLabelValues("render").Inc()
		logger.WithError(err).Error("failed to render final")
		r.stopLive(ctx, key, logger, p)
		return
	}
	if err := r.store.PutFinal(ctx, key, body); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to store final")
		return
	}
	_ = r.store.DropProgress(ctx, key)
	JobDuration.Observe(time.Since(start).Seconds())
	logger.WithField("seconds", time.Since(start).Seconds()).Info("live translation finished")
}

// stopLive is the last write of a live job that will not produce a final
// artifact. Clearing Live is what tells a reader that what is stored is all
// there will be; the partial itself stays for its TTL, so a viewer who
// comes back does not start from zero.
func (r *Runner) stopLive(ctx context.Context, key string, logger *log.Entry, p *Progress) {
	p.Live = false
	r.putProgress(ctx, key, logger, p)
}

// putProgress stores p on a context of its own: the job may be exiting
// because ctx was canceled (shutdown), and the work done so far still has
// to land.
func (r *Runner) putProgress(ctx context.Context, key string, logger *log.Entry, p *Progress) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.store.PutProgress(sctx, key, p); err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to store progress")
	}
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
