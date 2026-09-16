package services

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// LiveConfig tunes the live loop: how often the playlist is re-read, how
// long a lone pending cue waits for company before it is translated alone,
// and how long the job keeps going after the last request for its key.
type LiveConfig struct{ PollInterval, BatchWait, Idle time.Duration }

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
//
// Lock order: seenMu is the innermost lock. Ensure holds r.mu while
// seeding and clearing the mark, so r.mu may be held over seenMu but never
// the other way round.
func (r *Runner) Touch(key string) {
	r.seenMu.Lock()
	if r.lastSeen == nil {
		r.lastSeen = map[string]time.Time{}
	}
	r.lastSeen[key] = time.Now()
	r.seenMu.Unlock()
}

// sinceSeen is how long ago key was last asked for. Ensure seeds the mark
// when it registers the job, so a running job always has one; an unknown
// key can only be a job that is no longer running, and reads as "just now"
// rather than as "idle forever".
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

// LiveSnapshot renders what is known for a live key: every cue the source
// has fetched so far, carrying the translations stored for it. Nothing is
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
	// The stored indexes belong to whatever document the job that wrote them
	// saw, which may be another run of this key (a seek renumbers cues) or
	// another replica; only the cue key carries across, so the lines are
	// matched to this document by key rather than trusted positionally.
	doc := src.Doc().Snapshot()
	lines := alignLines(p, doc)
	body, err := doc.RenderByIndex(lines)
	if err != nil {
		return nil, err
	}
	// Live is what the job says, not what the playlist says: a job that
	// stopped early (the source went away, or outgrew its caps) clears it
	// while the playlist is still, formally, unfinished. No record yet means
	// the job has not written its first tick, which is as live as it gets.
	return &Snapshot{
		Body:  body,
		Done:  countDoneByIndex(lines, doc),
		Total: len(doc.Cues),
		Live:  p == nil || p.Live,
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

// knownLines is every translation a progress record carries, keyed by the
// identity of the cue it belongs to.
func knownLines(p *Progress) map[string]string {
	known := map[string]string{}
	if p == nil {
		return known
	}
	for i, k := range p.CueKeys {
		if k == "" || i >= len(p.Lines) || p.Lines[i] == "" {
			continue
		}
		known[k] = p.Lines[i]
	}
	return known
}

// alignLines projects the translations p carries onto doc's own cue
// indexes, which is what RenderByIndex and Progress.Lines are indexed by.
// A cue whose key p does not know comes back empty: the alternative is
// showing one cue's text under another, which is what positional reuse
// does the moment a seek renumbers the document. Pure: no reader of it
// needs to have run the job.
func alignLines(p *Progress, doc *Doc) []string {
	out := make([]string, len(doc.Cues))
	known := knownLines(p)
	if len(known) == 0 {
		return out
	}
	for _, c := range doc.Cues {
		if c.Index < 0 || c.Index >= len(out) {
			continue
		}
		out[c.Index] = known[CueKey(c.Start, c.End, c.Lines)]
	}
	return out
}

// syncLive lines p up with the document as it stands: every cue currently
// in the document gets its key and its (re-aligned) translation at its own
// index. Entries past the document are left alone rather than trimmed —
// they were paid for, readers are index-driven and bounds-checked, and a
// document that shrank may well grow back.
func syncLive(p *Progress, doc *Doc) {
	n := len(doc.Cues)
	aligned := alignLines(p, doc)
	for len(p.Lines) < n {
		p.Lines = append(p.Lines, "")
	}
	for len(p.CueKeys) < n {
		p.CueKeys = append(p.CueKeys, "")
	}
	for _, c := range doc.Cues {
		if c.Index < 0 || c.Index >= n {
			continue
		}
		p.CueKeys[c.Index] = CueKey(c.Start, c.End, c.Lines)
		p.Lines[c.Index] = aligned[c.Index]
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

// runLive translates a playlist that is still being written. It holds the
// same lock as run and lives until the playlist ends, the source goes away
// or nobody is watching any more; the document it translates grows under
// it, so every tick re-reads it rather than trusting the previous pass.
func (r *Runner) runLive(ctx context.Context, key, token string, logger *log.Entry, job *Job, start time.Time) {
	logger = logger.WithField("live", true)
	p, err := r.store.GetProgress(ctx, key)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to load progress")
		return
	}
	if p == nil {
		p = &Progress{}
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
		ref, err := r.pollLive(ctx, job)
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
		syncLive(p, doc)
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
			// A failed batch (upstream or store) leaves the record with
			// Live still set. That is the honest state: this job stopped,
			// but the playlist did not end, and the next request for the key
			// starts a job that picks the record up where it is.
			if !r.runBatch(ctx, key, token, logger, job, targetName, p, idx, texts) {
				return
			}
			if len(idx) == pending {
				firstPendingAt = time.Time{}
			}
			// Cues left over from a size-capped batch keep the clock they
			// have been waiting on: they are no younger than the ones just
			// translated, so restarting BatchWait for them would make the
			// tail of a burst wait longest.
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

// pollLive reads the playlist once, under a deadline of its own. Refresh
// deliberately holds the source's refresh lock across every segment fetch
// (a second pass would re-fetch the same new segments), so a transcoder
// that accepts the connection and then stalls would otherwise wedge this
// job's concurrency slot and any handler request waiting on the same
// source — for as long as the transcoder cares to hold the socket.
func (r *Runner) pollLive(ctx context.Context, job *Job) (Refresh, error) {
	rctx, cancel := context.WithTimeout(ctx, sourceFetchTimeout)
	defer cancel()
	return job.Live.Refresh(rctx)
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
	r.writeFinal(ctx, key, logger, body, start)
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
