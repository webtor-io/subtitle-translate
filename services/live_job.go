package services

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// LiveConfig tunes the live loop: how often the playlist is re-read, how
// long a lone pending cue waits for company before it is translated alone,
// how long the job keeps going after the last request for its key, and how
// many live jobs may run at once.
type LiveConfig struct {
	PollInterval, BatchWait, Idle time.Duration
	// MaxJobs bounds live jobs separately from offline ones. A live job
	// holds its slot for the length of a film while doing almost nothing —
	// it waits on a playlist and on the upstream — so the offline bound,
	// sized for jobs that finish in seconds and hold a whole parsed
	// document, is the wrong number for it.
	MaxJobs int
	// FreshRun is how long after a transcoder run starts (the first read of
	// the playlist, or a seek moving #EXT-X-SESSION-OFFSET) a partial batch
	// holding a cue the viewer can meet is translated at once instead of
	// waiting BatchWait for company. That is the moment the viewer is
	// standing on untranslated cues: they just seeked, or just pressed
	// play. BatchWait there came on top of the poll interval and the
	// upstream call, so the first line at a new position arrived 15-20 s
	// after the seek. Zero keeps the default; negative turns it off.
	FreshRun time.Duration
}

// defaultFreshRun is one seek quantum: the stretch of film a seek lands the
// viewer in.
const defaultFreshRun = 30 * time.Second

// liveWakeMinGap is the least time between two playlist reads the job does
// because of a wake-up. A wake-up means a new run; a playlist whose offset
// flaps (two tabs of one session seeking in turn, a stale response racing a
// restart) would otherwise have the job reading back to back.
const liveWakeMinGap = time.Second

// SetLive replaces the live loop's timings. A zero field keeps the default.
// Call it before the first job starts: it rebuilds the live semaphore.
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
	if cfg.MaxJobs < 1 {
		cfg.MaxJobs = 16
	}
	if cfg.FreshRun == 0 {
		cfg.FreshRun = defaultFreshRun
	}
	r.live = cfg
	r.liveSem = make(chan struct{}, cfg.MaxJobs)
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
	status := ""
	if p != nil {
		status = p.Status
	}
	offset := src.CurrentOffset()
	from, hasPending := pendingFrom(doc, lines, offset)
	return &Snapshot{
		Body:             body,
		Done:             countDoneByIndex(lines, doc),
		Total:            len(doc.Cues),
		Live:             p == nil || p.Live,
		Status:           status,
		PendingFrom:      from,
		HasPending:       hasPending,
		SessionOffset:    offset,
		HasSessionOffset: !src.RunStartedAt().IsZero(),
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

// knownCue is one stored translation with the cue identity it was filed
// under, taken apart so it can also be matched tolerantly.
type knownCue struct {
	start, end time.Duration
	line       string
}

// knownLines is every translation a progress record carries, indexed both
// by the exact cue key it was stored under and by cue text, so a run whose
// timeline is shifted (see cueMatchTolerance) still finds it.
type knownLines struct {
	exact  map[string]string
	byText map[string][]knownCue
}

// lookup returns the translation stored for this cue: the exact key first,
// then the same text within the tolerance window. Without the second step a
// seek re-pays for every cue of the replayed range, because the transcoder
// starts each run at its own keyframe and no key matches across runs.
func (k knownLines) lookup(start, end time.Duration, lines []string) string {
	if l, ok := k.exact[CueKey(start, end, lines)]; ok {
		return l
	}
	if len(lines) == 0 {
		return ""
	}
	for _, c := range k.byText[cueText(lines)] {
		if sameCue(c.start, c.end, start, end) {
			return c.line
		}
	}
	return ""
}

func (k knownLines) empty() bool { return len(k.exact) == 0 }

// parseCueKey takes a stored key back apart into the cue it describes.
func parseCueKey(k string) (start, end time.Duration, text string, ok bool) {
	parts := strings.SplitN(k, "|", 3)
	if len(parts) != 3 {
		return 0, 0, "", false
	}
	s, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	e, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	return time.Duration(s) * time.Millisecond, time.Duration(e) * time.Millisecond, parts[2], true
}

// collectKnown indexes every translation a progress record carries.
func collectKnown(p *Progress) knownLines {
	known := knownLines{exact: map[string]string{}, byText: map[string][]knownCue{}}
	if p == nil {
		return known
	}
	for i, k := range p.CueKeys {
		if k == "" || i >= len(p.Lines) || p.Lines[i] == "" {
			continue
		}
		known.exact[k] = p.Lines[i]
		if start, end, text, ok := parseCueKey(k); ok && text != "" {
			known.byText[text] = append(known.byText[text], knownCue{start: start, end: end, line: p.Lines[i]})
		}
	}
	return known
}

// alignLines projects the translations p carries onto doc's own cue
// indexes, which is what RenderByIndex and Progress.Lines are indexed by.
// A cue p does not know comes back empty: the alternative is showing one
// cue's text under another, which is what positional reuse does the moment
// a seek renumbers the document. Matching is by cue identity — exact key
// first, then the same text within cueMatchTolerance, since each run of the
// transcoder has its own keyframe-aligned timeline. Pure: no reader of it
// needs to have run the job.
func alignLines(p *Progress, doc *Doc) []string {
	out := make([]string, len(doc.Cues))
	known := collectKnown(p)
	if known.empty() {
		return out
	}
	for _, c := range doc.Cues {
		if c.Index < 0 || c.Index >= len(out) {
			continue
		}
		out[c.Index] = known.lookup(c.Start, c.End, c.Lines)
	}
	return out
}

// syncLive lines p up with the document as it stands: every cue currently
// in the document gets its key and its (re-aligned) translation at its own
// index.
//
// Translations the new layout displaces are parked past the document
// instead of being written over. A source does not always start where the
// last one did — a resume, a reload, a session swap, an evicted cache entry
// all produce a document that begins mid-film — and then the cue at index 0
// is not the cue index 0 was filed under. Overwriting in place destroyed the
// head of the record: keys replaced, lines blanked, cues someone had already
// paid to translate bought again on the next contiguous viewing.
//
// Parked entries are invisible to every reader of the document (RenderByIndex,
// countDoneByIndex and pendingByTime all index by a document cue's own Index,
// which is always below len(doc.Cues)) and visible to collectKnown, which is
// the one that has to see them: it is what a later run matches against. The
// record therefore keeps every translation it ever held, whatever order the
// documents arrive in, and each key appears once.
func syncLive(p *Progress, doc *Doc) {
	n := len(doc.Cues)
	aligned := alignLines(p, doc)
	lines := make([]string, n)
	keys := make([]string, n)
	inDoc := make(map[string]bool, n)
	for _, c := range doc.Cues {
		if c.Index < 0 || c.Index >= n {
			continue
		}
		k := CueKey(c.Start, c.End, c.Lines)
		keys[c.Index] = k
		lines[c.Index] = aligned[c.Index]
		inDoc[k] = true
	}
	for i, k := range p.CueKeys {
		if k == "" || inDoc[k] || i >= len(p.Lines) || p.Lines[i] == "" {
			continue
		}
		inDoc[k] = true
		keys = append(keys, k)
		lines = append(lines, p.Lines[i])
	}
	p.CueKeys = keys
	p.Lines = lines
	p.Total = n
}

// pendingByTime returns the cues still needing a translation: those at or
// ahead of the playhead first (earliest first), then the backlog behind it,
// earliest first. Indexes are Cue.Index, texts are the joined source lines.
//
// current is the offset of the run the playlist is on right now
// (LiveSource.CurrentOffset), compared against each cue's own Start rather
// than its ingest run (Cue.Run): ordering by document time alone put every
// pending cue of an abandoned earlier run ahead of the new position after a
// seek — there can be hundreds of them — so the cue playing right now waited
// out the whole backlog before it was even queued. Matching by ingest run
// instead of position would still misfire — #EXT-X-SESSION-OFFSET is
// quantized to 30 s, so a seek to 200 s starts a run at offset 180 while
// cues covering 180-240 s can already sit in the document tagged with an
// EARLIER run's offset, ingested before the seek while that run was still
// playing forward — deprioritizing cues that in fact sit at the new
// position. A run that resumes at an offset it already covered changes
// nothing here either: Refresh's `seen` map makes that a no-op, so there is
// nothing new to reorder.
func pendingByTime(doc *Doc, lines []string, current time.Duration) ([]int, []string) {
	var aheadIdx, behindIdx []int
	var aheadTexts, behindTexts []string
	for _, c := range doc.Cues {
		if len(c.Lines) == 0 || c.Index < 0 || c.Index >= len(lines) || lines[c.Index] != "" {
			continue
		}
		if c.Start >= current {
			aheadIdx = append(aheadIdx, c.Index)
			aheadTexts = append(aheadTexts, JoinLines(c))
		} else {
			behindIdx = append(behindIdx, c.Index)
			behindTexts = append(behindTexts, JoinLines(c))
		}
	}
	return append(aheadIdx, behindIdx...), append(aheadTexts, behindTexts...)
}

// pendingFrom reports the start of the earliest untranslated cue the viewer
// can still meet in the current run: same pending filter as pendingByTime,
// restricted to cues ending at or after the source's current offset. ok is
// false when there is none.
//
// End >= current rather than Start >= current: a cue straddling the
// playhead (Start < current <= End) is what is on screen right now, and a
// viewer who seeks into it must still see it counted. Cues that ended
// before current are behind the playhead — the viewer already passed
// them — and are left out so a job that went ahead-first (pendingByTime)
// does not keep the "translation is behind" banner up for a passage the
// viewer already left.
func pendingFrom(doc *Doc, lines []string, current time.Duration) (from time.Duration, ok bool) {
	for _, c := range doc.Cues {
		if len(c.Lines) == 0 || c.Index < 0 || c.Index >= len(lines) || lines[c.Index] != "" {
			continue
		}
		if c.End < current {
			continue
		}
		if !ok || c.Start < from {
			from = c.Start
			ok = true
		}
	}
	return from, ok
}

// freshRunBlocked reports whether the run started less than FreshRun ago and
// has an untranslated cue the viewer can still meet — the viewer who just
// seeked or pressed play is looking at it.
func (r *Runner) freshRunBlocked(src *LiveSource, doc *Doc, lines []string, now time.Time) bool {
	if r.live.FreshRun < 0 {
		return false
	}
	started := src.RunStartedAt()
	if started.IsZero() || now.Sub(started) >= r.live.FreshRun {
		return false
	}
	_, ok := pendingFrom(doc, lines, src.CurrentOffset())
	return ok
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
	// A record this run picked up may carry a terminal Status from a
	// previous run of the same source (RunEnded cleared by fresh cues —
	// see live_source.go — is what re-arms the job at all). That value
	// described a run that is no longer this one, so it is cleared here,
	// in the same place Live is forced back to true, rather than left to
	// read as this run's own outcome before this run has one.
	p.Status = ""
	targetName, _ := LangName(job.Lang)
	ticker := time.NewTicker(r.live.PollInterval)
	defer ticker.Stop()
	var firstPendingAt time.Time
	// published is the cue count the store has heard about, so a tick that
	// changed nothing does not rewrite the same record.
	published := -1
	lockedAt := time.Now()
	var lastPoll time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		// A new run seen by a handler's refresh (a viewer's poll after a
		// seek) wakes the job at once rather than on its next tick.
		case <-job.Live.RunStarted():
			// Delayed, not dropped: a seek landing right after the job's
			// own read still gets its read within liveWakeMinGap, or on the
			// next tick if that comes first.
			if d := liveWakeMinGap - time.Since(lastPoll); d > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
				case <-ticker.C:
				}
			}
		}
		// A wake-up queued before this read is answered by it. A handler's
		// read landing between this drain and the job's own leaves its token
		// for the next iteration: one extra read, never a lost wake-up.
		select {
		case <-job.Live.RunStarted():
		default:
		}
		lastPoll = time.Now()
		ref, err := r.pollLive(ctx, job)
		switch {
		case err == nil:
		case errors.Is(err, ErrSourceGone):
			logger.Info("source gone, stopping")
			JobErrors.WithLabelValues("source_gone").Inc()
			r.stopLive(ctx, key, logger, p, statusStopped)
			return
		case errors.Is(err, ErrSourceTooLarge):
			logger.Warn("source outgrew its caps, stopping")
			JobErrors.WithLabelValues("too_large").Inc()
			r.stopLive(ctx, key, logger, p, statusStopped)
			return
		default:
			if ctx.Err() != nil {
				return
			}
			// Transient: a timeout, a 5xx, a half-written playlist. The next
			// tick reads the whole playlist again, so nothing is lost.
			logger.WithError(err).Warn("failed to refresh the live source")
			// A transient failure (Refresh runs under its own 30s deadline,
			// so this is often context.DeadlineExceeded on a slow catch-up)
			// must not skip the lease refresh or the viewer-gone check: both
			// have to run every tick, not only on a tick that got a fresh
			// document.
			if !r.keepAlive(ctx, key, token, logger, &lockedAt, p) {
				return
			}
			continue
		}
		// One snapshot per tick: the cue keys and the cues have to describe
		// the same document, and the handler refreshes the source too.
		doc := job.Live.Doc().Snapshot()
		syncLive(p, doc)
		idx, texts := pendingByTime(doc, p.Lines, job.Live.CurrentOffset())
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
		// An ended playlist has no company coming, so it never waits, and
		// neither does a fresh run with a cue the viewer can meet (see
		// LiveConfig.FreshRun).
		batched := false
		if pending > 0 && (pending >= r.batchSize || now.Sub(firstPendingAt) >= r.live.BatchWait || ref.Ended || r.freshRunBlocked(job.Live, doc, p.Lines, now)) {
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
		}
		if ref.Ended && pending == 0 {
			// A playlist that ended with everything translated is finished
			// work; it is written before the lease is looked at again, so a
			// viewer who walked away on the last tick still gets the
			// artifact their session paid for.
			r.finishLive(ctx, key, logger, job, p, doc, start)
			return
		}
		if !r.keepAlive(ctx, key, token, logger, &lockedAt, p) {
			return
		}
	}
}

// keepAlive is the end of every tick, whether or not it got a document:
// refresh the lease (on its own schedule — ticks are seconds and the lease
// is minutes, so this is usually a no-op, and always one right after a
// batch, which refreshes the lease itself) and stop when nobody has asked
// for the key for --live-idle. It reports whether the job may keep going.
func (r *Runner) keepAlive(ctx context.Context, key, token string, logger *log.Entry, lockedAt *time.Time, p *Progress) bool {
	if !r.holdLock(ctx, key, token, logger, lockedAt) {
		return false
	}
	if r.sinceSeen(key) > r.live.Idle {
		logger.Info("viewer gone, stopping")
		JobErrors.WithLabelValues("viewer_gone").Inc()
		// Live stays set: the source has not ended, the translation is only
		// paused until someone asks for this track again.
		r.putProgress(ctx, key, logger, p)
		return false
	}
	return true
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
	// Reaching this function already means what statusDone describes: the
	// playlist ended and everything pending was translated (runLive only
	// calls it when ref.Ended && pending == 0). Both branches below leave
	// without a final artifact — one because the run cannot produce one,
	// the other because rendering it failed — but neither changes that
	// verdict, so both stop with the same status.
	if !job.Live.Contiguous() {
		// This run joined after a seek, so the document has holes no later
		// reader could detect. The partial stays, the artifact is not written.
		//
		// And the run is over: the playlist ended, everything it carried is
		// translated, and no final will ever be written from it. Without
		// saying so on the source, every poll of the key found no final and
		// no running job and started another one — a job that took the lock,
		// polled an ended playlist, found nothing pending and wrote the same
		// record again, once per poll per viewer, for as long as the tab
		// stayed open. The mark lives on the source, so it lasts exactly as
		// long as the session it describes: the next transcoder session
		// brings its own LiveSource and is new work.
		job.Live.MarkRunEnded()
		logger.Info("live source ended on a run that skipped ahead, keeping the partial")
		r.stopLive(ctx, key, logger, p, statusDone)
		return
	}
	body, err := doc.RenderByIndex(p.Lines)
	if err != nil {
		JobErrors.WithLabelValues("render").Inc()
		logger.WithError(err).Error("failed to render final")
		r.stopLive(ctx, key, logger, p, statusDone)
		return
	}
	r.writeFinal(ctx, key, logger, body, start)
}

// Status values for Progress.Status — see the field's doc comment. These
// are the only two terminal states stopLive ever writes, and the only two
// values X-Subtitle-Status ever carries.
const (
	statusDone    = "done"
	statusStopped = "stopped"
)

// stopLive is the last write of a live job that will not produce a final
// artifact. Clearing Live is what tells a reader that what is stored is all
// there will be; the partial itself stays for its TTL, so a viewer who
// comes back does not start from zero. status is recorded alongside, in
// the same write, so a reader never sees Live=false without knowing why.
func (r *Runner) stopLive(ctx context.Context, key string, logger *log.Entry, p *Progress, status string) {
	p.Live = false
	p.Status = status
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

// LivePollInterval is how often the live loop re-reads a playlist. The
// handler reads it too: it is the staleness bound for the source it serves
// (see LiveSource.RefreshIfStale).
func (r *Runner) LivePollInterval() time.Duration { return r.live.PollInterval }

// LiveHead is what LiveProgress reports: the counts and flags HEAD needs,
// without the body LiveSnapshot renders alongside them.
type LiveHead struct {
	Done, Total int
	Live        bool
	Status      string
	// PendingFrom and HasPending carry the same X-Subtitle-Pending-From
	// contract as Snapshot: see its doc comment.
	PendingFrom time.Duration
	HasPending  bool
	// SessionOffset and HasSessionOffset: the X-Subtitle-Session-Offset
	// contract, see Snapshot.
	SessionOffset    time.Duration
	HasSessionOffset bool
}

// LiveProgress is LiveSnapshot without the body: the same alignment, the
// same counts, no render. HEAD asks for exactly this, and rendering a whole
// document into a response that discards it is the most expensive thing a
// live key does per poll.
func (r *Runner) LiveProgress(ctx context.Context, key string, src *LiveSource) (LiveHead, error) {
	if _, ok, err := r.store.GetFinal(ctx, key); err != nil {
		return LiveHead{}, err
	} else if ok {
		// Same convention as LiveSnapshot: a finished artifact has no cue
		// count to report, and done == total reads as complete.
		return LiveHead{Done: 100, Total: 100}, nil
	}
	p, err := r.store.GetProgress(ctx, key)
	if err != nil {
		return LiveHead{}, err
	}
	doc := src.Doc().Snapshot()
	lines := alignLines(p, doc)
	st := ""
	if p != nil {
		st = p.Status
	}
	offset := src.CurrentOffset()
	from, hasPending := pendingFrom(doc, lines, offset)
	return LiveHead{
		Done:             countDoneByIndex(lines, doc),
		Total:            len(doc.Cues),
		Live:             p == nil || p.Live,
		Status:           st,
		PendingFrom:      from,
		HasPending:       hasPending,
		SessionOffset:    offset,
		HasSessionOffset: !src.RunStartedAt().IsZero(),
	}, nil
}
