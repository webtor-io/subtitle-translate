package services

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
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

// defaultFreshRun is one transcoder seek quantum's worth of wall time
// after a run starts — the window in which the viewer who caused the run
// is standing right at its start.
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

// LivePosKey is the store key a live viewer's position is kept under: the
// job key tagged with the run (#EXT-X-SESSION-OFFSET) the position was
// reported in. The tag is the whole staleness story: after a seek the new
// run has another offset, so the position left over from the old run --
// possibly an hour further into the film -- is simply never read, on this
// replica or the other, with no clean-up to get wrong.
func LivePosKey(key string, run time.Duration) string {
	return key + "@" + strconv.FormatInt(run.Milliseconds(), 10)
}

// liveCurrent is where a live job works from and what its frontier is
// measured against: the viewer's position in the current run when a poll
// has reported one, else the start of the run.
//
// Until 2026-09-18 it was always the start of the run. That is where the
// viewer is right after a seek, and nowhere near them later: a viewer who
// resumed at 15:49, watched to 27:00 and then turned the translation on
// had eleven minutes of cues -- about 170 -- queued ahead of the line they
// were listening to, and the player's hold gave up after ten seconds.
func (r *Runner) liveCurrent(ctx context.Context, key string, src *LiveSource) time.Duration {
	current, _ := r.liveCurrentFrom(ctx, key, src)
	return current
}

// liveCurrentFrom is liveCurrent with where the answer came from, for the
// batch log: "poll" is a position a viewer reported in this run, "run" is
// the start of the run standing in for one.
func (r *Runner) liveCurrentFrom(ctx context.Context, key string, src *LiveSource) (time.Duration, string) {
	offset := src.CurrentOffset()
	pos, ok, err := r.store.GetPos(ctx, LivePosKey(key, offset))
	if err != nil || !ok || pos < offset {
		// Best effort, like the file job's position: order is a quality
		// of service, not correctness.
		return offset, "run"
	}
	return pos, "poll"
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
	from, hasPending := liveFrontier(doc, lines, r.liveCurrent(ctx, key, src), src, time.Now())
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
// instead of position would still misfire — a run starts at or before the
// requested position (its keyframe; on pre-2026-09-17 transcoders also
// 30 s-quantized), so cues covering the new neighborhood can already sit
// in the document tagged with an EARLIER run's offset, ingested before the
// seek while that run was still playing forward — deprioritizing cues that
// in fact sit at the new position. A run that resumes at an offset it already covered changes
// nothing here either: Refresh's `seen` map makes that a no-op, so there is
// nothing new to reorder.
//
// leadIn moves the line between "ahead" and "behind" back from current, and
// the test is on End, not Start (owner, 2026-09-18): with Start >= current
// the cue on screen right now -- it began before the viewer got here -- and
// the exchange it answers were "behind", i.e. translated last, so a viewer
// who turned the translation on read nothing until the next line began, and
// nothing at all if they stepped back ten seconds. A batch is ~50 cues; half
// a minute of lead-in is a handful of them, in the same upstream call. The
// frontier (pendingFrom) stays on the true position: the lead-in is about
// what to translate first, not about what the viewer is still going to meet.
func pendingByTime(doc *Doc, lines []string, current, leadIn time.Duration) []int {
	ahead, behind := pendingSplit(doc, lines, current, leadIn)
	return append(ahead, behind...)
}

// pendingSplit is pendingByTime before the two halves are joined: what the
// viewer can still meet, and what they have already passed.
func pendingSplit(doc *Doc, lines []string, current, leadIn time.Duration) (ahead, behind []int) {
	from := current - leadIn
	if from < 0 {
		from = 0
	}
	var aheadIdx, behindIdx []int
	for _, c := range doc.Cues {
		if len(c.Lines) == 0 || c.Index < 0 || c.Index >= len(lines) || lines[c.Index] != "" {
			continue
		}
		if c.End >= from {
			aheadIdx = append(aheadIdx, c.Index)
		} else {
			behindIdx = append(behindIdx, c.Index)
		}
	}
	return aheadIdx, behindIdx
}

// liveBatchServesWithin is how far past a new run's start a cue may begin and
// still count as "what the viewer of that run is waiting for".
const liveBatchServesWithin = 5 * time.Minute

// batchServes reports whether a batch holds a cue the viewer of a run
// starting at offset is about to meet: ending at or after offset - leadIn,
// beginning within liveBatchServesWithin of it.
func batchServes(doc *Doc, idx []int, offset, leadIn time.Duration) bool {
	for _, ci := range idx {
		c := doc.Cues[ci]
		if c.End >= offset-leadIn && c.Start <= offset+liveBatchServesWithin {
			return true
		}
	}
	return false
}

// textsFor joins the source text of exactly the cues a batch will carry.
// Indexes come from pendingByTime, so texts are built once per chosen batch
// rather than once per pending cue per scan — at 5000 cues and batch 50
// that difference is a hundred scans' worth of transient garbage.
func textsFor(doc *Doc, idx []int) []string {
	out := make([]string, len(idx))
	for i, ci := range idx {
		out[i] = JoinLines(doc.Cues[ci])
	}
	return out
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

// liveUnreadHold bounds how long an unread run is reported as pending (see
// liveFrontier). Long enough for a source that is still downloading to
// produce its first subtitle segment, short enough that a stretch with no
// dialogue left in it -- the end credits, where no subtitle playlist is ever
// written and the run never "ends" for this reader -- does not hold a
// viewer for ever.
const liveUnreadHold = 2 * time.Minute

// liveFrontier is pendingFrom for a live source, with one more case: a run
// of which nothing has been read yet.
//
// pendingFrom can only speak of cues the document has. Right after a seek
// it has none for the new position -- the transcoder writes the subtitle
// playlist when the first subtitle segment closes, which takes the next cue
// and, on a source that is still downloading, minutes (measured 2026-09-19:
// a seek to 30:00 of a file cached to 10:00, no playlist a minute in). "No
// untranslated cue ahead of you" was then reported as "nothing pending",
// which a player reads as "the translation is ahead of you": it let the
// film go, without subtitles, under a pill saying "caught up".
//
// So while the playlist has not ended, the run is younger than
// liveUnreadHold, NOTHING has been read from it yet and the document holds
// no cue at or after the viewer's position -- translated or not, from this
// run or an earlier one -- the frontier is the position itself: what the
// viewer is about to hear has not even been read.
//
// "Nothing read from this run" is what keeps this apart from the ordinary
// case of a viewer who has simply got past the last cue a working run has
// produced (TestHandlerLiveFrontierFollowsTheViewer): there the reader is
// reading, the transcoder is 20-25x ahead of the viewer, and "nothing
// ahead" means what it says. The first segment of the run ends the special
// case for good and pendingFrom speaks for itself.
func liveFrontier(doc *Doc, lines []string, current time.Duration, src *LiveSource, now time.Time) (time.Duration, bool) {
	if from, ok := pendingFrom(doc, lines, current); ok {
		return from, true
	}
	if src == nil || src.Ended() {
		return 0, false
	}
	started := src.RunStartedAt()
	if started.IsZero() || now.Sub(started) >= liveUnreadHold || src.RunSegments() > 0 {
		return 0, false
	}
	for _, c := range doc.Cues {
		if c.End >= current {
			return 0, false
		}
	}
	return current, true
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
	var lastOffset time.Duration
	offsetLogged := false
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
		// A read worth a line: the first one of a run (a seek, as this job
		// sees it), and any read slow or large enough to be what a viewer is
		// waiting on. The rest -- every few seconds, nothing new -- stay in
		// the histograms.
		if took, off := time.Since(lastPoll), job.Live.CurrentOffset(); err == nil &&
			(!offsetLogged || off != lastOffset || took >= liveReadWorthLogging || ref.Segments >= liveSegmentsWorthLogging) {
			logger.WithFields(log.Fields{
				"run":       off.Seconds(),
				"newRun":    !offsetLogged || off != lastOffset,
				"segments":  ref.Segments,
				"cuesAdded": ref.Added,
				"ended":     ref.Ended,
				"seconds":   took.Seconds(),
			}).Info("live read")
			lastOffset, offsetLogged = off, true
		}
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
			// In a fresh run the viewer is waiting on exactly this read (a
			// new run's first segment is the likeliest to fail once): ask
			// for another one after liveWakeMinGap instead of a whole tick.
			if r.LiveFresh(job.Live) {
				job.Live.Nudge()
			}
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
		current, currentFrom := r.liveCurrentFrom(ctx, key, job.Live)
		// What the viewer can still meet goes first and goes ALONE. Until
		// 2026-09-19 a batch was topped up to --batch-size with cues the
		// viewer had already passed, and the first batch after a seek is
		// exactly where that hurt: measured in production, the new run had
		// 3 cues read, the batch carried those and 47 from eight minutes
		// into a film being watched at the 25th, and the viewer waited
		// 21.6 s for an upstream call that owed them 2. The backlog is
		// translated when nothing ahead of the viewer is pending -- which,
		// with the transcoder 20-25x ahead of them, is most of the time.
		ahead, behind := pendingSplit(doc, p.Lines, current, r.leadIn)
		idx := ahead
		if len(idx) == 0 {
			idx = behind
		}
		pending := len(ahead) + len(behind)
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
		if pending > 0 && (len(idx) >= r.batchSize || now.Sub(firstPendingAt) >= r.live.BatchWait || ref.Ended || r.freshRunBlocked(job.Live, doc, p.Lines, now)) {
			if r.batchSize > 0 && len(idx) > r.batchSize {
				idx = idx[:r.batchSize]
			}
			// A failed batch (upstream or store) leaves the record with
			// Live still set. That is the honest state: this job stopped,
			// but the playlist did not end, and the next request for the key
			// starts a job that picks the record up where it is.
			batchStarted := time.Now()
			// A seek while the call is out: a viewer's poll makes the
			// handler read the playlist, the new run is seen, and this batch
			// -- ordered for where the viewer WAS -- is what they now wait
			// behind (9 s of a 27.7 s call in the same measurement). It is
			// dropped, unless it happens to serve the new run too. What it
			// had translated stays; the token is put back so the loop wakes
			// for the new run the moment it is free.
			bctx, cancelBatch := context.WithCancel(ctx)
			var retargeted atomic.Bool
			runAtStart := job.Live.CurrentOffset()
			watcherDone := make(chan struct{})
			go func() {
				defer close(watcherDone)
				// A token taken here is the loop's wake-up too, so it is put
				// back on the way out -- not while waiting, or this goroutine
				// would receive its own token for ever. A token can also be
				// one left over from before the batch (the first read of the
				// run, a nudge): then the run has not changed and the wait
				// goes on.
				taken := false
				defer func() {
					if taken {
						job.Live.Nudge()
					}
				}()
				for {
					select {
					case <-bctx.Done():
						return
					case <-job.Live.RunStarted():
						taken = true
						if off := job.Live.CurrentOffset(); off != runAtStart && !batchServes(doc, idx, off, r.leadIn) {
							retargeted.Store(true)
							cancelBatch()
							return
						}
					}
				}
			}()
			ok := r.runBatch(bctx, key, token, logger, job, targetName, p, idx, textsFor(doc, idx))
			cancelBatch()
			<-watcherDone
			took := time.Since(batchStarted)
			BatchSeconds.WithLabelValues("live").Observe(took.Seconds())
			// One line per batch, and everything needed to answer "what was
			// the job doing while the viewer waited": which run it believes
			// it is on, where it thinks the viewer is and who told it, which
			// stretch of film it chose, and how long the call took.
			first, last := cueSpan(doc, idx)
			logger.WithFields(log.Fields{
				"run":        job.Live.CurrentOffset().Seconds(),
				"from":       current.Seconds(),
				"fromSource": currentFrom,
				"cues":       len(idx),
				"pending":    pending,
				"firstCue":   first.Seconds(),
				"lastCue":    last.Seconds(),
				"seconds":    took.Seconds(),
				"runAge":     time.Since(job.Live.RunStartedAt()).Seconds(),
				"ok":         ok,
				"dropped":    retargeted.Load(),
			}).Info("live batch")
			if !ok && retargeted.Load() && ctx.Err() == nil {
				// Not a failure: the batch was dropped for a new run.
				firstPendingAt = time.Time{}
				continue
			}
			if !ok {
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
	started := time.Now()
	ref, err := job.Live.Refresh(rctx)
	LiveRefreshSeconds.Observe(time.Since(started).Seconds())
	LiveRefreshSegments.Observe(float64(ref.Segments))
	return ref, err
}

// What makes one read of the playlist worth a log line of its own.
const (
	liveReadWorthLogging     = 2 * time.Second
	liveSegmentsWorthLogging = 25
)

// cueSpan is the movie time a batch covers: the earliest start and the
// latest end among the cues at idx.
func cueSpan(doc *Doc, idx []int) (first, last time.Duration) {
	for i, ci := range idx {
		c := doc.Cues[ci]
		if i == 0 || c.Start < first {
			first = c.Start
		}
		if c.End > last {
			last = c.End
		}
	}
	return first, last
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

// LiveFresh reports whether src's current run started less than FreshRun
// ago. The handler reads a fresh run's playlist more often: the player
// decides whether to hold playback for a seek's subtitles from the answers
// in those first seconds, and on a replica that does not own the job the
// poll-interval gate would leave them describing a document seconds old.
func (r *Runner) LiveFresh(src *LiveSource) bool {
	if r.live.FreshRun < 0 {
		return false
	}
	started := src.RunStartedAt()
	return !started.IsZero() && time.Since(started) < r.live.FreshRun
}

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
	from, hasPending := liveFrontier(doc, lines, r.liveCurrent(ctx, key, src), src, time.Now())
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
