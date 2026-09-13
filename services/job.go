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
	Doc        *Doc
}

type Snapshot struct {
	Body  []byte
	Done  int
	Total int
	Final bool
}

type Runner struct {
	store     Store
	tr        Translator
	model     string
	batchSize int
	lockTTL   time.Duration
	mu        sync.Mutex
	running   map[string]chan struct{}
}

func NewRunner(store Store, tr Translator, model string, batchSize int, lockTTL time.Duration) *Runner {
	return &Runner{store: store, tr: tr, model: model, batchSize: batchSize, lockTTL: lockTTL, running: map[string]chan struct{}{}}
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
func (r *Runner) Ensure(ctx context.Context, key string, job *Job) {
	r.mu.Lock()
	if _, ok := r.running[key]; ok {
		r.mu.Unlock()
		return
	}
	done := make(chan struct{})
	r.running[key] = done
	r.mu.Unlock()
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				JobErrors.WithLabelValues("panic").Inc()
				log.WithFields(log.Fields{"key": key, "lang": job.Lang, "panic": rec}).Error("translation job panicked")
			}
			r.mu.Lock()
			delete(r.running, key)
			r.mu.Unlock()
			close(done)
		}()
		// The background job outlives the request that triggered it, so it
		// gets its own context; the lock TTL bounds how long it may run.
		r.run(context.Background(), key, job)
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
	locked, err := r.store.TryLock(ctx, key, r.lockTTL)
	if err != nil {
		JobErrors.WithLabelValues("store").Inc()
		logger.WithError(err).Error("failed to acquire lock")
		return
	}
	if !locked {
		logger.Debug("another worker holds the lock")
		return
	}
	defer func() { _ = r.store.Unlock(ctx, key) }()

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
		if err := r.translateChunk(ctx, logger, job, targetName, p, idx, lines); err != nil {
			JobErrors.WithLabelValues("upstream").Inc()
			logger.WithError(err).WithField("batch", b).Error("upstream failed, stopping")
			_ = r.store.PutProgress(ctx, key, p)
			return
		}
		if err := r.store.PutProgress(ctx, key, p); err != nil {
			JobErrors.WithLabelValues("store").Inc()
			logger.WithError(err).Error("failed to store progress")
			return
		}
		_ = r.store.RefreshLock(ctx, key, r.lockTTL)
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
