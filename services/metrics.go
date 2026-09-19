package services

import "github.com/prometheus/client_golang/prometheus"

var (
	BatchesTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_batches_total", Help: "translated batches"})
	TokensInput  = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_tokens_input_total", Help: "upstream input tokens"})
	TokensOutput = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_tokens_output_total", Help: "upstream output tokens"})
	JobErrors    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "subtitle_translate_job_errors_total", Help: "job errors by code"}, []string{"code"})
	JobDuration  = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "subtitle_translate_job_seconds", Help: "job duration", Buckets: []float64{5, 15, 30, 60, 120, 300, 600}})
	LineMismatch = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_line_mismatch_total", Help: "batches whose reply line count did not match"})
	JobsRunning  = prometheus.NewGauge(prometheus.GaugeOpts{Name: "subtitle_translate_jobs_running", Help: "translation jobs holding a concurrency slot"})
	// BatchesFallback counts batches (or split halves) whose cues kept their
	// source text instead of a translation.
	BatchesFallback = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "subtitle_translate_batches_fallback_total", Help: "batches left untranslated by reason"}, []string{"reason"})
	// LiveSegmentsSkipped counts live segments given up on after repeated
	// failures. Skipping keeps the document moving, but it leaves a hole in
	// it, so this is the only externally visible trace of one.
	LiveSegmentsSkipped = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_live_segments_skipped_total", Help: "live segments given up on after repeated failures"})
	// The three numbers a viewer waits on after a seek, none of which were
	// visible from outside (2026-09-19: a 110 s hold in production could
	// not be told apart from "translated from the start of the film"): how
	// long one upstream call takes, how long one read of the playlist takes,
	// and how many segments that read had to fetch one by one.
	BatchSeconds        = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "subtitle_translate_batch_seconds", Help: "one batch: upstream call plus store write", Buckets: []float64{1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64}}, []string{"source"})
	LiveRefreshSeconds  = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "subtitle_translate_live_refresh_seconds", Help: "one read of a live playlist, segments included", Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 30}})
	LiveRefreshSegments = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "subtitle_translate_live_refresh_segments", Help: "segments fetched by one read of a live playlist", Buckets: []float64{0, 1, 5, 10, 25, 50, 100, 250, 500}})
)

func init() {
	prometheus.MustRegister(BatchesTotal, TokensInput, TokensOutput, JobErrors, JobDuration, LineMismatch, BatchesFallback, JobsRunning, LiveSegmentsSkipped, BatchSeconds, LiveRefreshSeconds, LiveRefreshSegments)
}
