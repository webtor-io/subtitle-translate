package services

import "github.com/prometheus/client_golang/prometheus"

var (
	BatchesTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_batches_total", Help: "translated batches"})
	TokensInput  = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_tokens_input_total", Help: "upstream input tokens"})
	TokensOutput = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_tokens_output_total", Help: "upstream output tokens"})
	JobErrors    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "subtitle_translate_job_errors_total", Help: "job errors by code"}, []string{"code"})
	JobDuration  = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "subtitle_translate_job_seconds", Help: "job duration", Buckets: []float64{5, 15, 30, 60, 120, 300, 600}})
	LineMismatch = prometheus.NewCounter(prometheus.CounterOpts{Name: "subtitle_translate_line_mismatch_total", Help: "batches whose reply line count did not match"})
)

func init() {
	prometheus.MustRegister(BatchesTotal, TokensInput, TokensOutput, JobErrors, JobDuration, LineMismatch)
}
