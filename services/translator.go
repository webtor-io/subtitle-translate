package services

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type BatchRequest struct {
	TargetLang string
	TargetName string
	SourceLang string
	Context    []string
	Glossary   []string
	Lines      []string
}

type BatchResult struct {
	Lines        []string
	InputTokens  int64
	OutputTokens int64
}

type Translator interface {
	Translate(ctx context.Context, req BatchRequest) (BatchResult, error)
}

var (
	ErrLineMismatch = errors.New("reply line count does not match the request")
	// ErrTruncated: the reply hit the output token limit. Retrying the
	// identical prompt would truncate again; the caller splits instead.
	ErrTruncated = errors.New("reply was truncated by the output token limit")
	// ErrRefused: the model declined to answer. Not retryable either.
	ErrRefused = errors.New("upstream refused to translate the batch")
)

var replyLineRe = regexp.MustCompile(`^\s*(\d+)\s*[:.)]\s*(.*)$`)

type batchCall func(context.Context, string, string, int) ([]string, bool, int64, int64, error)

// Both providers use the same retry and accounting policy. Only malformed
// numbered lines in a complete response are retried, once; the job runner
// handles truncation by splitting the batch and treats refusal as terminal.
func translateBatch(ctx context.Context, req BatchRequest, call batchCall) (BatchResult, error) {
	var res BatchResult
	user := BuildUserPrompt(req)
	const attempts = 2
	for attempt := 1; ; attempt++ {
		lines, complete, in, out, err := call(ctx, BuildSystemPrompt(req.TargetName), user, len(req.Lines))
		res.InputTokens += in
		res.OutputTokens += out
		TokensInput.Add(float64(in))
		TokensOutput.Add(float64(out))
		if err == nil {
			res.Lines = lines
			return res, nil
		}
		if errors.Is(err, ErrLineMismatch) {
			LineMismatch.Inc()
		}
		if !errors.Is(err, ErrLineMismatch) || !complete || attempt == attempts {
			return res, err
		}
		user += "\nReminder: output exactly " + strconv.Itoa(len(req.Lines)) + " numbered lines, nothing else.\n"
	}
}

// ParseReply accepts "<i>: <text>" lines (also "<i>." and "<i>)"), ignores
// blank lines and code fences, and requires exactly n lines numbered 1..n.
func ParseReply(reply string, n int) ([]string, error) {
	out := make([]string, 0, n)
	for _, raw := range strings.Split(reply, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "```") {
			continue
		}
		m := replyLineRe.FindStringSubmatch(line)
		if m == nil {
			return nil, errors.Wrapf(ErrLineMismatch, "unnumbered line %q", line)
		}
		idx, _ := strconv.Atoi(m[1])
		if idx != len(out)+1 {
			return nil, errors.Wrapf(ErrLineMismatch, "expected line %d, got %d", len(out)+1, idx)
		}
		out = append(out, strings.TrimSpace(m[2]))
	}
	if len(out) != n {
		return nil, errors.Wrapf(ErrLineMismatch, "expected %d lines, got %d", n, len(out))
	}
	return out, nil
}
