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

var ErrLineMismatch = errors.New("reply line count does not match the request")

var replyLineRe = regexp.MustCompile(`^\s*(\d+)\s*[:.)]\s*(.*)$`)

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
