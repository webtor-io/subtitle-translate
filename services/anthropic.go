package services

import (
	"context"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

type AnthropicTranslator struct {
	cl        anthropic.Client
	model     string
	timeout   time.Duration
	maxTokens int64
}

// NewAnthropicTranslator returns nil when no key is configured: the
// capability is absent, and the handler answers 501.
func NewAnthropicTranslator(c *cli.Context, opts ...option.RequestOption) *AnthropicTranslator {
	key := strings.TrimSpace(c.String(flagAPIKey))
	if key == "" {
		log.Info("no upstream API key: translation disabled")
		return nil
	}
	opts = append([]option.RequestOption{option.WithAPIKey(key)}, opts...)
	return &AnthropicTranslator{
		cl:        anthropic.NewClient(opts...),
		model:     translatorModel(c, defaultAnthropicModel),
		timeout:   time.Duration(c.Int(flagUpstreamTimeout)) * time.Second,
		maxTokens: int64(c.Int(flagMaxTokens)),
	}
}

func (t *AnthropicTranslator) Model() string { return t.model }

func (t *AnthropicTranslator) Translate(ctx context.Context, req BatchRequest) (BatchResult, error) {
	return translateBatch(ctx, req, func(ctx context.Context, system, user string, n int) ([]string, bool, int64, int64, error) {
		lines, stop, in, out, err := t.call(ctx, system, user, n)
		return lines, stop == anthropic.StopReasonEndTurn, in, out, err
	})
}

func (t *AnthropicTranslator) call(ctx context.Context, system, user string, n int) ([]string, anthropic.StopReason, int64, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	resp, err := t.cl.Messages.New(ctx, anthropic.MessageNewParams{
		Model:       anthropic.Model(t.model),
		MaxTokens:   t.maxTokens,
		System:      []anthropic.TextBlockParam{{Text: system}},
		Messages:    []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(user))},
		Temperature: anthropic.Float(0),
	})
	if err != nil {
		return nil, "", 0, 0, errors.Wrap(err, "upstream request failed")
	}
	in, out := resp.Usage.InputTokens, resp.Usage.OutputTokens
	// The stop reason decides before the text does: a reply cut off by the
	// token limit may still parse into a plausible (but short) list.
	switch resp.StopReason {
	case anthropic.StopReasonMaxTokens:
		return nil, resp.StopReason, in, out, errors.Wrapf(ErrTruncated, "batch of %d lines", n)
	case anthropic.StopReasonRefusal:
		return nil, resp.StopReason, in, out, errors.Wrapf(ErrRefused, "batch of %d lines", n)
	}
	var sb strings.Builder
	for _, b := range resp.Content {
		if b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	lines, err := ParseReply(sb.String(), n)
	return lines, resp.StopReason, in, out, err
}
