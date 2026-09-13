package services

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	flagAPIKey          = "anthropic-api-key"
	flagModel           = "model"
	flagUpstreamTimeout = "upstream-timeout"
	flagMaxTokens       = "max-tokens"
)

func RegisterTranslatorFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{Name: flagAPIKey, Usage: "upstream model API key; empty disables translation", EnvVar: "ANTHROPIC_API_KEY"},
		cli.StringFlag{Name: flagModel, Usage: "upstream model id", Value: "claude-haiku-4-5-20251001", EnvVar: "SUBTITLE_TRANSLATE_MODEL"},
		cli.IntFlag{Name: flagUpstreamTimeout, Usage: "per-batch upstream timeout, seconds", Value: 60, EnvVar: "SUBTITLE_TRANSLATE_UPSTREAM_TIMEOUT"},
		cli.IntFlag{Name: flagMaxTokens, Usage: "max output tokens per batch", Value: 4096, EnvVar: "SUBTITLE_TRANSLATE_MAX_TOKENS"},
	)
}

type AnthropicTranslator struct {
	cl        anthropic.Client
	model     string
	timeout   time.Duration
	maxTokens int64
}

// NewAnthropicTranslator returns nil when no key is configured: the
// capability is absent, and the handler answers 501.
func NewAnthropicTranslator(c *cli.Context, opts ...option.RequestOption) Translator {
	key := strings.TrimSpace(c.String(flagAPIKey))
	if key == "" {
		log.Info("no upstream API key: translation disabled")
		return nil
	}
	opts = append([]option.RequestOption{option.WithAPIKey(key)}, opts...)
	return &AnthropicTranslator{
		cl:        anthropic.NewClient(opts...),
		model:     c.String(flagModel),
		timeout:   time.Duration(c.Int(flagUpstreamTimeout)) * time.Second,
		maxTokens: int64(c.Int(flagMaxTokens)),
	}
}

func (t *AnthropicTranslator) Model() string { return t.model }

func (t *AnthropicTranslator) Translate(ctx context.Context, req BatchRequest) (BatchResult, error) {
	var res BatchResult
	user := BuildUserPrompt(req)
	for attempt := 1; attempt <= 2; attempt++ {
		lines, in, out, err := t.call(ctx, BuildSystemPrompt(req.TargetName), user, len(req.Lines))
		res.InputTokens += in
		res.OutputTokens += out
		TokensInput.Add(float64(in))
		TokensOutput.Add(float64(out))
		if err == nil {
			res.Lines = lines
			return res, nil
		}
		if !errors.Is(err, ErrLineMismatch) {
			return res, err
		}
		LineMismatch.Inc()
		user = user + "\nReminder: output exactly " + strconv.Itoa(len(req.Lines)) + " numbered lines, nothing else.\n"
		if attempt == 2 {
			return res, err
		}
	}
	return res, ErrLineMismatch
}

func (t *AnthropicTranslator) call(ctx context.Context, system, user string, n int) ([]string, int64, int64, error) {
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
		return nil, 0, 0, errors.Wrap(err, "upstream request failed")
	}
	var sb strings.Builder
	for _, b := range resp.Content {
		if b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	lines, err := ParseReply(sb.String(), n)
	return lines, resp.Usage.InputTokens, resp.Usage.OutputTokens, err
}
