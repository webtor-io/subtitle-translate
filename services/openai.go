package services

import (
	"context"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

type OpenAITranslator struct {
	cl        openai.Client
	model     string
	timeout   time.Duration
	maxTokens int64
}

func NewOpenAITranslator(c *cli.Context, opts ...option.RequestOption) *OpenAITranslator {
	key := strings.TrimSpace(c.String(flagOpenAIAPIKey))
	if key == "" {
		return nil
	}
	opts = append([]option.RequestOption{option.WithAPIKey(key), option.WithBaseURL(c.String(flagOpenAIBaseURL))}, opts...)
	return &OpenAITranslator{cl: openai.NewClient(opts...), model: translatorModel(c, defaultOpenAIModel),
		timeout: time.Duration(c.Int(flagUpstreamTimeout)) * time.Second, maxTokens: int64(c.Int(flagMaxTokens))}
}

func (t *OpenAITranslator) Model() string { return t.model }

func (t *OpenAITranslator) Translate(ctx context.Context, req BatchRequest) (BatchResult, error) {
	return translateBatch(ctx, req, t.call)
}

func (t *OpenAITranslator) call(ctx context.Context, system, user string, n int) ([]string, bool, int64, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	resp, err := t.cl.Responses.New(ctx, responses.ResponseNewParams{
		Model: t.model, MaxOutputTokens: openai.Int(t.maxTokens),
		Instructions: openai.String(system), Store: openai.Bool(false),
		Input: responses.ResponseNewParamsInputUnion{OfString: openai.String(user)},
		// Use model sampling defaults: reasoning models reject temperature.
	})
	if err != nil {
		return nil, false, 0, 0, errors.Wrap(err, "upstream request failed")
	}
	in, out := resp.Usage.InputTokens, resp.Usage.OutputTokens
	// Decide from status before parsing plausible partial text: even n
	// numbered lines can be an unfinished translation of the last cue.
	if resp.Status == responses.ResponseStatusIncomplete {
		switch resp.IncompleteDetails.Reason {
		case "max_output_tokens":
			return nil, false, in, out, errors.Wrapf(ErrTruncated, "batch of %d lines", n)
		case "content_filter":
			return nil, false, in, out, ErrRefused
		}
	}
	if resp.Status != responses.ResponseStatusCompleted {
		return nil, false, in, out, errors.Errorf("upstream response status %s (%s)", resp.Status, resp.IncompleteDetails.Reason)
	}
	for _, item := range resp.Output {
		for _, content := range item.Content {
			if content.Type == "refusal" {
				return nil, true, in, out, ErrRefused
			}
		}
	}
	lines, err := ParseReply(resp.OutputText(), n)
	return lines, true, in, out, err
}
