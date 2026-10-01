package services

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	flagAPIKey            = "anthropic-api-key"
	flagOpenAIAPIKey      = "openai-api-key"
	flagOpenAIBaseURL     = "openai-base-url"
	flagModel             = "model"
	flagUpstreamTimeout   = "upstream-timeout"
	flagMaxTokens         = "max-tokens"
	defaultAnthropicModel = "claude-haiku-4-5-20251001"
	defaultOpenAIModel    = "gpt-4.1-mini"
)

func RegisterTranslatorFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{Name: flagAPIKey, Usage: "Anthropic API key; takes precedence over OpenAI", EnvVar: "ANTHROPIC_API_KEY"},
		cli.StringFlag{Name: flagOpenAIAPIKey, Usage: "OpenAI API key; used when the Anthropic key is empty", EnvVar: "OPENAI_API_KEY"},
		cli.StringFlag{Name: flagOpenAIBaseURL, Usage: "OpenAI API base URL", Value: "https://api.openai.com/v1", EnvVar: "OPENAI_BASE_URL"},
		cli.StringFlag{Name: flagModel, Usage: "upstream model id; empty uses the configured provider default", EnvVar: "SUBTITLE_TRANSLATE_MODEL"},
		cli.IntFlag{Name: flagUpstreamTimeout, Usage: "per-batch upstream timeout, seconds", Value: 60, EnvVar: "SUBTITLE_TRANSLATE_UPSTREAM_TIMEOUT"},
		cli.IntFlag{Name: flagMaxTokens, Usage: "max output tokens per batch", Value: 8192, EnvVar: "SUBTITLE_TRANSLATE_MAX_TOKENS"},
	)
}

// ConfiguredTranslator adds the model id used by the artifact cache to the
// narrow Translator contract consumed by the job runner.
type ConfiguredTranslator interface {
	Translator
	Model() string
}

// NewTranslator chooses only from configured credentials. Upstream errors
// never select a different provider. Return interface-nil when neither key
// is configured so the existing 501 capability diagnostic remains intact.
func NewTranslator(c *cli.Context) ConfiguredTranslator {
	if strings.TrimSpace(c.String(flagAPIKey)) != "" {
		log.WithField("provider", "anthropic").Info("subtitle translation: selected configured provider")
		if strings.TrimSpace(c.String(flagOpenAIAPIKey)) != "" {
			log.Info("both API keys configured: using Anthropic")
		}
		return NewAnthropicTranslator(c)
	}
	if strings.TrimSpace(c.String(flagOpenAIAPIKey)) != "" {
		log.WithField("provider", "openai").Info("subtitle translation: selected configured provider")
		return NewOpenAITranslator(c)
	}
	log.Info("no upstream API key: translation disabled")
	return nil
}

func translatorModel(c *cli.Context, fallback string) string {
	if model := c.String(flagModel); model != "" {
		return model
	}
	return fallback
}
