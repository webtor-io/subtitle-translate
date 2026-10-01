package services

import (
	"flag"
	"testing"

	"github.com/urfave/cli"
)

func providerTestCtx(t *testing.T, anthropicKey, openAIKey, model string) *cli.Context {
	t.Helper()
	for _, name := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "OPENAI_BASE_URL", "SUBTITLE_TRANSLATE_MODEL"} {
		t.Setenv(name, "")
	}
	set := flag.NewFlagSet("translator", flag.ContinueOnError)
	for _, f := range RegisterTranslatorFlags(nil) {
		f.Apply(set)
	}
	for name, value := range map[string]string{flagAPIKey: anthropicKey, flagOpenAIAPIKey: openAIKey, flagModel: model} {
		if err := set.Set(name, value); err != nil {
			t.Fatal(err)
		}
	}
	return cli.NewContext(nil, set, nil)
}

func TestConfiguredProvider(t *testing.T) {
	for _, tc := range []struct{ name, anthropic, openai, model, wantModel, wantProvider string }{
		{"neither", "", "", "", "", ""},
		{"whitespace", " \t", " \n", "", "", ""},
		{"anthropic", " a ", "", "", "claude-haiku-4-5-20251001", "anthropic"},
		{"openai", "", " o ", "", "gpt-4.1-mini", "openai"},
		{"blank_anthropic", " \t", "o", "", "gpt-4.1-mini", "openai"},
		{"both", "a", "o", "", "claude-haiku-4-5-20251001", "anthropic"},
		{"anthropic_override", "a", "", "custom-claude", "custom-claude", "anthropic"},
		{"openai_override", "", "o", "custom-gpt", "custom-gpt", "openai"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewTranslator(providerTestCtx(t, tc.anthropic, tc.openai, tc.model))
			if tc.wantProvider == "" {
				if tr != nil {
					t.Fatal("missing credentials must return interface-nil")
				}
				return
			}
			if tr == nil || tr.Model() != tc.wantModel {
				t.Fatalf("unexpected translator/model: %T", tr)
			}
			switch tc.wantProvider {
			case "anthropic":
				if _, ok := tr.(*AnthropicTranslator); !ok {
					t.Fatalf("provider=%T", tr)
				}
			case "openai":
				if _, ok := tr.(*OpenAITranslator); !ok {
					t.Fatalf("provider=%T", tr)
				}
			}
		})
	}
}

func TestOpenAINilWithoutKey(t *testing.T) {
	for _, key := range []string{"", " \t"} {
		if tr := NewOpenAITranslator(providerTestCtx(t, "", key, "")); tr != nil {
			t.Fatal("expected nil")
		}
	}
}
