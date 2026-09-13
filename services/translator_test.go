package services

import (
	"errors"
	"strings"
	"testing"
)

func TestBuildUserPromptNumbersLinesAndCarriesContext(t *testing.T) {
	p := BuildUserPrompt(BatchRequest{
		TargetName: "Portuguese", SourceLang: "en",
		Context:  []string{"Previous line."},
		Glossary: []string{"Hildy", "Walter"},
		Lines:    []string{"Hello there, ⏎ General Kenobi.", "Fine."},
	})
	for _, want := range []string{"1: Hello there, ⏎ General Kenobi.", "2: Fine.", "Previous line.", "Hildy", "Walter", "Portuguese"} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt lacks %q:\n%s", want, p)
		}
	}
}

func TestParseReply(t *testing.T) {
	got, err := ParseReply("1: Olá, ⏎ General Kenobi.\n2: Tudo bem.\n", 2)
	if err != nil || len(got) != 2 || got[0] != "Olá, ⏎ General Kenobi." || got[1] != "Tudo bem." {
		t.Fatalf("got %v err %v", got, err)
	}
	// tolerated noise: blank lines, code fences, numbering with a dot
	got, err = ParseReply("```\n1. Olá\n\n2. Tudo bem\n```", 2)
	if err != nil || got[1] != "Tudo bem" {
		t.Fatalf("got %v err %v", got, err)
	}
	if _, err := ParseReply("1: only one", 2); !errors.Is(err, ErrLineMismatch) {
		t.Fatalf("expected ErrLineMismatch, got %v", err)
	}
	if _, err := ParseReply("1: a\n3: b", 2); !errors.Is(err, ErrLineMismatch) {
		t.Fatalf("wrong numbering must be a mismatch, got %v", err)
	}
}
