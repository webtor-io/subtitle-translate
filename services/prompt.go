package services

import (
	"fmt"
	"strings"
)

// PromptVersion is part of the artifact key: changing the prompt — or
// anything else that changes the text we would produce for the same track
// URL — invalidates cached translations. v2: web-ui reordered translation
// sources (the sidecar now outranks a hash-matched OpenSubtitles upload,
// whose text routinely carries injected ads), and the source is not part
// of the key, so artifacts translated from the old source had to go.
const PromptVersion = "v2"

func BuildSystemPrompt(targetName string) string {
	return fmt.Sprintf(`You translate film and TV subtitles into %s.
Rules:
- Translate every numbered line; output exactly the same number of lines, each as "<number>: <translation>", nothing else.
- Keep the " ⏎ " token where a line break belongs; do not add or remove tokens.
- Keep names from the glossary unchanged unless the language requires inflection.
- Preserve tone, register and profanity; do not soften, explain or add notes.
- Lines are consecutive dialogue; use the previous lines for context but do not translate them.
- If a line is not translatable (a name, a number), copy it as is.`, targetName)
}

func BuildUserPrompt(req BatchRequest) string {
	var b strings.Builder
	if req.SourceLang != "" {
		fmt.Fprintf(&b, "Source language: %s\n", req.SourceLang)
	}
	if len(req.Glossary) > 0 {
		fmt.Fprintf(&b, "Glossary (character names): %s\n", strings.Join(req.Glossary, ", "))
	}
	if len(req.Context) > 0 {
		b.WriteString("Previous lines (context only):\n")
		for _, c := range req.Context {
			b.WriteString("- " + c + "\n")
		}
	}
	fmt.Fprintf(&b, "Translate into %s:\n", req.TargetName)
	for i, l := range req.Lines {
		fmt.Fprintf(&b, "%d: %s\n", i+1, l)
	}
	return b.String()
}
