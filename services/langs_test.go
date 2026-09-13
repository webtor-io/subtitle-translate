package services

import "testing"

func TestLangName(t *testing.T) {
	for code, want := range map[string]string{"pt": "Portuguese", "ru": "Russian", "en": "English", "zh": "Chinese"} {
		if got, ok := LangName(code); !ok || got != want {
			t.Errorf("%s: got %q ok=%v", code, got, ok)
		}
	}
	for _, bad := range []string{"", "xx", "PT", "pt-BR", "eng"} {
		if _, ok := LangName(bad); ok {
			t.Errorf("%q must be unknown", bad)
		}
	}
}
