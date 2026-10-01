package services

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openai/openai-go/v3/option"
)

type openAIReply struct {
	status, reason, text string
	refusal              bool
	code                 int
}

type openAIFixture struct {
	srv    *httptest.Server
	mu     sync.Mutex
	bodies []map[string]any
}

func fakeOpenAI(t *testing.T, replies ...openAIReply) *openAIFixture {
	t.Helper()
	f := &openAIFixture{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" || r.Header.Get("Authorization") != "Bearer o" {
			t.Error("incorrect Responses endpoint or credential")
		}
		data, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(data, &body); err != nil {
			t.Error(err)
		}
		f.mu.Lock()
		i := len(f.bodies)
		f.bodies = append(f.bodies, body)
		f.mu.Unlock()
		if i >= len(replies) {
			i = len(replies) - 1
		}
		reply := replies[i]
		w.Header().Set("Content-Type", "application/json")
		if reply.code != 0 {
			w.WriteHeader(reply.code)
			io.WriteString(w, `{"error":{"message":"fixture error","type":"server_error"}}`)
			return
		}
		if reply.status == "" {
			reply.status = "completed"
		}
		content := []map[string]any{{"type": "output_text", "text": reply.text}}
		if reply.refusal {
			content = append(content, map[string]any{"type": "refusal", "refusal": "cannot translate"})
		}
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_fixture", "object": "response", "status": reply.status, "model": "test-model",
			"incomplete_details": map[string]any{"reason": reply.reason},
			"output":             []map[string]any{{"type": "message", "role": "assistant", "content": content}},
			"usage":              map[string]any{"input_tokens": 11, "output_tokens": 7},
		})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *openAIFixture) requests() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.bodies...)
}

func fixtureOpenAI(t *testing.T, f *openAIFixture) *OpenAITranslator {
	c := providerTestCtx(t, "", " o ", "test-model")
	c.Set(flagOpenAIBaseURL, f.srv.URL)
	return NewOpenAITranslator(c, option.WithMaxRetries(0))
}

func subtitleBatch() BatchRequest {
	return BatchRequest{TargetLang: "pt", TargetName: "Portuguese", SourceLang: "en",
		Context: []string{"Previous dialogue."}, Glossary: []string{"Alice"}, Lines: []string{"Hi ⏎ Alice", "Bye"}}
}

func TestOpenAITranslate(t *testing.T) {
	f := fakeOpenAI(t, openAIReply{text: "1: Olá ⏎ Alice\n2: Tchau"})
	tr := fixtureOpenAI(t, f)
	res, err := tr.Translate(context.Background(), subtitleBatch())
	if err != nil || len(res.Lines) != 2 || res.Lines[0] != "Olá ⏎ Alice" || res.Lines[1] != "Tchau" || res.InputTokens != 11 || res.OutputTokens != 7 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	body := f.requests()[0]
	if body["model"] != "test-model" || body["max_output_tokens"] != 8192.0 || body["store"] != false {
		t.Fatalf("unexpected parameters: %v", body)
	}
	if body["instructions"] != BuildSystemPrompt("Portuguese") || body["input"] != BuildUserPrompt(subtitleBatch()) {
		t.Fatal("prompt changed")
	}
	for _, field := range []string{"tools", "temperature", "previous_response_id", "conversation"} {
		if _, has := body[field]; has {
			t.Fatalf("unexpected %s", field)
		}
	}
}

func TestOpenAIRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replies []openAIReply
		want    error
		calls   int
	}{
		{"repair", []openAIReply{{text: "1: only"}, {text: "1: Olá\n2: Tchau"}}, nil, 2},
		{"give_up", []openAIReply{{text: "1: only"}, {text: "garbage"}}, ErrLineMismatch, 2},
		{"out_of_order", []openAIReply{{text: "2: Tchau\n1: Olá"}}, ErrLineMismatch, 2},
		{"truncated", []openAIReply{{status: "incomplete", reason: "max_output_tokens", text: "1: Olá\n2: Tchau"}}, ErrTruncated, 1},
		{"filtered", []openAIReply{{status: "incomplete", reason: "content_filter", text: "1: Olá\n2: Tchau"}}, ErrRefused, 1},
		{"refused", []openAIReply{{refusal: true, text: "1: Olá\n2: Tchau"}}, ErrRefused, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeOpenAI(t, tc.replies...)
			res, err := fixtureOpenAI(t, f).Translate(context.Background(), subtitleBatch())
			if (tc.want == nil && err != nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
			if len(f.requests()) != tc.calls || res.InputTokens != int64(tc.calls*11) || res.OutputTokens != int64(tc.calls*7) {
				t.Fatalf("res=%+v calls=%d", res, len(f.requests()))
			}
			if tc.name == "repair" && !strings.Contains(f.requests()[1]["input"].(string), "Reminder: output exactly 2 numbered lines") {
				t.Fatal("missing repair instruction")
			}
			if tc.want != nil && len(res.Lines) != 0 {
				t.Fatal("failed reply must not publish partial cues")
			}
		})
	}
}

func TestOpenAIRejectsNonCompletedStatus(t *testing.T) {
	for _, status := range []string{"failed", "queued", "in_progress", "cancelled", "incomplete"} {
		t.Run(status, func(t *testing.T) {
			f := fakeOpenAI(t, openAIReply{status: status, text: "1: Olá\n2: Tchau"})
			res, err := fixtureOpenAI(t, f).Translate(context.Background(), subtitleBatch())
			if err == nil || errors.Is(err, ErrLineMismatch) || len(f.requests()) != 1 || res.InputTokens != 11 || len(res.Lines) != 0 {
				t.Fatalf("res=%+v err=%v calls=%d", res, err, len(f.requests()))
			}
		})
	}
}

func TestOpenAIUpstreamError(t *testing.T) {
	f := fakeOpenAI(t, openAIReply{code: 500})
	res, err := fixtureOpenAI(t, f).Translate(context.Background(), subtitleBatch())
	if err == nil || errors.Is(err, ErrLineMismatch) || len(f.requests()) != 1 || len(res.Lines) != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestOpenAICancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	c := providerTestCtx(t, "", "o", "test-model")
	tr := NewOpenAITranslator(c, option.WithBaseURL(srv.URL), option.WithMaxRetries(0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := tr.Translate(ctx, subtitleBatch()); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not stop translation")
	}
}

func TestOpenAIHandlerPreservesTimingAndCaches(t *testing.T) {
	f := fakeOpenAI(t, openAIReply{text: "1: Olá\n2: Tchau"})
	tr := fixtureOpenAI(t, f)
	source := "WEBVTT\n\n00:00:01.000 --> 00:00:02.500\nHi\n\n00:00:03.000 --> 00:00:04.750\nBye\n"
	h, src := newHandlerForTest(t, tr, source)
	h.Model = tr.Model()
	t.Cleanup(h.Runner.Close)
	path := "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt"
	do(h, "GET", path, src.URL)
	h.Runner.Wait(ArtifactKey("abc", "/movie.srt~vtt/movie.vtt", "pt", tr.Model(), PromptVersion))
	for attempt := 0; attempt < 2; attempt++ {
		response := do(h, "GET", path, src.URL)
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		doc, err := ParseVTT(strings.NewReader(response.Body.String()))
		if err != nil || len(doc.Cues) != 2 {
			t.Fatalf("doc=%v err=%v", doc, err)
		}
		original, err := ParseVTT(strings.NewReader(source))
		if err != nil {
			t.Fatal(err)
		}
		for i, cue := range doc.Cues {
			if cue.Start != original.Cues[i].Start || cue.End != original.Cues[i].End {
				t.Fatal("timestamps changed")
			}
		}
		if !strings.Contains(response.Body.String(), "Olá") || !strings.Contains(response.Body.String(), "Tchau") {
			t.Fatal("missing translations")
		}
	}
	if len(f.requests()) != 1 {
		t.Fatal("cached track was translated again")
	}
}

func TestOpenAITimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(1500 * time.Millisecond):
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"1: Olá\n2: Tchau"}]}]}`)
	}))
	defer srv.Close()
	c := providerTestCtx(t, "", "o", "test-model")
	c.Set(flagUpstreamTimeout, "1")
	tr := NewOpenAITranslator(c, option.WithBaseURL(srv.URL), option.WithMaxRetries(0))
	_, err := tr.Translate(context.Background(), subtitleBatch())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want deadline", err)
	}
}
