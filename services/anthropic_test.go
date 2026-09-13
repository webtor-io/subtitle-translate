package services

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/urfave/cli"
)

func testCtx(t *testing.T, key string) *cli.Context {
	t.Helper()
	set := flag.NewFlagSet("t", 0)
	set.String(flagAPIKey, key, "")
	set.String(flagModel, "test-model", "")
	set.Int(flagUpstreamTimeout, 5, "")
	set.Int(flagMaxTokens, 512, "")
	return cli.NewContext(nil, set, nil)
}

func fakeAPI(t *testing.T, replies []string) (*httptest.Server, *int32, *[]map[string]any) {
	t.Helper()
	var n int32
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		bodies = append(bodies, m)
		i := int(atomic.AddInt32(&n, 1)) - 1
		if i >= len(replies) {
			i = len(replies) - 1
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "msg", "type": "message", "role": "assistant", "model": "test-model",
			"content":     []map[string]any{{"type": "text", "text": replies[i]}},
			"stop_reason": "end_turn",
			"usage":       map[string]any{"input_tokens": 11, "output_tokens": 7},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &n, &bodies
}

func TestNilWithoutKey(t *testing.T) {
	if tr := NewAnthropicTranslator(testCtx(t, "")); tr != nil {
		t.Fatal("expected nil translator without a key")
	}
}

func TestTranslateParsesTextAndUsage(t *testing.T) {
	srv, n, bodies := fakeAPI(t, []string{"1: Olá\n2: Tchau"})
	tr := NewAnthropicTranslator(testCtx(t, "k"), option.WithBaseURL(srv.URL))
	res, err := tr.Translate(context.Background(), BatchRequest{TargetLang: "pt", TargetName: "Portuguese", Lines: []string{"Hi", "Bye"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Lines) != 2 || res.Lines[0] != "Olá" || res.InputTokens != 11 || res.OutputTokens != 7 {
		t.Fatalf("res=%+v", res)
	}
	if *n != 1 {
		t.Fatalf("calls=%d", *n)
	}
	body := (*bodies)[0]
	if body["model"] != "test-model" || body["temperature"] != 0.0 {
		t.Fatalf("body=%v", body)
	}
	if _, has := body["tools"]; has {
		t.Fatal("must be a plain text call, no tools")
	}
}

func TestTranslateRetriesOnceOnMismatch(t *testing.T) {
	srv, n, _ := fakeAPI(t, []string{"1: only", "1: Olá\n2: Tchau"})
	tr := NewAnthropicTranslator(testCtx(t, "k"), option.WithBaseURL(srv.URL))
	res, err := tr.Translate(context.Background(), BatchRequest{TargetLang: "pt", TargetName: "Portuguese", Lines: []string{"Hi", "Bye"}})
	if err != nil || len(res.Lines) != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if *n != 2 {
		t.Fatalf("calls=%d want 2", *n)
	}
}

func TestTranslateGivesUpAfterSecondMismatch(t *testing.T) {
	srv, n, _ := fakeAPI(t, []string{"1: only", "garbage"})
	tr := NewAnthropicTranslator(testCtx(t, "k"), option.WithBaseURL(srv.URL))
	_, err := tr.Translate(context.Background(), BatchRequest{TargetLang: "pt", TargetName: "Portuguese", Lines: []string{"Hi", "Bye"}})
	if !errors.Is(err, ErrLineMismatch) || *n != 2 {
		t.Fatalf("err=%v calls=%d", err, *n)
	}
}
