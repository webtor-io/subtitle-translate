package services

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNotConfiguredHandlerReturns501(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/abc/movie.srt~vtt/movie.vtt~tr:pt/movie.vtt", nil)
	NotConfiguredHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := rec.Body.String(); got != "translation is not configured\n" {
		t.Fatalf("body=%q", got)
	}
}
