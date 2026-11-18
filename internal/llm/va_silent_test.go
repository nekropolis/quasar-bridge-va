package llm

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVASilentEmptyAnswer(t *testing.T) {
	var gotWake string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotWake = r.Header.Get("X-VA-Wake-At")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}],"va_silent":true}`))
	}))
	defer s.Close()
	c := Client{BaseURL: s.URL, Model: "fixture"}
	ctx := WithHeader(t.Context(), "X-VA-Wake-At", "1763445102000")
	if _, err := c.Complete(ctx, []Message{{Role: "user", Content: "q"}}, false); !errors.Is(err, ErrSilent) {
		t.Fatalf("err=%v, want ErrSilent", err)
	}
	if gotWake != "1763445102000" || Header(ctx, "X-VA-Wake-At") != "1763445102000" {
		t.Fatalf("wake header=%q", gotWake)
	}
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":""}}]}`))
	}))
	defer plain.Close()
	c = Client{BaseURL: plain.URL, Model: "fixture"}
	if _, err := c.Complete(t.Context(), []Message{{Role: "user", Content: "q"}}, false); err == nil || errors.Is(err, ErrSilent) {
		t.Fatalf("empty without va_silent must stay an error, got %v", err)
	}
}
