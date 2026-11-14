package llm

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithHeaderReachesLLMRequest(t *testing.T) {
	var got []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("X-VA-Voice"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Готов"},"finish_reason":"stop"}]}`))
	}))
	defer s.Close()
	c := Client{BaseURL: s.URL + "/v1", Model: "m", APIKey: "k"}
	if _, err := c.Complete(WithHeader(t.Context(), "X-VA-Voice", "male"), []Message{{Role: "user", Content: "Привет"}}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Complete(t.Context(), []Message{{Role: "user", Content: "Привет"}}, false); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "male" || got[1] != "" {
		t.Fatalf("headers %q", got)
	}
}
