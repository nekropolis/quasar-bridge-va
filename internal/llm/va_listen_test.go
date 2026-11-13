package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"quasarbridge/internal/reply"
)

func TestVAListenFlagSuppressesListening(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		structured bool
		want       bool
	}{
		{"false", `{"choices":[{"message":{"role":"assistant","content":"Готово."},"finish_reason":"stop"}],"va_listen":false}`, false, false},
		{"true", `{"choices":[{"message":{"role":"assistant","content":"Включить?"},"finish_reason":"stop"}],"va_listen":true}`, false, true},
		{"absent", `{"choices":[{"message":{"role":"assistant","content":"Ответ."},"finish_reason":"stop"}]}`, false, true},
		{"structured ignored", `{"choices":[{"message":{"role":"assistant","content":"{\"route\":\"llm\"}"},"finish_reason":"stop"}],"va_listen":false}`, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			ctx := reply.WithPolicy(context.Background())
			c := &Client{BaseURL: srv.URL, Model: "m"}
			if _, err := c.Complete(ctx, []Message{{Role: "user", Content: "q"}}, tc.structured); err != nil {
				t.Fatal(err)
			}
			if got := reply.ListeningAllowed(ctx); got != tc.want {
				t.Fatalf("ListeningAllowed=%v want %v", got, tc.want)
			}
		})
	}
}
