package llm

import "context"

type extraHeadersKey struct{}

// WithHeader (VA patch 0003) adds an HTTP header to LLM requests made with ctx.
// The bridge uses it to tell our core that the answer will be spoken by the
// local male voice («X-VA-Voice: male»).
func WithHeader(ctx context.Context, name, value string) context.Context {
	prev, _ := ctx.Value(extraHeadersKey{}).([][2]string)
	next := append(append([][2]string(nil), prev...), [2]string{name, value})
	return context.WithValue(ctx, extraHeadersKey{}, next)
}

func extraHeaders(ctx context.Context) [][2]string {
	h, _ := ctx.Value(extraHeadersKey{}).([][2]string)
	return h
}

// Header returns the value of an extra header set with WithHeader ("" if absent).
func Header(ctx context.Context, name string) string {
	for _, h := range extraHeaders(ctx) {
		if h[0] == name {
			return h[1]
		}
	}
	return ""
}
