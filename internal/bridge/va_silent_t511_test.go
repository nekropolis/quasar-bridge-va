package bridge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"quasarbridge/internal/llm"
)

type silentRouted struct {
	mu   sync.Mutex
	wake string
}

func (r *silentRouted) Decide(context.Context, string) RouteDecision {
	return RouteDecision{Route: "llm", Source: "rule"}
}
func (r *silentRouted) ResolveDecision(ctx context.Context, _ string, _ RouteDecision) (Resolution, error) {
	r.mu.Lock()
	r.wake = llm.Header(ctx, "X-VA-Wake-At")
	r.mu.Unlock()
	return Resolution{Route: "llm"}, llm.ErrSilent
}
func (r *silentRouted) Resolve(ctx context.Context, s string) (Resolution, error) {
	return r.ResolveDecision(ctx, s, r.Decide(ctx, s))
}

// t511: core's va_silent closes the request without the "no answer from the local model" phrase,
// and the core gets the wake time of this voice request.
func TestVASilentFromCoreNoErrorPhrase(t *testing.T) {
	r := &silentRouted{}
	n, _, down := vaChans(t, r)
	before := time.Now().UnixMilli()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("s1"))
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("s1", "включи горячую воду"))
	a := vaAnswer(t, down)
	if a.GetVoiceResponse().GetHasVoiceResponse() || a.GetVoiceResponse().GetOutputSpeech().GetText() != "" {
		t.Fatalf("must be silent, got %q", a.GetVoiceResponse().GetOutputSpeech().GetText())
	}
	if a.GetVoiceResponse().GetShouldListen() {
		t.Fatal("silent close must not reopen the mic")
	}
	r.mu.Lock()
	wake := r.wake
	r.mu.Unlock()
	ms, err := strconv.ParseInt(wake, 10, 64)
	if err != nil || ms < before-1000 || ms > time.Now().UnixMilli() {
		t.Fatalf("X-VA-Wake-At=%q", wake)
	}
}
