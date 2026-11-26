package bridge

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPCMToS16Clips(t *testing.T) {
	got := pcmToS16([]float32{0, 1, -1, 2, -2, .5})
	want := []int16{0, 32767, -32767, 32767, -32767, 16384}
	if len(got) != len(want)*2 {
		t.Fatalf("bytes %d", len(got))
	}
	for i, v := range want {
		if x := int16(binary.LittleEndian.Uint16(got[i*2:])); x != v {
			t.Fatalf("sample %d = %d, want %d", i, x, v)
		}
	}
}

func TestHTTPSTT(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"text":" включи свет "}`))
	}))
	defer srv.Close()
	text, err := HTTPSTT(srv.URL, "k", 0)(context.Background(), []byte{0, 0})
	if err != nil || text != "включи свет" {
		t.Fatalf("text=%q err=%v", text, err)
	}
	if _, err := HTTPSTT(srv.URL, "bad", 0)(context.Background(), []byte{0, 0}); err == nil {
		t.Fatal("expected auth error")
	}
}

func TestASRFallbackEnabledRequiresSTT(t *testing.T) {
	n := NewNative(context.Background(), nil, nil, nil)
	if n.asrFallbackEnabled() {
		t.Fatal("fallback must be off without STT")
	}
	n.LocalVoice = &LocalVoice{Enabled: true}
	if n.asrFallbackEnabled() {
		t.Fatal("fallback must be off without STT func")
	}
	n.LocalVoice.STT = func(context.Context, []byte) (string, error) { return "", nil }
	if !n.asrFallbackEnabled() {
		t.Fatal("fallback must be on")
	}
}

func TestASRFallbackPicksVoiceRequest(t *testing.T) {
	n := NewNative(context.Background(), nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old := &nativeRequest{id: "old", ctx: ctx, cancel: func() {}, audioInput: []byte{1}}
	now := &nativeRequest{id: "now", ctx: ctx, cancel: func() {}, audioInput: []byte{2}}
	old.wakeAt = now.wakeAt.Add(-1)
	n.requests = map[string]*nativeRequest{"old": old, "now": now}
	n.lastVoice = nil
	n.mu.Lock()
	got := n.asrFallbackRequestLocked()
	n.mu.Unlock()
	if got != now {
		t.Fatalf("picked %v", got.id)
	}
	now.started = true
	n.mu.Lock()
	got = n.asrFallbackRequestLocked()
	n.mu.Unlock()
	if got != old {
		t.Fatalf("started request must be skipped, picked %v", got)
	}
}
