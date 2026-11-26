package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	event_exception "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/event_exception"
	stream "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/stream_control"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
	"quasarbridge/internal/wire"
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

func TestStartASRFallbackAnswersInLocalVoice(t *testing.T) {
	var down [][]byte
	n := NewNative(context.Background(), func(context.Context, websocket.MessageType, []byte) error { return nil },
		func(_ context.Context, _ websocket.MessageType, data []byte) error {
			down = append(down, bytes.Clone(data))
			return nil
		}, fallbackResolver{})
	t.Cleanup(n.Close)
	var synths []string
	n.LocalVoice = &LocalVoice{Enabled: true, Synth: func(_ context.Context, text string) (SynthResult, error) {
		synths = append(synths, text)
		return SynthResult{URL: "http://x/a.mp3", Duration: time.Second}, nil
	}, STT: func(context.Context, []byte) (string, error) { return "", nil }, FallbackPhrase: "На связи тёмная сторона."}

	req := &request.TAliceRequestProto{Header: &request.TAliceRequestProto_THeader{DialogId: new("d"), RequestId: new("r")}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &nativeRequest{id: "fb", request: req, ctx: ctx, cancel: func() {}, wakeAt: time.Now(), header: &header.TEventHeader{StreamId: new(int64(1))}, audioInput: []byte("not-ogg")}
	p.expires = time.AfterFunc(time.Minute, func() {})
	n.mu.Lock()
	n.requests[p.id] = p
	n.lastVoice = p
	n.mu.Unlock()

	if !n.asrFallbackEnabled() {
		t.Fatal("fallback must be enabled")
	}
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	// Колонка закончила реплику: дальше fallback не ждёт.
	n.markAudioDone(1)
	deadline := time.Now().Add(3 * time.Second)
	for len(synths) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(synths) != 1 || !strings.HasPrefix(synths[0], "На связи тёмная сторона.") {
		t.Fatalf("synths=%q", synths)
	}
	var found bool
	for _, raw := range down {
		f := wire.Decode(raw, wire.ServerToClient)
		if f.Server == nil {
			continue
		}
		if r := f.Server.GetEvent().GetAliceResponse(); r != nil {
			for _, d := range r.GetResponse().GetDirectives() {
				if d.GetType() == "client_action" && d.GetName() == "audio_play" {
					found = true
				}
			}
			if r.GetVoiceResponse().GetHasVoiceResponse() && r.GetVoiceResponse().GetOutputSpeech().GetText() != "" {
				t.Fatalf("voice must stay muted while local MP3 plays: %q", r.GetVoiceResponse().GetOutputSpeech().GetText())
			}
		}
	}
	if !found {
		t.Fatalf("no audio_play directive in %d frames", len(down))
	}
}

func TestEventExceptionStartsASRFallbackAndIsSuppressed(t *testing.T) {
	var down [][]byte
	n := NewNative(context.Background(), func(context.Context, websocket.MessageType, []byte) error { return nil },
		func(_ context.Context, _ websocket.MessageType, data []byte) error {
			down = append(down, bytes.Clone(data))
			return nil
		}, fallbackResolver{})
	t.Cleanup(n.Close)
	var synth bool
	n.LocalVoice = &LocalVoice{Enabled: true, Synth: func(context.Context, string) (SynthResult, error) {
		synth = true
		return SynthResult{URL: "http://x/a.mp3", Duration: time.Second}, nil
	}, STT: func(context.Context, []byte) (string, error) { return "", nil }}

	inner := &request.TAliceRequestProto{
		Header:  &request.TAliceRequestProto_THeader{RequestId: new("app")},
		Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Type: new(common.EEventType_voice_input)}},
	}
	tev := &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: "evt", StreamId: new(int64(1))}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: inner}}
	voice := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: tev}}
	if err := n.Client(context.Background(), websocket.MessageBinary, aa(voice)); err != nil {
		t.Fatal(err)
	}
	audio := binary.BigEndian.AppendUint32(nil, 1)
	audio = append(audio, []byte("not-ogg")...)
	if err := n.Client(context.Background(), websocket.MessageBinary, audio); err != nil {
		t.Fatal(err)
	}
	end := &client.TClientMessage{Message: &client.TClientMessage_StreamControl{StreamControl: &stream.TStreamControl{
		StreamId: 1, MessageId: "evt", Action: &stream.TStreamControl_Close{Close: &stream.TStreamControl_TActionClose{}}}}}
	if err := n.Client(context.Background(), websocket.MessageBinary, aa(end)); err != nil {
		t.Fatal(err)
	}
	exc := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: "err", RefMessageId: "evt"},
		Event: &server.TServerMessage_TEvent_EventException{EventException: &event_exception.TEventException{
			Error: &event_exception.TEventException_TError{ErrorMessage: "asr_init_error: asr1_client: overloaded"}}}}}}
	if err := n.Server(context.Background(), websocket.MessageBinary, aa(exc)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !synth && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !synth {
		t.Fatal("fallback did not speak")
	}
	for _, raw := range down {
		if bytes.Contains(raw, []byte("asr_init_error")) {
			t.Fatal("Yandex ASR exception must not reach the speaker")
		}
	}
}
