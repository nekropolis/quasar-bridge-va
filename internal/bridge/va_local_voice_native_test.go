package bridge

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"sync"
	"testing"
	"time"

	"quasarbridge/internal/llm"

	"github.com/coder/websocket"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	asr "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result"
	recognition "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result/common"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
)

func TestNativeLocalVoiceUsesAudioPlayOrFallsBack(t *testing.T) {
	const audio = "http://192.168.0.151:8091/voice/0123abcd.mp3"
	for _, tc := range []struct {
		name, text string
		fail       bool
		wantAudio  bool
		wantSynth  bool
	}{
		{"selected-rule", "переходи на тёмную сторону как дела", false, true, true},
		{"synth-error-falls-back", "переходи на тёмную сторону как дела", true, false, true},
		{"other-llm-rule", "спроси ии как дела", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultRoutingConfig()
			cfg.DefaultRoute = "yandex"
			if err := json.Unmarshal([]byte(`[
				{"name":"dark_side_to_llm","route":"llm","match":{"contains_any":["тёмную сторону"]}},
				{"name":"force_to_llm","route":"llm","match":{"contains_any":["спроси ии"]}}
			]`), &cfg.Rules); err != nil {
				t.Fatal(err)
			}
			e := &Engine{LLM: completionFunc(func(context.Context, []llm.Message, bool) (string, error) { return "Всё хорошо", nil })}
			if err := e.SetRouting(cfg); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var mu sync.Mutex
			var down bytes.Buffer
			up := func(context.Context, websocket.MessageType, []byte) error { return nil }
			toStation := func(_ context.Context, _ websocket.MessageType, b []byte) error {
				mu.Lock()
				down.Write(b)
				mu.Unlock()
				return nil
			}
			n := NewNative(ctx, up, toStation, e)
			defer n.Close()
			var synthText string
			n.LocalVoice = &LocalVoice{Enabled: true, Rules: []string{"dark_side_to_llm", "wakeword_ey_sistema_to_llm"}, TimeoutMS: 2000,
				Synth: func(_ context.Context, text string) (string, error) {
					synthText = text
					if tc.fail {
						return "", errors.New("boom")
					}
					return audio, nil
				}}
			req := &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{}}}
			input := &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: "r1"}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: req}}
			if err := n.Client(ctx, websocket.MessageBinary, aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: input}})); err != nil {
				t.Fatal(err)
			}
			result := &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: "r1"},
				Event: &server.TServerMessage_TEvent_AsrResult{AsrResult: &asr.TAsrResult{EndOfUtt: new(true),
					Recognition: []*recognition.TAsrRecognition{{Normalized: new(tc.text)}}}}}
			if err := n.Server(ctx, websocket.MessageBinary, aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: result}})); err != nil {
				t.Fatal(err)
			}
			answered := func() bool {
				mu.Lock()
				defer mu.Unlock()
				return bytes.Contains(down.Bytes(), []byte("Всё хорошо"))
			}
			for !answered() {
				if ctx.Err() != nil {
					t.Fatal("no answer sent to station")
				}
				time.Sleep(10 * time.Millisecond)
			}
			mu.Lock()
			got := down.Bytes()
			hasAudio := bytes.Contains(got, []byte(audio)) && bytes.Contains(got, []byte("audio_play"))
			mu.Unlock()
			if hasAudio != tc.wantAudio {
				t.Fatalf("audio_play=%v, want %v", hasAudio, tc.wantAudio)
			}
			if (synthText == "Всё хорошо") != tc.wantSynth {
				t.Fatalf("synth text %q, wantSynth=%v", synthText, tc.wantSynth)
			}
		})
	}
}
