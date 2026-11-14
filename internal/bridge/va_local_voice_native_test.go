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
				Synth: func(_ context.Context, text string) (SynthResult, error) {
					synthText = text
					if tc.fail {
						return SynthResult{}, errors.New("boom")
					}
					return SynthResult{URL: audio}, nil
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

func TestNativeLocalVoiceListensAfterAudioAndKeepsRule(t *testing.T) {
	cfg := DefaultRoutingConfig()
	cfg.DefaultRoute = "yandex"
	if err := json.Unmarshal([]byte(`[
		{"name":"dark_side_to_llm","route":"llm","match":{"contains_any":["тёмную сторону"]}},
		{"name":"timer_to_yandex","route":"yandex","match":{"contains_any":["таймер"]}}
	]`), &cfg.Rules); err != nil {
		t.Fatal(err)
	}
	e := &Engine{LLM: completionFunc(func(context.Context, []llm.Message, bool) (string, error) { return "Да пребудет с тобой сила", nil })}
	if err := e.SetRouting(cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	send := func(context.Context, websocket.MessageType, []byte) error { return nil }
	n := NewNative(ctx, send, send, e)
	defer n.Close()
	n.ShouldListen = true
	listened := make(chan struct{}, 4)
	var mu sync.Mutex
	synths := 0
	n.LocalVoice = &LocalVoice{Enabled: true, Rules: []string{"dark_side_to_llm"}, TimeoutMS: 2000, ListenSlackMS: 1, FollowupS: 5,
		Synth: func(context.Context, string) (SynthResult, error) {
			mu.Lock()
			synths++
			mu.Unlock()
			return SynthResult{URL: "http://192.168.0.151:8091/voice/a.mp3", Duration: 50 * time.Millisecond}, nil
		},
		Listen: func(context.Context) error { listened <- struct{}{}; return nil }}
	decisions := make(chan [2]string, 8)
	n.OnRoute = func(id, route string) { decisions <- [2]string{id, route} }
	say := func(id, text string) {
		t.Helper()
		req := &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{}}}
		input := &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: id}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: req}}
		if err := n.Client(ctx, websocket.MessageBinary, aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: input}})); err != nil {
			t.Fatal(err)
		}
		result := &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: id},
			Event: &server.TServerMessage_TEvent_AsrResult{AsrResult: &asr.TAsrResult{EndOfUtt: new(true),
				Recognition: []*recognition.TAsrRecognition{{Normalized: new(text)}}}}}
		if err := n.Server(ctx, websocket.MessageBinary, aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: result}})); err != nil {
			t.Fatal(err)
		}
	}
	want := func(id, route string) {
		t.Helper()
		select {
		case got := <-decisions:
			if got != [2]string{id, route} {
				t.Fatalf("decision %v, want %s %s", got, id, route)
			}
		case <-ctx.Done():
			t.Fatalf("%s: no decision", id)
		}
	}
	waitListen := func() {
		t.Helper()
		select {
		case <-listened:
		case <-ctx.Done():
			t.Fatal("microphone was not opened")
		}
	}
	say("g1", "переходи на тёмную сторону")
	want("g1", "llm")
	waitListen()
	say("f1", "какая погода") // default route yandex → kept on dark_side_to_llm
	want("f1", "llm")
	waitListen() // answers of the follow-up keep the conversation open
	say("f2", "а где стоп-кран в поезде") // «стоп» inside a question keeps the dialogue
	want("f2", "llm")
	waitListen()
	say("f3", "поставь таймер") // explicit non-llm rule wins, no listen after it
	want("f3", "yandex")
	say("f4", "какая погода") // follow-up consumed once
	want("f4", "yandex")
	mu.Lock()
	if synths != 3 {
		t.Fatalf("synths=%d, want 3", synths)
	}
	mu.Unlock()
	// a new session that ends with an exit word: silent, no synth, no listen
	say("g2", "переходи на тёмную сторону")
	want("g2", "llm")
	waitListen()
	say("x1", "спасибо, всё")
	select {
	case got := <-decisions:
		t.Fatalf("exit word routed: %v", got)
	case <-listened:
		t.Fatal("microphone reopened after exit word")
	case <-time.After(300 * time.Millisecond):
	}
	mu.Lock()
	if synths != 4 {
		t.Fatalf("synths=%d, want 4", synths)
	}
	mu.Unlock()
}

func TestTakeLocalFollowCancelsPendingListen(t *testing.T) {
	n := &Native{LocalVoice: &LocalVoice{Listen: func(context.Context) error { t.Error("listen after cancel"); return nil }}, ctx: t.Context()}
	n.scheduleLocalListen("x", "r", 30*time.Millisecond)
	n.mu.Lock()
	if got := n.takeLocalFollow(true, false); got != "" {
		t.Fatalf("rule %q before listen", got)
	}
	n.mu.Unlock()
	time.Sleep(700 * time.Millisecond)
	n.localFollowRule, n.localFollowUntil = "r", time.Now().Add(time.Second)
	if n.takeLocalFollow(true, true) != "" {
		t.Fatal("tagged request must not inherit")
	}
}
