package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	asr "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result"
	recognition "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result/common"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"

	"quasarbridge/internal/wire"
)

func vaVoice(id string) []byte {
	return aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: id, StreamId: new(int64(1))}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Type: common.EEventType_voice_input.Enum()}}, Format: new("audio/opus")}}}}})
}
func vaASR(id, text string) []byte {
	return aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: id}, Event: &server.TServerMessage_TEvent_AsrResult{AsrResult: &asr.TAsrResult{EndOfUtt: new(true), Recognition: []*recognition.TAsrRecognition{{Normalized: new(text)}}}}}}})
}

func vaSession(t *testing.T, called *int) (*Native, chan wire.Frame, chan wire.Frame) {
	up, down := make(chan wire.Frame, 100), make(chan wire.Frame, 100)
	n := NewNative(t.Context(), func(_ context.Context, _ websocket.MessageType, b []byte) error {
		up <- wire.Decode(b, wire.ClientToServer)
		return nil
	}, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		down <- wire.Decode(b, wire.ServerToClient)
		return nil
	}, resolverFunc(func(context.Context, string) (Resolution, error) {
		*called++
		return Resolution{Route: "llm", Answer: "Готово."}, nil
	}))
	t.Cleanup(n.Close)
	return n, up, down
}

func TestVAEmptyFollowUpClosesSilently(t *testing.T) {
	calls := 0
	n, up, down := vaSession(t, &calls)
	n.listenArmed = time.Now()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("f1"))
	if k := (<-up).Kind(); k != "VoiceInput" {
		t.Fatalf("first up=%s", k)
	}
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("f1", ""))
	<-down
	sawCancel := false
	deadline := time.After(2 * time.Second)
	for !sawCancel {
		select {
		case f := <-up:
			switch f.Kind() {
			case "CancelRequest":
				sawCancel = true
			case "ContinueRequest", "TtsGenerate":
				t.Fatalf("unexpected %s", f.Kind())
			}
		case <-deadline:
			t.Fatal("no cancel")
		}
	}
	for {
		select {
		case f := <-down:
			if r := f.Server.GetEvent().GetAliceResponse(); r != nil {
				if r.GetVoiceResponse().GetShouldListen() || r.GetVoiceResponse().GetHasVoiceResponse() {
					t.Fatalf("must close without speech/listen: %v", r.GetVoiceResponse())
				}
				if calls != 0 {
					t.Fatal("resolver called")
				}
				select {
				case f := <-up:
					if f.Kind() == "TtsGenerate" || f.Kind() == "ContinueRequest" {
						t.Fatalf("unexpected %s", f.Kind())
					}
				case <-time.After(100 * time.Millisecond):
				}
				if !n.listenArmed.IsZero() {
					t.Fatal("must not re-arm (loop)")
				}
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no silent response")
		}
	}
}

func TestVAEmptyFirstRequestStillGoesToYandex(t *testing.T) {
	calls := 0
	n, up, down := vaSession(t, &calls)
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("f2"))
	<-up
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("f2", ""))
	<-down
	select {
	case f := <-up:
		if f.Kind() != "ContinueRequest" {
			t.Fatalf("kind=%s", f.Kind())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no continue")
	}
}

func TestVANonEmptyFollowUpRoutesNormally(t *testing.T) {
	calls := 0
	n, up, down := vaSession(t, &calls)
	n.listenArmed = time.Now()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("f3"))
	<-up
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("f3", "что в списке покупок"))
	<-down
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-down:
			if r := f.Server.GetEvent().GetAliceResponse(); r != nil {
				if calls != 1 || !r.GetVoiceResponse().GetHasVoiceResponse() {
					t.Fatalf("calls=%d resp=%v", calls, r.GetVoiceResponse())
				}
				return
			}
		case <-up:
		case <-deadline:
			t.Fatal("no answer")
		}
	}
}
