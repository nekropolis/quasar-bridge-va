package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
)

func contextWithCancel(n *Native) (context.Context, context.CancelFunc) {
	return context.WithCancel(n.ctx)
}

func vaText(id, text string) []byte {
	return aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: id}, Event: &client.TClientMessage_TEvent_TextInput{TextInput: &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Type: common.EEventType_text_input.Enum(), Text: new(text)}}}}}}})
}

func TestVAStopRegex(t *testing.T) {
	for _, s := range []string{"стоп", "Стоп.", "алиса стоп", "хватит", "замолчи", "отмена", "тихо", "хватит пожалуйста"} {
		if !vaIsStop(s) {
			t.Errorf("must match %q", s)
		}
	}
	for _, s := range []string{"стоп кран", "останови музыку", "тише", "хватит ли молока", "", "сделай тихо в зале"} {
		if vaIsStop(s) {
			t.Errorf("must not match %q", s)
		}
	}
}

func TestVAStopFollowUpClosesSilently(t *testing.T) {
	calls := 0
	n, up, down := vaSession(t, &calls)
	n.listenArmed = time.Now()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("s1"))
	<-up
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("s1", "стоп"))
	<-down
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-up:
			if f.Kind() == "ContinueRequest" || f.Kind() == "TtsGenerate" {
				t.Fatalf("stop must not go to Yandex/TTS: %s", f.Kind())
			}
		case f := <-down:
			if r := f.Server.GetEvent().GetAliceResponse(); r != nil {
				if r.GetVoiceResponse().GetShouldListen() || r.GetVoiceResponse().GetHasVoiceResponse() || calls != 0 {
					t.Fatalf("must close silently: calls=%d %v", calls, r.GetVoiceResponse())
				}
				return
			}
		case <-deadline:
			t.Fatal("no silent response")
		}
	}
}

func TestVAStopOutsideWindowGoesToResolver(t *testing.T) {
	calls := 0
	n, up, down := vaSession(t, &calls)
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("s2"))
	<-up
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("s2", "стоп"))
	<-down
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-up:
		case f := <-down:
			if f.Server.GetEvent().GetAliceResponse() != nil {
				if calls != 1 {
					t.Fatalf("outside window stop must be routed as before, calls=%d", calls)
				}
				return
			}
		case <-deadline:
			t.Fatal("no answer")
		}
	}
}

func vaLocal(n *Native, id, tts string) *nativeRequest {
	ctx, cancel := contextWithCancel(n)
	q := &nativeRequest{id: id, local: true, started: true, ttsID: tts, ctx: ctx, cancel: cancel, expires: time.NewTimer(time.Hour)}
	n.mu.Lock()
	n.requests[id] = q
	n.mu.Unlock()
	return q
}

func TestVANewVoiceInterruptsOurSpeech(t *testing.T) {
	calls := 0
	n, up, _ := vaSession(t, &calls)
	speaking := vaLocal(n, "old", "tts-old")
	done := vaLocal(n, "done", "tts-done")
	done.played = true
	music := vaLocal(n, "music", "tts-music")
	music.soundDirective = &soundDirective{}
	cloud := vaLocal(n, "cloud", "tts-cloud")
	cloud.cloud = true
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("new"))
	cancels := map[string]bool{}
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case f := <-up:
			if c := f.Client.GetEvent().GetCancelRequest(); c != nil {
				cancels[c.GetRequestMessageId()] = true
			}
			if f.Kind() == "VoiceInput" {
				break loop
			}
		case <-timeout:
			break loop
		}
	}
	if !speaking.canceled || !cancels["tts-old"] {
		t.Fatalf("our speech must be canceled: %v %v", speaking.canceled, cancels)
	}
	if done.canceled || music.canceled || cloud.canceled || cancels["tts-done"] || cancels["tts-music"] || cancels["tts-cloud"] {
		t.Fatal("finished/music/cloud requests must stay untouched")
	}
}

func TestVATextInputDoesNotInterrupt(t *testing.T) {
	calls := 0
	n, _, _ := vaSession(t, &calls)
	speaking := vaLocal(n, "old2", "tts-old2")
	_ = n.Client(t.Context(), websocket.MessageBinary, vaText("txt", "привет"))
	if speaking.canceled {
		t.Fatal("text input must not interrupt")
	}
}
