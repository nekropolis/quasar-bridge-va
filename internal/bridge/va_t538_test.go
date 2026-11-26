package bridge

import (
	"context"
	"encoding/binary"
	"math"
	"sync/atomic"
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
	"quasarbridge/internal/reply"
	"quasarbridge/internal/wire"
)

func vaLastPartListens(t *testing.T, frames [][]byte) bool {
	t.Helper()
	for _, raw := range frames {
		if part := wire.Decode(raw, wire.ServerToClient).Server.GetEvent().GetDeferredAliceResponse(); part != nil && part.GetIsLast() {
			return part.GetBaseResponse().GetShouldListen()
		}
	}
	t.Fatal("no last deferred part")
	return false
}

func vaListenCounter(n *Native) *atomic.Int32 {
	var calls atomic.Int32
	n.LocalVoice.ListenSlackMS = 10
	n.LocalVoice.Rules = []string{"wakeword_ey_sistema_to_llm", "dark_side_to_llm"}
	n.LocalVoice.Listen = func(context.Context) error { calls.Add(1); return nil }
	return &calls
}

func TestVAT538StreamListensViaListenURLNotShouldListen(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	calls := vaListenCounter(n)
	p := vaVoiceRequest(n, "lis")
	p.audioInput = vaTestOpus(9, 3)
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	n.markAudioDone(1)
	vaWait(t, "close", func() bool { return d.closed("lis") })
	if vaLastPartListens(t, d.snapshot()) {
		t.Fatal("our stream must not set ShouldListen")
	}
	vaWait(t, "listen_url", func() bool { return calls.Load() > 0 })
	n.mu.Lock()
	rule := n.localFollowRule
	n.mu.Unlock()
	if rule != "dark_side_to_llm" {
		t.Fatalf("follow rule %q", rule)
	}
}

func TestVAT538NotHeardDoesNotListen(t *testing.T) {
	for _, heard := range []string{"", "Алиса."} {
		rec := &vaOpusRec{}
		n, d := vaStreamNative(t, rec)
		calls := vaListenCounter(n)
		n.LocalVoice.STT = func(context.Context, []byte) (string, error) { return heard, nil }
		p := vaVoiceRequest(n, "nh")
		p.audioInput = vaTestOpus(9, 3)
		if !n.startASRFallback() {
			t.Fatal("fallback not started")
		}
		n.markAudioDone(1)
		vaWait(t, "close", func() bool { return d.closed("nh") })
		if got := rec.got(); len(got) == 0 || got[len(got)-1] != vaNotHeard {
			t.Fatalf("heard %q: spoken %q", heard, got)
		}
		time.Sleep(700 * time.Millisecond) // answer 0.5 s + slack
		if calls.Load() != 0 || vaLastPartListens(t, d.snapshot()) {
			t.Fatalf("heard %q: mic opened after «Не расслышал» (%d)", heard, calls.Load())
		}
	}
}

func TestVAT539ListenDespiteCoreNoListen(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	calls := vaListenCounter(n)
	p := vaVoiceRequest(n, "nolisten")
	p.audioInput = vaTestOpus(9, 3)
	p.ctx = reply.WithPolicy(p.ctx)
	reply.SuppressListening(p.ctx) // core answered va_listen=false
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	n.markAudioDone(1)
	vaWait(t, "close", func() bool { return d.closed("nolisten") })
	// t539: like speakLocal, va_listen=false only drops ShouldListen; listen_url still opens.
	vaWait(t, "listen_url despite va_listen=false", func() bool { return calls.Load() > 0 })
	if vaLastPartListens(t, d.snapshot()) {
		t.Fatal("ShouldListen must stay false")
	}
}

func vaTone(sec float64, amp float32) []float32 {
	out := make([]float32, int(sec*16000))
	for i := range out {
		out[i] = amp * float32(math.Sin(float64(i)*2*math.Pi*220/16000))
	}
	return out
}

func TestVAT538SpeechEnded(t *testing.T) {
	cat := func(parts ...[]float32) []float32 {
		var out []float32
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	if !vaSpeechEnded(cat(vaTone(.3, .001), vaTone(.5, .3), vaTone(.9, .001)), 16000) {
		t.Fatal("speech + 0.9 s silence must end")
	}
	if vaSpeechEnded(cat(vaTone(.3, .001), vaTone(.5, .3), vaTone(.5, .001)), 16000) {
		t.Fatal("0.5 s pause is not the end")
	}
	if vaSpeechEnded(vaTone(2, .001), 16000) {
		t.Fatal("silence only is not speech")
	}
	if vaSpeechEnded(cat(vaTone(.3, .001), vaTone(1.2, .3)), 16000) {
		t.Fatal("still speaking")
	}
}

func TestVAT538WaitCap(t *testing.T) {
	old := vaASRWaitCap
	vaASRWaitCap = 300 * time.Millisecond
	t.Cleanup(func() { vaASRWaitCap = old })
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	p := vaVoiceRequest(n, "cap")
	p.audioInput = vaTestOpus(9, 3) // the station never closes its stream
	start := time.Now()
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	vaWait(t, "close", func() bool { return d.closed("cap") })
	if el := time.Since(start); el > 2*time.Second || el < 250*time.Millisecond {
		t.Fatalf("waited %v, cap 300 ms", el)
	}
	if vaASRWaitCapDefault := old; vaASRWaitCapDefault != 3*time.Second {
		t.Fatalf("default cap %v, want 3 s", vaASRWaitCapDefault)
	}
}

func TestVAT538ForceWaitsForYandexEndOfSpeech(t *testing.T) {
	t.Setenv("QUASAR_ASR_FORCE_FALLBACK", "1")
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	inner := &request.TAliceRequestProto{
		Header:  &request.TAliceRequestProto_THeader{RequestId: new("app"), DialogId: new("d")},
		Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Type: new(common.EEventType_voice_input)}},
	}
	tev := &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: "evt", StreamId: new(int64(1))}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: inner}}
	if err := n.Client(context.Background(), websocket.MessageBinary, aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: tev}})); err != nil {
		t.Fatal(err)
	}
	audio := append(binary.BigEndian.AppendUint32(nil, 1), vaTestOpus(9, 3)...)
	if err := n.Client(context.Background(), websocket.MessageBinary, audio); err != nil {
		t.Fatal(err)
	}
	time.Sleep(900 * time.Millisecond) // the old stand fired after 0.6 s, before the phrase
	if len(rec.got()) != 0 {
		t.Fatal("force must wait for Yandex end of speech")
	}
	eou := aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: "asr", RefMessageId: "evt"},
		Event:  &server.TServerMessage_TEvent_AsrResult{AsrResult: &asr.TAsrResult{EndOfUtt: new(true), Recognition: []*recognition.TAsrRecognition{{Normalized: new("какая погода")}}}}}}})
	start := time.Now()
	if err := n.Server(context.Background(), websocket.MessageBinary, eou); err != nil {
		t.Fatal(err)
	}
	// Audio is complete at EOU: no waiting for the station to close its stream.
	vaWait(t, "close", func() bool { return d.closed("evt") })
	if el := time.Since(start); el > time.Second {
		t.Fatalf("answer took %v after EOU", el)
	}
	vaCheckSpeech(t, d.snapshot(), "evt", "Ответ после отказа")
}
