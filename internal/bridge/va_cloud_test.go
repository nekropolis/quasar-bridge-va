package bridge

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	directives "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/directives"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	response "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_response"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"

	"quasarbridge/internal/wire"
)

type vaRouted struct {
	source string
	mu     sync.Mutex
	fb     int
}

func (r *vaRouted) Decide(context.Context, string) RouteDecision {
	return RouteDecision{Route: "yandex", Source: r.source}
}
func (r *vaRouted) ResolveDecision(context.Context, string, RouteDecision) (Resolution, error) {
	return Resolution{Route: "yandex"}, nil
}
func (r *vaRouted) Resolve(ctx context.Context, s string) (Resolution, error) {
	return r.ResolveDecision(ctx, s, r.Decide(ctx, s))
}
func (r *vaRouted) Fallback(context.Context, string) (Resolution, error) {
	r.mu.Lock()
	r.fb++
	r.mu.Unlock()
	return Resolution{Route: "llm", Answer: "Локальный ответ"}, nil
}
func (r *vaRouted) calls() int { r.mu.Lock(); defer r.mu.Unlock(); return r.fb }

func vaMatched() []byte {
	return aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: newID()}, Event: &client.TClientMessage_TEvent_MatchedUser{MatchedUser: &request.TAliceRequestProto{}}}}})
}
func vaCloud(id, text string, dirs ...*directives.TDirective) []byte {
	return aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: id}, Event: &server.TServerMessage_TEvent_AliceResponse{AliceResponse: &response.TAliceResponseProto{Response: &response.TAliceResponseProto_TResponse{Directives: dirs}, VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{OutputSpeech: &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new(text)}}}}}}})
}

func vaChans(t *testing.T, r Resolver) (*Native, chan wire.Frame, chan wire.Frame) {
	up, down := make(chan wire.Frame, 200), make(chan wire.Frame, 200)
	n := NewNative(t.Context(), func(_ context.Context, _ websocket.MessageType, b []byte) error {
		up <- wire.Decode(b, wire.ClientToServer)
		return nil
	}, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		down <- wire.Decode(b, wire.ServerToClient)
		return nil
	}, r)
	t.Cleanup(n.Close)
	return n, up, down
}

// first spoken answer (non-empty AliceResponse) or the silent close
func vaAnswer(t *testing.T, down chan wire.Frame) *response.TAliceResponseProto {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-down:
			if r := f.Server.GetEvent().GetAliceResponse(); r != nil {
				return r
			}
		case <-deadline:
			t.Fatal("no response")
		}
	}
}

func vaEmptyFollowup(t *testing.T, n *Native, down chan wire.Frame, id string, matched int) *response.TAliceResponseProto {
	t.Helper()
	n.mu.Lock()
	n.listenArmed = time.Now()
	n.mu.Unlock()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice(id))
	for range matched {
		_ = n.Client(t.Context(), websocket.MessageBinary, vaMatched())
	}
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR(id, ""))
	return vaAnswer(t, down)
}

func TestVARepromptOnceWhenVoicePresent(t *testing.T) {
	calls := 0
	n, _, down := vaSession(t, &calls)
	rp := DefaultFollowupReprompt()
	n.Reprompt = &rp
	r := vaEmptyFollowup(t, n, down, "r1", 5)
	if got := r.GetVoiceResponse().GetOutputSpeech().GetText(); got != "Не расслышала, повтори." || !r.GetVoiceResponse().GetShouldListen() {
		t.Fatalf("reprompt=%q listen=%v", got, r.GetVoiceResponse().GetShouldListen())
	}
	// the post-reply state is set by the resolve goroutine right after the
	// response frame is sent; poll for it (never Fatal while holding n.mu)
	armed := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		n.mu.Lock()
		armed = !n.listenArmed.IsZero() && n.lastAsked
		n.mu.Unlock()
		if armed {
			break
		}
	}
	if !armed {
		t.Fatal("reprompt must reopen the mic and count as a question")
	}
	// second empty follow-up in the same chain closes silently
	r = vaEmptyFollowup(t, n, down, "r2", 5)
	if r.GetVoiceResponse().GetHasVoiceResponse() || r.GetVoiceResponse().GetShouldListen() {
		t.Fatal("second reprompt in one chain")
	}
	if calls != 0 {
		t.Fatal("resolver called")
	}
}

func TestVARepromptNeedsMatchedVoice(t *testing.T) {
	calls := 0
	n, _, down := vaSession(t, &calls)
	rp := DefaultFollowupReprompt()
	n.Reprompt = &rp
	r := vaEmptyFollowup(t, n, down, "r3", 2)
	if r.GetVoiceResponse().GetHasVoiceResponse() || r.GetVoiceResponse().GetShouldListen() {
		t.Fatal("reprompt without enough voice")
	}
}

func TestVARepromptDisabled(t *testing.T) {
	calls := 0
	n, _, down := vaSession(t, &calls)
	rp := DefaultFollowupReprompt()
	rp.Enabled = false
	n.Reprompt = &rp
	r := vaEmptyFollowup(t, n, down, "r4", 6)
	if r.GetVoiceResponse().GetHasVoiceResponse() {
		t.Fatal("reprompt while disabled")
	}
}

func vaCloudRequest(t *testing.T, n *Native, up chan wire.Frame, id, text string) {
	t.Helper()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice(id))
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR(id, text))
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-up:
			if f.Kind() == "ContinueRequest" {
				return
			}
		case <-deadline:
			t.Fatal("not sent to cloud")
		}
	}
}

// expects the spoken filler, feeds its audio, then the final local answer
func vaExpectFillerThenAnswer(t *testing.T, n *Native, up, down chan wire.Frame) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	sawCancel := false
	for {
		select {
		case f := <-up:
			if f.Kind() == "CancelRequest" {
				sawCancel = true
			}
			if g := f.Client.GetEvent().GetTtsGenerate(); g != nil {
				if g.GetText() != "Одну минуту." {
					t.Fatalf("first TTS=%q", g.GetText())
				}
				if !sawCancel {
					t.Fatal("cloud not canceled")
				}
				speech := []byte{0xf8, 0x77, 0x66, 0x55}
				feedSpeech(t, n, f.Client.GetEvent().GetHeader().GetMessageId(), 72, false, speech)
				heard := false
				for {
					select {
					case f := <-up:
						if g := f.Client.GetEvent().GetTtsGenerate(); g != nil {
							if g.GetText() != "Локальный ответ" {
								t.Fatalf("final TTS=%q", g.GetText())
							}
							if !heard {
								t.Fatal("filler audio was discarded")
							}
							return
						}
					case f := <-down:
						if f.Kind() == "opaque" && bytes.Contains(f.Raw, speech) {
							heard = true
						}
					case <-deadline:
						t.Fatal("no final answer")
					}
				}
			}
		case f := <-down:
			if f.Kind() == "opaque" && bytes.Contains(f.Raw, []byte{0xf8, 0x77, 0x66, 0x55}) {
				continue
			}
		case <-deadline:
			t.Fatal("no filler")
		}
	}
}

func TestVACloudEmptyAnswerFallsBackWithFiller(t *testing.T) {
	r := &vaRouted{source: "classifier"}
	n, up, down := vaChans(t, r)
	cf := DefaultCloudFallback()
	n.CloudFallback = &cf
	vaCloudRequest(t, n, up, "c1", "сколько лет черепахам")
	_ = n.Server(t.Context(), websocket.MessageBinary, vaCloud("c1", ""))
	vaExpectFillerThenAnswer(t, n, up, down)
	if r.calls() != 1 {
		t.Fatal("fallback not called once")
	}
}

func TestVACloudRefusalFallsBackWithFiller(t *testing.T) {
	r := &vaRouted{source: "classifier"}
	n, up, down := vaChans(t, r)
	cf := DefaultCloudFallback()
	n.CloudFallback = &cf
	vaCloudRequest(t, n, up, "c2", "почему кошки мурчат")
	_ = n.Server(t.Context(), websocket.MessageBinary, vaCloud("c2", "Я не знаю, что на это ответить.", &directives.TDirective{Type: "client_action", Name: "tts_play_placeholder"}))
	vaExpectFillerThenAnswer(t, n, up, down)
}

func TestVACloudTimeoutFallsBack(t *testing.T) {
	r := &vaRouted{source: "classifier"}
	n, up, down := vaChans(t, r)
	cf := DefaultCloudFallback()
	cf.TimeoutMS = 30
	n.CloudFallback = &cf
	vaCloudRequest(t, n, up, "c3", "сколько лет черепахам")
	vaExpectFillerThenAnswer(t, n, up, down)
	// a late cloud answer is dropped
	_ = n.Server(t.Context(), websocket.MessageBinary, vaCloud("c3", "Поздний ответ"))
	select {
	case f := <-down:
		if r := f.Server.GetEvent().GetAliceResponse(); r != nil && strings.Contains(r.GetVoiceResponse().GetOutputSpeech().GetText(), "Поздний") {
			t.Fatal("late cloud answer leaked")
		}
	case <-time.After(50 * time.Millisecond):
	}
}

func vaNoFallback(t *testing.T, source, text string, resp []byte, timeout int) {
	t.Helper()
	r := &vaRouted{source: source}
	n, up, down := vaChans(t, r)
	cf := DefaultCloudFallback()
	cf.TimeoutMS = timeout
	n.CloudFallback = &cf
	vaCloudRequest(t, n, up, "x1", text)
	if resp != nil {
		_ = n.Server(t.Context(), websocket.MessageBinary, resp)
	}
	time.Sleep(80 * time.Millisecond)
	if r.calls() != 0 {
		t.Fatalf("fallback for source=%s text=%q", source, text)
	}
	for len(up) > 0 {
		if f := <-up; f.Kind() == "TtsGenerate" {
			t.Fatal("local speech")
		}
	}
	_ = down
}

func TestVACloudFallbackSkips(t *testing.T) {
	t.Run("rule_route", func(t *testing.T) {
		vaNoFallback(t, "rule", "сколько лет черепахам", vaCloud("x1", ""), 0)
	})
	t.Run("timer_text", func(t *testing.T) {
		vaNoFallback(t, "classifier", "поставь таймер на пять минут", vaCloud("x1", ""), 0)
	})
	t.Run("device_directive", func(t *testing.T) {
		vaNoFallback(t, "classifier", "сколько лет черепахам", vaCloud("x1", "", &directives.TDirective{Type: "client_action", Name: "sound_louder"}), 0)
	})
	t.Run("normal_answer", func(t *testing.T) {
		vaNoFallback(t, "classifier", "сколько лет черепахам", vaCloud("x1", "До ста лет."), 30)
	})
	t.Run("timeout_rule", func(t *testing.T) { vaNoFallback(t, "rule", "сколько лет черепахам", nil, 20) })
	t.Run("timeout_excluded", func(t *testing.T) { vaNoFallback(t, "classifier", "включи музыку", nil, 20) })
}

func TestVAEventsPostCloudAndFollowup(t *testing.T) {
	got := make(chan map[string]any, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if r.Header.Get("Authorization") != "Bearer k" {
			m["auth"] = "missing"
		}
		got <- m
	}))
	defer srv.Close()
	r := &vaRouted{source: "classifier"}
	n, up, _ := vaChans(t, r)
	n.Events = &EventSink{URL: srv.URL, APIKey: "k", Model: "station-midi"}
	vaCloudRequest(t, n, up, "e1", "сколько будет два плюс два")
	_ = n.Server(t.Context(), websocket.MessageBinary, vaCloud("e1", "Четыре."))
	select {
	case m := <-got:
		if m["event"] != "cloud" || m["result"] != "answer" || m["text"] != "сколько будет два плюс два" || m["cloud_bytes"] != float64(len("Четыре.")) || m["model"] != "station-midi" || m["auth"] != nil {
			t.Fatalf("event=%v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no cloud event")
	}
	n.mu.Lock()
	n.listenArmed = time.Now()
	n.mu.Unlock()
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("e2"))
	_ = n.Client(t.Context(), websocket.MessageBinary, vaMatched())
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("e2", ""))
	select {
	case m := <-got:
		if m["event"] != "followup" || m["result"] != "silent" || m["matched"] != float64(1) || m["text"] != nil {
			t.Fatalf("event=%v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no followup event")
	}
}

func TestVACloudRefusalInVoiceBehindCard(t *testing.T) {
	r := &vaRouted{source: "classifier"}
	n, up, down := vaChans(t, r)
	cf := DefaultCloudFallback()
	n.CloudFallback = &cf
	vaCloudRequest(t, n, up, "c9", "сорок четыре рубля минус шесть процентов")
	card := new("Вот что нашлось")
	resp := aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: "c9"}, Event: &server.TServerMessage_TEvent_AliceResponse{AliceResponse: &response.TAliceResponseProto{
		Response:      &response.TAliceResponseProto_TResponse{Cards: []*response.TAliceResponseProto_TResponse_TCard{{Text: card}}, Directives: []*directives.TDirective{{Type: "client_action", Name: "tts_play_placeholder"}}},
		VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{OutputSpeech: &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new("Я не знаю, как это объяснить.")}}}}}}})
	_ = n.Server(t.Context(), websocket.MessageBinary, resp)
	vaExpectFillerThenAnswer(t, n, up, down)
}
