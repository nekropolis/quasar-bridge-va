package bridge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	sv "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/spotter_validation"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"

	"quasarbridge/internal/llm"
)

func vaValidation(id string, result sv.TSpotterValidation_EResult, canceled bool) []byte {
	return aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: id}, Event: &server.TServerMessage_TEvent_SpotterValidation{SpotterValidation: &sv.TSpotterValidation{Result: result, CanceledCauseOfMultiactivation: new(canceled)}}}}})
}

// dialog mode: bridge yandex rules are skipped, the core classifier is asked with X-VA-Dialog.
func TestT515DialogSkipsYandexRules(t *testing.T) {
	cfg := DefaultRoutingConfig()
	cfg.DefaultRoute = "classify"
	cfg.Rules = []RoutingRule{
		{Name: "general_temp_to_yandex", Route: "yandex", Match: RoutingMatch{ContainsAny: []string{"температур"}}},
		{Name: "home_to_llm", Route: "llm", Match: RoutingMatch{ContainsAny: []string{"горячую воду"}}},
	}
	var mu sync.Mutex
	var hdr []string
	route := `{"route":"llm"}`
	e := &Engine{LLM: completionFunc(func(ctx context.Context, _ []llm.Message, structured bool) (string, error) {
		mu.Lock()
		hdr = append(hdr, llm.Header(ctx, "X-VA-Dialog"))
		mu.Unlock()
		return route, nil
	})}
	if err := e.SetRouting(cfg); err != nil {
		t.Fatal(err)
	}
	q := "как сушить минтая сколько температура"
	if d := e.Decide(t.Context(), q); d.Route != "yandex" || d.Source != "rule" {
		t.Fatalf("outside dialog: %+v", d)
	}
	d := e.Decide(withVADialog(t.Context()), q)
	if d.Route != "llm" || d.Source != "classifier" {
		t.Fatalf("dialog: %+v", d)
	}
	if d := e.Decide(withVADialog(t.Context()), "включи горячую воду"); d.Route != "llm" || d.Source != "rule" {
		t.Fatalf("llm rules still apply in dialog: %+v", d)
	}
	route = `{"route":"yandex"}`
	if d := e.Decide(withVADialog(t.Context()), "поставь таймер на пять минут"); d.Route != "yandex" || d.Source != "classifier" || !vaDialogKeepsDecision(d) {
		t.Fatalf("core command → Alice: %+v", d)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(hdr) != 2 || hdr[0] != "1" || hdr[1] != "1" {
		t.Fatalf("X-VA-Dialog headers=%q", hdr)
	}
}

func TestT515DialogKeepsDecision(t *testing.T) {
	for _, tc := range []struct {
		d    RouteDecision
		keep bool
	}{
		{RouteDecision{Route: "yandex", Source: "classifier"}, true},
		{RouteDecision{Route: "yandex", Source: "rule"}, false},
		{RouteDecision{Route: "yandex", Source: "classifier_error"}, false},
		{RouteDecision{Route: "yandex", Source: "default"}, false},
		{RouteDecision{Route: "llm", Source: "classifier"}, false},
		{RouteDecision{Route: "playback", Source: "playback"}, true},
		{RouteDecision{Route: "yandex", Source: "home_disabled"}, false},
	} {
		if got := vaDialogKeepsDecision(tc.d); got != tc.keep {
			t.Fatalf("%+v keep=%v", tc.d, got)
		}
	}
}

type countRouted struct {
	mu sync.Mutex
	n  int
}

func (r *countRouted) Decide(context.Context, string) RouteDecision {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
	return RouteDecision{Route: "llm", Source: "rule"}
}
func (r *countRouted) ResolveDecision(context.Context, string, RouteDecision) (Resolution, error) {
	return Resolution{Route: "llm", Answer: "Ответ ядра"}, nil
}
func (r *countRouted) Resolve(ctx context.Context, s string) (Resolution, error) {
	return r.ResolveDecision(ctx, s, r.Decide(ctx, s))
}
func (r *countRouted) calls() int { r.mu.Lock(); defer r.mu.Unlock(); return r.n }

// multiactivation lost before the end of the phrase: silent, core is not asked.
func TestT515CanceledActivationSilent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		result   sv.TSpotterValidation_EResult
		canceled bool
	}{{"canceled", sv.TSpotterValidation_Ok, true}, {"rejected", sv.TSpotterValidation_SpottingRejected, false}} {
		t.Run(tc.name, func(t *testing.T) {
			r := &countRouted{}
			n, _, down := vaChans(t, r)
			_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("m1"))
			_ = n.Server(t.Context(), websocket.MessageBinary, vaValidation("m1", tc.result, tc.canceled))
			_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("m1", "какой сегодня курс доллара"))
			a := vaAnswer(t, down)
			if a.GetVoiceResponse().GetOutputSpeech().GetText() != "" || a.GetVoiceResponse().GetShouldListen() {
				t.Fatalf("must be silent: %q", a.GetVoiceResponse().GetOutputSpeech().GetText())
			}
			if r.calls() != 0 {
				t.Fatal("core was asked")
			}
		})
	}
}

func TestT515OkActivationAnswers(t *testing.T) {
	r := &countRouted{}
	n, _, down := vaChans(t, r)
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("m2"))
	_ = n.Server(t.Context(), websocket.MessageBinary, vaValidation("m2", sv.TSpotterValidation_Ok, false))
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("m2", "какой сегодня курс доллара"))
	deadline := time.After(2 * time.Second)
	for r.calls() == 0 {
		select {
		case <-deadline:
			t.Fatal("core not asked")
		case <-time.After(5 * time.Millisecond):
		}
	}
	_ = down
}

// cancel arrives after the request went to the cloud: no cloud→core fallback on timeout.
func TestT515CanceledNoCloudFallback(t *testing.T) {
	r := &vaRouted{source: "classifier"}
	n, up, _ := vaChans(t, r)
	cf := DefaultCloudFallback()
	cf.TimeoutMS = 30
	n.CloudFallback = &cf
	vaCloudRequest(t, n, up, "c9", "какой сегодня курс доллара")
	_ = n.Server(t.Context(), websocket.MessageBinary, vaValidation("c9", sv.TSpotterValidation_SpottingRejected, true))
	time.Sleep(100 * time.Millisecond)
	if r.calls() != 0 {
		t.Fatal("fallback to core after multiactivation cancel")
	}
}
