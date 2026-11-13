package bridge

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	requeststat "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/request_stat"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
)

func TestVAEllipsisDetect(t *testing.T) {
	c := DefaultEllipsisFollowup()
	for text, want := range map[string]bool{
		"А музыку":             true,
		"а музыку?":            true,
		"Алиса, а музыку":      true,
		"ну а погоду":          true,
		"а кто написал музыку": true,
		"А включи музыку":      false,
		"а поставь таймер":     false,
		"а громче":             false,
		"включи музыку":        false,
		"музыку":               false,
		"а":                    false,
		"а какая музыка была в том фильме": false,
	} {
		if got := c.IsEllipsis(text); got != want {
			t.Errorf("%q: %v", text, got)
		}
	}
	c.Enabled = false
	if c.IsEllipsis("а музыку") {
		t.Fatal("disabled")
	}
	var nilc *EllipsisFollowup
	if nilc.IsEllipsis("а музыку") || nilc.InWindow(time.Now(), time.Now()) {
		t.Fatal("nil config")
	}
}

func TestVAEllipsisWindow(t *testing.T) {
	c := DefaultEllipsisFollowup()
	now := time.Now()
	if !c.InWindow(now.Add(-14*time.Second), now) || c.InWindow(now.Add(-16*time.Second), now) || c.InWindow(time.Time{}, now) || c.InWindow(now.Add(time.Second), now) {
		t.Fatal("window")
	}
}

func TestVAEllipsisSkipsOnlyNamedRules(t *testing.T) {
	e := &Engine{}
	skip := WithSkipRules(t.Context(), []string{"yandex_services"})
	if d := e.Decide(t.Context(), "А музыку"); d.Rule != "yandex_services" {
		t.Fatalf("outside the window: %+v", d)
	}
	if d := e.Decide(skip, "А музыку"); d.Rule != "" || d.Source == "rule" { // no classifier here: classifier_error
		t.Fatalf("ellipsis must reach the classifier: %+v", d)
	}
	for _, text := range []string{"включи музыку", "а включи музыку"} {
		if d := e.Decide(t.Context(), text); d.Route != "yandex" {
			t.Fatalf("%q: %+v", text, d)
		}
	}
	if d := e.Decide(skip, "хватит"); d.Route != "yandex" || d.Rule != "speaker_playback" {
		t.Fatalf("other rules must stay: %+v", d)
	}
}

type vaSkipRecorder struct{ got chan []string }

func (r *vaSkipRecorder) Decide(ctx context.Context, string string) RouteDecision {
	r.got <- skipRulesFrom(ctx)
	return RouteDecision{Route: "yandex", Source: "rule"}
}
func (r *vaSkipRecorder) ResolveDecision(context.Context, string, RouteDecision) (Resolution, error) {
	return Resolution{Route: "yandex"}, nil
}
func (r *vaSkipRecorder) Resolve(ctx context.Context, s string) (Resolution, error) {
	return r.ResolveDecision(ctx, s, r.Decide(ctx, s))
}

func vaStatEnded(id string) []byte {
	return aa(&client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: newID()}, Event: &client.TClientMessage_TEvent_RequestStat{RequestStat: &requeststat.TRequestStat{RequestMessageId: id, Timestamps: &requeststat.TRequestStat_TTimestamps{OnSoundPlayerEndTime: new("1761930000000")}}}}}})
}

func TestVAEllipsisNativeWindow(t *testing.T) {
	r := &vaSkipRecorder{got: make(chan []string, 10)}
	n, _, _ := vaChans(t, r)
	ef := DefaultEllipsisFollowup()
	n.Ellipsis = &ef
	ask := func(id, text string) []string {
		t.Helper()
		_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice(id))
		_ = n.Server(t.Context(), websocket.MessageBinary, vaASR(id, text))
		select {
		case s := <-r.got:
			return s
		case <-time.After(2 * time.Second):
			t.Fatal("no decision")
		}
		return nil
	}
	if s := ask("e1", "а музыку"); s != nil {
		t.Fatalf("no answer yet: %v", s)
	}
	_ = n.Client(t.Context(), websocket.MessageBinary, vaStatEnded("e1"))
	if s := ask("e2", "А музыку"); !slices.Equal(s, []string{"yandex_services"}) {
		t.Fatalf("in the window: %v", s)
	}
	if s := ask("e3", "а включи музыку"); s != nil {
		t.Fatalf("explicit command: %v", s)
	}
	n.mu.Lock()
	n.lastAnswer = time.Now().Add(-20 * time.Second)
	n.mu.Unlock()
	if s := ask("e4", "а музыку"); s != nil {
		t.Fatalf("window expired: %v", s)
	}
	n.Ellipsis = nil
	_ = n.Client(t.Context(), websocket.MessageBinary, vaStatEnded("e4"))
	if s := ask("e5", "а музыку"); s != nil {
		t.Fatalf("feature off: %v", s)
	}
}
