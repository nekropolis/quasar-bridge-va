package bridge

// t515 (VA): dialog mode and cloud multiactivation verdict.
//
// Dialog mode — a follow-up after a local-voice (eugene) answer: the bridge's own
// «→ yandex» keyword rules are not applied; the route comes from our core
// (classifier request with X-VA-Dialog: 1). The core returns yandex only for an
// explicit speaker command (timer, alarm, music, volume, TV, pause); anything
// else is answered by the model.
//
// Multiactivation — when Yandex cancels this speaker's activation
// (SpotterValidation.CanceledCauseOfMultiactivation or Result=SpottingRejected),
// the speaker stays silent: no request to the core and no cloud→core fallback.

import (
	"context"
	"log/slog"

	sv "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/spotter_validation"

	"quasarbridge/internal/llm"
)

type vaDialogKey struct{}

func withVADialog(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, vaDialogKey{}, true)
	return llm.WithHeader(ctx, "X-VA-Dialog", "1")
}

func vaDialogFrom(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(vaDialogKey{}).(bool)
	return v
}

// vaYandexRules lists rule names that route to the cloud (skipped in dialog mode).
func (p *RoutingPolicy) vaYandexRules() []string {
	if p == nil {
		return nil
	}
	var out []string
	for _, r := range p.rules {
		if r.route == "yandex" {
			out = append(out, r.name)
		}
	}
	return out
}

// vaDialogKeepsDecision: in dialog mode a decision stays as is only when the core
// classifier chose the cloud (explicit speaker command) or it is a special
// bridge route (music, playback, airplay, recovery...). Everything else → model.
func vaDialogKeepsDecision(d RouteDecision) bool {
	switch d.Route {
	case "llm":
		return false
	case "yandex":
		return d.Source == "classifier"
	default:
		return d.Source != "home_disabled"
	}
}

// Spotter verdicts.
const (
	spotterOK       = "ok"
	spotterCanceled = "canceled"
	spotterRejected = "rejected"
)

func spotterVerdict(v *sv.TSpotterValidation) string {
	if v.GetCanceledCauseOfMultiactivation() {
		return spotterCanceled
	}
	if v.GetResult() != sv.TSpotterValidation_Ok {
		return spotterRejected
	}
	return spotterOK
}

// spotterSilencedLocked: this activation lost/was rejected by the cloud. Follow-ups
// opened without the wake word are exempt. Call with n.mu held.
func (p *nativeRequest) spotterSilencedLocked() bool {
	if p.localFollowRule != "" || p.afterAnswer || p.followUp || p.isText {
		return false
	}
	return p.spotter == spotterCanceled || p.spotter == spotterRejected
}

// vaSpotterSilence closes the request quietly before routing if the cloud
// cancelled this activation. Returns true when handled.
func (n *Native) vaSpotterSilence(p *nativeRequest) bool {
	n.mu.Lock()
	if p.canceled || !p.spotterSilencedLocked() {
		n.mu.Unlock()
		return false
	}
	verdict := p.spotter
	p.local, p.silent, p.route = true, true, "silent"
	n.mu.Unlock()
	n.discardSound(p)
	slog.Info("native_route", "request", requestTag(p.id), "route", "silent", "reason", "multiactivation", "verdict", verdict)
	if p.gated && !p.streaming {
		if err := n.sendCancel(p.ctx, p.id); err != nil {
			return true
		}
	}
	if err := n.reply(p, Resolution{}); err != nil {
		slog.Warn("native_reply_failed", "request", requestTag(p.id))
	}
	return true
}
