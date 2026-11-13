package bridge

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// FollowupReprompt asks once to repeat when the follow-up window ends with an
// empty transcript but the station still reported a matched voice.
type FollowupReprompt struct {
	Enabled    bool   `json:"enabled"`
	MinMatched int    `json:"min_matched"`
	Text       string `json:"text"`
}

func DefaultFollowupReprompt() FollowupReprompt {
	return FollowupReprompt{Enabled: true, MinMatched: 3, Text: "Не расслышала, повтори."}
}

// CloudFallback answers locally when a cloud reply is empty, a refusal, or late.
// Empty and timeout replacement is limited to the listed routing sources and
// skips requests whose text contains an excluded fragment. A nil value keeps
// the upstream refusal-only fallback, without a filler or a timeout.
type CloudFallback struct {
	Enabled         bool     `json:"enabled"`
	Filler          string   `json:"filler"`
	OnEmpty         bool     `json:"on_empty"`
	OnRefusal       bool     `json:"on_refusal"`
	TimeoutMS       int      `json:"timeout_ms"`
	Sources         []string `json:"sources"`
	ExcludeContains []string `json:"exclude_contains"`
}

func DefaultCloudFallback() CloudFallback {
	return CloudFallback{Enabled: true, Filler: "Одну минуту.", OnEmpty: true, OnRefusal: true, TimeoutMS: 7000,
		Sources: []string{"classifier", "classifier_error", "default", "dialogue"},
		ExcludeContains: strings.Fields("таймер будильник напомни напоминан музык песн трек радио плейлист громк тише громче пауз " +
			"стоп продолж следующ предыдущ включи выключи включить выключить поставь запусти открой закрой свет ламп телевизор канал " +
			"кондиционер пылесос розетк штор отопл котел котёл")}
}

// allowed reports whether an empty or timed-out cloud answer may be replaced.
func (c *CloudFallback) allowed(text, source string) bool {
	if c == nil || !c.Enabled || len(strings.Fields(text)) == 0 {
		return false
	}
	ok := false
	for _, item := range c.Sources {
		if item == source {
			ok = true
		}
	}
	if !ok {
		return false
	}
	lower := strings.ReplaceAll(strings.ToLower(text), "ё", "е")
	for _, item := range c.ExcludeContains {
		item = strings.ReplaceAll(strings.ToLower(strings.TrimSpace(item)), "ё", "е")
		if item != "" && strings.Contains(lower, item) {
			return false
		}
	}
	return true
}

// EventSink posts route, cloud and follow-up records to the voice core.
// Texts are not written to the bridge log.
type EventSink struct {
	URL      string
	APIKey   string
	Model    string
	HTTP     *http.Client
	inflight atomic.Int32
}

func (s *EventSink) Emit(fields map[string]any) {
	if s == nil || s.URL == "" {
		return
	}
	if s.inflight.Add(1) > 8 {
		s.inflight.Add(-1)
		return
	}
	fields["model"] = s.Model
	go func() {
		defer s.inflight.Add(-1)
		body, err := json.Marshal(fields)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if s.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+s.APIKey)
		}
		client := s.HTTP
		if client == nil {
			client = http.DefaultClient
		}
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
}

func (n *Native) event(fields map[string]any) {
	if n.Events != nil {
		n.Events.Emit(fields)
	}
}

// emitCloudLocked records one cloud-routed request. n.mu is held by the caller.
func (n *Native) emitCloudLocked(p *nativeRequest, kind string) {
	if p.cloudLogged {
		return
	}
	p.cloudLogged = true
	if n.Events == nil {
		return
	}
	n.Events.Emit(map[string]any{"event": "cloud", "result": kind, "text": p.text, "source": p.source, "cloud_bytes": p.cloudBytes, "matched": p.matched, "follow_up": p.followUp})
}

// closeFollowUp handles silence, stop words and filler inside the listen window.
// It returns true when the request was fully handled.
func (n *Native) closeFollowUp(p *nativeRequest, text string) bool {
	n.mu.Lock()
	if p.canceled {
		n.mu.Unlock()
		return true
	}
	filler := p.followUp && p.gated && !p.prevAsked && strings.TrimSpace(text) != "" && !vaIsStop(text) && n.Filler.IsFiller(text)
	if !(p.followUp && p.gated && (strings.TrimSpace(text) == "" || vaIsStop(text) || filler)) {
		n.mu.Unlock()
		return false
	}
	reason := "empty_follow_up"
	if filler {
		reason = "followup_filler"
	} else if strings.TrimSpace(text) != "" {
		reason = "stop_follow_up"
	}
	matched, partials, partialText := p.matched, p.partials, p.partialText
	if reason == "empty_follow_up" && n.Reprompt != nil && n.Reprompt.Enabled && strings.TrimSpace(n.Reprompt.Text) != "" && !n.repromptUsed && matched >= n.Reprompt.MinMatched {
		n.repromptUsed = true
		p.local = true
		p.route = "reprompt"
		p.ttsID = newID()
		n.tts[p.ttsID] = p
		prompt := n.Reprompt.Text
		n.mu.Unlock()
		slog.Info("native_route", "request", requestTag(p.id), "route", "reprompt", "reason", reason, "matched", matched, "partials", partials, "partial_text", partialText)
		n.event(map[string]any{"event": "followup", "result": "reprompt", "reason": reason, "matched": matched, "partials": partials, "partial_text": partialText})
		if err := n.sendCancel(p.ctx, p.id); err != nil {
			return true
		}
		if err := n.reply(p, Resolution{Answer: prompt}); err != nil {
			slog.Warn("native_reply_failed", "request", requestTag(p.id))
		}
		n.mu.Lock()
		n.lastAsked = true
		n.mu.Unlock()
		return true
	}
	p.local = true
	p.silent = true
	p.route = "silent"
	n.mu.Unlock()
	slog.Info("native_route", "request", requestTag(p.id), "route", "silent", "reason", reason, "matched", matched, "partials", partials, "partial_text", partialText)
	ev := map[string]any{"event": "followup", "result": "silent", "reason": reason, "matched": matched, "partials": partials, "partial_text": partialText}
	if reason != "empty_follow_up" {
		ev["text"] = text
	}
	n.event(ev)
	if err := n.sendCancel(p.ctx, p.id); err != nil {
		return true
	}
	if err := n.reply(p, Resolution{}); err != nil {
		slog.Warn("native_reply_failed", "request", requestTag(p.id))
	}
	return true
}

// cloudFallbackUnlock replaces a cloud answer with a local one.
// Called with n.mu held; it unlocks.
func (n *Native) cloudFallbackUnlock(p *nativeRequest, reason string) {
	resolver, ok := n.resolver.(FallbackResolver)
	if !ok || p.local || p.canceled {
		n.mu.Unlock()
		return
	}
	p.local = true
	p.cloudAnswered = true
	if p.cloudTimer != nil {
		p.cloudTimer.Stop()
	}
	if n.OnRoute != nil {
		n.OnRoute(p.id, "llm")
	}
	filler := ""
	if n.CloudFallback != nil && n.CloudFallback.Enabled {
		filler = strings.TrimSpace(n.CloudFallback.Filler)
	}
	if filler == "" {
		p.ttsID = newID()
		n.tts[p.ttsID] = p
	} else {
		p.progressSpeech = filler
	}
	n.emitCloudLocked(p, reason)
	query := p.text
	n.mu.Unlock()
	slog.Info("native_cloud_fallback", "request", requestTag(p.id), "reason", reason, "filler", filler != "")
	n.wg.Go(func() {
		stop := func() {}
		if filler != "" {
			stop = n.progressAfter(p, 0)
		}
		result, err := resolver.Fallback(p.ctx, query)
		stop()
		if p.ctx.Err() != nil {
			return
		}
		if errors.Is(err, context.Canceled) {
			n.discardStale(p)
			return
		}
		if err != nil {
			n.discardSound(p)
			result.Answer = "Сейчас не получилось получить ответ локальной модели."
		}
		n.mu.Lock()
		if !p.streaming && p.ttsID == "" {
			p.ttsID = newID()
			n.tts[p.ttsID] = p
		}
		streaming := p.streaming
		n.mu.Unlock()
		if !streaming {
			_ = n.sendCancel(p.ctx, p.id)
		}
		if err := n.reply(p, result); err != nil {
			slog.Warn("native_reply_failed", "request", requestTag(p.id))
		}
		slog.Info("native_fallback", "request", requestTag(p.id), "route", "llm", "reason", reason)
	})
}
