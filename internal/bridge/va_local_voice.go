package bridge

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"quasarbridge/internal/sound"
)

// LocalVoice (VA patch 0003) speaks llm answers of selected routing rules with
// an external TTS (our core) through the station's audio_play directive
// instead of Yandex TTS. Any synthesis error falls back to the normal text
// answer in Alice's voice. Disabled unless enabled with at least one rule.
type LocalVoice struct {
	Enabled   bool     `json:"enabled"`
	Rules     []string `json:"rules,omitempty"`
	Endpoint  string   `json:"endpoint"`
	TimeoutMS int      `json:"timeout_ms"`
	// ListenURL (station-wakeword «POST /api/listen» on this bridge's speaker)
	// opens the microphone after the MP3 has played, when the core allows
	// listening. The next utterance within FollowupS stays on the same rule.
	ListenURL     string `json:"listen_url,omitempty"`
	ListenSlackMS int    `json:"listen_slack_ms,omitempty"`
	FollowupS     int    `json:"followup_s,omitempty"`

	// Synth returns an audio URL reachable by the speaker. Set by main from
	// Endpoint; tests may replace it.
	Synth func(ctx context.Context, text string) (SynthResult, error) `json:"-"`
	// Listen opens the speaker's microphone. Set by main from ListenURL.
	Listen func(ctx context.Context) error `json:"-"`
}

// SynthResult is the playable answer and its length.
type SynthResult struct {
	URL      string
	Duration time.Duration
}

func DefaultLocalVoice() LocalVoice { return LocalVoice{TimeoutMS: 6000} }

func (v LocalVoice) Validate() error {
	if !v.Enabled {
		return nil
	}
	if len(v.Rules) == 0 {
		return errors.New("local_voice.rules must name at least one routing rule")
	}
	for _, r := range v.Rules {
		if strings.TrimSpace(r) == "" {
			return errors.New("local_voice.rules has an empty name")
		}
	}
	u, err := url.Parse(v.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("local_voice.endpoint must be an http(s) URL")
	}
	if v.TimeoutMS < 500 || v.TimeoutMS > 20000 {
		return errors.New("local_voice.timeout_ms must be between 500 and 20000")
	}
	if v.ListenURL != "" {
		u, err := url.Parse(v.ListenURL)
		if err != nil || u.Scheme != "http" || u.Hostname() == "" {
			return errors.New("local_voice.listen_url must be an http URL")
		}
	}
	if v.ListenSlackMS < 0 || v.ListenSlackMS > 5000 || v.FollowupS < 0 || v.FollowupS > 120 {
		return errors.New("local_voice.listen_slack_ms must be 0..5000 and followup_s 0..120")
	}
	return nil
}

// Applies reports whether an answer selected by this rule should use the local voice.
func (v *LocalVoice) Applies(rule string) bool {
	return v != nil && v.Enabled && v.Synth != nil && rule != "" && slices.Contains(v.Rules, rule)
}

var voiceMarkup = regexp.MustCompile(`<[^>]*>|sil\s*<\[\d+\]>`)

// SpeakableText strips TTS markup that only Yandex understands.
func SpeakableText(s string) string {
	return strings.Join(strings.Fields(voiceMarkup.ReplaceAllString(s, " ")), " ")
}

// HTTPSynth posts {"text"} to endpoint and expects {"url"} back.
func HTTPSynth(endpoint, apiKey string, timeout time.Duration) func(context.Context, string) (SynthResult, error) {
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context, text string) (SynthResult, error) {
		body, err := json.Marshal(map[string]string{"text": text})
		if err != nil {
			return SynthResult{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return SynthResult{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return SynthResult{}, errors.New("local voice transport failed")
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		if err != nil {
			return SynthResult{}, err
		}
		if resp.StatusCode != http.StatusOK {
			return SynthResult{}, fmt.Errorf("local voice HTTP %d", resp.StatusCode)
		}
		var out struct {
			URL        string `json:"url"`
			DurationMS int    `json:"duration_ms"`
		}
		if json.Unmarshal(data, &out) != nil || out.URL == "" {
			return SynthResult{}, errors.New("local voice: no url in response")
		}
		u, err := url.Parse(out.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
			return SynthResult{}, errors.New("local voice: invalid url")
		}
		return SynthResult{URL: out.URL, Duration: time.Duration(out.DurationMS) * time.Millisecond}, nil
	}
}

// speakLocal synthesizes the answer and queues it as audio_play. On failure it
// only logs: the caller then speaks the text with Yandex TTS as usual.
func (n *Native) speakLocal(p *nativeRequest, rule, answer string) {
	text := SpeakableText(answer)
	if text == "" {
		return
	}
	timeout := time.Duration(n.LocalVoice.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 6 * time.Second
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	started := time.Now()
	res, err := n.LocalVoice.Synth(ctx, text)
	if err == nil {
		err = n.queueSound(p.ctx, p, soundURLRequest(res.URL))
	}
	ms := time.Since(started).Milliseconds()
	if err != nil {
		slog.Warn("native_local_voice", "request", requestTag(p.id), "rule", rule, "ok", false, "ms", ms, "fallback", "yandex_tts", "error", err.Error())
		return
	}
	slog.Info("native_local_voice", "request", requestTag(p.id), "rule", rule, "ok", true, "ms", ms, "chars", len([]rune(text)), "audio_ms", res.Duration.Milliseconds())
	// t501: a full dialogue — listen after every local-voice answer; exit words,
	// silence or Alice commands end it (closeFollowUp / explicit rules).
	if n.LocalVoice.Listen != nil && res.Duration > 0 {
		n.scheduleLocalListen(p.id, rule, res.Duration)
	}
}

// scheduleLocalListen opens the microphone once the MP3 has played. Any new
// request from the speaker (e.g. «Алиса, стоп») cancels the pending listen.
func (n *Native) scheduleLocalListen(id, rule string, audio time.Duration) {
	slack := time.Duration(n.LocalVoice.ListenSlackMS) * time.Millisecond
	if slack == 0 {
		slack = 400 * time.Millisecond
	}
	follow := time.Duration(n.LocalVoice.FollowupS) * time.Second
	if follow == 0 {
		follow = 20 * time.Second
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.localListen != nil {
		n.localListen.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(audio+slack, func() {
		n.mu.Lock()
		if n.localListen != t {
			n.mu.Unlock()
			return
		}
		n.localListen = nil
		n.localFollowRule, n.localFollowUntil = rule, time.Now().Add(follow)
		n.listenArmed = time.Now() // our opened mic is a follow-up: silence/stop/filler close it
		n.lastAsked = false
		n.mu.Unlock()
		n.openLocalListen(id, rule, 1)
	})
	n.localListen = t
}

// localListenRetries: the station sometimes ignores START_CONVERSATION while
// it is still finishing the MP3 (t502, Миди). If no request arrives within
// localListenCheck (a granted listen shows up in ~30 ms), ask again.
const localListenRetries = 3

var localListenCheck = 700 * time.Millisecond

func (n *Native) openLocalListen(id, rule string, attempt int) {
	n.mu.Lock()
	fired := time.Now()
	n.localListenFired = fired
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
	err := n.LocalVoice.Listen(ctx)
	cancel()
	if err != nil {
		slog.Warn("native_local_voice_listen", "request", requestTag(id), "ok", false, "attempt", attempt, "error", err.Error())
	} else {
		slog.Info("native_local_voice_listen", "request", requestTag(id), "ok", true, "attempt", attempt, "rule", rule)
	}
	if attempt > localListenRetries {
		if err != nil {
			n.mu.Lock()
			n.localFollowRule = ""
			n.mu.Unlock()
		}
		return
	}
	var t *time.Timer
	n.mu.Lock()
	t = time.AfterFunc(localListenCheck, func() {
		n.mu.Lock()
		if n.localListen != t || n.ctx.Err() != nil {
			n.mu.Unlock()
			return
		}
		n.localListen = nil
		heard := n.lastRequestAt.After(fired)
		n.mu.Unlock()
		if heard {
			return
		}
		slog.Info("native_local_voice_listen_retry", "request", requestTag(id), "attempt", attempt+1)
		n.openLocalListen(id, rule, attempt+1)
	})
	n.localListen = t
	n.mu.Unlock()
}

// takeLocalFollow is called under n.mu for every new request: it cancels a
// pending listen and returns the rule for a voice follow-up opened by us.
func (n *Native) takeLocalFollow(voice, tagged bool) string {
	n.lastRequestAt = time.Now()
	if n.localListen != nil {
		n.localListen.Stop()
		n.localListen = nil
	}
	rule := n.localFollowRule
	n.localFollowRule = ""
	if !voice || tagged || rule == "" || time.Now().After(n.localFollowUntil) {
		return ""
	}
	return rule
}

// HTTPListen posts to station-wakeword's /api/listen.
func HTTPListen(listenURL string) func(context.Context) error {
	client := &http.Client{Timeout: 3 * time.Second}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, listenURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return errors.New("listen transport failed")
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("listen HTTP %d", resp.StatusCode)
		}
		return nil
	}
}

func soundURLRequest(u string) sound.Request { return sound.Request{URL: u, Title: "Ответ"} }

// vaLocalExitRE ends a local-voice dialogue: the whole utterance is a short
// farewell («стоп», «спасибо, всё», «конец связи»), never a word inside a question.
var vaLocalExitRE = regexp.MustCompile(`(?i)^\s*(?:алиса[,.\s]+)?(?:(?:ну|ладно|хорошо|окей|ок|вс[её]|ага)[,.\s]+)?(?:стоп|хватит|ясно|понятно|спасибо(?:\s+большое)?|вс[её]|достаточно|отбой|пока|конец\s+связи|выход|вс[её]\s+(?:понятно|ясно)|на\s+этом\s+вс[её])(?:[,.\s]+(?:спасибо|пока|алиса|пожалуйста|вс[её]|хватит|стоп|отбой))?[\s.!?,]*$`)

func vaIsLocalExit(text string) bool { return vaLocalExitRE.MatchString(text) }
