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

	// Synth returns an audio URL reachable by the speaker. Set by main from
	// Endpoint; tests may replace it.
	Synth func(ctx context.Context, text string) (string, error) `json:"-"`
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
func HTTPSynth(endpoint, apiKey string, timeout time.Duration) func(context.Context, string) (string, error) {
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context, text string) (string, error) {
		body, err := json.Marshal(map[string]string{"text": text})
		if err != nil {
			return "", err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", errors.New("local voice transport failed")
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("local voice HTTP %d", resp.StatusCode)
		}
		var out struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(data, &out) != nil || out.URL == "" {
			return "", errors.New("local voice: no url in response")
		}
		u, err := url.Parse(out.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
			return "", errors.New("local voice: invalid url")
		}
		return out.URL, nil
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
	u, err := n.LocalVoice.Synth(ctx, text)
	if err == nil {
		err = n.queueSound(p.ctx, p, soundURLRequest(u))
	}
	ms := time.Since(started).Milliseconds()
	if err != nil {
		slog.Warn("native_local_voice", "request", requestTag(p.id), "rule", rule, "ok", false, "ms", ms, "fallback", "yandex_tts", "error", err.Error())
		return
	}
	slog.Info("native_local_voice", "request", requestTag(p.id), "rule", rule, "ok", true, "ms", ms, "chars", len([]rune(text)))
}

func soundURLRequest(u string) sound.Request { return sound.Request{URL: u, Title: "Ответ"} }
