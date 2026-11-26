package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"quasarbridge/internal/llm"
)

// t532: когда ASR Яндекса недоступен (EventException asr_init_error), не ждём
// облако: распознаём накопленное аудио локальным STT, отвечаем нашей LLM и
// озвучиваем ответ голосом eugene (local_voice) — без тишины и ошибки на колонке.

// HTTPSTT posts raw PCM s16le 16 kHz to the core and returns recognized text.
func HTTPSTT(endpoint, apiKey string, timeout time.Duration) func(context.Context, []byte) (string, error) {
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context, pcm []byte) (string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(pcm))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", errors.New("local stt transport failed")
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return "", errors.New("local stt HTTP " + resp.Status)
		}
		var out struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(data, &out) != nil {
			return "", errors.New("local stt: bad response")
		}
		return strings.TrimSpace(out.Text), nil
	}
}

// HTTPStream posts PCM to the core and gets back a URL that streams the phrase
// immediately and appends the model answer when ready (t535).
func HTTPStream(endpoint, chat, apiKey string, timeout time.Duration) func(context.Context, []byte) (string, error) {
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context, pcm []byte) (string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(pcm))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if chat != "" {
			req.Header.Set("X-VA-Chat", chat)
		}
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", errors.New("local stream transport failed")
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			return "", errors.New("local stream HTTP " + resp.Status)
		}
		var out struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(data, &out) != nil || strings.TrimSpace(out.URL) == "" {
			return "", errors.New("local stream: no url")
		}
		return out.URL, nil
	}
}

func pcmToS16(pcm []float32) []byte {
	out := make([]byte, len(pcm)*2)
	for i, v := range pcm {
		if v > 1 {
			v = 1
		} else if v < -1 {
			v = -1
		}
		binary.LittleEndian.PutUint16(out[i*2:], uint16(int16(math.Round(float64(v)*32767))))
	}
	return out
}

func (n *Native) asrFallbackEnabled() bool {
	return n.LocalVoice != nil && n.LocalVoice.Enabled && n.LocalVoice.STT != nil
}

// asrFallbackRequestLocked picks the active voice request whose audio was just
// rejected by Yandex. Called with n.mu held.
func (n *Native) asrFallbackRequestLocked() *nativeRequest {
	p := n.lastVoice
	if p == nil || p.canceled || p.started || p.ctx.Err() != nil || len(p.audioInput) == 0 {
		p = nil // t537: never fall back on a request already answered or without audio
		for _, q := range n.requests {
			if q.canceled || q.isText || q.started || q.ctx.Err() != nil || len(q.audioInput) == 0 {
				continue
			}
			if p == nil || q.wakeAt.After(p.wakeAt) {
				p = q
			}
		}
	}
	return p
}

// startASRFallback handles an ASR rejection: the audio already collected for the
// request is recognized locally and answered by the local model in eugene voice.
func (n *Native) startASRFallback() bool { return n.vaStartASRFallback(nil, false) }

// vaForceFallback (t538, QUASAR_ASR_FORCE_FALLBACK): answer p locally once Yandex
// has found the end of speech; the audio is complete then, no waiting.
func (n *Native) vaForceFallback(p *nativeRequest, reason string) bool {
	if !n.asrFallbackEnabled() || !n.vaStartASRFallback(p, reason == "eou") {
		return false
	}
	slog.Info("native_asr_fallback_forced", "request", requestTag(p.id), "reason", reason)
	return true
}

func (n *Native) vaStartASRFallback(target *nativeRequest, complete bool) bool {
	n.mu.Lock()
	p := target
	if p == nil {
		p = n.asrFallbackRequestLocked()
	} else if p.canceled || p.started || p.ctx.Err() != nil || len(p.audioInput) == 0 {
		p = nil
	}
	if p == nil || p.asrFallback {
		n.mu.Unlock()
		return false
	}
	p.asrFallback = true
	p.started = true
	p.local = true
	if complete {
		p.audioInputDone = true
	}
	done := make(chan struct{})
	p.audioDone = done
	if p.cloudTimer != nil {
		p.cloudTimer.Stop()
	}
	n.takeCloudLocked(p, true)
	n.mu.Unlock()
	n.vaLocalBusy.Add(1)
	slog.Info("native_asr_fallback", "request", requestTag(p.id), "stage", "wait_audio")
	if p.gated {
		_ = n.sendCancel(p.ctx, p.id)
	}
	n.wg.Go(func() {
		defer n.vaLocalBusy.Add(-1)
		// Колонка продолжает отдавать звук и после отказа Яндекса: ждём конца
		// реплики (StreamControl), локальной тишины после речи или потолка.
		n.mu.Lock()
		already := p.audioInputDone
		n.mu.Unlock()
		reason := "complete"
		if !already {
			reason = n.vaWaitSpeechEnd(p, done)
			if reason == "" {
				return
			}
		}
		n.mu.Lock()
		if p.audioDone == done {
			close(done)
			p.audioDone = nil
		}
		audio := p.audioInput
		p.audioInput = nil
		p.audioInputDone = true
		n.mu.Unlock()
		slog.Info("native_asr_fallback", "request", requestTag(p.id), "stage", "stt", "audio_bytes", len(audio), "end", reason)
		n.runASRFallback(p, audio)
	})
	return true
}

// t538: потолок ожидания конца реплики и локальная тишина после речи.
var (
	vaASRWaitCap   = 3 * time.Second
	vaASRSilence   = 800 * time.Millisecond
	vaASRPoll      = 150 * time.Millisecond
	vaForceMaxWait = 8 * time.Second
	vaSpeechMinRun = 100 * time.Millisecond
)

// vaWaitSpeechEnd returns why the utterance is over ("stream", "silence", "cap"),
// or "" if the request ended.
func (n *Native) vaWaitSpeechEnd(p *nativeRequest, done <-chan struct{}) string {
	limit := time.NewTimer(vaASRWaitCap)
	defer limit.Stop()
	tick := time.NewTicker(vaASRPoll)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return "stream"
		case <-limit.C:
			return "cap"
		case <-p.ctx.Done():
			return ""
		case <-tick.C:
			n.mu.Lock()
			audio := bytes.Clone(p.audioInput)
			n.mu.Unlock()
			if pcm, err := decodeWhisperOpus(audio); err == nil && vaSpeechEnded(pcm, 16000) {
				return "silence"
			}
		}
	}
}

// vaSpeechEnded: there was speech (a run of loud 20 ms frames) and the last
// vaASRSilence is quiet. Thresholds follow the noise floor of this recording.
func vaSpeechEnded(pcm []float32, rate int) bool {
	frame := rate / 50
	if frame == 0 || len(pcm) < frame {
		return false
	}
	rms := make([]float64, 0, len(pcm)/frame)
	floor := math.Inf(1)
	for i := 0; i+frame <= len(pcm); i += frame {
		var e float64
		for _, v := range pcm[i : i+frame] {
			e += float64(v) * float64(v)
		}
		r := math.Sqrt(e / float64(frame))
		rms = append(rms, r)
		floor = min(floor, r)
	}
	loud := max(0.02, 4*floor)
	quiet := max(0.008, 2*floor)
	tail := int(vaASRSilence / (20 * time.Millisecond))
	need := int(vaSpeechMinRun / (20 * time.Millisecond))
	if len(rms) < tail+need {
		return false
	}
	for _, r := range rms[len(rms)-tail:] {
		if r >= quiet {
			return false
		}
	}
	run := 0
	for _, r := range rms[:len(rms)-tail] {
		if r >= loud {
			run++
			if run >= need {
				return true
			}
		} else {
			run = 0
		}
	}
	return false
}

func (n *Native) runASRFallback(p *nativeRequest, audio []byte) {
	if n.LocalVoice == nil || n.LocalVoice.STT == nil {
		return
	}
	timeout := time.Duration(n.LocalVoice.FallbackTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 12 * time.Second
	}
	// t537: сразу открываем один поток речи и говорим фразу голосом eugene, ответ
	// модели допишется в тот же поток. Нет Opus — прежний путь одним MP3.
	streamed := false
	if n.localOpusEnabled() {
		started, err := n.vaStartLocalStream(p, n.vaFallbackPhrase())
		switch {
		case started && err != nil:
			slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "phrase", "error", err.Error())
			p.cancel()
			return
		case started:
			streamed = true
		default:
			slog.Warn("native_local_stream", "request", requestTag(p.id), "stage", "phrase", "ok", false, "fallback", "mp3", "error", err.Error())
		}
	}
	say := func(heard, answer string) {
		if !streamed {
			n.fallbackAnswer(p, heard, answer)
			return
		}
		if strings.TrimSpace(answer) == "" {
			answer = vaNoAnswer
		}
		// t538: микрофон — только после настоящего ответа модели, не на «Не расслышал».
		n.vaFinishLocalStream(p, answer, heard != "" && answer != vaNoAnswer && answer != vaNotHeard)
		slog.Info("native_asr_fallback", "request", requestTag(p.id), "ok", true, "heard_chars", len([]rune(heard)), "answer_chars", len([]rune(answer)), "voice", "local_stream")
	}
	pcm, err := decodeWhisperOpus(audio)
	if err != nil {
		slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "decode", "error", err.Error())
		say("", vaNotHeard)
		return
	}
	s16 := pcmToS16(pcm)
	if !streamed && n.LocalVoice.Stream != nil {
		sctx, scancel := context.WithTimeout(p.ctx, timeout)
		url, serr := n.LocalVoice.Stream(sctx, s16)
		scancel()
		if serr == nil {
			n.answerStream(p, url)
			slog.Info("native_asr_fallback_stream", "request", requestTag(p.id), "ok", true, "audio_bytes", len(audio))
			return
		}
		slog.Warn("native_asr_fallback_stream", "request", requestTag(p.id), "ok", false, "error", serr.Error())
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	text, err := n.LocalVoice.STT(ctx, s16)
	cancel()
	if err != nil {
		slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "stt", "error", err.Error())
		say("", vaNotHeard)
		return
	}
	if text == "" || vaWakeOnly(text) {
		slog.Info("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "empty", "wake_only", text != "")
		say("", vaNotHeard)
		return
	}
	slog.Info("native_asr_fallback_stt", "request", requestTag(p.id), "ok", true, "chars", len([]rune(text)))
	resolver, ok := n.resolver.(FallbackResolver)
	if !ok {
		say(text, "")
		return
	}
	lctx := llm.WithHeader(p.ctx, "X-VA-Voice", "male")
	result, err := resolver.Fallback(lctx, text)
	if err != nil || strings.TrimSpace(result.Answer) == "" {
		slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "llm")
		say(text, vaNoAnswer)
		return
	}
	slog.Info("native_asr_fallback_llm", "request", requestTag(p.id), "ok", true, "route", result.Route, "chars", len([]rune(result.Answer)))
	say(text, result.Answer)
}

// answerStream queues one growing MP3 as the answer (the phrase is already playing).
func (n *Native) answerStream(p *nativeRequest, url string) error {
	if err := n.queueSound(p.ctx, p, soundURLRequest(url)); err != nil {
		return err
	}
	// Непустой текст: станция иначе считает ответ пустым и не играет директиву.
	return n.reply(p, Resolution{Route: "llm", Answer: "Ответ голосом."})
}

// fallbackAnswer speaks text in eugene voice (local_voice TTS + audio_play).
// If synthesis fails the text still goes out through Yandex TTS.
func (n *Native) fallbackAnswer(p *nativeRequest, heard, answer string) {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return
	}
	phrase := ""
	if n.LocalVoice != nil {
		phrase = strings.TrimSpace(n.LocalVoice.FallbackPhrase)
	}
	speak := SpeakableText(strings.TrimSpace(phrase + " " + answer))
	if n.LocalVoice != nil && n.LocalVoice.Synth != nil && speak != "" {
		timeout := time.Duration(n.LocalVoice.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = 6 * time.Second
		}
		ctx, cancel := context.WithTimeout(p.ctx, timeout)
		res, err := n.LocalVoice.Synth(ctx, speak)
		cancel()
		if err == nil {
			err = n.queueSound(p.ctx, p, soundURLRequest(res.URL))
		}
		if err != nil {
			slog.Warn("native_asr_fallback_voice", "request", requestTag(p.id), "ok", false, "error", err.Error())
		}
	}
	if err := n.reply(p, Resolution{Route: "llm", Answer: speak}); err != nil {
		slog.Warn("native_reply_failed", "request", requestTag(p.id))
	}
	slog.Info("native_asr_fallback", "request", requestTag(p.id), "ok", true, "heard_chars", len([]rune(heard)), "answer_chars", len([]rune(speak)), "voice", n.LocalVoice != nil && n.LocalVoice.Synth != nil)
}
