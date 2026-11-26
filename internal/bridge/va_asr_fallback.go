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

	directive "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/directives"
	response "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_response"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
	"quasarbridge/internal/llm"
	"quasarbridge/internal/wire"
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
func (n *Native) startASRFallback() bool {
	n.mu.Lock()
	p := n.asrFallbackRequestLocked()
	if p == nil || p.asrFallback {
		n.mu.Unlock()
		return false
	}
	p.asrFallback = true
	p.started = true
	p.local = true
	done := make(chan struct{})
	p.audioDone = done
	if p.cloudTimer != nil {
		p.cloudTimer.Stop()
	}
	n.takeCloudLocked(p, true)
	n.mu.Unlock()
	slog.Info("native_asr_fallback", "request", requestTag(p.id), "stage", "wait_audio")
	if p.gated {
		_ = n.sendCancel(p.ctx, p.id)
	}
	n.wg.Go(func() {
		// Колонка продолжает отдавать звук ещё 2-4 с после asr_init_error: ждём
		// конца реплики (StreamControl) или таймаута, иначе распознаем обрывок.
		n.mu.Lock()
		already := p.audioInputDone
		n.mu.Unlock()
		if !already {
			timer := time.NewTimer(6 * time.Second)
			defer timer.Stop()
			select {
			case <-done:
			case <-timer.C:
			case <-p.ctx.Done():
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
		slog.Info("native_asr_fallback", "request", requestTag(p.id), "stage", "stt", "audio_bytes", len(audio))
		n.runASRFallback(p, audio)
	})
	return true
}

func (n *Native) runASRFallback(p *nativeRequest, audio []byte) {
	if n.LocalVoice == nil || n.LocalVoice.STT == nil {
		return
	}
	// t534: фраза-прикрытие звучит сразу (еуген), не дожидаясь модели: пока LLM
	// думает, колонка уже говорит «На связи тёмная сторона».
	phrase := strings.TrimSpace(n.LocalVoice.FallbackPhrase)
	phraseSent := make(chan struct{})
	go func() {
		defer close(phraseSent)
		if url, ok := n.synthLocal(phrase); ok {
			if err := n.sendAudioAnswer(p, url, ""); err != nil {
				slog.Warn("native_asr_fallback_voice", "request", requestTag(p.id), "stage", "phrase", "error", err.Error())
			}
		}
	}()

	timeout := time.Duration(n.LocalVoice.FallbackTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 12 * time.Second
	}
	pcm, err := decodeWhisperOpus(audio)
	if err != nil {
		slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "decode", "error", err.Error())
		n.fallbackAnswer(p, phraseSent, "", "Не расслышала, повтори, пожалуйста.")
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	text, err := n.LocalVoice.STT(ctx, pcmToS16(pcm))
	cancel()
	if err != nil {
		slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "stt", "error", err.Error())
		n.fallbackAnswer(p, phraseSent, "", "Не расслышала, повтори, пожалуйста.")
		return
	}
	if text == "" {
		slog.Info("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "empty")
		n.fallbackAnswer(p, phraseSent, "", "Не расслышала, повтори, пожалуйста.")
		return
	}
	slog.Info("native_asr_fallback_stt", "request", requestTag(p.id), "ok", true, "chars", len([]rune(text)))
	resolver, ok := n.resolver.(FallbackResolver)
	if !ok {
		n.fallbackAnswer(p, phraseSent, text, "")
		return
	}
	lctx := llm.WithHeader(p.ctx, "X-VA-Voice", "male")
	result, err := resolver.Fallback(lctx, text)
	if err != nil || strings.TrimSpace(result.Answer) == "" {
		slog.Warn("native_asr_fallback", "request", requestTag(p.id), "ok", false, "stage", "llm")
		n.fallbackAnswer(p, phraseSent, text, "Сейчас не получилось получить ответ локальной модели.")
		return
	}
	slog.Info("native_asr_fallback_llm", "request", requestTag(p.id), "ok", true, "route", result.Route, "chars", len([]rune(result.Answer)))
	n.fallbackAnswer(p, phraseSent, text, result.Answer)
}

// synthLocal synthesizes text in eugene voice.
func (n *Native) synthLocal(text string) (string, bool) {
	text = SpeakableText(text)
	if text == "" || n.LocalVoice == nil || n.LocalVoice.Synth == nil {
		return "", false
	}
	timeout := time.Duration(n.LocalVoice.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 6 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := n.LocalVoice.Synth(ctx, text)
	if err != nil {
		return "", false
	}
	return res.URL, true
}

// sendAudioAnswer speaks one eugene MP3 on the speaker as an answer.
func (n *Native) sendAudioAnswer(p *nativeRequest, url, text string) error {
	speech, err := makeSoundDirective(soundURLRequest(url))
	if err != nil {
		return err
	}
	ans := &response.TAliceResponseProto{
		Header: &response.TAliceResponseProto_THeader{
			RequestId: new(p.applicationID()), ResponseId: new(newID()),
			DialogId: new(p.request.GetHeader().GetDialogId()), SequenceNumber: new(p.request.GetHeader().GetSequenceNumber()),
		},
		VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{
			OutputSpeech: &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new("")}, HasVoiceResponse: true,
		},
		Response: &response.TAliceResponseProto_TResponse{Directives: []*directive.TDirective{speech}},
	}
	if strings.TrimSpace(text) != "" {
		ans.Response.Cards = []*response.TAliceResponseProto_TResponse_TCard{{Type: "simple_text", Text: new(text), CardId: new(newID())}}
	}
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: newID(), RefMessageId: p.id},
		Event:  &server.TServerMessage_TEvent_AliceResponse{AliceResponse: ans},
	}}}
	b, err := (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		return err
	}
	return n.down(p.ctx, 2, b)
}

// fallbackAnswer finishes the fallback: waits for the phrase to be sent, then
// speaks the answer in eugene voice; if synthesis fails, falls back to Yandex TTS.
func (n *Native) fallbackAnswer(p *nativeRequest, phraseSent <-chan struct{}, heard, answer string) {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return
	}
	if url, ok := n.synthLocal(answer); ok {
		<-phraseSent
		if err := n.sendAudioAnswer(p, url, answer); err != nil {
			slog.Warn("native_asr_fallback_voice", "request", requestTag(p.id), "stage", "answer", "error", err.Error())
		}
		slog.Info("native_asr_fallback", "request", requestTag(p.id), "ok", true, "heard_chars", len([]rune(heard)), "answer_chars", len([]rune(answer)), "voice", true)
		return
	}
	<-phraseSent
	if err := n.reply(p, Resolution{Route: "llm", Answer: answer}); err != nil {
		slog.Warn("native_reply_failed", "request", requestTag(p.id))
	}
	slog.Info("native_asr_fallback", "request", requestTag(p.id), "ok", true, "heard_chars", len([]rune(heard)), "answer_chars", len([]rune(answer)), "voice", false)
}
