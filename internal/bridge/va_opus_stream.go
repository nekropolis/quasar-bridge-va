package bridge

// t537: фолбэк при сбое облака Алисы — наша модель и голос eugene одним потоком
// речи колонки, как штатная озвучка: AliceResponse (streaming, tts_play_placeholder)
// → TtsSpeak{audio/opus} с нашими Ogg-страницами фразы → тишина Opus, пока модель
// думает → страницы ответа в тот же поток → EOS + StreamControl Close. Один ответ
// на запрос, без audio_play. Схема — upstream PR #32 (custom_voice.go) и local_progress:
// кадры проходят через n.server, поэтому подмена id, склейка сегментов (oggSpeech),
// тишина ожидания (waitAudio) и отмена работают тем же кодом, что и для облачного TTS.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	directive "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/directives"
	response "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_response"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	stream "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/stream_control"
	speak "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/tts_speak"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
	"quasarbridge/internal/llm"
	"quasarbridge/internal/reply"
	"quasarbridge/internal/wire"
)

// Our streams use a range Yandex does not hand out, with a zero high byte so an
// audio frame can never be mistaken for a protobuf message (as in PR #32).
const vaVoiceStreamBase = 0x00100000

var vaVoiceStreams atomic.Uint32

func vaNewVoiceStream() uint32 { return vaVoiceStreamBase + 2*vaVoiceStreams.Add(1) }

const (
	vaOpusMaxBytes  = 4 << 20
	vaOpusChunk     = 8 << 10
	vaDefaultPhrase = "На связи тёмная сторона."
	vaNotHeard      = "Не расслышал, повтори, пожалуйста."
	vaNoAnswer      = "Сейчас не получилось получить ответ локальной модели."
	vaLocalSorry    = "Сейчас не получилось ответить."
)

// HTTPOpus posts {"text"} to the core (/v1/voice/opus) and returns verified mono Ogg/Opus.
func HTTPOpus(endpoint, apiKey string, timeout time.Duration) func(context.Context, string) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context, text string) ([]byte, error) {
		body, err := json.Marshal(map[string]string{"text": text})
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, errors.New("local opus transport failed")
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, vaOpusMaxBytes+1))
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("local opus HTTP %d", resp.StatusCode)
		}
		if len(data) > vaOpusMaxBytes {
			return nil, errors.New("local opus: too large")
		}
		if _, _, err := vaOggPages(data); err != nil {
			return nil, err
		}
		return data, nil
	}
}

// vaOggPages splits verified mono Ogg/Opus into pages and returns its duration.
func vaOggPages(b []byte) ([][]byte, time.Duration, error) {
	var pages [][]byte
	var granule uint64
	var serial uint32
	for len(b) > 0 {
		if len(b) < 27 || string(b[:4]) != "OggS" || b[4] != 0 {
			return nil, 0, errors.New("invalid Ogg page")
		}
		count := int(b[26])
		if len(b) < 27+count {
			return nil, 0, errors.New("truncated Ogg page")
		}
		size := 27 + count
		for _, v := range b[27 : 27+count] {
			size += int(v)
		}
		if len(b) < size {
			return nil, 0, errors.New("truncated Ogg page")
		}
		page := b[:size]
		if oggChecksum(page) != binary.LittleEndian.Uint32(page[22:26]) {
			return nil, 0, errors.New("invalid Ogg checksum")
		}
		s := binary.LittleEndian.Uint32(page[14:18])
		if len(pages) == 0 {
			serial = s
			if page[5]&2 == 0 {
				return nil, 0, errors.New("Ogg stream without BOS")
			}
		} else if s != serial {
			return nil, 0, errors.New("several Ogg streams")
		}
		if g := binary.LittleEndian.Uint64(page[6:14]); g != ^uint64(0) {
			granule = g
		}
		pages = append(pages, page)
		b = b[size:]
	}
	if len(pages) < 3 {
		return nil, 0, errors.New("speech service returned too little audio")
	}
	first := pages[0]
	head := first[27+int(first[26]):]
	if len(head) < 19 || !bytes.HasPrefix(head, []byte("OpusHead")) {
		return nil, 0, errors.New("speech service did not return Ogg/Opus")
	}
	if head[9] != 1 {
		return nil, 0, errors.New("speech service must return mono audio")
	}
	if pages[len(pages)-1][5]&4 == 0 {
		return nil, 0, errors.New("Ogg stream without EOS")
	}
	pre := uint64(binary.LittleEndian.Uint16(head[10:12]))
	var d time.Duration
	if granule > pre {
		d = time.Duration(granule-pre) * time.Second / 48000
	}
	return pages, d, nil
}

func (n *Native) localOpusEnabled() bool {
	return n.LocalVoice != nil && n.LocalVoice.Enabled && n.LocalVoice.Opus != nil
}

func (n *Native) vaFallbackPhrase() string {
	if n.LocalVoice != nil {
		if s := strings.TrimSpace(n.LocalVoice.FallbackPhrase); s != "" {
			return s
		}
	}
	return vaDefaultPhrase
}

// Fallback phrases are synthesized once per process; answers never are cached here.
var vaOpusCache sync.Map

func (n *Native) localOpus(ctx context.Context, text string, cache bool) ([][]byte, time.Duration, error) {
	text = SpeakableText(text)
	if text == "" {
		return nil, 0, errors.New("empty speech")
	}
	if cache {
		if v, ok := vaOpusCache.Load(text); ok {
			return vaOggPages(v.([]byte))
		}
	}
	timeout := time.Duration(n.LocalVoice.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 6 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	b, err := n.LocalVoice.Opus(ctx, text)
	if err != nil {
		return nil, 0, err
	}
	pages, d, err := vaOggPages(b)
	if err != nil {
		return nil, 0, err
	}
	if cache {
		vaOpusCache.Store(text, b)
	}
	return pages, d, nil
}

// vaInject replays a frame through the server path (not the cloud gate): our
// speech is local, it must not wait for a cloud decision.
func (n *Native) vaInject(ctx context.Context, data []byte) error {
	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	if err := n.ctx.Err(); err != nil {
		return err
	}
	return n.server(ctx, websocket.MessageBinary, data)
}

func (n *Native) vaInjectSpeak(p *nativeRequest, ttsID string, id uint32) error {
	sid := int64(id)
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{MessageId: newID(), RefMessageId: ttsID, StreamId: &sid}, Event: &server.TServerMessage_TEvent_TtsSpeak{TtsSpeak: &speak.TTtsSpeak{Format: "audio/opus"}}}}}
	b, err := (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		return err
	}
	return n.vaInject(p.ctx, b)
}

// Pages go to the speaker as they come, grouped into chunks of a few kilobytes.
func (n *Native) vaInjectPages(p *nativeRequest, id uint32, pages [][]byte) error {
	var chunk []byte
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		err := n.vaInject(p.ctx, append(binary.BigEndian.AppendUint32(nil, id), chunk...))
		chunk = nil
		return err
	}
	for _, page := range pages {
		if len(chunk)+len(page) > vaOpusChunk {
			if err := flush(); err != nil {
				return err
			}
		}
		chunk = append(chunk, page...)
	}
	return flush()
}

func (n *Native) vaInjectClose(p *nativeRequest, ttsID string, id uint32) error {
	m := &server.TServerMessage{Message: &server.TServerMessage_StreamControl{StreamControl: &stream.TStreamControl{MessageId: ttsID, StreamId: int64(id), Action: &stream.TStreamControl_Close{Close: &stream.TStreamControl_TActionClose{}}}}}
	b, err := (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		return err
	}
	return n.vaInject(p.ctx, b)
}

// vaBeginDeferred is beginDeferred without the Yandex TTS request: one streaming
// AliceResponse with a placeholder, then the first empty deferred part.
func (n *Native) vaBeginDeferred(p *nativeRequest) error {
	r := &response.TAliceResponseProto{Header: &response.TAliceResponseProto_THeader{RequestId: new(p.applicationID()), ResponseId: new(p.responseID), DialogId: new(p.request.GetHeader().GetDialogId()), SequenceNumber: new(p.request.GetHeader().GetSequenceNumber())}, VoiceResponse: &response.TAliceResponseProto_TVoiceResponse{HasVoiceResponse: true, OutputSpeech: &response.TAliceResponseProto_TVoiceResponse_TOutputSpeech{Text: new("")}}, Response: &response.TAliceResponseProto_TResponse{IsStreaming: true, Directives: []*directive.TDirective{{Type: "client_action", Name: "tts_play_placeholder"}}}}
	r.VoiceResponse.ShouldListen = n.ShouldListen && p.listenOnStreamStart && reply.ListeningAllowed(p.ctx)
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{MessageId: newID(), RefMessageId: p.id}, Event: &server.TServerMessage_TEvent_AliceResponse{AliceResponse: r}}}}
	b, err := (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		return err
	}
	if err := n.down(p.ctx, websocket.MessageBinary, b); err != nil {
		return err
	}
	return n.deferredPart(p, "", "", false)
}

// vaStartLocalStream opens the single speech stream of p and plays phrase in our
// voice. started=false: nothing reached the speaker, the caller may use the
// single-MP3 path. started=true with an error: the request is broken, cancel it.
func (n *Native) vaStartLocalStream(p *nativeRequest, phrase string) (started bool, err error) {
	pages, dur, err := n.localOpus(p.ctx, phrase, true)
	if err != nil {
		return false, err
	}
	n.mu.Lock()
	if p.canceled || p.ctx.Err() != nil || p.streaming {
		n.mu.Unlock()
		return false, errors.New("request is not available for local speech")
	}
	p.local = true
	p.streaming = true
	p.responseID = newID()
	p.ttsID = newID()
	p.progressTTS = p.ttsID
	p.progressSpeech = phrase // keep the phrase audio (progress audio is discarded otherwise)
	p.progressAudioDone = make(chan struct{})
	n.tts[p.ttsID] = p
	ttsID, needAck := p.ttsID, !p.gated && !p.cloud
	n.mu.Unlock()
	if needAck {
		if err := n.ack(p); err != nil {
			return true, err
		}
	}
	if err := n.vaBeginDeferred(p); err != nil {
		return true, err
	}
	id := vaNewVoiceStream()
	if err := n.vaInjectSpeak(p, ttsID, id); err != nil {
		return true, err
	}
	if err := n.vaInjectPages(p, id, pages); err != nil {
		return true, err
	}
	slog.Info("native_local_stream", "request", requestTag(p.id), "stage", "phrase", "stream", id, "pages", len(pages), "audio_ms", dur.Milliseconds())
	// Close the phrase segment right away: the joiner keeps the stream alive with
	// Opus silence behind the queued phrase until the answer arrives.
	if err := n.vaInjectClose(p, ttsID, id); err != nil {
		return true, err
	}
	return true, nil
}

// vaFinishLocalStream appends the answer to the open speech stream and ends it.
func (n *Native) vaFinishLocalStream(p *nativeRequest, text string) {
	select {
	case <-p.ctx.Done():
		return
	case <-p.progressAudioDone:
	}
	speech := SpeakableText(text)
	pages, dur, err := n.localOpus(p.ctx, speech, false)
	if err != nil {
		slog.Warn("native_local_stream", "request", requestTag(p.id), "stage", "answer_voice", "ok", false, "error", err.Error())
		pages, dur, err = n.localOpus(p.ctx, vaLocalSorry, true)
	}
	if p.ctx.Err() != nil {
		return
	}
	if err != nil { // no speech at all: end the stream with silence, text only
		if ferr := n.finishSilentAudio(p); ferr != nil || n.deferredPart(p, text, "", true) != nil {
			p.cancel()
			return
		}
		reply.Delivered(p.ctx)
		slog.Warn("native_local_stream", "request", requestTag(p.id), "stage", "answer", "ok", false, "error", err.Error())
		return
	}
	n.mu.Lock()
	p.ttsID = newID()
	n.tts[p.ttsID] = p
	ttsID := p.ttsID
	n.mu.Unlock()
	if derr := n.deferredPart(p, text, speech, true); derr != nil {
		p.cancel()
		return
	}
	id := vaNewVoiceStream()
	if err := n.vaInjectSpeak(p, ttsID, id); err != nil {
		p.cancel()
		return
	}
	if err := n.vaInjectPages(p, id, pages); err != nil {
		p.cancel()
		return
	}
	if err := n.vaInjectClose(p, ttsID, id); err != nil {
		p.cancel()
		return
	}
	n.vaPlayUntil.Store(time.Now().Add(dur + time.Second).UnixNano())
	slog.Info("native_local_stream", "request", requestTag(p.id), "stage", "answer", "ok", true, "stream", id, "pages", len(pages), "audio_ms", dur.Milliseconds())
}

func vaCloudFailureReason(reason string) bool {
	return reason == "timeout" || reason == "upstream_lost" || strings.HasPrefix(reason, "cloud_")
}

// vaCloudLocalStream answers a request the cloud failed (error, timeout, lost
// connection) with our model in our voice. Without Opus it keeps the old path.
func (n *Native) vaCloudLocalStream(p *nativeRequest, resolver FallbackResolver, query, reason string) {
	defer n.vaLocalBusy.Add(-1)
	_ = n.sendCancel(p.ctx, p.id)
	started, err := n.vaStartLocalStream(p, n.vaFallbackPhrase())
	if started && err != nil {
		slog.Warn("native_local_stream", "request", requestTag(p.id), "stage", "phrase", "ok", false, "error", err.Error())
		p.cancel()
		return
	}
	ctx := p.ctx
	if started {
		ctx = llm.WithHeader(ctx, "X-VA-Voice", "male")
	}
	result, ferr := resolver.Fallback(ctx, query)
	if p.ctx.Err() != nil {
		return
	}
	if errors.Is(ferr, context.Canceled) {
		n.discardStale(p)
		return
	}
	if ferr != nil || strings.TrimSpace(result.Answer) == "" {
		n.discardSound(p)
		result = Resolution{Route: "llm", Answer: vaNoAnswer}
	}
	if started {
		n.vaFinishLocalStream(p, result.Answer)
		slog.Info("native_fallback", "request", requestTag(p.id), "route", "llm", "reason", reason, "voice", "local_stream")
		return
	}
	slog.Warn("native_local_stream", "request", requestTag(p.id), "stage", "phrase", "ok", false, "fallback", "yandex_tts", "error", err.Error())
	n.mu.Lock()
	if p.ttsID == "" {
		p.ttsID = newID()
		n.tts[p.ttsID] = p
	}
	n.mu.Unlock()
	if err := n.reply(p, result); err != nil {
		slog.Warn("native_reply_failed", "request", requestTag(p.id))
	}
	slog.Info("native_fallback", "request", requestTag(p.id), "route", "llm", "reason", reason)
}

// vaCloudReadyLocked: a request already sent to the cloud, still unanswered and
// allowed to fall back (same policy as the timeout fallback).
func (n *Native) vaCloudReadyLocked(p *nativeRequest) bool {
	if p == nil || !p.started || !p.cloud || p.local || p.canceled || p.cloudAnswered || p.ttsStarted || p.ctx.Err() != nil {
		return false
	}
	if _, ok := n.resolver.(FallbackResolver); !ok {
		return false
	}
	return n.CloudFallback.allowed(p.text, p.source)
}

func (n *Native) vaCloudPendingLocked() *nativeRequest {
	var best *nativeRequest
	for _, p := range n.requests {
		if !n.vaCloudReadyLocked(p) {
			continue
		}
		if best == nil || p.wakeAt.After(best.wakeAt) {
			best = p
		}
	}
	return best
}

// vaCloudFailureFrame (t537) handles EventException, Error and GoAway from Yandex:
// a voice request still in ASR is recognized locally (the old asr_init_error path,
// now for any ASR-stage failure), a request already in the cloud and unanswered is
// answered by our model. Reports whether the frame must not reach the speaker.
func (n *Native) vaCloudFailureFrame(f wire.Frame, data []byte) bool {
	kind := f.Kind()
	event := kind == "EventException"
	asr := bytes.Contains(data, []byte("asr_init_error"))
	n.mu.Lock()
	var target *nativeRequest
	if event {
		target, _ = n.findLocked(f.Server.GetEvent().GetHeader())
	}
	asrStage := target != nil && !target.started && !target.canceled
	n.mu.Unlock()
	if n.asrFallbackEnabled() && (asr || !event || asrStage) && n.startASRFallback() {
		slog.Info("native_cloud_failure", "kind", kind, "asr_error", asr, "stage", "asr")
		return event
	}
	n.mu.Lock()
	p := target
	if p == nil && !event {
		p = n.vaCloudPendingLocked()
	}
	if !n.vaCloudReadyLocked(p) {
		n.mu.Unlock()
		return false
	}
	slog.Info("native_cloud_failure", "kind", kind, "asr_error", asr, "stage", "cloud", "request", requestTag(p.id))
	n.cloudFallbackUnlock(p, "cloud_"+strings.ToLower(kind))
	return event
}

// UpstreamLost (t537): the Yandex socket closed. Requests in flight are answered
// locally; the channel closes when they are done and the answer has played.
func (n *Native) UpstreamLost() <-chan struct{} {
	n.vaUpLost.Store(true)
	started := n.asrFallbackEnabled() && n.startASRFallback()
	n.mu.Lock()
	if p := n.vaCloudPendingLocked(); p != nil {
		n.cloudFallbackUnlock(p, "upstream_lost")
		started = true
	} else {
		n.mu.Unlock()
	}
	slog.Warn("native_upstream_lost", "local_answer", started)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if !started {
			return
		}
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for n.ctx.Err() == nil && (n.vaLocalBusy.Load() > 0 || time.Now().UnixNano() < n.vaPlayUntil.Load()) {
			<-tick.C
		}
	}()
	return done
}
