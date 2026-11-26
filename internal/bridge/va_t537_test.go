package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	event_exception "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/event_exception"
	go_away "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/go_away"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
	"quasarbridge/internal/wire"
)

// vaTestOpus builds valid mono Ogg/Opus with n 20 ms CELT packets.
func vaTestOpus(serial uint32, packets int) []byte {
	o := &oggSpeech{serial: serial}
	head := []byte("OpusHead")
	head = append(head, 1, 1)
	head = binary.LittleEndian.AppendUint16(head, 312)
	head = binary.LittleEndian.AppendUint32(head, 48000)
	head = append(head, 0, 0, 0)
	tags := []byte("OpusTags")
	tags = binary.LittleEndian.AppendUint32(tags, 4)
	tags = append(tags, "test"...)
	tags = binary.LittleEndian.AppendUint32(tags, 0)
	out := o.page(head, 2, 0)
	out = append(out, o.page(tags, 0, 0)...)
	g := uint64(312)
	for i := range packets {
		g += 960
		flags := byte(0)
		if i == packets-1 {
			flags = 4
		}
		out = append(out, o.page([]byte{0xf8, 0xff, 0xfe}, flags, g)...)
	}
	return out
}

type vaDown struct {
	mu     sync.Mutex
	frames [][]byte
}

func (d *vaDown) send(_ context.Context, _ websocket.MessageType, b []byte) error {
	d.mu.Lock()
	d.frames = append(d.frames, bytes.Clone(b))
	d.mu.Unlock()
	return nil
}

func (d *vaDown) snapshot() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([][]byte(nil), d.frames...)
}

func (d *vaDown) closed(id string) bool {
	for _, raw := range d.snapshot() {
		f := wire.Decode(raw, wire.ServerToClient)
		if sc := f.Server.GetStreamControl(); sc != nil && sc.GetClose() != nil && sc.GetMessageId() == id {
			return true
		}
	}
	return false
}

func vaWait(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type vaOpusRec struct {
	mu    sync.Mutex
	texts []string
	fail  map[string]bool
}

func (r *vaOpusRec) synth(_ context.Context, text string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.texts = append(r.texts, text)
	if r.fail[text] || r.fail["*"] {
		return nil, errors.New("tts down")
	}
	return vaTestOpus(uint32(len(r.texts))*7919, 25), nil
}

func (r *vaOpusRec) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.texts...)
}

// vaCheckSpeech verifies the whole downstream of one request: one streaming
// AliceResponse, one TtsSpeak audio/opus on our stream, valid Ogg pages of one
// serial (phrase, silence, answer), EOS last, Close on the same stream, the
// final deferred part with the answer, and no audio_play.
func vaCheckSpeech(t *testing.T, frames [][]byte, id, answer string) {
	t.Helper()
	var responses, speaks int
	var streamID uint32
	var audio []byte
	var closed, lastPart bool
	for _, raw := range frames {
		f := wire.Decode(raw, wire.ServerToClient)
		if f.Server == nil {
			if len(raw) < 4 {
				t.Fatal("short audio frame")
			}
			if closed {
				t.Fatal("audio after Close")
			}
			if s := binary.BigEndian.Uint32(raw); streamID == 0 || s != streamID {
				t.Fatalf("audio on stream %x, want %x", s, streamID)
			}
			audio = append(audio, raw[4:]...)
			continue
		}
		if sc := f.Server.GetStreamControl(); sc != nil && sc.GetClose() != nil {
			if sc.GetMessageId() != id || uint32(sc.GetStreamId()) != streamID {
				t.Fatalf("close %q stream %x", sc.GetMessageId(), sc.GetStreamId())
			}
			closed = true
			continue
		}
		e := f.Server.GetEvent()
		if r := e.GetAliceResponse(); r != nil {
			responses++
			if !r.GetResponse().GetIsStreaming() {
				t.Fatal("response must be streaming")
			}
			for _, d := range r.GetResponse().GetDirectives() {
				if d.GetName() == "audio_play" {
					t.Fatal("audio_play must not be used")
				}
			}
		}
		if s := e.GetTtsSpeak(); s != nil {
			speaks++
			if s.GetFormat() != "audio/opus" || e.GetHeader().GetRefMessageId() != id {
				t.Fatalf("tts speak %q ref %q", s.GetFormat(), e.GetHeader().GetRefMessageId())
			}
			streamID = uint32(e.GetHeader().GetStreamId())
			if streamID < vaVoiceStreamBase {
				t.Fatalf("stream id %x is not ours", streamID)
			}
		}
		if f.Kind() == "DeferredAliceResponse" && strings.Contains(string(raw), answer) {
			lastPart = true
		}
	}
	if responses != 1 || speaks != 1 || !closed || !lastPart {
		t.Fatalf("responses=%d speaks=%d closed=%v answer_part=%v", responses, speaks, closed, lastPart)
	}
	pages, _, err := vaOggPages(audio)
	if err != nil {
		t.Fatalf("speaker stream: %v", err)
	}
	// phrase 25 + answer 25 packets + at least one silence page + headers
	if len(pages) < 2+50 {
		t.Fatalf("pages=%d", len(pages))
	}
}

func vaStreamNative(t *testing.T, rec *vaOpusRec) (*Native, *vaDown) {
	t.Helper()
	d := &vaDown{}
	n := NewNative(context.Background(), func(context.Context, websocket.MessageType, []byte) error { return nil }, d.send, fallbackResolver{})
	t.Cleanup(n.Close)
	n.LocalVoice = &LocalVoice{Enabled: true, TimeoutMS: 2000, FallbackPhrase: "На связи тёмная сторона.",
		Synth: func(context.Context, string) (SynthResult, error) {
			return SynthResult{URL: "http://x/a.mp3", Duration: time.Second}, nil
		},
		STT:  func(context.Context, []byte) (string, error) { return "какая погода", nil },
		Opus: rec.synth}
	return n, d
}

func vaVoiceRequest(n *Native, id string) *nativeRequest {
	req := &request.TAliceRequestProto{Header: &request.TAliceRequestProto_THeader{DialogId: new("d"), RequestId: new("r-" + id)}}
	ctx, cancel := context.WithCancel(context.Background())
	p := &nativeRequest{id: id, request: req, ctx: ctx, cancel: cancel, wakeAt: time.Now(), header: &header.TEventHeader{StreamId: new(int64(1))}}
	p.expires = time.AfterFunc(time.Minute, func() {})
	n.mu.Lock()
	n.requests[p.id] = p
	n.lastVoice = p
	n.mu.Unlock()
	return p
}

func TestVAOggPagesValidates(t *testing.T) {
	b := vaTestOpus(5, 50)
	pages, d, err := vaOggPages(b)
	if err != nil || len(pages) != 52 || d != time.Second {
		t.Fatalf("pages=%d d=%v err=%v", len(pages), d, err)
	}
	bad := bytes.Clone(b)
	bad[len(bad)-1] ^= 1
	if _, _, err := vaOggPages(bad); err == nil {
		t.Fatal("broken CRC accepted")
	}
	if _, _, err := vaOggPages(append(bytes.Clone(b), vaTestOpus(6, 3)...)); err == nil {
		t.Fatal("two streams accepted")
	}
	if _, _, err := vaOggPages(b[:len(b)-30]); err == nil {
		t.Fatal("truncated stream accepted")
	}
	stereo := vaTestOpus(5, 3)
	stereo[27+1+9] = 2
	binary.LittleEndian.PutUint32(stereo[22:], 0)
	if _, _, err := vaOggPages(stereo); err == nil {
		t.Fatal("stereo / broken head accepted")
	}
}

func TestVAHTTPOpus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "audio/ogg")
		_, _ = w.Write(vaTestOpus(1, 5))
	}))
	defer srv.Close()
	if b, err := HTTPOpus(srv.URL, "k", time.Second)(context.Background(), "привет"); err != nil || len(b) == 0 {
		t.Fatalf("err=%v", err)
	}
	if _, err := HTTPOpus(srv.URL, "x", time.Second)(context.Background(), "привет"); err == nil {
		t.Fatal("auth error expected")
	}
}

func TestVAStreamIDsAreOurs(t *testing.T) {
	a, b := vaNewVoiceStream(), vaNewVoiceStream()
	if a < vaVoiceStreamBase || b != a+2 || a>>24 != 0 {
		t.Fatalf("ids %x %x", a, b)
	}
}

func TestVAASRFallbackStreamsPhraseSilenceAnswer(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	p := vaVoiceRequest(n, "asr")
	p.audioInput = vaTestOpus(9, 3)                     // decodes to silence, STT stub answers
	n.resolver = vaSlowResolver{350 * time.Millisecond} // the model "thinks": silence must fill the gap
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	n.markAudioDone(1)
	vaWait(t, "close", func() bool { return d.closed("asr") })
	frames := d.snapshot()
	vaCheckSpeech(t, frames, "asr", "Ответ после отказа")
	got := rec.got()
	if len(got) != 2 || got[0] != "На связи тёмная сторона." || got[1] != "Ответ после отказа" {
		t.Fatalf("opus texts=%q", got)
	}
	var audio []byte
	for _, raw := range frames {
		if wire.Decode(raw, wire.ServerToClient).Server == nil {
			audio = append(audio, raw[4:]...)
		}
	}
	// phrase 0.5 s + answer 0.5 s; the 350 ms the model "thought" is Opus silence
	if _, d, err := vaOggPages(audio); err != nil || d < 1200*time.Millisecond {
		t.Fatalf("speaker stream %v err=%v: no Opus silence while the model was thinking", d, err)
	}
}

func TestVAASRFallbackOpusFailureUsesMP3(t *testing.T) {
	rec := &vaOpusRec{fail: map[string]bool{"*": true}}
	n, d := vaStreamNative(t, rec)
	n.LocalVoice.FallbackPhrase = "Фраза без кэша." // the phrase cache is per process
	p := vaVoiceRequest(n, "mp3")
	p.audioInput = vaTestOpus(9, 3)
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	n.markAudioDone(1)
	vaWait(t, "audio_play", func() bool {
		for _, raw := range d.snapshot() {
			if strings.Contains(string(raw), "audio_play") {
				return true
			}
		}
		return false
	})
	var responses int
	for _, raw := range d.snapshot() {
		f := wire.Decode(raw, wire.ServerToClient)
		if f.Server.GetEvent().GetAliceResponse() != nil {
			responses++
		}
		if f.Server.GetEvent().GetTtsSpeak() != nil {
			t.Fatal("no speech stream expected when Opus fails")
		}
	}
	if responses != 1 {
		t.Fatalf("responses=%d", responses)
	}
}

func vaCloudPending(n *Native, id string) *nativeRequest {
	p := vaVoiceRequest(n, id)
	n.mu.Lock()
	p.started, p.cloud, p.text, p.source = true, true, "расскажи анекдот", "voice"
	n.mu.Unlock()
	return p
}

func vaException(ref, msg string) []byte {
	return aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{
		Header: &header.TEventHeader{MessageId: "exc-" + ref, RefMessageId: ref},
		Event: &server.TServerMessage_TEvent_EventException{EventException: &event_exception.TEventException{
			Error: &event_exception.TEventException_TError{ErrorMessage: msg}}}}}})
}

func TestVACloudExceptionAnswersLocally(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	n.CloudFallback = &CloudFallback{Enabled: true, Sources: []string{"voice"}}
	vaCloudPending(n, "cl")
	if err := n.Server(context.Background(), websocket.MessageBinary, vaException("cl", "vins: internal error")); err != nil {
		t.Fatal(err)
	}
	vaWait(t, "close", func() bool { return d.closed("cl") })
	frames := d.snapshot()
	for _, raw := range frames {
		if wire.Decode(raw, wire.ServerToClient).Kind() == "EventException" {
			t.Fatal("exception must not reach the speaker")
		}
	}
	vaCheckSpeech(t, frames, "cl", "Ответ после отказа")
}

func TestVACloudExceptionNotAllowedPassesThrough(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	n.CloudFallback = &CloudFallback{Enabled: false}
	vaCloudPending(n, "off")
	if err := n.Server(context.Background(), websocket.MessageBinary, vaException("off", "vins: internal error")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(rec.got()) != 0 {
		t.Fatal("fallback must stay off")
	}
	var passed bool
	for _, raw := range d.snapshot() {
		passed = passed || wire.Decode(raw, wire.ServerToClient).Kind() == "EventException"
	}
	if !passed {
		t.Fatal("exception must be forwarded when fallback is off")
	}
}

func TestVAGoAwayStartsASRFallback(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	p := vaVoiceRequest(n, "ga")
	p.audioInput = vaTestOpus(9, 3)
	goAway := aa(&server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{MessageId: "bye"}, Event: &server.TServerMessage_TEvent_GoAway{GoAway: &go_away.TGoAway{}}}}})
	if k := wire.Decode(goAway, wire.ServerToClient).Kind(); k != "GoAway" {
		t.Skipf("frame kind %q", k)
	}
	if err := n.Server(context.Background(), websocket.MessageBinary, goAway); err != nil {
		t.Fatal(err)
	}
	n.markAudioDone(1)
	vaWait(t, "close", func() bool { return d.closed("ga") })
	vaCheckSpeech(t, d.snapshot(), "ga", "Ответ после отказа")
}

func TestVAUpstreamLostAnswersAndWaits(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	var ups int
	var upMu sync.Mutex
	n2 := NewNative(context.Background(), func(context.Context, websocket.MessageType, []byte) error {
		upMu.Lock()
		ups++
		upMu.Unlock()
		return nil
	}, d.send, fallbackResolver{})
	t.Cleanup(n2.Close)
	n2.LocalVoice, n2.CloudFallback = n.LocalVoice, &CloudFallback{Enabled: true, Sources: []string{"voice"}}
	vaCloudPending(n2, "lost")
	done := n2.UpstreamLost()
	_ = n2.up(context.Background(), websocket.MessageBinary, []byte("x"))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("UpstreamLost did not finish")
	}
	if !d.closed("lost") {
		t.Fatal("answer not finished before upstream close")
	}
	upMu.Lock()
	defer upMu.Unlock()
	if ups != 0 {
		t.Fatalf("frames sent to a lost upstream: %d", ups)
	}
	vaCheckSpeech(t, d.snapshot(), "lost", "Ответ после отказа")
}

func TestVAUpstreamLostIdleReturnsAtOnce(t *testing.T) {
	n := NewNative(context.Background(), func(context.Context, websocket.MessageType, []byte) error { return nil }, func(context.Context, websocket.MessageType, []byte) error { return nil }, fallbackResolver{})
	t.Cleanup(n.Close)
	select {
	case <-n.UpstreamLost():
	case <-time.After(time.Second):
		t.Fatal("idle session must not wait")
	}
}

func TestVALocalVoiceValidatesOpusEndpoint(t *testing.T) {
	v := LocalVoice{Enabled: true, Rules: []string{"x"}, Endpoint: "http://core:8090/v1/voice/synth", TimeoutMS: 6000, OpusEndpoint: "ftp://x"}
	if v.Validate() == nil {
		t.Fatal("bad opus endpoint accepted")
	}
	v.OpusEndpoint = "http://core:8090/v1/voice/opus"
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
}

type vaSlowResolver struct{ d time.Duration }

func (vaSlowResolver) Resolve(context.Context, string) (Resolution, error) {
	return Resolution{Route: "yandex"}, nil
}

func (r vaSlowResolver) Fallback(context.Context, string) (Resolution, error) {
	time.Sleep(r.d)
	return Resolution{Route: "llm", Answer: "Ответ после отказа"}, nil
}
