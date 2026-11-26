package bridge

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"quasarbridge/internal/wire"
)

func TestVAT539StopClosesSilently(t *testing.T) {
	rec := &vaOpusRec{}
	n, d := vaStreamNative(t, rec)
	calls := vaListenCounter(n)
	n.LocalVoice.STT = func(context.Context, []byte) (string, error) { return "Стоп.", nil }
	p := vaVoiceRequest(n, "stop")
	p.audioInput = vaTestOpus(9, 3)
	if !n.startASRFallback() {
		t.Fatal("fallback not started")
	}
	n.markAudioDone(1)
	vaWait(t, "response", func() bool {
		for _, raw := range d.snapshot() {
			if wire.Decode(raw, wire.ServerToClient).Server.GetEvent().GetAliceResponse() != nil {
				return true
			}
		}
		return false
	})
	time.Sleep(300 * time.Millisecond)
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("stop word must not be spoken: %q", got)
	}
	for _, raw := range d.snapshot() {
		f := wire.Decode(raw, wire.ServerToClient)
		if f.Server.GetEvent().GetTtsSpeak() != nil {
			t.Fatal("no speech stream on a stop word")
		}
		if r := f.Server.GetEvent().GetAliceResponse(); r != nil && r.GetVoiceResponse().GetOutputSpeech().GetText() != "" {
			t.Fatalf("spoken text %q", r.GetVoiceResponse().GetOutputSpeech().GetText())
		}
	}
	if calls.Load() != 0 {
		t.Fatal("no microphone after a stop word")
	}
}

func vaSTTSeq(texts ...string) (func(context.Context, []byte) (string, error), *atomic.Int32) {
	var calls atomic.Int32
	return func(context.Context, []byte) (string, error) {
		i := int(calls.Add(1)) - 1
		if i < len(texts) {
			return texts[i], nil
		}
		return texts[len(texts)-1], nil
	}, &calls
}

func TestVAT539EmptySTTWithSpeechRetriesOnce(t *testing.T) {
	old := vaSpeechCheck
	t.Cleanup(func() { vaSpeechCheck = old })
	for _, tc := range []struct {
		name   string
		speech bool
		texts  []string
		calls  int32
		last   string
	}{
		{"retry_heard", true, []string{"", "какая погода"}, 2, "Ответ после отказа"},
		{"retry_empty", true, []string{""}, 2, vaNotHeard},
		{"no_speech", false, []string{""}, 1, vaNotHeard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			speech := tc.speech
			vaSpeechCheck = func([]float32, int) bool { return speech }
			rec := &vaOpusRec{}
			n, d := vaStreamNative(t, rec)
			stt, calls := vaSTTSeq(tc.texts...)
			n.LocalVoice.STT = stt
			p := vaVoiceRequest(n, "re-"+tc.name)
			p.audioInput = vaTestOpus(9, 3)
			if !n.startASRFallback() {
				t.Fatal("fallback not started")
			}
			n.markAudioDone(1)
			vaWait(t, "close", func() bool { return d.closed("re-" + tc.name) })
			if calls.Load() != tc.calls {
				t.Fatalf("stt calls %d, want %d", calls.Load(), tc.calls)
			}
			if got := rec.got(); len(got) == 0 || got[len(got)-1] != tc.last {
				t.Fatalf("spoken %q, want last %q", got, tc.last)
			}
		})
	}
}

func TestVAT539HasSpeech(t *testing.T) {
	if !vaHasSpeech(append(vaTone(.3, .001), vaTone(.4, .3)...), 16000) {
		t.Fatal("tone is speech")
	}
	if vaHasSpeech(vaTone(1, .001), 16000) {
		t.Fatal("silence is not speech")
	}
}
