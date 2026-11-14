package bridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLocalVoiceApplies(t *testing.T) {
	var nilVoice *LocalVoice
	if nilVoice.Applies("x") {
		t.Fatal("nil must not apply")
	}
	v := &LocalVoice{Enabled: true, Rules: []string{"wakeword_ey_sistema_to_llm", "dark_side_to_llm"}, Synth: func(context.Context, string) (string, error) { return "", nil }}
	for rule, want := range map[string]bool{"wakeword_ey_sistema_to_llm": true, "dark_side_to_llm": true, "force_to_llm": false, "": false} {
		if v.Applies(rule) != want {
			t.Errorf("%q: want %v", rule, want)
		}
	}
	v.Enabled = false
	if v.Applies("dark_side_to_llm") {
		t.Fatal("disabled must not apply")
	}
}

func TestLocalVoiceValidate(t *testing.T) {
	if err := DefaultLocalVoice().Validate(); err != nil {
		t.Fatalf("default off must be valid: %v", err)
	}
	bad := []LocalVoice{
		{Enabled: true, Endpoint: "http://core:8090/v1/voice/tts", TimeoutMS: 6000},
		{Enabled: true, Rules: []string{"a"}, Endpoint: "ftp://x", TimeoutMS: 6000},
		{Enabled: true, Rules: []string{"a"}, Endpoint: "http://core:8090/v1/voice/tts", TimeoutMS: 10},
	}
	for i, v := range bad {
		if v.Validate() == nil {
			t.Errorf("case %d must fail", i)
		}
	}
	ok := LocalVoice{Enabled: true, Rules: []string{"a"}, Endpoint: "http://core:8090/v1/voice/tts", TimeoutMS: 6000}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSpeakableText(t *testing.T) {
	got := SpeakableText(`<speaker is_whisper="true">Привет,  sil <[300]> мир`)
	if got != "Привет, мир" {
		t.Fatalf("got %q", got)
	}
}

func TestHTTPSynth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			w.WriteHeader(401)
			return
		}
		w.Write([]byte(`{"url":"http://192.168.0.151:8091/voice/abc.mp3"}`))
	}))
	defer srv.Close()
	u, err := HTTPSynth(srv.URL, "k", 2*time.Second)(context.Background(), "текст")
	if err != nil || !strings.HasSuffix(u, "/voice/abc.mp3") {
		t.Fatalf("u=%q err=%v", u, err)
	}
	if _, err := HTTPSynth(srv.URL, "wrong", 2*time.Second)(context.Background(), "текст"); err == nil {
		t.Fatal("401 must fail")
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"url":"file:///etc/passwd"}`)) }))
	defer bad.Close()
	if _, err := HTTPSynth(bad.URL, "", 2*time.Second)(context.Background(), "т"); err == nil {
		t.Fatal("non-http url must fail")
	}
}

func TestSoundURLDirective(t *testing.T) {
	d, err := makeSoundDirective(soundURLRequest("http://192.168.0.151:8091/voice/abc.mp3"))
	if err != nil || d.Name != "audio_play" {
		t.Fatalf("d=%v err=%v", d, err)
	}
}
