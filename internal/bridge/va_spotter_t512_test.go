package bridge

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	asr "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result"
	recognition "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/asr_result/common"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	sv "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/spotter_validation"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"

	"quasarbridge/internal/wire"
)

type spotRec struct {
	mu    sync.Mutex
	lines []string
}

func (r *spotRec) log(msg string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, strings.TrimSpace(msg+" "+fmt.Sprintln(args...)))
}

func voiceIn(id string) wire.Frame {
	return wire.Frame{Client: &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: id}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: &request.TAliceRequestProto{}}}}}}
}

func asrEnd(ref, text string) wire.Frame {
	return wire.Frame{Server: &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: ref}, Event: &server.TServerMessage_TEvent_AsrResult{AsrResult: &asr.TAsrResult{EndOfUtt: new(true), Recognition: []*recognition.TAsrRecognition{{Normalized: new(text)}}}}}}}}
}

func validation(ref string, canceled bool) wire.Frame {
	return wire.Frame{Server: &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{RefMessageId: ref}, Event: &server.TServerMessage_TEvent_SpotterValidation{SpotterValidation: &sv.TSpotterValidation{Result: sv.TSpotterValidation_Ok, CanceledCauseOfMultiactivation: new(canceled)}}}}}}
}

func TestT512SpotterValidationLogged(t *testing.T) {
	now := time.Unix(1000, 0)
	r := &spotRec{}
	s := &SpotterLog{Now: func() time.Time { return now }, Log: r.log}
	s.Observe(voiceIn("a"))
	now = now.Add(2 * time.Second)
	s.Observe(asrEnd("a", "включи горячую воду"))
	now = now.Add(600 * time.Millisecond)
	s.Observe(validation("a", true))
	now = now.Add(time.Minute)
	s.Flush()
	if len(r.lines) != 1 {
		t.Fatalf("lines=%q", r.lines)
	}
	l := r.lines[0]
	for _, want := range []string{"validation true", "matched true", "result Ok", "canceled_multiactivation true", "wake_ms 2600", "eou_ms 600", "включи горячую воду", requestTag("a")} {
		if !strings.Contains(l, want) {
			t.Fatalf("missing %q in %q", want, l)
		}
	}
}

func TestT512SpotterValidationMissing(t *testing.T) {
	now := time.Unix(1000, 0)
	r := &spotRec{}
	s := &SpotterLog{Now: func() time.Time { return now }, Log: r.log, Timeout: 5 * time.Second}
	s.Observe(voiceIn("b"))
	now = now.Add(time.Second)
	s.Observe(asrEnd("b", "свет"))
	now = now.Add(4 * time.Second)
	s.Flush()
	if len(r.lines) != 0 {
		t.Fatalf("too early: %q", r.lines)
	}
	now = now.Add(1100 * time.Millisecond)
	s.Flush()
	if len(r.lines) != 1 || !strings.Contains(r.lines[0], "validation false") || !strings.Contains(r.lines[0], "eou_ms 5100") {
		t.Fatalf("lines=%q", r.lines)
	}
	s.Flush()
	if len(r.lines) != 1 {
		t.Fatalf("logged twice: %q", r.lines)
	}
}

func TestT512SpotterNoAsrWakeTimeout(t *testing.T) {
	now := time.Unix(1000, 0)
	r := &spotRec{}
	s := &SpotterLog{Now: func() time.Time { return now }, Log: r.log, WakeTimeout: 20 * time.Second}
	s.Observe(voiceIn("c"))
	now = now.Add(21 * time.Second)
	s.Flush()
	if len(r.lines) != 1 || !strings.Contains(r.lines[0], "validation false") {
		t.Fatalf("lines=%q", r.lines)
	}
}
