package bridge

// t512: observation only. Logs Yandex cloud multiactivation verdicts
// (SpotterValidation) per voice request so we can later check whether the
// cloud picks the nearest speaker. Behaviour is not changed.

import (
	"log/slog"
	"sync"
	"time"

	"quasarbridge/internal/wire"
)

const spotterMaxText = 60

type spotterReq struct {
	tag      string
	wake     time.Time
	eou      time.Time
	text     string
	seen     bool
	deadline time.Time
}

// SpotterLog tracks VoiceInput -> AsrResult(EndOfUtt) -> SpotterValidation.
type SpotterLog struct {
	Timeout     time.Duration // after end of utterance (or WakeTimeout after wake)
	WakeTimeout time.Duration
	Now         func() time.Time
	Log         func(msg string, args ...any)

	mu   sync.Mutex
	reqs map[string]*spotterReq
	last string
}

func (s *SpotterLog) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *SpotterLog) log(args ...any) {
	if s.Log != nil {
		s.Log("va_spotter", args...)
		return
	}
	slog.Info("va_spotter", args...)
}

func (s *SpotterLog) timeouts() (time.Duration, time.Duration) {
	t, w := s.Timeout, s.WakeTimeout
	if t <= 0 {
		t = 5 * time.Second
	}
	if w <= 0 {
		w = 20 * time.Second
	}
	return t, w
}

func ms(a, b time.Time) any {
	if a.IsZero() || b.IsZero() {
		return nil
	}
	return b.Sub(a).Milliseconds()
}

func cut(s string) string {
	r := []rune(s)
	if len(r) > spotterMaxText {
		return string(r[:spotterMaxText])
	}
	return s
}

// Observe must be called for every decoded frame (both directions).
func (s *SpotterLog) Observe(f wire.Frame) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reqs == nil {
		s.reqs = map[string]*spotterReq{}
	}
	s.expireLocked(now)
	if f.Client != nil {
		e := f.Client.GetEvent()
		if e != nil && e.GetVoiceInput() != nil {
			id := e.GetHeader().GetMessageId()
			_, w := s.timeouts()
			s.reqs[id] = &spotterReq{tag: requestTag(id), wake: now, deadline: now.Add(w)}
			s.last = id
		}
		return
	}
	if f.Server == nil || f.Server.GetEvent() == nil {
		return
	}
	e := f.Server.GetEvent()
	if asr := e.GetAsrResult(); asr != nil && asr.GetEndOfUtt() {
		if p := s.findLocked(e.GetHeader().GetRefMessageId()); p != nil && p.eou.IsZero() {
			p.eou = now
			if r := asr.GetRecognition(); len(r) > 0 {
				p.text = cut(r[0].GetNormalized())
			}
			t, _ := s.timeouts()
			p.deadline = now.Add(t)
		}
		return
	}
	if v := e.GetSpotterValidation(); v != nil {
		ref := e.GetHeader().GetRefMessageId()
		p := s.findLocked(ref)
		if p == nil {
			s.log("request", requestTag(ref), "validation", true, "matched", false, "result", v.GetResult().String(), "canceled_multiactivation", v.GetCanceledCauseOfMultiactivation())
			return
		}
		p.seen = true
		s.log("request", p.tag, "validation", true, "matched", true, "result", v.GetResult().String(), "canceled_multiactivation", v.GetCanceledCauseOfMultiactivation(), "wake_ms", ms(p.wake, now), "eou_ms", ms(p.eou, now), "wake_unix_ms", p.wake.UnixMilli(), "text", p.text)
	}
}

func (s *SpotterLog) findLocked(ref string) *spotterReq {
	if p := s.reqs[ref]; p != nil {
		return p
	}
	if p := s.reqs[s.last]; p != nil && ref == "" {
		return p
	}
	return nil
}

// Flush logs "no validation" for requests whose deadline passed.
func (s *SpotterLog) Flush() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)
}

func (s *SpotterLog) expireLocked(now time.Time) {
	for id, p := range s.reqs {
		if now.Before(p.deadline) {
			continue
		}
		if !p.seen {
			s.log("request", p.tag, "validation", false, "wake_ms", ms(p.wake, now), "eou_ms", ms(p.eou, now), "wake_unix_ms", p.wake.UnixMilli(), "text", p.text)
		}
		delete(s.reqs, id)
	}
}

// Run flushes periodically until stop is closed.
func (s *SpotterLog) Run(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.Flush()
		}
	}
}
