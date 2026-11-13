package bridge

import (
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestVAFillerClassifier(t *testing.T) {
	f := DefaultFollowupFilter()
	for _, s := range []string{"понятно", "Понятно.", "ага", "спасибо", "ну ладно", "да-да", "угу", "окей", "хорошо", "ясно спасибо", "мама пришла", "интересная штука"} {
		if !f.IsFiller(s) {
			t.Errorf("must be filler: %q", s)
		}
	}
	for _, s := range []string{"а завтра", "а в детской", "громче", "выключи свет", "да", "нет", "давай", "ок", "Алиса понятно",
		"сколько градусов", "что?", "включи", "в зале", "почему", "поставь таймер на 5 минут", "расскажи подробнее про погоду завтра", "10"} {
		if f.IsFiller(s) {
			t.Errorf("must pass: %q", s)
		}
	}
	off := DefaultFollowupFilter()
	off.Enabled = false
	if off.IsFiller("понятно") {
		t.Error("disabled filter must pass everything")
	}
	var nilF *FollowupFilter
	if nilF.IsFiller("понятно") {
		t.Error("nil filter must pass")
	}
}

func vaRunFollowUp(t *testing.T, asked bool, text string) (silent bool, calls int) {
	n, up, down := vaSession(t, &calls)
	f := DefaultFollowupFilter()
	n.Filler = &f
	n.listenArmed = time.Now()
	n.lastAsked = asked
	_ = n.Client(t.Context(), websocket.MessageBinary, vaVoice("ff"))
	<-up
	_ = n.Server(t.Context(), websocket.MessageBinary, vaASR("ff", text))
	<-down
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-up:
		case fr := <-down:
			if r := fr.Server.GetEvent().GetAliceResponse(); r != nil {
				return !r.GetVoiceResponse().GetHasVoiceResponse(), calls
			}
		case <-deadline:
			t.Fatal("no response")
		}
	}
}

func TestVAFillerFollowUpSilent(t *testing.T) {
	if silent, calls := vaRunFollowUp(t, false, "понятно"); !silent || calls != 0 {
		t.Fatalf("filler must close silently: silent=%v calls=%d", silent, calls)
	}
}

func TestVAFillerAfterOurQuestionPasses(t *testing.T) {
	if silent, calls := vaRunFollowUp(t, true, "хорошо"); silent || calls != 1 {
		t.Fatalf("after our question reply must pass: silent=%v calls=%d", silent, calls)
	}
}

func TestVAQuestionFollowUpPasses(t *testing.T) {
	if silent, calls := vaRunFollowUp(t, false, "а завтра"); silent || calls != 1 {
		t.Fatalf("question must pass: silent=%v calls=%d", silent, calls)
	}
}
