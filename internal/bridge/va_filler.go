package bridge

import (
	"strings"
	"unicode"
)

// FollowupFilter drops short acknowledgements and background fragments heard in
// the window after a local reply. A nil filter or Enabled false keeps them.
type FollowupFilter struct {
	Enabled       bool     `json:"enabled"`
	Fillers       []string `json:"fillers"`
	Keep          []string `json:"keep"`
	ShortMaxWords int      `json:"short_max_words"`
}

func DefaultFollowupFilter() FollowupFilter {
	return FollowupFilter{Enabled: true, ShortMaxWords: 2,
		Fillers: strings.Fields("понятно ага ясно спасибо ладно хорошо окей угу ну да-да ага-ага угу-угу ясненько понял поняла принято отлично супер класс круто ок-ок спасибочки благодарю мерси"),
		Keep: strings.Fields("алиса да нет не неа давай давайте ок конечно ага-да верно точно отмена стоп хватит громче тише еще ещё дальше повтори снова опять " +
			"а и что как где когда сколько какой какая какое какие почему зачем кто чей ли куда откуда в во на про для с со у о об по до от из за под над при"),
	}
}

func vaWords(s string) []string {
	s = strings.ReplaceAll(strings.ToLower(s), "ё", "е")
	return strings.FieldsFunc(s, func(r rune) bool { return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-') })
}

func (f *FollowupFilter) set(xs []string) map[string]bool {
	m := map[string]bool{}
	for _, x := range xs {
		m[strings.Join(vaWords(x), " ")] = true
	}
	return m
}

// IsFiller reports whether a follow-up utterance should be ignored silently.
func (f *FollowupFilter) IsFiller(text string) bool {
	if f == nil || !f.Enabled || strings.Contains(text, "?") {
		return false
	}
	words := vaWords(text)
	if len(words) == 0 {
		return false
	}
	keep, fill := f.set(f.Keep), f.set(f.Fillers)
	for _, word := range words {
		if keep[word] {
			return false
		}
	}
	if fill[strings.Join(words, " ")] {
		return true
	}
	allFill := true
	for _, word := range words {
		if !fill[word] {
			allFill = false
		}
	}
	if allFill {
		return true
	}
	if len(words) > f.ShortMaxWords {
		return false
	}
	for _, word := range words {
		runes := []rune(word)
		if unicode.IsDigit(runes[0]) {
			return false
		}
		if len(runes) > 3 && (strings.HasSuffix(word, "и") || strings.HasSuffix(word, "й") || strings.HasSuffix(word, "ь") || strings.HasSuffix(word, "те")) {
			return false
		}
	}
	return true
}
