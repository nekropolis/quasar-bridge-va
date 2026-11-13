package bridge

import (
	"context"
	"slices"
	"strings"
	"time"
)

// EllipsisFollowup keeps a short elliptical continuation such as «а музыку»
// from being captured by keyword rules that force the cloud. Explicit commands
// still match those rules. Nil or Enabled false leaves routing unchanged.
type EllipsisFollowup struct {
	Enabled   bool     `json:"enabled"`
	WindowS   int      `json:"window_s"`
	MaxWords  int      `json:"max_words"`
	Prefixes  []string `json:"prefixes"`
	Commands  []string `json:"commands"`
	SkipRules []string `json:"skip_rules"`
}

func DefaultEllipsisFollowup() EllipsisFollowup {
	return EllipsisFollowup{
		Enabled:   true,
		WindowS:   15,
		MaxWords:  4,
		Prefixes:  []string{"ну а", "а"},
		Commands:  strings.Fields("включи включить включай выключи выключить поставь поставить запусти открой закрой сделай убавь прибавь найди сыграй играй давай отмени останови продолжи следующий следующую предыдущий громче тише стоп хватит пауза вруби выруби"),
		SkipRules: []string{"yandex_services"},
	}
}

// IsEllipsis reports whether text looks like «а …»: a configured prefix, at
// most MaxWords words, and the word after the prefix is not a command verb.
func (c *EllipsisFollowup) IsEllipsis(text string) bool {
	if c == nil || !c.Enabled {
		return false
	}
	words := vaWords(text)
	if len(words) > 0 && words[0] == "алиса" {
		words = words[1:]
	}
	if len(words) < 2 || len(words) > c.MaxWords {
		return false
	}
	for _, prefix := range c.Prefixes {
		pw := vaWords(prefix)
		if len(pw) == 0 || len(words) <= len(pw) || !slices.Equal(words[:len(pw)], pw) {
			continue
		}
		next := words[len(pw)]
		for _, cmd := range c.Commands {
			if cw := vaWords(cmd); len(cw) == 1 && cw[0] == next {
				return false
			}
		}
		return true
	}
	return false
}

// InWindow reports whether an utterance that began at start follows an answer
// that finished at last closely enough.
func (c *EllipsisFollowup) InWindow(last, start time.Time) bool {
	if c == nil || !c.Enabled || last.IsZero() || start.IsZero() || start.Before(last) {
		return false
	}
	return start.Sub(last) <= time.Duration(c.WindowS)*time.Second
}

type skipRulesKey struct{}

// WithSkipRules marks a request whose keyword rules with these names are ignored.
func WithSkipRules(ctx context.Context, rules []string) context.Context {
	if len(rules) == 0 {
		return ctx
	}
	return context.WithValue(ctx, skipRulesKey{}, slices.Clone(rules))
}

func skipRulesFrom(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	rules, _ := ctx.Value(skipRulesKey{}).([]string)
	return rules
}
