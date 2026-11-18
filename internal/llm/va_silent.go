package llm

import "errors"

// ErrSilent (VA patch t511) — our core answered with empty content and
// "va_silent": true: the other speaker answers this utterance, this one stays
// silent instead of saying the generic "no answer from the local model".
var ErrSilent = errors.New("core asked for intentional silence")
