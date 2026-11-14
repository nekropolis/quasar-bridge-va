# Quasar Bridge voice-assistant patches for v0.7.5

Image build checks out pristine [quasar-bridge v0.7.5](https://github.com/skrashevich/quasar-bridge/releases/tag/v0.7.5) (`eba23c92160d3a09d22f64426d2dba9e440dd7b5`) and applies `patches/` in order. Configuration is mounted at `/app/config.json`. This tree does not contain a deployment config, credentials, or device addresses.

```sh
docker build -f deploy/quasar-bridge/Dockerfile deploy/quasar-bridge
```

`verify.sh` applies the same series to a local archive of that commit and runs the image test set. It does not start the bridge.

## What stock v0.7.5 already does

- **Refusal fallback.** A cloud answer that matches `routing.refusal` is replaced by the local model (`FallbackResolver`). There is no filler phrase, no empty-answer path, and no timeout.
- **Microphone policy.** `listen_after_reply` and `reply.ListeningAllowed` already decide `should_listen`. AirPlay start, recovery, and configuration tools can suppress the microphone for one reply.
- **Dialogue follow-up.** After keyword rules miss, a short turn such as «а …» can stay on the local model when dialogue history exists (`dialogue.IsFollowup`). A phrase that hits a keyword rule, including «а музыку» via `yandex_services`, still goes to Yandex.
- **Progress tone.** Local waits can synthesize a discarded «а» so the stream stays open. That is not a filter for what the microphone heard.
- **Access flags.** `assistant_tools.updates` and `assistant_tools.configuration` default to true. AirPlay is enabled unless `airplay.disabled` is true. Explicit false values are honored.

Open upstream pull requests 42 and 43 (wake-word routes) and 44 (acoustic whisper) are not merged and are not part of this series. Ellipsis handling only skips named keyword rules, so a later wake-word route that runs before those rules would still run.

## What this series still customizes

`0001-va-session-behaviors.patch`

- **Quiet follow-up.** For two minutes after a local reply that reopened the microphone, an empty transcript is closed without speech, without listening again, and without Yandex. An empty first request is unchanged.
- **Stop and interruption.** In that window, «стоп», «хватит», «замолчи», «помолчи», «отмена», «отмени», «тихо», and «достаточно» (optionally with «Алиса») close the same way. Outside the window they follow normal routing. A new voice request cancels our unfinished local speech. Played, cloud, text, and sound or music requests are left alone.
- **Filler filter.** `followup_filter` drops acknowledgements and short background fragments in that window, except after a reply that contained `?`. `enabled: false` turns it off. The JSON shape is unchanged.
- **Empty-follow-up reprompt.** `followup_reprompt` speaks its text once when the transcript is empty but at least `min_matched` voice frames arrived, then listens again. The next empty result in the same chain closes quietly.
- **Cloud fallback and events.** Empty cloud answers and timeouts use `cloud_fallback` (filler, source list, excluded fragments) and then the local model. Refusal replacement stays the existing path and is not limited by that source list; with `cloud_fallback` unset it is unchanged, and `on_refusal: false` while the block is enabled suppresses it. `va_events` posts route records to `<llm_base_url>/va/event`. The API key is read from `QUASAR_LLM_API_KEY` and is not written into the image.
- **Ellipsis.** `ellipsis_followup` treats a short «а …» / «ну а …» within `window_s` of playback end as a continuation: the named rules (default `yandex_services`) are skipped so the existing classifier or dialogue follow-up can decide. Command verbs such as «а включи музыку» still match the rules.
- **`contains_alone`.** `routing.refusal.contains_alone` matches phrases anywhere in the answer, without a prefix. Spoken text behind a card is checked as well.
- **`va_listen`.** A top-level `va_listen: false` on a chat-completions answer suppresses the microphone for that reply. Absence leaves the previous policy. Structured classifier responses ignore the field.

`0002-deployment-safe-defaults.patch`

Omitted `assistant_tools.updates` and `assistant_tools.configuration` stay false, and omitted `airplay.disabled` stays true. Explicit true or false in the config file still wins, including the values in the upstream example config.

## Checks

The series applies with `git apply` to a clean v0.7.5 tree. `go test ./...` passed on the patched tree, and `go test ./internal/bridge/ ./internal/llm/ ./cmd/quasar-bridge/` passed again after applying the series to a second pristine tree. Patch checksums are in `SHA256SUMS`.
