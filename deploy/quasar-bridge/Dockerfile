# Quasar Bridge v0.7.5 plus the local voice-assistant patches in patches/.
# Pin: tag v0.7.5, commit eba23c92160d3a09d22f64426d2dba9e440dd7b5.
FROM golang:1.27-bookworm AS build
ARG QB_REF=eba23c92160d3a09d22f64426d2dba9e440dd7b5
RUN git clone https://github.com/skrashevich/quasar-bridge /src \
 && git -C /src checkout "$QB_REF"
WORKDIR /src
COPY patches/ /patches/
RUN set -eu; for p in $(ls /patches/*.patch | sort); do git apply --verbose "$p"; done
RUN go test ./internal/bridge/ ./internal/llm/ ./cmd/quasar-bridge/ -count=1 \
 && CGO_ENABLED=0 go build -trimpath -o /out/quasar-bridge ./cmd/quasar-bridge

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/quasar-bridge /quasar-bridge
WORKDIR /app
EXPOSE 8443
ENTRYPOINT ["/quasar-bridge", "-config", "/app/config.json"]
