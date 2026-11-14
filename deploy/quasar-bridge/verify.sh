#!/bin/sh
# Apply deploy/quasar-bridge/patches to a pristine v0.7.5 tree and run the
# same tests the image build runs. Does not start a service or open a device.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
repo=$(CDPATH= cd -- "$root/../.." && pwd)
pin=eba23c92160d3a09d22f64426d2dba9e440dd7b5
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git -C "$repo" archive "$pin" | tar -C "$work" -x
cd "$work"
for p in $(ls "$root"/patches/*.patch | sort); do
  git apply --verbose "$p"
done
go test ./internal/bridge/ ./internal/llm/ ./cmd/quasar-bridge/ -count=1
