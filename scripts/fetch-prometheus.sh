#!/bin/sh
# Optional test tools, pinned to the same release as the embedded rule engine.
set -eu
version=3.15.0
platform=${PROMETHEUS_PLATFORM:-linux-amd64}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
archive="prometheus-$version.$platform.tar.gz"
base="https://github.com/prometheus/prometheus/releases/download/v$version"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
curl -fL --retry 2 "$base/$archive" -o "$tmp/$archive"
curl -fL --retry 2 "$base/sha256sums.txt" -o "$tmp/sha256sums.txt"
(cd "$tmp"; grep " $archive\$" sha256sums.txt | sha256sum -c -)
mkdir -p "$root/.tools"
tar -xzf "$tmp/$archive" -C "$root/.tools" --strip-components=1 \
 "prometheus-$version.$platform/prometheus" "prometheus-$version.$platform/promtool"
printf 'Installed Prometheus %s test tools in %s/.tools\n' "$version" "$root"
