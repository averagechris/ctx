#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
uploader="$repo_root/scripts/upload-release-artifacts.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mock_srht="$tmp/srht"
cat >"$mock_srht" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_LOG"
if [[ "$*" == *" artifact list "* ]]; then
  printf '%s\n' "$MOCK_LIST_JSON"
elif [[ "$*" != *" artifact upload "* ]]; then
  printf 'unexpected srht invocation: %s\n' "$*" >&2
  exit 2
fi
EOF
chmod +x "$mock_srht"

artifact="$tmp/ctx-v1.2.3-x86_64-linux.tar.gz"
checksum="$artifact.sha256"
printf artifact >"$artifact"
printf checksum >"$checksum"

export TAG=v1.2.3 REPO=ctx SRHT_BIN="$mock_srht" JQ_BIN="${JQ_BIN:-jq}"
export MOCK_LOG="$tmp/calls"

assert_calls() {
  local expected="$1"
  local -a calls=()
  mapfile -t calls <"$MOCK_LOG"
  if [[ "${#calls[@]}" -ne "$expected" ]]; then
    printf 'expected %s srht calls, got %s:\n' "$expected" "${#calls[@]}" >&2
    printf '  %s\n' "${calls[@]}" >&2
    exit 1
  fi
}

assert_call() {
  local line="$1" expected="$2"
  local -a calls=()
  mapfile -t calls <"$MOCK_LOG"
  if [[ "${calls[$line]}" != "$expected" ]]; then
    printf 'unexpected srht call %s:\n  expected: %s\n  actual:   %s\n' \
      "$line" "$expected" "${calls[$line]}" >&2
    exit 1
  fi
}

: >"$MOCK_LOG"
export MOCK_LIST_JSON='{"items":[{"filename":"ctx-v1.2.3-x86_64-linux.tar.gz"}]}'
"$uploader" "$artifact"
assert_calls 1
assert_call 0 '--json git artifact list --repo ctx --rev v1.2.3'

: >"$MOCK_LOG"
export MOCK_LIST_JSON='{"items":[]}'
"$uploader" "$artifact" "$checksum"
assert_calls 4
assert_call 0 '--json git artifact list --repo ctx --rev v1.2.3'
assert_call 1 "git artifact upload --repo ctx --rev v1.2.3 $artifact"
assert_call 2 '--json git artifact list --repo ctx --rev v1.2.3'
assert_call 3 "git artifact upload --repo ctx --rev v1.2.3 $checksum"

: >"$MOCK_LOG"
export MOCK_LIST_JSON='{"unexpected":[]}'
if "$uploader" "$artifact"; then
  printf 'malformed artifact listing unexpectedly allowed an upload\n' >&2
  exit 1
fi
assert_calls 1

printf 'release artifact upload tests passed\n'
