#!/usr/bin/env bash
set -euo pipefail

repo="${REPO:-ctx}"
tag="${TAG:?TAG must name the release tag}"

if [[ -n "${SRHT_BIN:-}" ]]; then
  srht_cmd=("$SRHT_BIN")
else
  srht_cmd=(nix run --inputs-from . fleet#srht --)
fi

if [[ -n "${JQ_BIN:-}" ]]; then
  jq_cmd=("$JQ_BIN")
else
  jq_cmd=(nix run --inputs-from . nixpkgs#jq --)
fi

artifact_exists() {
  local artifact="$1"
  local filename listing status
  filename="${artifact##*/}"
  listing="$("${srht_cmd[@]}" --json git artifact list --repo "$repo" --rev "$tag")"

  # The single-quoted variables below belong to jq, not the shell.
  # shellcheck disable=SC2016
  if printf '%s\n' "$listing" | "${jq_cmd[@]}" -e --arg filename "$filename" '
    .items as $items
    | if ($items | type) != "array"
      then error("artifact listing has no items array")
      else any($items[]; .filename == $filename)
      end
  ' >/dev/null; then
    return 0
  else
    status=$?
    if [[ "$status" -eq 1 ]]; then
      return 1
    fi
    printf 'failed to parse artifact listing for %s at %s\n' "$repo" "$tag" >&2
    return "$status"
  fi
}

upload_artifact() {
  local artifact="$1"
  local filename="${artifact##*/}" status

  if [[ ! -f "$artifact" ]]; then
    printf 'release artifact not found: %s\n' "$artifact" >&2
    return 1
  fi

  if artifact_exists "$artifact"; then
    printf 'artifact already attached to %s at %s; skipping: %s\n' "$repo" "$tag" "$filename"
    return 0
  else
    status=$?
    if [[ "$status" -ne 1 ]]; then
      return "$status"
    fi
  fi

  "${srht_cmd[@]}" git artifact upload --repo "$repo" --rev "$tag" "$artifact"
}

if [[ "$#" -eq 0 ]]; then
  printf 'usage: TAG=vX.Y.Z %s ARTIFACT [...]\n' "${0##*/}" >&2
  exit 2
fi

for artifact in "$@"; do
  upload_artifact "$artifact"
done
