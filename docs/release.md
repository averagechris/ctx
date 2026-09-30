# Manual GitHub release

Fleet prepares and validates release refs locally. GitHub Actions only rebuilds
the two public artifacts from a trusted annotated tag. A person verifies the
bytes and publishes the GitHub Release. This repository has no PR release
workflow, automatic publisher, release secret, or automatic Pages dispatch.

## 1. Check the release

Start from an empty jj working-copy commit whose parent, local `main`, and
`main@origin` are identical:

```bash
nix run .#release -- --version X.Y.Z --check
```

The check is read-only. It checks refs and version state, but does not run gates
or build artifacts.

## 2. Run the local release

```bash
nix run .#release -- --version X.Y.Z
```

This prepares the version and changelog, preserves the Cargo version and
lockfile stamping, runs fmt, clippy, tests, and `ci-docs`, then atomically pushes
`main` and annotated `vX.Y.Z`. It does not create a GitHub Release or upload
assets. The GitHub backend rejects `--submit-linux-build`.

## 3. Check the artifact run

The annotated tag push calls Fleet at
`e31a02573d79dfeb2496fec6c21cf74a0ece4d79` for `aarch64-darwin` and
`x86_64-linux`. The caller and reusable workflow have read-only repository
permission. Local release validation happens before the tag push. Actions checks
the published annotated-tag identity, then builds and verifies each artifact
from that tag.

Use only one completed, successful run for `vX.Y.Z`. If the tag-push run must be
recovered, dispatch `Build release artifacts (manual publication required)` from
`main` and supply the existing annotated tag. Do not dispatch against an
unreviewed branch or a lightweight tag. Do not select a run by recency: supply
the exact run ID of the run you reviewed. The metadata check below accepts the
normal tag push or the documented `workflow_dispatch` recovery run. For a
recovery run, the workflow's existing validation must have proved that the
selected tag commit is an ancestor of the selected `main` commit; the identity
files below still bind the downloaded artifacts to the selected tag. Download
the two named artifacts (`release-aarch64-darwin` and `release-x86_64-linux`)
into a new, empty directory. The directory must contain exactly these six files
and no others:

```bash
set -euo pipefail
tag=vX.Y.Z
remote=$(git ls-remote --tags origin "refs/tags/$tag" "refs/tags/$tag^{}")
tag_object=$(awk -v ref="refs/tags/$tag" '$2 == ref {print $1}' <<<"$remote")
peeled_commit=$(awk -v ref="refs/tags/$tag^{}" '$2 == ref {print $1}' <<<"$remote")
test -n "$tag_object" && test -n "$peeled_commit"

read -r -p "Exact successful release run ID for $tag: " run_id
[[ "$run_id" =~ ^[0-9]+$ ]]
run=$(gh run view "$run_id" --repo averagechris/ctx \
  --json workflowName,conclusion,event,headBranch,headSha)
test "$(jq -r '.workflowName' <<<"$run")" = \
  'Build release artifacts (manual publication required)'
test "$(jq -r '.conclusion' <<<"$run")" = success
run_event=$(jq -r '.event' <<<"$run")
run_branch=$(jq -r '.headBranch' <<<"$run")
run_sha=$(jq -r '.headSha' <<<"$run")
if [[ "$run_event" == push ]]; then
  test "$run_branch" = "$tag"
  test "$run_sha" = "$peeled_commit"
elif [[ "$run_event" == workflow_dispatch ]]; then
  test "$run_branch" = main
else
  printf 'release run must be a tag push or main workflow_dispatch\n' >&2
  exit 1
fi

rm -rf actions
mkdir actions
(cd actions && gh run download "$run_id" --repo averagechris/ctx \
  --name release-aarch64-darwin)
(cd actions && gh run download "$run_id" --repo averagechris/ctx \
  --name release-x86_64-linux)
cd actions
```

```text
ctx-vX.Y.Z-aarch64-darwin.tar.gz
ctx-vX.Y.Z-aarch64-darwin.tar.gz.sha256
release-identity-aarch64-darwin
ctx-vX.Y.Z-x86_64-linux.tar.gz
ctx-vX.Y.Z-x86_64-linux.tar.gz.sha256
release-identity-x86_64-linux
```

If any file is missing, duplicated, or unexpected, stop. Verify the six-file
set, both sidecars, and both identity files before creating a release:

```bash
expected_files=(
  ctx-vX.Y.Z-aarch64-darwin.tar.gz
  ctx-vX.Y.Z-aarch64-darwin.tar.gz.sha256
  release-identity-aarch64-darwin
  ctx-vX.Y.Z-x86_64-linux.tar.gz
  ctx-vX.Y.Z-x86_64-linux.tar.gz.sha256
  release-identity-x86_64-linux
)
actual_files=$(find . -type f -print | sed 's#^./##' | sort)
test "$actual_files" = "$(printf '%s\n' "${expected_files[@]}" | sort)"

shasum -a 256 -c ctx-vX.Y.Z-aarch64-darwin.tar.gz.sha256
shasum -a 256 -c ctx-vX.Y.Z-x86_64-linux.tar.gz.sha256

remote=$(git ls-remote --tags origin refs/tags/vX.Y.Z refs/tags/vX.Y.Z^{})
tag_object=$(awk '$2 == "refs/tags/vX.Y.Z" {print $1}' <<<"$remote")
peeled_commit=$(awk '$2 == "refs/tags/vX.Y.Z^{}" {print $1}' <<<"$remote")
test -n "$tag_object" && test -n "$peeled_commit"
for identity in release-identity-aarch64-darwin release-identity-x86_64-linux; do
  test "$(wc -l <"$identity" | tr -d ' ')" -eq 2
  test "$(sed -n '1p' "$identity")" = "$tag_object"
  test "$(sed -n '2p' "$identity")" = "$peeled_commit"
done
```

The identity files must agree with each other and with the remote annotated tag
object and peeled commit. Publish only the two tarballs and their two sidecars.

## 4. Draft, verify, and publish

Create a draft before uploading bytes. Upload exactly the four expected assets;
uploads are no-clobber. If an asset name already exists, stop and compare it
rather than using `--clobber`.

```bash
tag=vX.Y.Z
assets=(
  "ctx-${tag}-aarch64-darwin.tar.gz"
  "ctx-${tag}-aarch64-darwin.tar.gz.sha256"
  "ctx-${tag}-x86_64-linux.tar.gz"
  "ctx-${tag}-x86_64-linux.tar.gz.sha256"
)

gh release create "$tag" --repo averagechris/ctx --draft --verify-tag \
  --title "ctx $tag" --generate-notes

# Resolve the draft through the list endpoint. Do not use GET /releases/tags/{tag}:
# GitHub's by-tag lookup does not reliably return draft releases.
release=$(gh api --paginate 'repos/averagechris/ctx/releases?per_page=100' |
  jq -s --arg tag "$tag" '
    add | map(select(.tag_name == $tag and .draft == true)) |
    if length == 1 then .[0] else error("expected exactly one draft release") end')
release_id=$(jq -er '.id' <<<"$release")
upload_url=$(jq -er '.upload_url' <<<"$release" | sed 's/{?name,label}//')

for asset in "${assets[@]}"; do
  gh api --method POST "${upload_url}?name=${asset}" \
    -H 'Content-Type: application/octet-stream' --input "$asset" >/dev/null
done
```

Re-fetch the release by ID, require the exact four-name set, download every
asset (including both sidecars) through its GitHub API asset URL, and compare
both the bytes and the API digest with the verified Actions files. A missing or
non-SHA-256 API digest is a failure.

```bash
release=$(gh api "repos/averagechris/ctx/releases/$release_id")
expected_names=$(printf '%s\n' "${assets[@]}" | sort)
actual_names=$(jq -r '.assets[].name' <<<"$release" | sort)
test "$actual_names" = "$expected_names"

rm -rf published
mkdir published
while IFS=$'\t' read -r name url digest; do
  [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]
  gh api "$url" -H 'Accept: application/octet-stream' >"published/$name"
  downloaded_sha="sha256:$(shasum -a 256 "published/$name" | awk '{print $1}')"
  test "$downloaded_sha" = "$digest"
  cmp "$name" "published/$name"
done < <(jq -r '.assets[] | [.name, .url, .digest] | @tsv' <<<"$release")

(cd published && shasum -a 256 -c -- *.sha256)
```

Before removing draft status, recheck both the annotated tag object and peeled
commit. Leave the release as a draft if either value changed:

```bash
final_remote=$(git ls-remote --tags origin refs/tags/vX.Y.Z refs/tags/vX.Y.Z^{})
test "$(awk '$2 == "refs/tags/vX.Y.Z" {print $1}' <<<"$final_remote")" = "$tag_object"
test "$(awk '$2 == "refs/tags/vX.Y.Z^{}" {print $1}' <<<"$final_remote")" = "$peeled_commit"
gh api "repos/averagechris/ctx/releases/$release_id" --method PATCH -F draft=false >/dev/null
test "$(gh api "repos/averagechris/ctx/releases/$release_id" --jq .draft)" = false
```

After the public release, refresh the live GitHub Pages registry with the exact
peeled commit. Do not edit a site registry manually:

```bash
gh workflow run pages.yml -R averagechris/averagechris.github.io --ref main -f project=ctx -f tag="$tag" -f sha="$peeled_commit"
```

In the repository's Actions UI, open the newly dispatched run—not an older or
concurrent Pages run—and confirm its inputs are `project=ctx`,
`tag=$tag`, and `sha=$peeled_commit`. Copy that run's exact numeric ID. Do not
choose a run with `gh run list` or by recency. Wait for that exact run to
succeed and verify its live state, then fetch the published `manifest.json` and
each listed download. Check that the manifest reports `vX.Y.Z` and run
`shasum -a 256 -c` against each downloaded sidecar. For the two tarballs, also
compare the downloaded bytes with the four verified release downloads:

```bash
read -r -p "Exact newly dispatched Pages run ID: " pages_run_id
[[ "$pages_run_id" =~ ^[0-9]+$ ]]
gh run watch "$pages_run_id" -R averagechris/averagechris.github.io --exit-status
manifest=$(curl -fsSL https://averagechris.github.io/ctx/manifest.json)
test "$(jq -r .version <<<"$manifest")" = "$tag"
rm -rf pages
mkdir pages
for asset in \
  "ctx-${tag}-aarch64-darwin.tar.gz" \
  "ctx-${tag}-x86_64-linux.tar.gz"; do
  curl -fsSL "https://averagechris.github.io/ctx/downloads/$asset" -o "pages/$asset"
  curl -fsSL "https://averagechris.github.io/ctx/downloads/$asset.sha256" -o "pages/$asset.sha256"
  (cd pages && shasum -a 256 -c "$asset.sha256")
  cmp "published/$asset" "pages/$asset"
  cmp "published/$asset.sha256" "pages/$asset.sha256"
done
```

The Pages workflow owns the registry update; this repository stores no Pages
token and must not automate the dispatch.
