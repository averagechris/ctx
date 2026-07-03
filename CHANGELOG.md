# Changelog

## Unreleased

## v1.0.1 - 2026-07-03

### Added

- Reproducible `release-artifact` tarballs plus a static downloads page
  published to SourceHut Pages (`build-pages`, `publish-pages`).

### Changed

- README shows the tag-pinned `nix run` invocation for installs.

### Fixed

- OpenCode history import follows the current message/part schema.

## v1.0.0 - 2026-07-03

### Added

- Nix flake with devShell and local CI wrappers (`ci-fmt`, `ci-clippy`,
  `ci-test`, `ci-docs`) plus SourceHut CI running the same gates.
- `jj lint` fast gates for pre-push validation.

### Changed

- Rebaselined fork versioning at 1.0.0 (hard fork of ctxrs/ctx at upstream
  `38241f0c`).
- Removed telemetry/identity, self-upgrade, SDKs, wire contracts, Bazel, and
  Buildkite plumbing; the binary makes no network calls.

### Fixed

- Made Nix CI apps self-contained and fixed newer clippy lints.
- Initialize the store before probing provider filter aliases in tests.
