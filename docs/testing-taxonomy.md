# Testing Taxonomy

Public verification focuses on fast local confidence for the search CLI.

## Commands

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --workspace
bash scripts/check-docs.sh
```

The behavioral contract for the CLI lives in `crates/ctx-cli/tests/cli.rs`;
run it directly with `cargo test -p ctx --test cli` when a narrower check is
enough.

All default public tests must be hermetic. They must not require API keys,
network access, provider accounts, hidden model calls, or writes into source
repositories.
