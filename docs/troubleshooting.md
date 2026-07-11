# Troubleshooting

## No Sources Found

Run:

```bash
ctx sources --json
```

Confirm the provider keeps history on this machine and pass an explicit path if
needed:

```bash
ctx import --provider codex --path ~/.codex/sessions
```

## Search Misses Recent Work

Re-run import:

```bash
ctx import --all
ctx search "the missing phrase"
```

Use `ctx import --resume --json` when you want output to mark the run as an
idempotent rescan.

## Import Reports Zero-Yield Anomaly

`zero_yield_anomaly` means ctx scanned a non-empty source but imported no
sessions, events, or edges and did not observe a safe all-skipped, empty, or
plugin cursor-only result. The warning intentionally omits source paths,
queries, and transcript content. Next steps:

```bash
ctx sources
ctx import --provider <provider> --path <path>
ctx doctor --json
```

`ctx import --strict` is useful in automation: it prints the complete report
first, then exits 1 if an anomaly was detected. Doctor can report only anomalies
persisted through existing manifested `source_import_files` and
`catalog_sessions` ledgers. Custom
JSONL, history-source plugin, and unmanifested import paths do not have
universal durable zero-yield coverage in this no-schema slice.

After upgrading to `0.10.x` or newer, a refresh can take longer once because ctx marks
older provider import cache rows pending and re-reads source transcripts to
populate touched-file metadata and unredacted local transcript text.

If the raw provider file moved, indexed text may still be searchable, but source
citations should report that the raw path is unavailable.

## OpenCode Import Finds Few or No Sessions

Versions before the message/part-aware adapter read OpenCode's
`session_message` table, which is nearly empty on current OpenCode builds, so
large databases imported almost nothing. After upgrading ctx, the first
`ctx import --provider opencode` detects the old cursor format and
automatically rescans the whole database; `--resume` forces the same full
rescan manually. The rescan is idempotent — previously imported sessions and
events are not duplicated. If a populated database still yields zero events,
the import reports a schema-mismatch failure with table row counts; file an
issue with that message. See [providers.md](providers.md#opencode) for the
adapter's schema-detection rules.

## JSON Consumer Fails

Run the same command without `--json` to inspect warnings, then run:

```bash
ctx doctor --json
```

Check the command contract in [contracts/json.md](contracts/json.md), including
whether the field is documented as nullable or compatibility-only.

## Upgrades

This fork makes no network calls; there is no telemetry and no self-update.
Update via Nix / SourceHut release tags.

## Store Problems

Find the active root:

```bash
ctx status
```

The default is `~/.ctx`. Check permissions and available disk space. Treat the
database and logs as private local history when collecting diagnostics.
