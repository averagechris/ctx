use std::io::{Read, Seek, SeekFrom};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ffi::CString,
    fs,
    os::raw::c_char,
    path::{Path, PathBuf},
    ptr,
    str::FromStr,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use chrono::{DateTime, Utc};
use ctx_history_core::{
    new_id, utc_now, AgentType, Artifact, ArtifactKind, CaptureProvider, CaptureSource,
    CaptureSourceDescriptor, CtxIdPrefix, EntityTimestamps, Event, EventRole, EventType, Fidelity,
    FileTouched, HistoryRecord, HistoryRecordLink, RedactionState, Run, RunStatus, RunType,
    SearchMatchMode, SearchQueryPlan, Session, SessionEdge, SessionHistoryArchive, SessionStatus,
    Summary, SyncCursor, SyncMetadata, SyncState, VcsChange, VcsWorkspace, Visibility,
};

pub const SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE: &str = "zero_yield_anomaly";
pub const CATALOG_IMPORT_OUTCOME_UNATTRIBUTED_CODE: &str = "import_outcome_unattributed";
use rusqlite::{
    ffi,
    limits::Limit,
    params, params_from_iter,
    types::{Value as SqlValue, ValueRef},
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;
const BATCH_RECORD_ID_CHUNK_SIZE: usize = 500;
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("time parse error: {0}")]
    Time(#[from] chrono::ParseError),
    #[error("uuid parse error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("record not found: {0}")]
    NotFound(Uuid),
    #[error("unsupported history store schema version: {0}")]
    UnsupportedSchemaVersion(i64),
    #[error("unsupported session history archive version: {0}")]
    UnsupportedArchiveVersion(u32),
    #[error("archive conflicts with existing {kind}: {id}")]
    ImportConflict { kind: &'static str, id: Uuid },
    #[error("archive artifact {id} content does not match its blob hash")]
    ArchiveArtifactHashMismatch { id: Uuid },
    #[error("unsafe blob path in local store: {0}")]
    UnsafeBlobPath(String),
    #[error("archive artifact {id} content byte size does not match archive metadata")]
    ArchiveArtifactSizeMismatch { id: Uuid },
    #[error("archive artifact {id} blob path is not canonical for its content hash")]
    ArchiveArtifactPathMismatch { id: Uuid },
    #[error("archive artifact {id} blob file is not a regular file: {path:?}")]
    ArchiveArtifactNonRegularFile { id: Uuid, path: PathBuf },
    #[error("archive artifact {id} is missing matching blob content")]
    ArchiveArtifactMissingContent { id: Uuid },
    #[error("provider event conflict for {provider}/{external_session_id} at index {provider_index}: existing hash {existing_hash}, new hash {new_hash}")]
    ProviderEventConflict {
        provider: String,
        external_session_id: String,
        provider_index: u64,
        existing_hash: String,
        new_hash: String,
    },
    #[error("SQL query is empty")]
    RawSqlEmpty,
    #[error("SQL query contains an interior NUL byte")]
    RawSqlInteriorNul,
    #[error("SQL query must be read-only")]
    RawSqlNotReadOnly,
    #[error("SQL query parameters are not supported")]
    RawSqlHasParameters,
    #[error("SQL query must return at least one column")]
    RawSqlNoColumns,
    #[error("SQL query returned {columns} columns; maximum is {max_columns}")]
    RawSqlTooManyColumns { columns: usize, max_columns: usize },
    #[error("{field} must be between {min} and {max}, got {value}")]
    RawSqlLimitOutOfRange {
        field: &'static str,
        value: usize,
        min: usize,
        max: usize,
    },
    #[error("SQL query timed out after {timeout_ms}ms")]
    RawSqlTimedOut { timeout_ms: u64 },
    #[error("{field} is outside the supported numeric range")]
    NumericOutOfRange { field: &'static str },
}

/// Evidence-only FTS5 storage spike for SourceHut #269.
///
/// This deliberately does not use [`FTS_TABLES_SQL`]. The three schemas below
/// are disposable benchmark schemas with identical columns and default FTS5
/// storage settings; only `detail=full` (the current implicit setting),
/// `detail=column`, and `detail=none` vary. In particular, this helper does
/// not exercise contentless or external-content tables and cannot change a
/// production schema or schema version.
///
/// Run exactly as follows when collecting evidence:
///
/// ```text
/// cargo test -q -p ctx-history-store --release fts_storage_spike_evidence -- --ignored --nocapture --test-threads=1
/// ```
#[cfg(test)]
mod fts_storage_spike_benches {
    use super::*;

    const CORPUS_ROWS: usize = 200_000;
    const WARM_SAMPLES: usize = 5;
    const SPIKE_COMMAND: &str = "cargo test -q -p ctx-history-store --release fts_storage_spike_evidence -- --ignored --nocapture --test-threads=1";

    #[derive(Debug)]
    struct VariantEvidence {
        detail: &'static str,
        db_size_bytes: u64,
        fts_size_bytes: Option<u64>,
        warm_samples_us: Vec<u64>,
        rebuild_us: u64,
        correctness: serde_json::Value,
    }

    #[test]
    #[ignore = "evidence-only FTS5 storage spike for SourceHut #269"]
    fn fts_storage_spike_evidence() {
        let mut variants = Vec::new();
        for detail in ["full", "column", "none"] {
            variants.push(run_variant(detail));
        }

        let baseline = variants
            .iter()
            .find(|variant| variant.detail == "full")
            .unwrap();
        let candidate_reports = variants
            .iter()
            .filter(|variant| variant.detail != "full")
            .map(|variant| {
                let db_reduction = percent_reduction(baseline.db_size_bytes, variant.db_size_bytes);
                let fts_reduction = match (baseline.fts_size_bytes, variant.fts_size_bytes) {
                    (Some(before), Some(after)) => Some(percent_reduction(before, after)),
                    _ => None,
                };
                let latency_regression = percent_change(
                    p50(&variant.warm_samples_us),
                    p50(&baseline.warm_samples_us),
                );
                let latency_p95_regression = percent_change(
                    p95(&variant.warm_samples_us),
                    p95(&baseline.warm_samples_us),
                );
                let rebuild_regression = percent_change(variant.rebuild_us, baseline.rebuild_us);
                serde_json::json!({
                    "detail": variant.detail,
                    "db_size_reduction_percent": db_reduction,
                    "fts_size_reduction_percent": fts_reduction,
                    "warm_p50_regression_percent": latency_regression,
                    "warm_p95_regression_percent": latency_p95_regression,
                    "rebuild_regression_percent": rebuild_regression,
                    "required_contracts_intact": variant.correctness["contract_intact"],
                    "qualifies_individually": {
                        "size_reduction_at_least_20_percent": db_reduction >= 20.0 || fts_reduction.is_some_and(|value| value >= 20.0),
                        "required_contracts_intact": variant.correctness["contract_intact"],
                        "warm_latency_regression_at_most_5_percent": latency_regression <= 5.0 && latency_p95_regression <= 5.0,
                        "rebuild_regression_at_most_10_percent": rebuild_regression <= 10.0,
                    },
                })
            })
            .collect::<Vec<_>>();

        let evidence = serde_json::json!({
            "profile": "fts5-storage-spike-v1",
            "issue": "~averagechris/projects#269",
            "corpus": {
                "rows": CORPUS_ROWS,
                "shape": "one deterministic event_search projection plus identical base rows and empty sibling FTS projections",
                "text": "ASCII deterministic tokens with six small correctness rows embedded at event-000000 through event-000005",
            },
            "schemas": {
                "full": "current FTS5 storage: detail=full (explicit in disposable schema; production remains implicit default)",
                "column": "FTS5 detail=column; all other FTS5 storage options unchanged",
                "none": "FTS5 detail=none; all other FTS5 storage options unchanged",
                "excluded": ["contentless", "external-content", "columnsize=0", "prefix indexes"],
            },
            "canonical_reproduction_command": SPIKE_COMMAND,
            "jj": jj_provenance(),
            "variants": variants.iter().map(|variant| {
                serde_json::json!({
                    "detail": variant.detail,
                    "db_size_bytes": variant.db_size_bytes,
                    "fts_specific_size_bytes": variant.fts_size_bytes,
                    "warm_total_latency_us": {
                        "sample_count": variant.warm_samples_us.len(),
                        "samples": variant.warm_samples_us,
                        "p50": p50(&variant.warm_samples_us),
                        "p95": p95(&variant.warm_samples_us),
                        "meaning": "end-to-end SELECT of the first 100 ordered event IDs and safe_preview_text values after one warmup query",
                    },
                    "rebuild_us": variant.rebuild_us,
                    "correctness": variant.correctness,
                })
            }).collect::<Vec<_>>(),
            "gate_comparison_to_current_full": candidate_reports,
            "decision": {
                "go": false,
                "recommendation": "no-go; do not create a production detail migration from this spike",
                "reason": "detail=column and detail=none both explicitly reject the public phrase-query contract; unsupported phrase semantics were not masked or rewritten",
                "optional_1m": "not run: the 200k result is already a no-go on required-contract preservation",
            },
        });
        println!(
            "fts storage spike evidence: {}",
            serde_json::to_string_pretty(&evidence).unwrap()
        );
    }

    fn run_variant(detail: &'static str) -> VariantEvidence {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        let temp = tempfile::Builder::new()
            .prefix("ctx-history-store-fts-spike-")
            .tempdir_in(root)
            .unwrap();
        let path = temp.path().join(format!("{detail}.sqlite"));
        let conn = Connection::open(&path).unwrap();
        create_disposable_schema(&conn, detail);
        populate_corpus(&conn);
        rebuild_projection(&conn);

        let mut correctness = check_correctness_matrix(&conn, detail);

        // Exercise the write paths and the explicit rowid-map cache without
        // changing the deterministic corpus used for the measurements.
        correctness["insert_update_delete"] = exercise_insert_update_delete(&conn);
        let rebuild_started = Instant::now();
        rebuild_projection(&conn);
        let rebuild_us = elapsed_us(rebuild_started.elapsed());
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM benchmark_events"),
            CORPUS_ROWS as i64
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM event_search"),
            CORPUS_ROWS as i64
        );
        assert_maps_are_exact(&conn);
        correctness["rebuild"] = serde_json::json!({
            "passed": true,
            "base_rows": CORPUS_ROWS,
            "projection_rows": CORPUS_ROWS,
            "rowid_map_rows": CORPUS_ROWS,
            "maps_exact_after_rebuild": true,
        });

        // VACUUM is outside the measured rebuild. It makes the size comparison
        // compare compacted, otherwise identical disposable databases rather
        // than free pages left by the CRUD exercise.
        conn.execute_batch("VACUUM").unwrap();
        assert_eq!(scalar_string(&conn, "PRAGMA integrity_check"), "ok");
        drop(conn);

        let conn = Connection::open(&path).unwrap();
        assert_eq!(scalar_string(&conn, "PRAGMA integrity_check"), "ok");
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM event_search"),
            CORPUS_ROWS as i64
        );
        assert_maps_are_exact(&conn);
        let reopened_ids = ordered_ids(
            &conn,
            &SearchQueryPlan::new(SearchMatchMode::All, ["matrixall alpha"]),
        )
        .unwrap();
        assert_eq!(reopened_ids, vec!["event-000000", "event-000001"]);
        correctness["reopen_integrity"] = serde_json::json!({
            "passed": true,
            "sqlite_integrity_check": "ok",
            "ordered_ids_after_reopen": reopened_ids,
        });
        correctness["fts_rowid_map"]["identity_after_reopen"] = true.into();

        let benchmark_plan = SearchQueryPlan::new(SearchMatchMode::All, ["perfneedle commonterm"]);
        let expected_sample_ids = benchmark_ids(&conn, &benchmark_plan);
        let _ = benchmark_ids(&conn, &benchmark_plan); // one warmup, excluded from samples
        let mut warm_samples_us = Vec::with_capacity(WARM_SAMPLES);
        for _ in 0..WARM_SAMPLES {
            let started = Instant::now();
            let ids = benchmark_ids(&conn, &benchmark_plan);
            let elapsed = elapsed_us(started.elapsed());
            assert_eq!(ids, expected_sample_ids);
            warm_samples_us.push(elapsed.max(1));
        }
        let fts_size_bytes = fts_dbstat_bytes(&conn);
        drop(conn);

        VariantEvidence {
            detail,
            db_size_bytes: fs::metadata(&path).unwrap().len(),
            fts_size_bytes,
            warm_samples_us,
            rebuild_us,
            correctness,
        }
    }

    fn create_disposable_schema(conn: &Connection, detail: &str) {
        assert!(matches!(detail, "full" | "column" | "none"));
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = DELETE;
            PRAGMA synchronous = OFF;
            CREATE TABLE benchmark_events (
                event_id TEXT PRIMARY KEY,
                safe_preview_text TEXT NOT NULL
            );
            CREATE TABLE event_search_rowids (
                event_id TEXT PRIMARY KEY,
                search_rowid INTEGER NOT NULL UNIQUE
            ) WITHOUT ROWID;
            "#,
        )
        .unwrap();
        conn.execute_batch(&format!(
            r#"
            CREATE VIRTUAL TABLE ctx_history_search USING fts5(
                record_id UNINDEXED, title, summary, primary_user_text,
                decision_text, context_text, tag_text, detail={detail}
            );
            CREATE VIRTUAL TABLE event_search USING fts5(
                event_id UNINDEXED, history_record_id UNINDEXED,
                session_id UNINDEXED, role UNINDEXED, safe_preview_text,
                rank_bucket UNINDEXED, detail={detail}
            );
            CREATE VIRTUAL TABLE artifact_search USING fts5(
                artifact_id UNINDEXED, history_record_id UNINDEXED,
                safe_preview_text, detail={detail}
            );
            "#
        ))
        .unwrap();
    }

    fn populate_corpus(conn: &Connection) {
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        {
            let mut insert = conn
                .prepare(
                    "INSERT INTO benchmark_events (event_id, safe_preview_text) VALUES (?1, ?2)",
                )
                .unwrap();
            for index in 0..CORPUS_ROWS {
                let id = format!("event-{index:06}");
                let text = match index {
                    0 | 1 => "matrixall alpha ordered source".to_owned(),
                    2 => "matrixanyalpha source".to_owned(),
                    3 => "matrixanybeta source".to_owned(),
                    4 => "matrixphrase ordered phrase source".to_owned(),
                    5 => "matrixphrase ordered other source".to_owned(),
                    index => format!(
                        "perfneedle commonterm event {index:06} alpha{} beta{} gamma",
                        if index % 2 == 0 { "" } else { "-absent" },
                        if index % 3 == 0 { "" } else { "-absent" },
                    ),
                };
                insert.execute(params![id, text]).unwrap();
            }
        }
        conn.execute_batch("COMMIT").unwrap();
    }

    fn rebuild_projection(conn: &Connection) {
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.execute("DELETE FROM event_search", []).unwrap();
        conn.execute("DELETE FROM event_search_rowids", []).unwrap();
        conn.execute(
            "INSERT INTO event_search (event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
             SELECT event_id, NULL, NULL, 'user', safe_preview_text, 'message'
             FROM benchmark_events ORDER BY event_id",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO event_search_rowids (event_id, search_rowid)
             SELECT event_id, rowid FROM event_search",
            [],
        )
        .unwrap();
        conn.execute_batch("COMMIT").unwrap();
    }

    fn check_correctness_matrix(conn: &Connection, detail: &'static str) -> serde_json::Value {
        let all_plan = SearchQueryPlan::new(SearchMatchMode::All, ["matrixall alpha"]);
        let any_plan = SearchQueryPlan::new(SearchMatchMode::Any, ["matrixanyalpha matrixanybeta"]);
        let phrase_plan =
            SearchQueryPlan::new(SearchMatchMode::Phrase, ["matrixphrase ordered phrase"]);
        assert_eq!(
            all_plan.fts_match_query().as_deref(),
            Some("(\"matrixall\" AND \"alpha\")")
        );
        assert_eq!(
            any_plan.fts_match_query().as_deref(),
            Some("(\"matrixanyalpha\" OR \"matrixanybeta\")")
        );
        assert_eq!(
            phrase_plan.fts_match_query().as_deref(),
            Some("(\"matrixphrase ordered phrase\")")
        );

        let all_ids = ordered_ids(conn, &all_plan).unwrap();
        let any_ids = ordered_ids(conn, &any_plan).unwrap();
        assert_eq!(all_ids, vec!["event-000000", "event-000001"]);
        assert_eq!(any_ids, vec!["event-000002", "event-000003"]);

        let phrase_attempt = ordered_ids(conn, &phrase_plan);
        let (phrase_supported, phrase_unsupported_error) = match phrase_attempt {
            Ok(ids) => {
                assert_eq!(ids, vec!["event-000004"]);
                (true, None)
            }
            Err(error) => {
                assert!(
                    error.contains("phrase queries are not supported"),
                    "{error}"
                );
                (false, Some(error))
            }
        };
        assert_eq!(phrase_supported, detail == "full");

        let snippet_rows = conn
            .prepare(
                "SELECT event_id, safe_preview_text FROM event_search
                 WHERE event_search MATCH 'matrixall' ORDER BY rowid",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect::<Vec<_>>();
        let snippet_source_available = snippet_rows.len() == 2
            && snippet_rows
                .iter()
                .all(|(_, text)| text.contains("matrixall"));
        assert!(snippet_source_available);
        let snippet_auxiliary_available = conn
            .query_row(
                "SELECT snippet(event_search, 4, '<b>', '</b>', '…', 12)
                 FROM event_search WHERE event_search MATCH 'matrixall' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map(|snippet| !snippet.is_empty())
            .unwrap_or(false);
        assert!(snippet_auxiliary_available);

        let maps_exact_before_crud = maps_are_exact(conn);
        assert!(maps_exact_before_crud);
        serde_json::json!({
            "fts_match_query": {
                "all": all_plan.fts_match_query(),
                "any": any_plan.fts_match_query(),
                "phrase": phrase_plan.fts_match_query(),
            },
            "ordered_ids": {
                "all": all_ids,
                "any": any_ids,
                "phrase": if phrase_supported { json_ids(&["event-000004"]) } else { serde_json::Value::Null },
            },
            "phrase": {
                "supported": phrase_supported,
                "explicit_unsupported_error": phrase_unsupported_error,
                "expected_for_variant": detail == "full",
            },
            "snippet": {
                "stored_safe_preview_source_available": snippet_source_available,
                "sqlite_snippet_auxiliary_available": snippet_auxiliary_available,
            },
            "fts_rowid_map": {
                "explicit_map_table": "event_search_rowids",
                "identity_before_crud": maps_exact_before_crud,
                "point_delete_with_verified_rowid_and_stale_fallback": true,
            },
            "contract_intact": phrase_supported
                && snippet_source_available
                && snippet_auxiliary_available
                && maps_exact_before_crud,
        })
    }

    fn exercise_insert_update_delete(conn: &Connection) -> serde_json::Value {
        let insert_id = "crud-insert";
        conn.execute(
            "INSERT INTO benchmark_events (event_id, safe_preview_text) VALUES (?1, ?2)",
            params![insert_id, "crud insert needle"],
        )
        .unwrap();
        insert_projection(conn, insert_id, "crud insert needle");
        assert_eq!(
            ordered_ids(
                conn,
                &SearchQueryPlan::new(SearchMatchMode::All, ["crud insert"]),
            )
            .unwrap(),
            vec![insert_id.to_owned()]
        );

        conn.execute(
            "UPDATE benchmark_events SET safe_preview_text = ?2 WHERE event_id = ?1",
            params![insert_id, "crud updated needle"],
        )
        .unwrap();
        delete_projection(conn, insert_id);
        insert_projection(conn, insert_id, "crud updated needle");
        assert!(ordered_ids(
            conn,
            &SearchQueryPlan::new(SearchMatchMode::All, ["crud insert"])
        )
        .unwrap()
        .is_empty());
        assert_eq!(
            ordered_ids(
                conn,
                &SearchQueryPlan::new(SearchMatchMode::All, ["crud updated"]),
            )
            .unwrap(),
            vec![insert_id.to_owned()]
        );

        conn.execute(
            "DELETE FROM benchmark_events WHERE event_id = ?1",
            params![insert_id],
        )
        .unwrap();
        delete_projection(conn, insert_id);
        assert!(ordered_ids(
            conn,
            &SearchQueryPlan::new(SearchMatchMode::All, ["crud updated"])
        )
        .unwrap()
        .is_empty());

        let stale_id = "crud-stale-map";
        conn.execute(
            "INSERT INTO benchmark_events (event_id, safe_preview_text) VALUES (?1, ?2)",
            params![stale_id, "crud stale needle"],
        )
        .unwrap();
        insert_projection(conn, stale_id, "crud stale needle");
        conn.execute(
            "UPDATE event_search_rowids SET search_rowid = search_rowid + 999999 WHERE event_id = ?1",
            params![stale_id],
        )
        .unwrap();
        assert_eq!(delete_projection(conn, stale_id), "full_scan");
        assert!(ordered_ids(
            conn,
            &SearchQueryPlan::new(SearchMatchMode::All, ["crud stale"])
        )
        .unwrap()
        .is_empty());
        conn.execute(
            "DELETE FROM benchmark_events WHERE event_id = ?1",
            params![stale_id],
        )
        .unwrap();
        assert_maps_are_exact(conn);
        serde_json::json!({
            "passed": true,
            "insert": "projection and rowid-map entry created",
            "update": "verified point delete followed by replacement and map refresh",
            "delete": "verified point delete followed by map removal",
            "stale_map": "verified mismatch fell back to full-scan delete and removed stale map entry",
        })
    }

    fn insert_projection(conn: &Connection, event_id: &str, text: &str) {
        conn.execute(
            "INSERT INTO event_search (event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
             VALUES (?1, NULL, NULL, 'user', ?2, 'message')",
            params![event_id, text],
        )
        .unwrap();
        let search_rowid = conn.last_insert_rowid();
        conn.execute(
            "INSERT OR REPLACE INTO event_search_rowids (event_id, search_rowid) VALUES (?1, ?2)",
            params![event_id, search_rowid],
        )
        .unwrap();
    }

    fn delete_projection(conn: &Connection, event_id: &str) -> &'static str {
        let mapped = conn
            .query_row(
                "SELECT search_rowid FROM event_search_rowids WHERE event_id = ?1",
                params![event_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .unwrap();
        let point_delete = mapped.is_some_and(|search_rowid| {
            conn.query_row(
                "SELECT event_id FROM event_search WHERE rowid = ?1",
                params![search_rowid],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap()
            .is_some_and(|actual_id| actual_id == event_id)
        });
        if let Some(search_rowid) = mapped.filter(|_| point_delete) {
            conn.execute(
                "DELETE FROM event_search WHERE rowid = ?1",
                params![search_rowid],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM event_search_rowids WHERE event_id = ?1",
                params![event_id],
            )
            .unwrap();
            "point"
        } else {
            conn.execute(
                "DELETE FROM event_search WHERE event_id = ?1",
                params![event_id],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM event_search_rowids WHERE event_id = ?1",
                params![event_id],
            )
            .unwrap();
            "full_scan"
        }
    }

    fn ordered_ids(
        conn: &Connection,
        plan: &SearchQueryPlan,
    ) -> std::result::Result<Vec<String>, String> {
        let match_query = plan
            .fts_match_query()
            .ok_or_else(|| "empty FTS query".to_owned())?;
        let mut stmt = conn
            .prepare("SELECT event_id FROM event_search WHERE event_search MATCH ?1 ORDER BY rowid")
            .map_err(|error| error.to_string())?;
        let rows = stmt
            .query_map(params![match_query], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| error.to_string());
        rows
    }

    fn benchmark_ids(conn: &Connection, plan: &SearchQueryPlan) -> Vec<String> {
        let match_query = plan.fts_match_query().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT event_id, safe_preview_text FROM event_search
                 WHERE event_search MATCH ?1 ORDER BY rowid LIMIT 100",
            )
            .unwrap();
        stmt.query_map(params![match_query], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .map(|row| {
            let (id, source) = row.unwrap();
            assert!(!source.is_empty());
            id
        })
        .collect()
    }

    fn maps_are_exact(conn: &Connection) -> bool {
        scalar_i64(
            conn,
            "SELECT COUNT(*) FROM event_search f
             JOIN event_search_rowids m ON m.event_id = f.event_id AND m.search_rowid = f.rowid",
        ) == scalar_i64(conn, "SELECT COUNT(*) FROM event_search")
            && scalar_i64(
                conn,
                "SELECT COUNT(*) FROM event_search_rowids m
                 LEFT JOIN event_search f ON f.rowid = m.search_rowid AND f.event_id = m.event_id
                 WHERE f.rowid IS NULL",
            ) == 0
    }

    fn assert_maps_are_exact(conn: &Connection) {
        assert!(
            maps_are_exact(conn),
            "event_search_rowids is not an exact FTS identity map"
        );
    }

    fn fts_dbstat_bytes(conn: &Connection) -> Option<u64> {
        let sql = "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat
                   WHERE (name = 'event_search' OR name LIKE 'event_search_%'
                       OR name = 'ctx_history_search' OR name LIKE 'ctx_history_search_%'
                       OR name = 'artifact_search' OR name LIKE 'artifact_search_%')
                     AND name NOT IN ('event_search_rowids')";
        conn.query_row(sql, [], |row| row.get::<_, i64>(0))
            .ok()
            .map(|bytes| bytes as u64)
    }

    fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn scalar_string(conn: &Connection, sql: &str) -> String {
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn elapsed_us(duration: Duration) -> u64 {
        duration.as_micros().min(u128::from(u64::MAX)) as u64
    }

    fn p50(samples: &[u64]) -> u64 {
        percentile(samples, 50)
    }

    fn p95(samples: &[u64]) -> u64 {
        percentile(samples, 95)
    }

    fn percentile(samples: &[u64], percentile: usize) -> u64 {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let index = ((sorted.len() * percentile).saturating_add(99) / 100).saturating_sub(1);
        sorted[index.min(sorted.len().saturating_sub(1))]
    }

    fn percent_reduction(before: u64, after: u64) -> f64 {
        if before == 0 {
            return 0.0;
        }
        ((before.saturating_sub(after) as f64) / before as f64) * 100.0
    }

    fn percent_change(after: u64, before: u64) -> f64 {
        if before == 0 {
            return 0.0;
        }
        ((after as f64 - before as f64) / before as f64) * 100.0
    }

    fn json_ids(ids: &[&str]) -> serde_json::Value {
        ids.iter()
            .map(|id| serde_json::Value::String((*id).to_owned()))
            .collect()
    }

    fn jj_provenance() -> serde_json::Value {
        let output = std::process::Command::new("jj")
            .args([
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\" ++ change_id ++ \"\\n\"",
            ])
            .output();
        match output {
            Ok(output) if output.status.success() => {
                let ids = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "commit_id": ids.first(),
                    "change_id": ids.get(1),
                })
            }
            _ => serde_json::json!({"commit_id": null, "change_id": null}),
        }
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq)]
pub enum IdPrefixResolution<T> {
    Found(T),
    NotFound,
    Ambiguous(IdPrefixAmbiguity),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdPrefixAmbiguity {
    pub candidate_count: usize,
    pub minimum_total_hex_digits: usize,
    pub additional_hex_digits: usize,
}

impl IdPrefixAmbiguity {
    pub fn message(&self, kind: &str, prefix: &CtxIdPrefix) -> String {
        let digit_word = if self.additional_hex_digits == 1 {
            "digit"
        } else {
            "digits"
        };
        format!(
            "{kind} id prefix {:?} is ambiguous across {} {kind}s; add {} additional hex {digit_word} ({} total hex digits)",
            prefix.canonical(),
            self.candidate_count,
            self.additional_hex_digits,
            self.minimum_total_hex_digits
        )
    }
}

/// Current schema version. The ported upstream migration chain is v1–v15;
/// this fork's first schema divergence jumps to 1000 (docs/fork-plan.md,
/// decision 9) so fork migrations can never collide with upstream's chain.
/// v1000 is the landed rowid-map migration; v1001 adds the bounded
/// pagination keyset indexes ([`V1001_INDEXES_SQL`]). The next fork
/// migration is 1002.
///
/// Any binary whose chain ends at v15 refuses to *open* a fork-versioned
/// store, both read-only (exact-version check in [`Store::open_read_only`])
/// and read-write (the greater-than guard in [`Store::migrate`]). That
/// rejection is a load-bearing part of the FTS rowid-map invariants (see
/// [`SearchRowidMapSpec`]): no older binary can newly open the store and
/// change the search projections without maintaining the maps. The gate is
/// enforced at open time only — a pre-upgrade process that already holds an
/// open connection can keep writing until it restarts, which is why release
/// guidance says to restart long-lived ctx processes after upgrading and
/// why map entries are verified before every point delete.
const SCHEMA_VERSION: i64 = 1001;
/// First schema version of this fork's migration chain (the v1000 rowid-map
/// migration). Writable opens migrate every reviewed version at or above
/// this up to [`SCHEMA_VERSION`]; the fork chain has no gaps.
const FORK_SCHEMA_VERSION_MIN: i64 = 1000;
/// Last schema version of the ported upstream chain. A `user_version`
/// strictly between this and [`FORK_SCHEMA_VERSION_MIN`] could only come
/// from a newer upstream schema this fork has not reviewed; it is rejected
/// instead of migrated blind.
const UPSTREAM_SCHEMA_VERSION_MAX: i64 = 15;

/// True when a writable open of this binary can migrate the given on-disk
/// schema version to the current one: everything at or below the ported
/// upstream chain (≤ v15) migrates, and so does every reviewed fork version
/// (currently exactly v1000, which upgrades in place to v1001 without
/// touching the rowid maps or FTS projections). Versions in the (15, 1000)
/// gap and versions above [`SCHEMA_VERSION`] belong to other (newer or
/// unreviewed) binaries and are rejected rather than migrated; callers
/// should steer users toward upgrading ctx or restoring a matching database
/// instead of suggesting an impossible migration.
pub fn schema_version_is_migratable(user_version: i64) -> bool {
    user_version <= UPSTREAM_SCHEMA_VERSION_MAX
        || (FORK_SCHEMA_VERSION_MIN..SCHEMA_VERSION).contains(&user_version)
}
const BUSY_TIMEOUT: Duration = Duration::from_millis(30_000);
/// Page budget for the degraded no-FTS record fallback scan in
/// [`Store::search_records_plan_page`]. With the per-call page size of
/// `max(limit * 20, 100)` this caps the scan window at
/// `20 * max(limit * 20, 100)` newest records (2,000 at the minimum page
/// size, 80,000 at the CLI's 200-result cap) instead of an unbounded walk of
/// the whole table. Mirrors the ranked event path's scan-budget rationale
/// (`FILTERED_SEARCH_MAX_PAGES` in `ctx-history-search`): a degraded index
/// should degrade to bounded work, not to an O(table) scan per query.
const RECORD_FALLBACK_SCAN_MAX_PAGES: usize = 20;
const OBJECTS_DIR: &str = "objects";
const SPOOL_DIR: &str = "spool";
const LEGACY_HISTORY_DIR_NAME: &str = "work-record";
const LEGACY_BLOBS_DIR: &str = "blobs";
const LEGACY_INBOX_DIR: &str = "inbox";
pub const RAW_SQL_DEFAULT_MAX_ROWS: usize = 100;
pub const RAW_SQL_MAX_ROWS_CAP: usize = 10_000;
pub const RAW_SQL_DEFAULT_MAX_COLUMNS: usize = 64;
pub const RAW_SQL_MAX_COLUMNS_CAP: usize = 256;
pub const RAW_SQL_DEFAULT_MAX_VALUE_BYTES: usize = 512;
pub const RAW_SQL_MAX_VALUE_BYTES_CAP: usize = 1_048_576;
const RAW_SQL_MIN_SQLITE_LENGTH_LIMIT_BYTES: usize = 64 * 1024;
const RAW_SQL_VALUE_LENGTH_MARGIN_BYTES: usize = 1024;
pub const RAW_SQL_DEFAULT_MAX_SQL_BYTES: usize = 64 * 1024;
pub const RAW_SQL_MAX_SQL_BYTES_CAP: usize = 1_048_576;
pub const RAW_SQL_DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
pub const RAW_SQL_MAX_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard bound for every event window/page read performed by the store.
pub const MAX_BOUNDED_EVENT_READ: usize = 10_000;

/// Store-owned transcript selection, kept independent of presentation crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedEventMode {
    /// Message events whose role is user, assistant, or system.
    Full,
    /// User messages plus the final assistant message before the next user or
    /// session end. Tool, system, and non-message events do not participate.
    Lite,
    /// Every event in key order.
    Log,
}

/// The closed set of FTS5 search projection tables. `event_search` and
/// `artifact_search` are optional (older stores may lack them); every
/// maintenance entry point probes existence before touching a table.
const SEARCH_PROJECTION_FTS_TABLES: [&str; 3] =
    ["ctx_history_search", "event_search", "artifact_search"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSqlOptions {
    pub max_rows: usize,
    pub max_columns: usize,
    pub max_value_bytes: usize,
    pub max_sql_bytes: usize,
    pub timeout: Duration,
}

impl Default for RawSqlOptions {
    fn default() -> Self {
        Self {
            max_rows: RAW_SQL_DEFAULT_MAX_ROWS,
            max_columns: RAW_SQL_DEFAULT_MAX_COLUMNS,
            max_value_bytes: RAW_SQL_DEFAULT_MAX_VALUE_BYTES,
            max_sql_bytes: RAW_SQL_DEFAULT_MAX_SQL_BYTES,
            timeout: RAW_SQL_DEFAULT_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSqlColumn {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RawSqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text {
        value: String,
        bytes: usize,
        truncated: bool,
    },
    Blob {
        bytes: usize,
        preview_hex: String,
        truncated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSqlTruncation {
    pub rows: bool,
    pub values: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSqlLimits {
    pub max_rows: usize,
    pub max_columns: usize,
    pub max_value_bytes: usize,
    pub max_sql_bytes: usize,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawSqlResult {
    pub columns: Vec<RawSqlColumn>,
    pub rows: Vec<Vec<RawSqlValue>>,
    pub returned_rows: usize,
    pub truncated: RawSqlTruncation,
    pub elapsed: Duration,
    pub limits: RawSqlLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteProfileMetadata {
    pub version: String,
    pub journal_mode: String,
    pub synchronous: i64,
    pub page_size: i64,
    pub foreign_keys: i64,
    pub user_version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileTableCounts {
    pub records: u64,
    pub capture_sources: u64,
    pub sessions: u64,
    pub runs: u64,
    pub events: u64,
    pub summaries: u64,
    pub files_touched: u64,
    pub record_fts: u64,
    pub event_fts: u64,
    pub artifact_fts: u64,
}

impl RawSqlValue {
    fn is_truncated(&self) -> bool {
        match self {
            Self::Text { truncated, .. } | Self::Blob { truncated, .. } => *truncated,
            Self::Null | Self::Integer(_) | Self::Real(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDeviceIdentity {
    pub id: Uuid,
    pub stable_device_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalWorkspaceIdentity {
    pub id: Uuid,
    pub device_id: Uuid,
    pub vcs_workspace_id: Option<Uuid>,
    pub repo_fingerprint: String,
    pub root_path_hash: String,
    pub display_root: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogSession {
    pub provider: CaptureProvider,
    pub source_format: String,
    pub source_root: String,
    pub source_path: String,
    pub external_session_id: Option<String>,
    pub parent_external_session_id: Option<String>,
    pub agent_type: AgentType,
    pub role_hint: Option<String>,
    pub external_agent_id: Option<String>,
    pub cwd: Option<String>,
    pub session_started_at_ms: Option<i64>,
    pub file_size_bytes: u64,
    pub file_modified_at_ms: i64,
    pub cataloged_at_ms: i64,
    pub metadata: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogSourceIndexUpdate<'a> {
    pub source_root: &'a str,
    pub source_path: &'a str,
    pub file_size_bytes: u64,
    pub file_modified_at_ms: i64,
    pub file_sha256: Option<&'a str>,
    pub event_count: Option<u64>,
    pub indexed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSourceIndexState {
    pub last_imported_file_size_bytes: Option<u64>,
    pub last_imported_file_modified_at_ms: Option<i64>,
    pub last_imported_event_count: Option<u64>,
    pub last_imported_at_ms: Option<i64>,
    pub last_imported_file_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceImportFile {
    pub provider: CaptureProvider,
    pub source_format: String,
    pub source_root: String,
    pub source_path: String,
    pub file_size_bytes: u64,
    pub file_modified_at_ms: i64,
    pub observed_at_ms: i64,
    pub metadata: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceImportFileIndexUpdate<'a> {
    pub source_root: &'a str,
    pub source_path: &'a str,
    pub file_size_bytes: u64,
    pub file_modified_at_ms: i64,
    pub indexed_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogCounts {
    pub total: usize,
    pub indexed: usize,
    pub stale: usize,
    pub pending: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexedHistoryCounts {
    pub sessions: usize,
    pub events: usize,
}

impl IndexedHistoryCounts {
    pub fn items(self) -> usize {
        self.sessions.saturating_add(self.events)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogIndexedStatus {
    Pending,
    Indexed,
    Failed,
}

impl CatalogIndexedStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Indexed => "indexed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventSearchHit {
    pub event_id: Uuid,
    pub history_record_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub session_parent_session_id: Option<Uuid>,
    pub session_root_session_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub seq: u64,
    pub event_type: EventType,
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    pub preview: String,
    pub score: f64,
    pub provider: Option<CaptureProvider>,
    pub session_external_session_id: Option<String>,
    pub history_source: Option<String>,
    pub history_source_plugin: Option<String>,
    pub provider_key: Option<String>,
    pub source_id: Option<String>,
    pub source_format: Option<String>,
    pub agent_type: Option<AgentType>,
    pub session_is_primary: Option<bool>,
    pub cwd: Option<String>,
    pub raw_source_path: Option<String>,
    pub cursor: Option<String>,
    pub record_title: Option<String>,
    pub record_kind: Option<String>,
    pub record_workspace: Option<String>,
    pub tool_names: Vec<String>,
}

/// One row of the bounded ranked record stream. The score is retained with
/// the ID so the materialized ordering remains explicit and inspectable even
/// though record hydration is deferred to logical pages.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordSearchHit {
    pub record_id: Uuid,
    pub score: f64,
}

/// Narrow, store-owned projections for fallback search hydration.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchSessionRow {
    pub id: Uuid,
    pub parent_session_id: Option<Uuid>,
    pub root_session_id: Option<Uuid>,
    pub capture_source_id: Option<Uuid>,
    pub provider: CaptureProvider,
    pub external_session_id: Option<String>,
    pub external_agent_id: Option<String>,
    pub agent_type: AgentType,
    pub role_hint: Option<String>,
    pub is_primary: bool,
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRunRow {
    pub id: Uuid,
    pub session_id: Option<Uuid>,
    pub run_type: RunType,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub exit_code: Option<i32>,
    pub cwd: Option<String>,
    pub command_preview: Option<String>,
    pub source_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchEventRow {
    pub id: Uuid,
    pub seq: u64,
    pub session_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub event_type: EventType,
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    pub capture_source_id: Option<Uuid>,
    pub payload: Value,
    pub dedupe_key: Option<String>,
    pub redaction_state: RedactionState,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchArtifactRow {
    pub id: Uuid,
    pub kind: ArtifactKind,
    pub blob_path: String,
    pub media_type: Option<String>,
    pub preview_text: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub source_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchFileTouchedRow {
    pub id: Uuid,
    pub event_id: Option<Uuid>,
    pub path: String,
    pub change_kind: Option<ctx_history_core::FileChangeKind>,
    pub old_path: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub source_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchVcsChangeRow {
    pub id: Uuid,
    pub kind: ctx_history_core::VcsChangeKind,
    pub change_id: String,
    pub parent_change_ids: Vec<String>,
    pub branch_or_bookmark: Option<String>,
    pub tree_hash: Option<String>,
    pub author_time: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub source_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchSummaryRow {
    pub id: Uuid,
    pub text: String,
    pub updated_at: DateTime<Utc>,
    pub source_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchCaptureSourceRow {
    pub id: Uuid,
    pub provider: CaptureProvider,
    pub cwd: Option<String>,
    pub raw_source_path: Option<String>,
    pub external_session_id: Option<String>,
    pub metadata: Value,
}

/// Agent-scope predicate that can be enforced inside the ranked event-search
/// SQL page. For schema-valid rows produced by supported store write paths,
/// both variants mirror `event_hit_matches_agent_scope` in
/// `ctx-history-search` on the exact hit columns
/// (`COALESCE(s.is_primary, rs.is_primary)` / `COALESCE(s.agent_type,
/// rs.agent_type)`), so a row passes the SQL predicate if and only if the
/// hydrated `EventSearchHit` passes the Rust check. Out-of-band database
/// corruption is outside this equivalence claim and may fail hydration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSearchAgentScope {
    /// Default scope (`include_subagents = false`): keep rows whose session is
    /// primary, whose agent type is `primary`, or that carry no session
    /// identity at all (both scope columns NULL).
    PrimaryOrSessionless,
    /// `primary_only`: keep only rows proven primary; sessionless rows drop.
    PrimaryOnly,
}

/// Exact-semantics subset of the event-search filters that
/// `search_event_hits_page_filtered` pushes into the ranked SQL page query.
///
/// Every predicate is expressed on the same COALESCE fallback chains that
/// hydrate the corresponding `EventSearchHit` field, so for schema-valid rows
/// produced by supported store write paths SQL filtering is equivalent to
/// filtering the unfiltered hit stream in Rust:
/// - `session_id` matches `COALESCE(e.session_id, event_search.session_id,
///   s.id, rs.id)` (the hydrated `hit.session_id`). Supported writes persist
///   UUIDs using `Uuid::to_string()`'s canonical lowercase hyphenated text, so
///   SQL text equality and parsed `Uuid` equality agree.
/// - `provider` matches the five-way provider fallback chain
///   (`hit.provider`); provider strings are CHECK-constrained under the store
///   schema, so string equality agrees with `CaptureProvider` equality for
///   reachable rows. Corrupt/out-of-band values are not covered and fail enum
///   hydration rather than silently participating in Rust filtering.
/// - `since` keeps rows with `occurred_at >= since`. Stored timestamps are
///   whole milliseconds, so a sub-millisecond `since` is ceiled to the next
///   representable millisecond (see `event_search_since_threshold_ms`).
/// - `event_type` matches `e.event_type` (CHECK-constrained text enum).
/// - `agent_scope` see `EventSearchAgentScope`.
/// - `roles` / `exclude_roles` match `e.role` (CHECK-constrained text enum or
///   NULL, the hydrated `hit.role`) through a total CASE-to-bitmask mapping;
///   NULL maps to no bit, so include sets reject NULL-role rows and exclude
///   sets keep them, exactly like the Rust `Option<EventRole>` predicate.
/// - `exclude_tool_noise` drops the fixed tool/command `e.event_type` set the
///   Rust predicate names (`tool_call`, `tool_output`, `command_started`,
///   `command_output`, `command_finished`).
///
/// File-touch scope is request-scoped but is pushed as bound identity arrays
/// and also retained in Rust as an oracle. Other request-scoped filters (repo
/// substring matching, excluded provider sessions, history-source identity)
/// and payload-derived data (`exclude_tool_names`, which parses tool/command
/// executables out of `payload_json`) stay only in Rust; callers keep
/// `event_hit_matches_filters` as the final authority over every returned
/// row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventSearchSqlFilters {
    pub session_id: Option<Uuid>,
    pub provider: Option<CaptureProvider>,
    pub since: Option<DateTime<Utc>>,
    pub event_type: Option<EventType>,
    pub agent_scope: Option<EventSearchAgentScope>,
    /// Include only events whose stored `events.role` is one of these roles;
    /// events with a NULL role never match a non-empty include set (mirrors
    /// `role.is_some_and(..)` in the Rust predicate).
    pub roles: Vec<EventRole>,
    /// Exclude events whose stored `events.role` is one of these roles;
    /// events with a NULL role are never excluded.
    pub exclude_roles: Vec<EventRole>,
    /// Exclude tool/command noise event types (`tool_call`, `tool_output`,
    /// `command_started`, `command_output`, `command_finished`).
    pub exclude_tool_noise: bool,
    /// Request-scoped file-touch identity sets. The candidate predicate tests
    /// the same hydrated event/run/session/history-record identity chains as
    /// the Rust residual oracle. Values are passed as bound JSON arrays.
    pub file_scope: Option<FileTouchScope>,
}

impl EventSearchSqlFilters {
    pub fn is_empty(&self) -> bool {
        self.session_id.is_none()
            && self.provider.is_none()
            && self.since.is_none()
            && self.event_type.is_none()
            && self.agent_scope.is_none()
            && self.roles.is_empty()
            && self.exclude_roles.is_empty()
            && !self.exclude_tool_noise
            && self.file_scope.is_none()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileTouchScope {
    pub history_record_ids: BTreeSet<Uuid>,
    pub session_ids: BTreeSet<Uuid>,
    pub run_ids: BTreeSet<Uuid>,
    pub event_ids: BTreeSet<Uuid>,
    pub source_ids: BTreeSet<Uuid>,
}

impl FileTouchScope {
    pub fn is_empty(&self) -> bool {
        self.history_record_ids.is_empty()
            && self.session_ids.is_empty()
            && self.run_ids.is_empty()
            && self.event_ids.is_empty()
            && self.source_ids.is_empty()
    }
}

const HISTORY_RECORD_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "summary",
        definition: "summary TEXT",
    },
    ColumnSpec {
        name: "status",
        definition: "status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'active', 'completed', 'abandoned', 'archived'))",
    },
    ColumnSpec {
        name: "primary_vcs_workspace_id",
        definition: "primary_vcs_workspace_id TEXT REFERENCES vcs_workspaces(id)",
    },
    ColumnSpec {
        name: "started_at_ms",
        definition: "started_at_ms INTEGER",
    },
    ColumnSpec {
        name: "last_activity_at_ms",
        definition: "last_activity_at_ms INTEGER NOT NULL DEFAULT 0",
    },
    ColumnSpec {
        name: "completed_at_ms",
        definition: "completed_at_ms INTEGER",
    },
    ColumnSpec {
        name: "confidence",
        definition: "confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown'))",
    },
    ColumnSpec {
        name: "created_at_ms",
        definition: "created_at_ms INTEGER NOT NULL DEFAULT 0",
    },
    ColumnSpec {
        name: "updated_at_ms",
        definition: "updated_at_ms INTEGER NOT NULL DEFAULT 0",
    },
    ColumnSpec {
        name: "source_id",
        definition: "source_id TEXT REFERENCES capture_sources(id)",
    },
    ColumnSpec {
        name: "visibility",
        definition: "visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld'))",
    },
    ColumnSpec {
        name: "fidelity",
        definition: "fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only'))",
    },
    ColumnSpec {
        name: "sync_state",
        definition: "sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld'))",
    },
    ColumnSpec {
        name: "sync_version",
        definition: "sync_version INTEGER NOT NULL DEFAULT 0",
    },
    ColumnSpec {
        name: "deleted_at_ms",
        definition: "deleted_at_ms INTEGER",
    },
    ColumnSpec {
        name: "metadata_json",
        definition: "metadata_json TEXT NOT NULL DEFAULT '{}'",
    },
];

const CATALOG_SESSION_IMPORT_STATE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "indexed_at_ms",
        definition: "indexed_at_ms INTEGER",
    },
    ColumnSpec {
        name: "indexed_file_size_bytes",
        definition: "indexed_file_size_bytes INTEGER",
    },
    ColumnSpec {
        name: "indexed_file_modified_at_ms",
        definition: "indexed_file_modified_at_ms INTEGER",
    },
    ColumnSpec {
        name: "indexed_status",
        definition: "indexed_status TEXT NOT NULL DEFAULT 'pending' CHECK (indexed_status IN ('pending', 'indexed', 'failed'))",
    },
    ColumnSpec {
        name: "indexed_error",
        definition: "indexed_error TEXT",
    },
    ColumnSpec {
        name: "indexed_event_count",
        definition: "indexed_event_count INTEGER",
    },
    ColumnSpec {
        name: "last_imported_at_ms",
        definition: "last_imported_at_ms INTEGER",
    },
    ColumnSpec {
        name: "last_imported_file_size_bytes",
        definition: "last_imported_file_size_bytes INTEGER",
    },
    ColumnSpec {
        name: "last_imported_file_modified_at_ms",
        definition: "last_imported_file_modified_at_ms INTEGER",
    },
    ColumnSpec {
        name: "last_imported_file_sha256",
        definition: "last_imported_file_sha256 TEXT",
    },
    ColumnSpec {
        name: "last_imported_event_count",
        definition: "last_imported_event_count INTEGER",
    },
];

const CREATE_TABLES_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS capture_sources (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('provider_import', 'provider_hook', 'direct_cli', 'manual')),
    provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'pi', 'opencode', 'antigravity', 'gemini', 'cursor', 'copilot_cli', 'factory_ai_droid', 'openclaw', 'hermes', 'nanoclaw', 'astrbot', 'shell', 'git', 'jj', 'gh', 'custom', 'unknown')),
    machine_id TEXT NOT NULL,
    process_id INTEGER,
    cwd TEXT,
    raw_source_path TEXT,
    external_session_id TEXT,
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    fidelity TEXT NOT NULL CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS catalog_sessions (
    source_path TEXT PRIMARY KEY NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'pi', 'opencode', 'antigravity', 'gemini', 'cursor', 'copilot_cli', 'factory_ai_droid', 'openclaw', 'hermes', 'nanoclaw', 'astrbot', 'shell', 'git', 'jj', 'gh', 'custom', 'unknown')),
    source_format TEXT NOT NULL,
    source_root TEXT NOT NULL,
    external_session_id TEXT,
    parent_external_session_id TEXT,
    agent_type TEXT NOT NULL CHECK (agent_type IN ('primary', 'subagent', 'agent_team_member', 'reviewer', 'implementer', 'unknown')),
    role_hint TEXT,
    external_agent_id TEXT,
    cwd TEXT,
    session_started_at_ms INTEGER,
    file_size_bytes INTEGER NOT NULL,
    file_modified_at_ms INTEGER NOT NULL,
    cataloged_at_ms INTEGER NOT NULL,
    is_stale INTEGER NOT NULL DEFAULT 0,
    indexed_at_ms INTEGER,
    indexed_file_size_bytes INTEGER,
    indexed_file_modified_at_ms INTEGER,
    indexed_status TEXT NOT NULL DEFAULT 'pending' CHECK (indexed_status IN ('pending', 'indexed', 'failed')),
    indexed_error TEXT,
    indexed_event_count INTEGER,
    last_imported_at_ms INTEGER,
    last_imported_file_size_bytes INTEGER,
    last_imported_file_modified_at_ms INTEGER,
    last_imported_file_sha256 TEXT,
    last_imported_event_count INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS source_import_files (
    provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'pi', 'opencode', 'antigravity', 'gemini', 'cursor', 'copilot_cli', 'factory_ai_droid', 'openclaw', 'hermes', 'nanoclaw', 'astrbot', 'shell', 'git', 'jj', 'gh', 'custom', 'unknown')),
    source_format TEXT NOT NULL,
    source_root TEXT NOT NULL,
    source_path TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    file_modified_at_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    is_stale INTEGER NOT NULL DEFAULT 0,
    indexed_at_ms INTEGER,
    indexed_file_size_bytes INTEGER,
    indexed_file_modified_at_ms INTEGER,
    indexed_status TEXT NOT NULL DEFAULT 'pending' CHECK (indexed_status IN ('pending', 'indexed', 'failed')),
    indexed_error TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (provider, source_root, source_path)
);

CREATE TABLE IF NOT EXISTS vcs_workspaces (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('git', 'jj')),
    root_path TEXT NOT NULL,
    repo_fingerprint TEXT NOT NULL,
    primary_remote_url_normalized TEXT,
    host TEXT NOT NULL DEFAULT 'unknown' CHECK (host IN ('github', 'gitlab', 'bitbucket', 'local', 'unknown')),
    owner TEXT,
    name TEXT,
    monorepo_subpath TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(kind, repo_fingerprint)
);

CREATE TABLE IF NOT EXISTS history_records (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'active', 'completed', 'abandoned', 'archived')),
    primary_vcs_workspace_id TEXT REFERENCES vcs_workspaces(id),
    started_at_ms INTEGER,
    last_activity_at_ms INTEGER NOT NULL DEFAULT 0,
    completed_at_ms INTEGER,
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    created_at_ms INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL DEFAULT 0,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    body TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    kind TEXT NOT NULL DEFAULT 'note',
    workspace TEXT,
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('transcript', 'stdout', 'stderr', 'screenshot', 'report', 'diff', 'file_snapshot', 'json', 'markdown', 'binary')),
    blob_hash TEXT NOT NULL,
    blob_path TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    media_type TEXT,
    preview_text TEXT,
    redaction_state TEXT NOT NULL DEFAULT 'safe_preview' CHECK (redaction_state IN ('raw', 'redacted', 'safe_preview', 'withheld')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(blob_hash, kind)
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY NOT NULL,
    history_record_id TEXT REFERENCES history_records(id),
    parent_session_id TEXT REFERENCES sessions(id),
    root_session_id TEXT REFERENCES sessions(id),
    capture_source_id TEXT REFERENCES capture_sources(id),
    provider TEXT NOT NULL,
    external_session_id TEXT,
    external_agent_id TEXT,
    agent_type TEXT NOT NULL CHECK (agent_type IN ('primary', 'subagent', 'agent_team_member', 'reviewer', 'implementer', 'unknown')),
    role_hint TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('started', 'active', 'idle', 'completed', 'failed', 'interrupted', 'imported')),
    fidelity TEXT NOT NULL CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    transcript_blob_id TEXT REFERENCES artifacts(id),
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS session_edges (
    id TEXT PRIMARY KEY NOT NULL,
    from_session_id TEXT NOT NULL REFERENCES sessions(id),
    to_session_id TEXT NOT NULL REFERENCES sessions(id),
    edge_type TEXT NOT NULL CHECK (edge_type IN ('parent_child', 'delegated', 'reviewed', 'spawned', 'resumed_from', 'imported_related')),
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    source_id TEXT REFERENCES capture_sources(id),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY NOT NULL,
    history_record_id TEXT REFERENCES history_records(id),
    session_id TEXT REFERENCES sessions(id),
    run_type TEXT NOT NULL CHECK (run_type IN ('agent_turn', 'command', 'tool_call', 'review', 'import', 'summary')),
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'partial')),
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    exit_code INTEGER,
    cwd TEXT,
    command_preview TEXT,
    input_blob_id TEXT REFERENCES artifacts(id),
    output_blob_id TEXT REFERENCES artifacts(id),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY NOT NULL,
    seq INTEGER NOT NULL UNIQUE,
    history_record_id TEXT REFERENCES history_records(id),
    session_id TEXT REFERENCES sessions(id),
    run_id TEXT REFERENCES runs(id),
    event_type TEXT NOT NULL CHECK (event_type IN ('message', 'tool_call', 'tool_output', 'command_started', 'command_output', 'command_finished', 'file_touched', 'vcs_change', 'artifact', 'summary', 'notice')),
    role TEXT CHECK (role IS NULL OR role IN ('user', 'assistant', 'system', 'tool', 'unknown')),
    occurred_at_ms INTEGER NOT NULL,
    capture_source_id TEXT REFERENCES capture_sources(id),
    payload_json TEXT NOT NULL DEFAULT '{}',
    payload_blob_id TEXT REFERENCES artifacts(id),
    dedupe_key TEXT,
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    redaction_state TEXT NOT NULL DEFAULT 'safe_preview' CHECK (redaction_state IN ('raw', 'redacted', 'safe_preview', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS vcs_changes (
    id TEXT PRIMARY KEY NOT NULL,
    vcs_workspace_id TEXT NOT NULL REFERENCES vcs_workspaces(id),
    kind TEXT NOT NULL CHECK (kind IN ('git_commit', 'git_branch', 'git_worktree', 'jj_change', 'jj_bookmark', 'patch', 'working_copy')),
    change_id TEXT NOT NULL,
    parent_change_ids_json TEXT NOT NULL DEFAULT '[]',
    branch_or_bookmark TEXT,
    tree_hash TEXT,
    author_time_ms INTEGER,
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(vcs_workspace_id, kind, change_id)
);

CREATE TABLE IF NOT EXISTS history_record_links (
    id TEXT PRIMARY KEY NOT NULL,
    history_record_id TEXT NOT NULL REFERENCES history_records(id),
    target_type TEXT NOT NULL CHECK (target_type IN ('session', 'run', 'event', 'vcs_workspace', 'vcs_change', 'artifact')),
    target_id TEXT NOT NULL,
    link_type TEXT NOT NULL CHECK (link_type IN ('produced', 'touched', 'references', 'likely_related')),
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    source_id TEXT REFERENCES capture_sources(id),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(history_record_id, target_type, target_id, link_type)
);

CREATE TABLE IF NOT EXISTS summaries (
    id TEXT PRIMARY KEY NOT NULL,
    history_record_id TEXT REFERENCES history_records(id),
    session_id TEXT REFERENCES sessions(id),
    kind TEXT NOT NULL CHECK (kind IN ('imported_provider_summary', 'ctx_generated', 'agent_supplied', 'human_note')),
    model_or_source TEXT,
    text TEXT NOT NULL,
    citations_json TEXT NOT NULL DEFAULT '[]',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS files_touched (
    id TEXT PRIMARY KEY NOT NULL,
    history_record_id TEXT REFERENCES history_records(id),
    run_id TEXT REFERENCES runs(id),
    event_id TEXT REFERENCES events(id),
    vcs_workspace_id TEXT REFERENCES vcs_workspaces(id),
    path TEXT NOT NULL,
    change_kind TEXT CHECK (change_kind IS NULL OR change_kind IN ('read', 'created', 'modified', 'deleted', 'renamed', 'unknown')),
    old_path TEXT,
    line_count_delta INTEGER,
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS tags (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL DEFAULT 'user' CHECK (kind IN ('user', 'system', 'inferred')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS history_record_tags (
    history_record_id TEXT NOT NULL REFERENCES history_records(id),
    tag_id TEXT NOT NULL REFERENCES tags(id),
    source_id TEXT REFERENCES capture_sources(id),
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (history_record_id, tag_id)
);

CREATE TABLE IF NOT EXISTS record_edges (
    id TEXT PRIMARY KEY NOT NULL,
    from_record_id TEXT NOT NULL REFERENCES history_records(id),
    to_record_id TEXT NOT NULL REFERENCES history_records(id),
    edge_type TEXT NOT NULL CHECK (edge_type IN ('continues', 'duplicates', 'blocks', 'related', 'supersedes', 'split_from')),
    confidence TEXT NOT NULL DEFAULT 'unknown' CHECK (confidence IN ('explicit', 'high', 'medium', 'low', 'unknown')),
    source_id TEXT REFERENCES capture_sources(id),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
    fidelity TEXT NOT NULL DEFAULT 'partial' CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
    sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
    sync_version INTEGER NOT NULL DEFAULT 0,
    deleted_at_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS sync_cursors (
    id TEXT PRIMARY KEY NOT NULL,
    team_id TEXT,
    device_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    cursor TEXT NOT NULL,
    last_synced_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(team_id, device_id, stream)
);

CREATE TABLE IF NOT EXISTS sync_batches (
    id TEXT PRIMARY KEY NOT NULL,
    team_id TEXT,
    device_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('upload', 'download')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    row_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS sync_outbox (
    id TEXT PRIMARY KEY NOT NULL,
    local_table TEXT NOT NULL,
    local_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete', 'blob_upload')),
    team_id TEXT,
    device_id TEXT NOT NULL,
    sync_state TEXT NOT NULL DEFAULT 'pending' CHECK (sync_state IN ('pending', 'synced', 'failed', 'withheld')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_ms INTEGER,
    last_error TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(local_table, local_id, operation, team_id)
);

CREATE TABLE IF NOT EXISTS local_devices (
    id TEXT PRIMARY KEY NOT NULL,
    stable_device_id TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS local_workspaces (
    id TEXT PRIMARY KEY NOT NULL,
    device_id TEXT NOT NULL REFERENCES local_devices(id),
    vcs_workspace_id TEXT REFERENCES vcs_workspaces(id),
    repo_fingerprint TEXT NOT NULL,
    root_path_hash TEXT NOT NULL,
    display_root TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(device_id, repo_fingerprint, root_path_hash)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY NOT NULL,
    actor_kind TEXT NOT NULL CHECK (actor_kind IN ('human', 'agent', 'system')),
    actor_id TEXT,
    action TEXT NOT NULL,
    target_table TEXT,
    target_id TEXT,
    occurred_at_ms INTEGER NOT NULL,
    source_id TEXT REFERENCES capture_sources(id),
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
"#;

const INDEXES_SQL: &str = r#"
CREATE INDEX IF NOT EXISTS idx_capture_sources_external_session_id ON capture_sources(provider, external_session_id);

CREATE INDEX IF NOT EXISTS idx_catalog_sessions_provider_external_session_id ON catalog_sessions(provider, external_session_id);
CREATE INDEX IF NOT EXISTS idx_catalog_sessions_provider_source_root_stale ON catalog_sessions(provider, source_root, is_stale);
CREATE INDEX IF NOT EXISTS idx_catalog_sessions_provider_source_root_import ON catalog_sessions(provider, source_root, is_stale, indexed_status);
CREATE INDEX IF NOT EXISTS idx_catalog_sessions_started_at ON catalog_sessions(session_started_at_ms);
CREATE INDEX IF NOT EXISTS idx_catalog_sessions_cwd ON catalog_sessions(cwd);
CREATE INDEX IF NOT EXISTS idx_source_import_files_provider_source_root_import ON source_import_files(provider, source_root, is_stale, indexed_status);
CREATE INDEX IF NOT EXISTS idx_source_import_files_provider_source_root_stale ON source_import_files(provider, source_root, is_stale);
CREATE INDEX IF NOT EXISTS idx_sessions_provider_external_session_id ON sessions(provider, external_session_id);

CREATE INDEX IF NOT EXISTS idx_history_records_primary_vcs_workspace_id ON history_records(primary_vcs_workspace_id);
CREATE INDEX IF NOT EXISTS idx_history_records_source_id ON history_records(source_id);
CREATE INDEX IF NOT EXISTS idx_history_records_last_activity_at_ms ON history_records(last_activity_at_ms);
CREATE INDEX IF NOT EXISTS idx_history_records_created_at ON history_records(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_history_record_id ON sessions(history_record_id);
CREATE INDEX IF NOT EXISTS idx_sessions_parent_session_id ON sessions(parent_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_root_session_id ON sessions(root_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_capture_source_id ON sessions(capture_source_id);
CREATE INDEX IF NOT EXISTS idx_sessions_transcript_blob_id ON sessions(transcript_blob_id);

CREATE INDEX IF NOT EXISTS idx_session_edges_from_session_id ON session_edges(from_session_id);
CREATE INDEX IF NOT EXISTS idx_session_edges_to_session_id ON session_edges(to_session_id);
CREATE INDEX IF NOT EXISTS idx_session_edges_source_id ON session_edges(source_id);

CREATE INDEX IF NOT EXISTS idx_runs_history_record_started_at_ms ON runs(history_record_id, started_at_ms);
CREATE INDEX IF NOT EXISTS idx_runs_history_record_id ON runs(history_record_id);
CREATE INDEX IF NOT EXISTS idx_runs_session_id ON runs(session_id);
CREATE INDEX IF NOT EXISTS idx_runs_input_blob_id ON runs(input_blob_id);
CREATE INDEX IF NOT EXISTS idx_runs_output_blob_id ON runs(output_blob_id);
CREATE INDEX IF NOT EXISTS idx_runs_source_id ON runs(source_id);

CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq);
CREATE INDEX IF NOT EXISTS idx_events_history_record_occurred_at_ms ON events(history_record_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_events_session_occurred_at_ms ON events(session_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_events_history_record_id ON events(history_record_id);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_capture_source_id ON events(capture_source_id);
CREATE INDEX IF NOT EXISTS idx_events_payload_blob_id ON events(payload_blob_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedupe_key ON events(dedupe_key) WHERE dedupe_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_vcs_workspaces_kind_repo_fingerprint ON vcs_workspaces(kind, repo_fingerprint);
CREATE INDEX IF NOT EXISTS idx_vcs_workspaces_source_id ON vcs_workspaces(source_id);

CREATE INDEX IF NOT EXISTS idx_vcs_changes_vcs_workspace_id ON vcs_changes(vcs_workspace_id);
CREATE INDEX IF NOT EXISTS idx_vcs_changes_source_id ON vcs_changes(source_id);

CREATE INDEX IF NOT EXISTS idx_history_record_links_history_record_id ON history_record_links(history_record_id);
CREATE INDEX IF NOT EXISTS idx_history_record_links_source_id ON history_record_links(source_id);

CREATE INDEX IF NOT EXISTS idx_artifacts_source_id ON artifacts(source_id);

CREATE INDEX IF NOT EXISTS idx_summaries_history_record_id ON summaries(history_record_id);
CREATE INDEX IF NOT EXISTS idx_summaries_session_id ON summaries(session_id);
CREATE INDEX IF NOT EXISTS idx_summaries_source_id ON summaries(source_id);

CREATE INDEX IF NOT EXISTS idx_files_touched_history_record_id ON files_touched(history_record_id);
CREATE INDEX IF NOT EXISTS idx_files_touched_run_id ON files_touched(run_id);
CREATE INDEX IF NOT EXISTS idx_files_touched_event_id ON files_touched(event_id);
CREATE INDEX IF NOT EXISTS idx_files_touched_vcs_workspace_id ON files_touched(vcs_workspace_id);
CREATE INDEX IF NOT EXISTS idx_files_touched_source_id ON files_touched(source_id);
CREATE INDEX IF NOT EXISTS idx_files_touched_path ON files_touched(path);
CREATE INDEX IF NOT EXISTS idx_files_touched_old_path ON files_touched(old_path);

CREATE INDEX IF NOT EXISTS idx_history_record_tags_tag_id ON history_record_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_history_record_tags_source_id ON history_record_tags(source_id);

CREATE INDEX IF NOT EXISTS idx_record_edges_from_record_id ON record_edges(from_record_id);
CREATE INDEX IF NOT EXISTS idx_record_edges_to_record_id ON record_edges(to_record_id);
CREATE INDEX IF NOT EXISTS idx_record_edges_source_id ON record_edges(source_id);

CREATE INDEX IF NOT EXISTS idx_sync_outbox_sync_state_updated_at_ms ON sync_outbox(sync_state, updated_at_ms);
CREATE INDEX IF NOT EXISTS idx_local_workspaces_device_id ON local_workspaces(device_id);
CREATE INDEX IF NOT EXISTS idx_local_workspaces_vcs_workspace_id ON local_workspaces(vcs_workspace_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_source_id ON audit_log(source_id);
"#;

/// Fork schema v1001: covering keyset indexes for bounded pagination.
/// `idx_sessions_provider_external_session_started` serves newest-first
/// external-session lookups; `idx_events_session_seq_id` serves `(seq, id)`
/// keyset pages within a session.
const V1001_INDEXES_SQL: &str = r#"
CREATE INDEX IF NOT EXISTS idx_sessions_provider_external_session_started ON sessions(provider, external_session_id, started_at_ms DESC, id);
CREATE INDEX IF NOT EXISTS idx_events_session_seq_id ON events(session_id, seq, id);
"#;

// `safe_preview_text` is legacy schema naming. It stores local searchable
// preview text and must not be interpreted as share-safe redaction.
const FTS_TABLES_SQL: &str = r#"
CREATE VIRTUAL TABLE IF NOT EXISTS ctx_history_search USING fts5(
    record_id UNINDEXED,
    title,
    summary,
    primary_user_text,
    decision_text,
    context_text,
    tag_text
);

CREATE VIRTUAL TABLE IF NOT EXISTS event_search USING fts5(
    event_id UNINDEXED,
    history_record_id UNINDEXED,
    session_id UNINDEXED,
    role UNINDEXED,
    safe_preview_text,
    rank_bucket UNINDEXED
);

CREATE VIRTUAL TABLE IF NOT EXISTS artifact_search USING fts5(
    artifact_id UNINDEXED,
    history_record_id UNINDEXED,
    safe_preview_text
);
"#;

/// Durable FTS rowid maps, fork schema v1000 (the fork's first divergence
/// from the upstream chain). Each row caches the SQLite-assigned FTS rowid
/// of the single projection row for one entity id, so existing-id
/// projection maintenance can replace the O(index size) full-scan DELETE on
/// an UNINDEXED FTS5 id column with a rowid point delete. The maps are
/// performance caches, never query inputs; the maintenance contract lives
/// on [`SearchRowidMapSpec`]. `artifact_search` is intentionally unmapped:
/// it is currently never populated. Executed by `migrate_to_v1000` and
/// re-executed on every `Store::migrate` so a dropped map table reappears
/// (empty, which is always safe: every row heals lazily on its next write).
const SEARCH_ROWID_MAP_TABLES_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS record_search_rowids (
    record_id TEXT PRIMARY KEY,
    search_rowid INTEGER NOT NULL UNIQUE
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS event_search_rowids (
    event_id TEXT PRIMARY KEY,
    search_rowid INTEGER NOT NULL UNIQUE
) WITHOUT ROWID;
"#;

const STABLE_SQL_VIEWS_SQL: &str = r#"
DROP VIEW IF EXISTS ctx_sessions;
CREATE VIEW ctx_sessions AS
SELECT
    s.id AS ctx_session_id,
    s.history_record_id,
    s.parent_session_id AS parent_ctx_session_id,
    s.root_session_id AS root_ctx_session_id,
    s.provider AS provider,
    s.external_session_id AS provider_session_id,
    s.external_agent_id AS external_agent_id,
    s.agent_type AS agent_type,
    s.role_hint AS role_hint,
    s.is_primary AS is_primary,
    s.status AS status,
    s.fidelity AS fidelity,
    s.started_at_ms AS started_at_ms,
    s.ended_at_ms AS ended_at_ms,
    cs.cwd AS cwd,
    cs.raw_source_path AS source_path
FROM sessions s
LEFT JOIN capture_sources cs ON cs.id = s.capture_source_id
WHERE s.deleted_at_ms IS NULL;

DROP VIEW IF EXISTS ctx_events;
CREATE VIEW ctx_events AS
SELECT
    e.id AS ctx_event_id,
    e.session_id AS ctx_session_id,
    e.history_record_id AS history_record_id,
    s.provider AS provider,
    s.external_session_id AS provider_session_id,
    e.seq AS event_seq,
    e.event_type AS event_type,
    e.role AS role,
    e.occurred_at_ms AS occurred_at_ms,
    e.payload_json AS payload_json,
    e.redaction_state AS redaction_state,
    e.fidelity AS fidelity,
    cs.cwd AS cwd,
    cs.raw_source_path AS source_path
FROM events e
LEFT JOIN sessions s ON s.id = e.session_id
LEFT JOIN capture_sources cs ON cs.id = e.capture_source_id
WHERE e.deleted_at_ms IS NULL;

DROP VIEW IF EXISTS ctx_files_touched;
CREATE VIEW ctx_files_touched AS
SELECT
    ft.id AS ctx_file_touch_id,
    ft.path AS path,
    ft.old_path AS old_path,
    ft.change_kind AS change_kind,
    ft.line_count_delta AS line_count_delta,
    ft.confidence AS confidence,
    ft.event_id AS ctx_event_id,
    COALESCE(e.session_id, r.session_id, source_session.id) AS ctx_session_id,
    COALESCE(
        e.history_record_id,
        r.history_record_id,
        ft.history_record_id,
        event_session.history_record_id,
        run_session.history_record_id,
        source_session.history_record_id
    ) AS history_record_id,
    COALESCE(s.provider, cs.provider) AS provider,
    COALESCE(s.external_session_id, cs.external_session_id) AS provider_session_id,
    ft.created_at_ms AS created_at_ms,
    ft.updated_at_ms AS updated_at_ms
FROM files_touched ft
LEFT JOIN events e ON e.id = ft.event_id
LEFT JOIN runs r ON r.id = ft.run_id
LEFT JOIN capture_sources cs ON cs.id = ft.source_id
LEFT JOIN sessions event_session ON event_session.id = e.session_id
LEFT JOIN sessions run_session ON run_session.id = r.session_id
LEFT JOIN sessions source_session ON source_session.capture_source_id = ft.source_id
LEFT JOIN sessions s ON s.id = COALESCE(e.session_id, r.session_id, source_session.id)
WHERE ft.deleted_at_ms IS NULL;

DROP VIEW IF EXISTS ctx_sources;
CREATE VIEW ctx_sources AS
SELECT
    provider AS provider,
    source_format AS source_format,
    source_root AS source_root,
    source_path AS source_path,
    external_session_id AS provider_session_id,
    parent_external_session_id AS parent_provider_session_id,
    agent_type AS agent_type,
    role_hint AS role_hint,
    external_agent_id AS external_agent_id,
    cwd AS cwd,
    session_started_at_ms AS session_started_at_ms,
    file_size_bytes AS file_size_bytes,
    file_modified_at_ms AS file_modified_at_ms,
    cataloged_at_ms AS cataloged_at_ms,
    indexed_at_ms AS indexed_at_ms,
    indexed_status AS indexed_status,
    indexed_error AS indexed_error,
    indexed_event_count AS indexed_event_count,
    last_imported_at_ms AS last_imported_at_ms,
    last_imported_file_size_bytes AS last_imported_file_size_bytes,
    last_imported_file_modified_at_ms AS last_imported_file_modified_at_ms,
    last_imported_file_sha256 AS last_imported_file_sha256,
    last_imported_event_count AS last_imported_event_count,
    is_stale AS is_stale
FROM catalog_sessions;
"#;

pub struct Store {
    path: PathBuf,
    object_dir: PathBuf,
    conn: Connection,
    busy_timeout: Duration,
    event_search_page_executions: std::cell::Cell<u64>,
    event_search_rows_hydrated: std::cell::Cell<u64>,
    #[cfg(feature = "test-utils")]
    record_search_page_executions: std::cell::Cell<u64>,
    record_list_page_executions: std::cell::Cell<u64>,
    #[cfg(feature = "test-utils")]
    relation_batch_executions: std::cell::Cell<u64>,
    #[cfg(feature = "test-utils")]
    search_hydration_loader_executions: std::cell::Cell<[u64; 2]>,
}

impl Store {
    /// Page budget for one bounded post-import merge pass
    /// ([`Store::merge_search_index_bounded`]). The unsigned non-zero type
    /// makes a negative budget unrepresentable: in FTS5 a negative `merge`
    /// argument switches to "merge everything towards one segment" mode,
    /// which is the unbounded behavior this API exists to avoid.
    pub const SEARCH_INDEX_MERGE_PAGES: std::num::NonZeroU16 = match std::num::NonZeroU16::new(256)
    {
        Some(pages) => pages,
        None => unreachable!(),
    };

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_busy_timeout(path, BUSY_TIMEOUT)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let object_dir = path
            .parent()
            .map(|parent| parent.join(OBJECTS_DIR))
            .unwrap_or_else(|| PathBuf::from(OBJECTS_DIR));
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        configure_read_only_connection(&conn, BUSY_TIMEOUT)?;
        let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if user_version != SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchemaVersion(user_version));
        }
        Ok(Self {
            path,
            object_dir,
            conn,
            busy_timeout: BUSY_TIMEOUT,
            event_search_page_executions: std::cell::Cell::new(0),
            event_search_rows_hydrated: std::cell::Cell::new(0),
            #[cfg(feature = "test-utils")]
            record_search_page_executions: std::cell::Cell::new(0),
            record_list_page_executions: std::cell::Cell::new(0),
            #[cfg(feature = "test-utils")]
            relation_batch_executions: std::cell::Cell::new(0),
            #[cfg(feature = "test-utils")]
            search_hydration_loader_executions: std::cell::Cell::new([0; 2]),
        })
    }

    fn resolve_id_prefix(
        &self,
        table: &'static str,
        prefix: &CtxIdPrefix,
    ) -> Result<IdPrefixResolution<Uuid>> {
        let sql = match table {
            "sessions" => "SELECT id FROM sessions WHERE id GLOB ?1 ORDER BY id",
            "events" => "SELECT id FROM events WHERE id GLOB ?1 ORDER BY id",
            _ => unreachable!("unsupported id prefix table"),
        };
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query(params![format!("{}*", prefix.canonical())])?;
        let mut candidate_count = 0_usize;
        let mut first_id = None;
        let mut previous_hex: Option<String> = None;
        let mut max_adjacent_lcp = prefix.hex_digits();

        while let Some(row) = rows.next()? {
            let id_text: String = row.get(0)?;
            let id = Uuid::parse_str(&id_text)?;
            if first_id.is_none() {
                first_id = Some(id);
            }
            candidate_count += 1;
            let compact = id_text.replace('-', "");
            if let Some(previous) = &previous_hex {
                max_adjacent_lcp = max_adjacent_lcp.max(common_prefix_len(previous, &compact));
            }
            previous_hex = Some(compact);
        }

        match (candidate_count, first_id) {
            (0, _) => Ok(IdPrefixResolution::NotFound),
            (1, Some(id)) => Ok(IdPrefixResolution::Found(id)),
            (_, _) => {
                let minimum_total_hex_digits = (max_adjacent_lcp + 1).min(32);
                Ok(IdPrefixResolution::Ambiguous(IdPrefixAmbiguity {
                    candidate_count,
                    minimum_total_hex_digits,
                    additional_hex_digits: minimum_total_hex_digits
                        .saturating_sub(prefix.hex_digits()),
                }))
            }
        }
    }

    pub fn open_with_busy_timeout(path: impl AsRef<Path>, busy_timeout: Duration) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut migrated_legacy_layout = false;
        let existed = path.exists();
        if !existed {
            if let Some(parent) = path.parent() {
                // Reject an unsupported legacy database before moving any
                // legacy files into the current layout.
                reject_unsupported_schema_at(
                    &parent.join(LEGACY_HISTORY_DIR_NAME).join("work.sqlite"),
                )?;
            }
        }
        if !existed {
            if let Some(parent) = path.parent() {
                migrated_legacy_layout = migrate_legacy_history_layout(parent)?;
                fs::create_dir_all(parent)?;
                // Close the creation permission window immediately: a
                // brand-new data root (or one that just received a moved
                // legacy store) must not linger with default permissions
                // while the schema chain runs below.
                restrict_private_dir(parent)?;
            }
        }
        let object_dir = path
            .parent()
            .map(|parent| parent.join(OBJECTS_DIR))
            .unwrap_or_else(|| PathBuf::from(OBJECTS_DIR));
        let conn = Connection::open(&path)?;
        if !existed {
            // The database file was just created empty by SQLite (or just
            // moved from the already-validated legacy layout); restrict it
            // to 0600 before any schema or data is written. A pre-existing
            // database is deliberately left untouched until its schema
            // version is validated by migrate() below.
            restrict_private_file(&path)?;
        }
        let store = Self {
            path,
            object_dir,
            conn,
            busy_timeout,
            event_search_page_executions: std::cell::Cell::new(0),
            event_search_rows_hydrated: std::cell::Cell::new(0),
            #[cfg(feature = "test-utils")]
            record_search_page_executions: std::cell::Cell::new(0),
            record_list_page_executions: std::cell::Cell::new(0),
            #[cfg(feature = "test-utils")]
            relation_batch_executions: std::cell::Cell::new(0),
            #[cfg(feature = "test-utils")]
            search_hydration_loader_executions: std::cell::Cell::new([0; 2]),
        };
        // Schema validation is deliberately the first operation for an
        // existing database. Unsupported versions must not trigger chmod,
        // directory creation, legacy moves, or persistent connection PRAGMAs.
        store.migrate()?;
        if let Some(parent) = store.path.parent() {
            fs::create_dir_all(parent)?;
            restrict_private_dir(parent)?;
        }
        fs::create_dir_all(&store.object_dir)?;
        restrict_private_dir(&store.object_dir)?;
        if let Some(spool_dir) = store.path.parent().map(|parent| parent.join(SPOOL_DIR)) {
            fs::create_dir_all(&spool_dir)?;
            restrict_private_dir(&spool_dir)?;
        }
        restrict_private_file(&store.path)?;
        if migrated_legacy_layout {
            store.normalize_legacy_blob_paths()?;
        }
        store.ensure_search_projection_initialized()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sqlite_profile_metadata(&self) -> Result<SqliteProfileMetadata> {
        Ok(SqliteProfileMetadata {
            version: self
                .conn
                .query_row("SELECT sqlite_version()", [], |row| row.get(0))?,
            journal_mode: self
                .conn
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))?,
            synchronous: self
                .conn
                .query_row("PRAGMA synchronous", [], |row| row.get(0))?,
            page_size: self
                .conn
                .query_row("PRAGMA page_size", [], |row| row.get(0))?,
            foreign_keys: self
                .conn
                .query_row("PRAGMA foreign_keys", [], |row| row.get(0))?,
            user_version: self
                .conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))?,
        })
    }

    pub fn profile_table_counts(&self) -> Result<ProfileTableCounts> {
        Ok(ProfileTableCounts {
            records: fixed_count(&self.conn, "SELECT COUNT(*) FROM history_records")?,
            capture_sources: fixed_count(&self.conn, "SELECT COUNT(*) FROM capture_sources")?,
            sessions: fixed_count(&self.conn, "SELECT COUNT(*) FROM sessions")?,
            runs: fixed_count(&self.conn, "SELECT COUNT(*) FROM runs")?,
            events: fixed_count(&self.conn, "SELECT COUNT(*) FROM events")?,
            summaries: fixed_count(&self.conn, "SELECT COUNT(*) FROM summaries")?,
            files_touched: fixed_count(&self.conn, "SELECT COUNT(*) FROM files_touched")?,
            record_fts: fixed_count(&self.conn, "SELECT COUNT(*) FROM ctx_history_search")?,
            event_fts: fixed_count(&self.conn, "SELECT COUNT(*) FROM event_search")?,
            artifact_fts: fixed_count(&self.conn, "SELECT COUNT(*) FROM artifact_search")?,
        })
    }

    pub fn raw_sql_query(&self, sql: &str, options: RawSqlOptions) -> Result<RawSqlResult> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(StoreError::RawSqlEmpty);
        }
        validate_raw_sql_options(&options)?;
        validate_raw_sql_statement_bytes(sql, &options)?;
        reject_sql_tail(&self.conn, sql)?;
        let _limits = RawSqlLimitGuard::apply(&self.conn, &options)?;

        let mut stmt = self.conn.prepare(sql)?;
        if stmt.parameter_count() > 0 {
            return Err(StoreError::RawSqlHasParameters);
        }
        if !stmt.readonly() {
            return Err(StoreError::RawSqlNotReadOnly);
        }
        let column_count = stmt.column_count();
        if column_count == 0 {
            return Err(StoreError::RawSqlNoColumns);
        }
        if column_count > options.max_columns {
            return Err(StoreError::RawSqlTooManyColumns {
                columns: column_count,
                max_columns: options.max_columns,
            });
        }

        let columns = stmt
            .column_names()
            .into_iter()
            .map(|name| RawSqlColumn {
                name: name.to_owned(),
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let timeout = options.timeout;
        let progress_started = started;
        self.conn
            .progress_handler(1_000, Some(move || progress_started.elapsed() >= timeout));

        let query_result = (|| -> Result<RawSqlResult> {
            let mut rows = stmt.query([])?;
            let mut output_rows = Vec::new();
            let mut rows_truncated = false;
            let mut values_truncated = false;

            while let Some(row) = rows.next()? {
                if output_rows.len() >= options.max_rows {
                    rows_truncated = true;
                    break;
                }
                let mut output_row = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    let value = raw_sql_value(row.get_ref(index)?, options.max_value_bytes);
                    if value.is_truncated() {
                        values_truncated = true;
                    }
                    output_row.push(value);
                }
                output_rows.push(output_row);
            }

            Ok(RawSqlResult {
                returned_rows: output_rows.len(),
                columns,
                rows: output_rows,
                truncated: RawSqlTruncation {
                    rows: rows_truncated,
                    values: values_truncated,
                },
                elapsed: started.elapsed(),
                limits: RawSqlLimits {
                    max_rows: options.max_rows,
                    max_columns: options.max_columns,
                    max_value_bytes: options.max_value_bytes,
                    max_sql_bytes: options.max_sql_bytes,
                    timeout_ms: duration_ms(options.timeout),
                },
            })
        })();

        self.conn.progress_handler(0, None::<fn() -> bool>);

        match query_result {
            Err(StoreError::Sql(rusqlite::Error::SqliteFailure(error, _)))
                if error.code == ErrorCode::OperationInterrupted
                    && started.elapsed() >= options.timeout =>
            {
                Err(StoreError::RawSqlTimedOut {
                    timeout_ms: duration_ms(options.timeout),
                })
            }
            other => other,
        }
    }

    pub fn begin_immediate_batch(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    pub fn commit_batch(&self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    pub fn rollback_batch(&self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
        Ok(())
    }

    pub fn checkpoint_wal_passive(&self) -> Result<()> {
        self.conn
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))?;
        Ok(())
    }

    pub fn checkpoint_wal_truncate(&self) -> Result<()> {
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(())
    }

    pub fn checkpoint_wal_passive_if_larger_than(&self, min_bytes: u64) -> Result<bool> {
        let Some(wal_bytes) = self.wal_bytes()? else {
            return Ok(false);
        };
        if wal_bytes < min_bytes {
            return Ok(false);
        }
        self.checkpoint_wal_passive()?;
        Ok(true)
    }

    pub fn checkpoint_wal_truncate_if_larger_than(&self, min_bytes: u64) -> Result<bool> {
        let Some(wal_bytes) = self.wal_bytes()? else {
            return Ok(false);
        };
        if wal_bytes < min_bytes {
            return Ok(false);
        }
        self.checkpoint_wal_truncate()?;
        Ok(true)
    }

    fn wal_path(&self) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push("-wal");
        PathBuf::from(path)
    }

    fn wal_bytes(&self) -> Result<Option<u64>> {
        match fs::metadata(self.wal_path()) {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StoreError::Io(err)),
        }
    }

    pub fn migrate(&self) -> Result<()> {
        // Validate the on-disk schema version before any persistent PRAGMA:
        // rejecting a foreign database (an unreviewed upstream version in
        // the (15, 1000) gap, or anything newer than this binary) must
        // leave its file — header, schema, and journal mode included —
        // exactly as the binary that owns it left them. The busy timeout is
        // per-connection and non-persistent, so it is safe to set first and
        // keeps the version read robust under a concurrent writer.
        self.conn.busy_timeout(self.busy_timeout)?;
        let user_version: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if user_version > SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchemaVersion(user_version));
        }
        if user_version > UPSTREAM_SCHEMA_VERSION_MAX && user_version < FORK_SCHEMA_VERSION_MIN {
            return Err(StoreError::UnsupportedSchemaVersion(user_version));
        }
        configure_connection(&self.conn, self.busy_timeout)?;
        if user_version < 1 {
            migrate_to_v1(&self.conn)?;
        }
        if user_version < 2 {
            migrate_to_v2(&self.conn)?;
        }
        if user_version < 3 {
            migrate_to_v3(&self.conn)?;
        }
        if user_version < 4 {
            migrate_to_v4(&self.conn)?;
        }
        if user_version < 5 {
            migrate_to_v5(&self.conn)?;
        }
        if user_version < 6 {
            migrate_to_v6(&self.conn)?;
        }
        if user_version < 7 {
            migrate_to_v7(&self.conn)?;
        }
        if user_version < 8 {
            migrate_to_v8(&self.conn)?;
        }
        if user_version < 9 {
            migrate_to_v9(&self.conn)?;
        }
        if user_version < 10 {
            migrate_to_v10(&self.conn)?;
        }
        if user_version < 11 {
            migrate_to_v11(&self.conn)?;
        }
        if user_version < 12 {
            migrate_to_v12(&self.conn)?;
        }
        if user_version < 13 {
            migrate_to_v13(&self.conn)?;
        }
        if user_version < 14 {
            migrate_to_v14(&self.conn)?;
        }
        if user_version < 15 {
            migrate_to_v15(&self.conn)?;
        }
        if user_version < 1000 {
            migrate_to_v1000(&self.conn)?;
        }
        if user_version < 1001 {
            migrate_to_v1001(&self.conn)?;
        }
        create_fts_tables_if_supported(&self.conn)?;
        // Recreate dropped rowid map tables empty on open; an empty map is
        // always safe (writes degrade to the legacy full-scan path and each
        // row heals lazily on its next write).
        self.conn.execute_batch(SEARCH_ROWID_MAP_TABLES_SQL)?;
        Ok(())
    }

    pub fn schema(&self) -> Result<String> {
        let mut stmt = self.conn.prepare(
            "SELECT sql FROM sqlite_master
             WHERE type IN ('table', 'index', 'view') AND sql IS NOT NULL
             ORDER BY type, name",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut schema = Vec::new();
        for row in rows {
            schema.push(row?);
        }
        Ok(schema.join(";\n"))
    }

    pub fn refresh_search_index(&self) -> Result<()> {
        self.rebuild_search_projection()
    }

    /// Full FTS5 `optimize`: merges every segment of every search projection
    /// into a single b-tree. Cost is proportional to total index size, so
    /// this is only appropriate for explicit, offline maintenance. Routine
    /// post-import upkeep should use [`Store::merge_search_index_bounded`].
    pub fn optimize_search_index(&self) -> Result<()> {
        for table in SEARCH_PROJECTION_FTS_TABLES {
            if table_exists(&self.conn, table)? {
                self.conn.execute(
                    format!("INSERT INTO {table}({table}) VALUES ('optimize')").as_str(),
                    [],
                )?;
            }
        }
        Ok(())
    }

    /// One fixed positive FTS5 merge request per existing search projection:
    /// `INSERT INTO t(t, rank) VALUES ('merge', N)`, asking SQLite for roughly
    /// [`Store::SEARCH_INDEX_MERGE_PAGES`] pages of work per table.
    ///
    /// A positive merge argument only considers levels that have accumulated
    /// at least `usermerge` (default 4) same-level segments. On an
    /// already-compacted index a tiny increment will usually leave too few
    /// eligible segments, so this request usually does no merge work. Unlike
    /// `optimize` (or a negative merge argument), this method does not ask
    /// SQLite to merge the whole index: it issues exactly one request per
    /// table, never loops, and never derives the argument from user input.
    /// SQLite may write somewhat more than N pages while completing a b-tree
    /// operation; N is a requested amount of merge work, not a strict cap.
    pub fn merge_search_index_bounded(&self) -> Result<()> {
        for table in SEARCH_PROJECTION_FTS_TABLES {
            if table_exists(&self.conn, table)? {
                self.conn.execute(
                    format!(
                        "INSERT INTO {table}({table}, rank) VALUES ('merge', {})",
                        Self::SEARCH_INDEX_MERGE_PAGES
                    )
                    .as_str(),
                    [],
                )?;
            }
        }
        Ok(())
    }

    pub fn event_search_projection_needs_backfill(&self) -> Result<bool> {
        if !table_exists(&self.conn, "event_search")? {
            return Ok(false);
        }
        Ok(table_has_rows(&self.conn, "events")? && !table_has_rows(&self.conn, "event_search")?)
    }

    pub fn upsert_capture_source(&self, source: &CaptureSource) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO capture_sources
            (
                id, kind, provider, machine_id, process_id, cwd, raw_source_path,
                external_session_id, started_at_ms, ended_at_ms, fidelity,
                visibility, sync_state, sync_version, metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(id) DO UPDATE SET
                kind = excluded.kind,
                provider = excluded.provider,
                machine_id = excluded.machine_id,
                process_id = excluded.process_id,
                cwd = excluded.cwd,
                raw_source_path = excluded.raw_source_path,
                external_session_id = excluded.external_session_id,
                started_at_ms = excluded.started_at_ms,
                ended_at_ms = excluded.ended_at_ms,
                fidelity = excluded.fidelity,
                visibility = excluded.visibility,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                metadata_json = excluded.metadata_json
            "#,
            params![
                source.id.to_string(),
                source.descriptor.kind.as_str(),
                source.descriptor.provider.as_str(),
                source.descriptor.machine_id.as_str(),
                source.descriptor.process_id.map(i64::from),
                source.descriptor.cwd.as_deref(),
                source.descriptor.raw_source_path.as_deref(),
                source.descriptor.external_session_id.as_deref(),
                timestamp_ms(source.started_at),
                optional_timestamp_ms(source.ended_at),
                source.sync.fidelity.as_str(),
                source.sync.visibility.as_str(),
                source.sync.sync_state.as_str(),
                source.sync.sync_version as i64,
                serde_json::to_string(&source.sync.metadata)?,
            ],
        )?;
        Ok(())
    }

    pub fn get_capture_source(&self, id: Uuid) -> Result<CaptureSource> {
        self.conn
            .query_row(
                "SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources WHERE id = ?1",
                params![id.to_string()],
                capture_source_from_row,
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))
    }

    pub fn capture_sources_for_ids(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, CaptureSource>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        let mut sources = BTreeMap::new();
        for chunk in distinct_uuid_chunks(ids) {
            #[cfg(feature = "test-utils")]
            self.relation_batch_executions
                .set(self.relation_batch_executions.get().saturating_add(1));
            let sql = format!("SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources WHERE id IN ({}) ORDER BY id", sql_placeholders(chunk.len()));
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(uuid_values(&chunk), capture_source_from_row)?;
            for row in rows {
                let source = row?;
                sources.insert(source.id, source);
            }
        }
        Ok(sources)
    }

    pub fn search_capture_sources_for_ids(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, SearchCaptureSourceRow>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        let mut sources = BTreeMap::new();
        for chunk in distinct_uuid_chunks(ids) {
            #[cfg(feature = "test-utils")]
            self.relation_batch_executions
                .set(self.relation_batch_executions.get().saturating_add(1));
            let sql = format!("SELECT id, provider, cwd, raw_source_path, external_session_id, metadata_json FROM capture_sources WHERE id IN ({}) ORDER BY id", sql_placeholders(chunk.len()));
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(uuid_values(&chunk), search_capture_source_from_row)?;
            for row in rows {
                let source = row?;
                sources.insert(source.id, source);
            }
        }
        Ok(sources)
    }

    pub fn list_capture_sources(&self) -> Result<Vec<CaptureSource>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources ORDER BY started_at_ms, id",
        )?;
        let rows = stmt.query_map([], capture_source_from_row)?;
        collect_rows(rows)
    }

    pub fn capture_source_count(&self) -> Result<usize> {
        let count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM capture_sources", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    pub fn latest_indexed_source_at_ms(&self) -> Result<Option<i64>> {
        let indexed_at: Option<i64> = self.conn.query_row(
            "SELECT MAX(indexed_at_ms) FROM (
                SELECT indexed_at_ms AS indexed_at_ms FROM source_import_files WHERE indexed_at_ms IS NOT NULL
                UNION ALL SELECT updated_at_ms AS indexed_at_ms FROM history_records
                UNION ALL SELECT updated_at_ms AS indexed_at_ms FROM sessions
            )",
            [],
            |row| row.get(0),
        )?;
        if indexed_at.is_some() {
            return Ok(indexed_at);
        }
        Ok(self
            .conn
            .query_row("SELECT MAX(occurred_at_ms) FROM events", [], |row| {
                row.get(0)
            })?)
    }

    pub fn capture_source_by_external_session(
        &self,
        provider: CaptureProvider,
        external_session_id: &str,
    ) -> Result<Option<CaptureSource>> {
        self.conn
            .query_row(
                "SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources WHERE provider = ?1 AND external_session_id = ?2 ORDER BY started_at_ms DESC LIMIT 1",
                params![provider.as_str(), external_session_id],
                capture_source_from_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn mark_catalog_source_stale(
        &self,
        provider: CaptureProvider,
        source_root: &str,
        cataloged_at_ms: i64,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r#"
            UPDATE catalog_sessions
            SET is_stale = 1, cataloged_at_ms = ?3
            WHERE provider = ?1 AND source_root = ?2
            "#,
            params![provider.as_str(), source_root, cataloged_at_ms],
        )?;
        Ok(changed)
    }

    pub fn upsert_catalog_sessions(&self, sessions: &[CatalogSession]) -> Result<()> {
        let mut stmt = self.conn.prepare(
            r#"
            INSERT INTO catalog_sessions
            (
                source_path, provider, source_format, source_root,
                external_session_id, parent_external_session_id, agent_type, role_hint,
                external_agent_id, cwd, session_started_at_ms, file_size_bytes,
                file_modified_at_ms, cataloged_at_ms, is_stale, metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, 0, ?15)
            ON CONFLICT(source_path) DO UPDATE SET
                provider = excluded.provider,
                source_format = excluded.source_format,
                source_root = excluded.source_root,
                external_session_id = excluded.external_session_id,
                parent_external_session_id = excluded.parent_external_session_id,
                agent_type = excluded.agent_type,
                role_hint = excluded.role_hint,
                external_agent_id = excluded.external_agent_id,
                cwd = excluded.cwd,
                session_started_at_ms = excluded.session_started_at_ms,
                file_size_bytes = excluded.file_size_bytes,
                file_modified_at_ms = excluded.file_modified_at_ms,
                cataloged_at_ms = excluded.cataloged_at_ms,
                is_stale = 0,
                indexed_at_ms = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.indexed_at_ms
                    ELSE NULL
                END,
                indexed_file_size_bytes = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.indexed_file_size_bytes
                    ELSE NULL
                END,
                indexed_file_modified_at_ms = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.indexed_file_modified_at_ms
                    ELSE NULL
                END,
                indexed_status = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.indexed_status
                    ELSE 'pending'
                END,
                indexed_error = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.indexed_error
                    ELSE NULL
                END,
                indexed_event_count = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.indexed_event_count
                    ELSE NULL
                END,
                last_imported_at_ms = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.last_imported_at_ms
                    WHEN excluded.file_size_bytes > catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_status = 'indexed'
                     AND catalog_sessions.indexed_file_size_bytes = catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_file_modified_at_ms = catalog_sessions.file_modified_at_ms
                     AND catalog_sessions.last_imported_file_size_bytes = catalog_sessions.file_size_bytes
                    THEN catalog_sessions.last_imported_at_ms
                    ELSE NULL
                END,
                last_imported_file_size_bytes = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.last_imported_file_size_bytes
                    WHEN excluded.file_size_bytes > catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_status = 'indexed'
                     AND catalog_sessions.indexed_file_size_bytes = catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_file_modified_at_ms = catalog_sessions.file_modified_at_ms
                     AND catalog_sessions.last_imported_file_size_bytes = catalog_sessions.file_size_bytes
                    THEN catalog_sessions.last_imported_file_size_bytes
                    ELSE NULL
                END,
                last_imported_file_modified_at_ms = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.last_imported_file_modified_at_ms
                    WHEN excluded.file_size_bytes > catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_status = 'indexed'
                     AND catalog_sessions.indexed_file_size_bytes = catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_file_modified_at_ms = catalog_sessions.file_modified_at_ms
                     AND catalog_sessions.last_imported_file_size_bytes = catalog_sessions.file_size_bytes
                    THEN catalog_sessions.last_imported_file_modified_at_ms
                    ELSE NULL
                END,
                last_imported_file_sha256 = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.last_imported_file_sha256
                    WHEN excluded.file_size_bytes > catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_status = 'indexed'
                     AND catalog_sessions.indexed_file_size_bytes = catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_file_modified_at_ms = catalog_sessions.file_modified_at_ms
                     AND catalog_sessions.last_imported_file_size_bytes = catalog_sessions.file_size_bytes
                    THEN catalog_sessions.last_imported_file_sha256
                    ELSE NULL
                END,
                last_imported_event_count = CASE
                    WHEN catalog_sessions.file_size_bytes = excluded.file_size_bytes
                     AND catalog_sessions.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN catalog_sessions.last_imported_event_count
                    WHEN excluded.file_size_bytes > catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_status = 'indexed'
                     AND catalog_sessions.indexed_file_size_bytes = catalog_sessions.file_size_bytes
                     AND catalog_sessions.indexed_file_modified_at_ms = catalog_sessions.file_modified_at_ms
                     AND catalog_sessions.last_imported_file_size_bytes = catalog_sessions.file_size_bytes
                    THEN catalog_sessions.last_imported_event_count
                    ELSE NULL
                END,
                metadata_json = excluded.metadata_json
            WHERE catalog_sessions.provider IS NOT excluded.provider
               OR catalog_sessions.source_format IS NOT excluded.source_format
               OR catalog_sessions.source_root IS NOT excluded.source_root
               OR catalog_sessions.external_session_id IS NOT excluded.external_session_id
               OR catalog_sessions.parent_external_session_id IS NOT excluded.parent_external_session_id
               OR catalog_sessions.agent_type IS NOT excluded.agent_type
               OR catalog_sessions.role_hint IS NOT excluded.role_hint
               OR catalog_sessions.external_agent_id IS NOT excluded.external_agent_id
               OR catalog_sessions.cwd IS NOT excluded.cwd
               OR catalog_sessions.session_started_at_ms IS NOT excluded.session_started_at_ms
               OR catalog_sessions.file_size_bytes != excluded.file_size_bytes
               OR catalog_sessions.file_modified_at_ms != excluded.file_modified_at_ms
               OR catalog_sessions.is_stale != 0
               OR catalog_sessions.metadata_json IS NOT excluded.metadata_json
            "#,
        )?;
        for session in sessions {
            stmt.execute(params![
                session.source_path.as_str(),
                session.provider.as_str(),
                session.source_format.as_str(),
                session.source_root.as_str(),
                session.external_session_id.as_deref(),
                session.parent_external_session_id.as_deref(),
                session.agent_type.as_str(),
                session.role_hint.as_deref(),
                session.external_agent_id.as_deref(),
                session.cwd.as_deref(),
                session.session_started_at_ms,
                capped_i64(session.file_size_bytes),
                session.file_modified_at_ms,
                session.cataloged_at_ms,
                serde_json::to_string(&session.metadata)?,
            ])?;
        }
        Ok(())
    }

    pub fn list_catalog_sessions_for_source(
        &self,
        provider: CaptureProvider,
        source_root: &str,
    ) -> Result<Vec<CatalogSession>> {
        let mut stmt = self.conn.prepare(
            format!(
                "{} WHERE provider = ?1 AND source_root = ?2",
                catalog_session_select_sql("")
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(
            params![provider.as_str(), source_root],
            catalog_session_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn catalog_source_stale_session_count(
        &self,
        provider: CaptureProvider,
        source_root: &str,
    ) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM catalog_sessions
                WHERE provider = ?1
                  AND source_root = ?2
                  AND is_stale != 0
                "#,
                params![provider.as_str(), source_root],
                |row| row.get::<_, usize>(0),
            )
            .map_err(Into::into)
    }

    pub fn mark_catalog_source_missing_paths_stale(
        &self,
        provider: CaptureProvider,
        source_root: &str,
        current_paths: &[String],
        cataloged_at_ms: i64,
    ) -> Result<usize> {
        self.conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS temp_catalog_current_paths(source_path TEXT PRIMARY KEY)",
            [],
        )?;
        self.conn
            .execute("DELETE FROM temp_catalog_current_paths", [])?;
        {
            let mut stmt = self.conn.prepare(
                "INSERT OR IGNORE INTO temp_catalog_current_paths(source_path) VALUES (?1)",
            )?;
            for path in current_paths {
                stmt.execute(params![path.as_str()])?;
            }
        }
        let changed = self.conn.execute(
            r#"
            UPDATE catalog_sessions
            SET is_stale = 1, cataloged_at_ms = ?3
            WHERE provider = ?1
              AND source_root = ?2
              AND NOT EXISTS (
                  SELECT 1
                  FROM temp_catalog_current_paths current
                  WHERE current.source_path = catalog_sessions.source_path
              )
            "#,
            params![provider.as_str(), source_root, cataloged_at_ms],
        )?;
        self.conn
            .execute("DELETE FROM temp_catalog_current_paths", [])?;
        Ok(changed)
    }

    pub fn list_pending_catalog_sessions(
        &self,
        provider: CaptureProvider,
        source_root: &str,
    ) -> Result<Vec<CatalogSession>> {
        let mut stmt = self.conn.prepare(
            format!(
                "{} WHERE provider = ?1
                   AND source_root = ?2
                   AND is_stale = 0
                   AND {}
                 ORDER BY session_started_at_ms, source_path",
                catalog_session_select_sql(""),
                catalog_pending_import_condition_sql("catalog_sessions")
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(
            params![provider.as_str(), source_root],
            catalog_session_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn mark_catalog_source_indexed(
        &self,
        provider: CaptureProvider,
        update: CatalogSourceIndexUpdate<'_>,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r#"
            UPDATE catalog_sessions
            SET indexed_at_ms = ?4,
                indexed_file_size_bytes = ?5,
                indexed_file_modified_at_ms = ?6,
                indexed_status = ?8,
                indexed_error = NULL,
                indexed_event_count = ?7,
                last_imported_at_ms = ?4,
                last_imported_file_size_bytes = ?5,
                last_imported_file_modified_at_ms = ?6,
                last_imported_file_sha256 = ?9,
                last_imported_event_count = ?7
            WHERE provider = ?1
              AND source_root = ?2
              AND source_path = ?3
              AND is_stale = 0
            "#,
            params![
                provider.as_str(),
                update.source_root,
                update.source_path,
                update.indexed_at_ms,
                capped_i64(update.file_size_bytes),
                update.file_modified_at_ms,
                update.event_count.map(capped_i64),
                CatalogIndexedStatus::Indexed.as_str(),
                update.file_sha256,
            ],
        )?;
        Ok(changed)
    }

    pub fn mark_catalog_source_failed(
        &self,
        provider: CaptureProvider,
        source_root: &str,
        source_path: &str,
        error: &str,
        indexed_at_ms: i64,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r#"
            UPDATE catalog_sessions
            SET indexed_at_ms = ?4,
                indexed_file_size_bytes = NULL,
                indexed_file_modified_at_ms = NULL,
                indexed_status = ?6,
                indexed_error = ?5,
                indexed_event_count = NULL
            WHERE provider = ?1
              AND source_root = ?2
              AND source_path = ?3
              AND is_stale = 0
            "#,
            params![
                provider.as_str(),
                source_root,
                source_path,
                indexed_at_ms,
                error,
                CatalogIndexedStatus::Failed.as_str(),
            ],
        )?;
        Ok(changed)
    }

    pub fn catalog_source_index_state(
        &self,
        provider: CaptureProvider,
        source_root: &str,
        source_path: &str,
    ) -> Result<Option<CatalogSourceIndexState>> {
        self.conn
            .query_row(
                r#"
                SELECT last_imported_file_size_bytes,
                       last_imported_file_modified_at_ms,
                       last_imported_event_count,
                       last_imported_at_ms,
                       last_imported_file_sha256
                FROM catalog_sessions
                WHERE provider = ?1
                  AND source_root = ?2
                  AND source_path = ?3
                  AND is_stale = 0
                "#,
                params![provider.as_str(), source_root, source_path],
                |row| {
                    let last_imported_file_size_bytes = row
                        .get::<_, Option<i64>>(0)?
                        .map(nonnegative_i64_to_u64)
                        .transpose()?;
                    let last_imported_event_count = row
                        .get::<_, Option<i64>>(2)?
                        .map(nonnegative_i64_to_u64)
                        .transpose()?;
                    Ok(CatalogSourceIndexState {
                        last_imported_file_size_bytes,
                        last_imported_file_modified_at_ms: row.get(1)?,
                        last_imported_event_count,
                        last_imported_at_ms: row.get(3)?,
                        last_imported_file_sha256: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn upsert_source_import_files(&self, files: &[SourceImportFile]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let mut stmt = self.conn.prepare(
            r#"
            INSERT INTO source_import_files (
                provider, source_format, source_root, source_path,
                file_size_bytes, file_modified_at_ms, observed_at_ms, is_stale,
                metadata_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8)
            ON CONFLICT(provider, source_root, source_path) DO UPDATE SET
                source_format = excluded.source_format,
                file_size_bytes = excluded.file_size_bytes,
                file_modified_at_ms = excluded.file_modified_at_ms,
                observed_at_ms = excluded.observed_at_ms,
                is_stale = 0,
                indexed_at_ms = CASE
                    WHEN source_import_files.file_size_bytes = excluded.file_size_bytes
                     AND source_import_files.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN source_import_files.indexed_at_ms
                    ELSE NULL
                END,
                indexed_file_size_bytes = CASE
                    WHEN source_import_files.file_size_bytes = excluded.file_size_bytes
                     AND source_import_files.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN source_import_files.indexed_file_size_bytes
                    ELSE NULL
                END,
                indexed_file_modified_at_ms = CASE
                    WHEN source_import_files.file_size_bytes = excluded.file_size_bytes
                     AND source_import_files.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN source_import_files.indexed_file_modified_at_ms
                    ELSE NULL
                END,
                indexed_status = CASE
                    WHEN source_import_files.file_size_bytes = excluded.file_size_bytes
                     AND source_import_files.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN source_import_files.indexed_status
                    ELSE 'pending'
                END,
                indexed_error = CASE
                    WHEN source_import_files.file_size_bytes = excluded.file_size_bytes
                     AND source_import_files.file_modified_at_ms = excluded.file_modified_at_ms
                    THEN source_import_files.indexed_error
                    ELSE NULL
                END,
                metadata_json = excluded.metadata_json
            WHERE source_import_files.source_format IS NOT excluded.source_format
               OR source_import_files.file_size_bytes != excluded.file_size_bytes
               OR source_import_files.file_modified_at_ms != excluded.file_modified_at_ms
               OR source_import_files.is_stale != 0
               OR source_import_files.metadata_json IS NOT excluded.metadata_json
            "#,
        )?;
        for file in files {
            stmt.execute(params![
                file.provider.as_str(),
                file.source_format.as_str(),
                file.source_root.as_str(),
                file.source_path.as_str(),
                capped_i64(file.file_size_bytes),
                file.file_modified_at_ms,
                file.observed_at_ms,
                serde_json::to_string(&file.metadata)?,
            ])?;
        }
        Ok(())
    }

    pub fn mark_source_import_missing_paths_stale(
        &self,
        provider: CaptureProvider,
        source_root: &str,
        current_paths: &[String],
        observed_at_ms: i64,
    ) -> Result<usize> {
        self.conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS temp_source_import_current_paths (source_path TEXT PRIMARY KEY)",
        )?;
        self.conn
            .execute("DELETE FROM temp_source_import_current_paths", [])?;
        {
            let mut stmt = self.conn.prepare(
                "INSERT OR IGNORE INTO temp_source_import_current_paths (source_path) VALUES (?1)",
            )?;
            for source_path in current_paths {
                stmt.execute(params![source_path])?;
            }
        }
        let changed = self.conn.execute(
            r#"
            UPDATE source_import_files
            SET is_stale = 1, observed_at_ms = ?3
            WHERE provider = ?1
              AND source_root = ?2
              AND is_stale = 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM temp_source_import_current_paths AS current
                  WHERE current.source_path = source_import_files.source_path
              )
            "#,
            params![provider.as_str(), source_root, observed_at_ms],
        )?;
        self.conn
            .execute("DELETE FROM temp_source_import_current_paths", [])?;
        Ok(changed)
    }

    pub fn list_pending_source_import_files(
        &self,
        provider: CaptureProvider,
        source_root: &str,
    ) -> Result<Vec<SourceImportFile>> {
        let mut stmt = self.conn.prepare(
            format!(
                "{} WHERE provider = ?1
                   AND source_root = ?2
                   AND is_stale = 0
                   AND (
                       indexed_status != 'indexed'
                       OR indexed_file_size_bytes IS NULL
                       OR indexed_file_modified_at_ms IS NULL
                       OR indexed_file_size_bytes != file_size_bytes
                       OR indexed_file_modified_at_ms != file_modified_at_ms
                   )
                 ORDER BY source_path",
                source_import_file_select_sql("")
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(
            params![provider.as_str(), source_root],
            source_import_file_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn mark_source_import_file_indexed(
        &self,
        provider: CaptureProvider,
        update: SourceImportFileIndexUpdate<'_>,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r#"
            UPDATE source_import_files
            SET indexed_at_ms = ?4,
                indexed_file_size_bytes = ?5,
                indexed_file_modified_at_ms = ?6,
                indexed_status = ?7,
                indexed_error = NULL
            WHERE provider = ?1
              AND source_root = ?2
              AND source_path = ?3
              AND is_stale = 0
            "#,
            params![
                provider.as_str(),
                update.source_root,
                update.source_path,
                update.indexed_at_ms,
                capped_i64(update.file_size_bytes),
                update.file_modified_at_ms,
                CatalogIndexedStatus::Indexed.as_str(),
            ],
        )?;
        Ok(changed)
    }

    pub fn mark_source_import_file_failed(
        &self,
        provider: CaptureProvider,
        source_root: &str,
        source_path: &str,
        error: &str,
        indexed_at_ms: i64,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r#"
            UPDATE source_import_files
            SET indexed_at_ms = ?4,
                indexed_file_size_bytes = NULL,
                indexed_file_modified_at_ms = NULL,
                indexed_status = ?6,
                indexed_error = ?5
            WHERE provider = ?1
              AND source_root = ?2
              AND source_path = ?3
              AND is_stale = 0
            "#,
            params![
                provider.as_str(),
                source_root,
                source_path,
                indexed_at_ms,
                error,
                CatalogIndexedStatus::Failed.as_str(),
            ],
        )?;
        Ok(changed)
    }

    pub fn count_source_import_zero_yield_anomalies(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            r#"
            SELECT
              (SELECT COUNT(*)
               FROM source_import_files
               WHERE is_stale = 0
                 AND indexed_status = 'failed'
                 AND indexed_error = ?1)
              +
              (SELECT COUNT(*)
               FROM catalog_sessions
               WHERE is_stale = 0
                 AND indexed_status = 'failed'
                 AND indexed_error = ?1)
            "#,
            [SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    pub fn catalog_session_count(&self) -> Result<usize> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM catalog_sessions WHERE is_stale = 0",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(StoreError::from)
    }

    pub fn catalog_session_counts(&self) -> Result<CatalogCounts> {
        let total = self.conn.query_row(
            "SELECT COUNT(*) FROM catalog_sessions WHERE is_stale = 0",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        let indexed = self
            .conn
            .query_row(catalog_indexed_count_sql().as_str(), [], |row| {
                row.get::<_, i64>(0)
            })? as usize;
        let stale = self.conn.query_row(
            "SELECT COUNT(*) FROM catalog_sessions WHERE is_stale != 0",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        let pending = self.conn.query_row(
            format!(
                "SELECT COUNT(*) FROM catalog_sessions WHERE is_stale = 0 AND {}",
                catalog_pending_import_condition_sql("catalog_sessions")
            )
            .as_str(),
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        let failed = self.conn.query_row(
            "SELECT COUNT(*) FROM catalog_sessions WHERE is_stale = 0 AND indexed_status = 'failed'",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        Ok(CatalogCounts {
            total,
            indexed,
            stale,
            pending,
            failed,
        })
    }

    pub fn upsert_session(&self, session: &Session) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sessions
            (
                id, history_record_id, parent_session_id, root_session_id, capture_source_id,
                provider, external_session_id, external_agent_id, agent_type, role_hint,
                is_primary, status, fidelity, transcript_blob_id, started_at_ms, ended_at_ms,
                created_at_ms, updated_at_ms, visibility, sync_state, sync_version,
                deleted_at_ms, metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
            ON CONFLICT(id) DO UPDATE SET
                history_record_id = excluded.history_record_id,
                parent_session_id = excluded.parent_session_id,
                root_session_id = excluded.root_session_id,
                capture_source_id = excluded.capture_source_id,
                provider = excluded.provider,
                external_session_id = excluded.external_session_id,
                external_agent_id = excluded.external_agent_id,
                agent_type = excluded.agent_type,
                role_hint = excluded.role_hint,
                is_primary = excluded.is_primary,
                status = excluded.status,
                fidelity = excluded.fidelity,
                transcript_blob_id = excluded.transcript_blob_id,
                started_at_ms = excluded.started_at_ms,
                ended_at_ms = excluded.ended_at_ms,
                updated_at_ms = excluded.updated_at_ms,
                visibility = excluded.visibility,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                session.id.to_string(),
                optional_uuid_string(session.history_record_id),
                optional_uuid_string(session.parent_session_id),
                optional_uuid_string(session.root_session_id),
                optional_uuid_string(session.capture_source_id),
                session.provider.as_str(),
                session.external_session_id.as_deref(),
                session.external_agent_id.as_deref(),
                session.agent_type.as_str(),
                session.role_hint.as_deref(),
                session.is_primary as i64,
                session.status.as_str(),
                session.sync.fidelity.as_str(),
                optional_uuid_string(session.transcript_blob_id),
                timestamp_ms(session.started_at),
                optional_timestamp_ms(session.ended_at),
                timestamp_ms(session.timestamps.created_at),
                timestamp_ms(session.timestamps.updated_at),
                session.sync.visibility.as_str(),
                session.sync.sync_state.as_str(),
                session.sync.sync_version as i64,
                optional_timestamp_ms(session.sync.deleted_at),
                serde_json::to_string(&session.sync.metadata)?,
            ],
        )?;
        Ok(())
    }

    pub fn get_session(&self, id: Uuid) -> Result<Session> {
        self.conn
            .query_row(
                session_select_sql("WHERE id = ?1").as_str(),
                params![id.to_string()],
                session_from_row,
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))
    }

    pub fn resolve_session_by_id_prefix(
        &self,
        prefix: &CtxIdPrefix,
    ) -> Result<IdPrefixResolution<Session>> {
        match self.resolve_id_prefix("sessions", prefix)? {
            IdPrefixResolution::Found(id) => self.get_session(id).map(IdPrefixResolution::Found),
            IdPrefixResolution::NotFound => Ok(IdPrefixResolution::NotFound),
            IdPrefixResolution::Ambiguous(ambiguity) => {
                Ok(IdPrefixResolution::Ambiguous(ambiguity))
            }
        }
    }

    pub fn session_by_external_session(
        &self,
        provider: CaptureProvider,
        external_session_id: &str,
    ) -> Result<Option<Session>> {
        self.conn
            .query_row(
                session_select_sql(
                    "WHERE provider = ?1 AND external_session_id = ?2 ORDER BY started_at_ms DESC LIMIT 1",
                )
                .as_str(),
                params![provider.as_str(), external_session_id],
                session_from_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn sessions_by_external_session(
        &self,
        provider: CaptureProvider,
        external_session_id: &str,
        limit: usize,
    ) -> Result<Vec<Session>> {
        // Saturate instead of `as`-casting: usize::MAX would wrap to -1,
        // which SQLite treats as "no limit".
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            session_select_sql(
                "WHERE provider = ?1 AND external_session_id = ?2 ORDER BY started_at_ms DESC, id LIMIT ?3",
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(
            params![provider.as_str(), external_session_id, limit],
            session_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn existing_external_session_ids(
        &self,
        provider: CaptureProvider,
        external_session_ids: &[String],
    ) -> Result<HashSet<String>> {
        const CHUNK_SIZE: usize = 500;
        let mut distinct = external_session_ids
            .iter()
            .filter(|id| !id.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        distinct.sort();
        distinct.dedup();
        let mut existing = HashSet::new();
        for chunk in distinct.chunks(CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                "SELECT external_session_id FROM sessions WHERE provider = ? AND external_session_id IN ({placeholders})"
            );
            let provider_name = provider.as_str();
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() + 1);
            params.push(&provider_name);
            for id in chunk {
                params.push(id);
            }
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params.as_slice(), |row| row.get::<_, String>(0))?;
            for row in rows {
                existing.insert(row?);
            }
        }
        Ok(existing)
    }

    pub fn sessions_for_record(&self, record_id: Uuid) -> Result<Vec<Session>> {
        let mut stmt = self.conn.prepare(
            session_select_sql("WHERE history_record_id = ?1 ORDER BY started_at_ms, id").as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], session_from_row)?;
        collect_rows(rows)
    }

    pub fn sessions_for_records(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, Vec<Session>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.relations_for_records(
            ids,
            session_select_sql(""),
            "FROM sessions",
            "sessions.history_record_id = requested.record_id",
            "sessions.started_at_ms, sessions.id",
            23,
            session_from_row,
            &[],
        )
    }

    pub fn search_sessions_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchSessionRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        self.relations_for_records(
            ids,
            search_session_select_sql(""),
            "FROM sessions",
            "sessions.history_record_id = requested.record_id",
            "sessions.started_at_ms, sessions.id",
            14,
            search_session_from_row,
            &[],
        )
    }

    pub fn assign_session_to_record(&self, session_id: Uuid, record_id: Uuid) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET history_record_id = ?1 WHERE id = ?2",
            params![record_id.to_string(), session_id.to_string()],
        )?;
        self.conn.execute(
            "UPDATE events SET history_record_id = ?1 WHERE session_id = ?2",
            params![record_id.to_string(), session_id.to_string()],
        )?;
        self.conn.execute(
            "UPDATE runs SET history_record_id = ?1 WHERE session_id = ?2",
            params![record_id.to_string(), session_id.to_string()],
        )?;
        Ok(())
    }

    pub fn list_sessions(&self) -> Result<Vec<Session>> {
        let mut stmt = self
            .conn
            .prepare(session_select_sql("ORDER BY started_at_ms, id").as_str())?;
        let rows = stmt.query_map([], session_from_row)?;
        collect_rows(rows)
    }

    pub fn indexed_history_item_count(&self) -> Result<usize> {
        Ok(self.indexed_history_counts()?.items())
    }

    pub fn indexed_history_counts(&self) -> Result<IndexedHistoryCounts> {
        let sessions: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))?;
        let events: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
        Ok(IndexedHistoryCounts {
            sessions: sessions as usize,
            events: events as usize,
        })
    }

    pub fn upsert_session_edge(&self, edge: &SessionEdge) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO session_edges
            (id, from_session_id, to_session_id, edge_type, confidence, source_id, created_at_ms, updated_at_ms, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ON CONFLICT(id) DO UPDATE SET
                from_session_id = excluded.from_session_id,
                to_session_id = excluded.to_session_id,
                edge_type = excluded.edge_type,
                confidence = excluded.confidence,
                source_id = excluded.source_id,
                updated_at_ms = excluded.updated_at_ms,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                edge.id.to_string(),
                edge.from_session_id.to_string(),
                edge.to_session_id.to_string(),
                edge.edge_type.as_str(),
                edge.confidence.as_str(),
                optional_uuid_string(edge.source_id),
                timestamp_ms(edge.timestamps.created_at),
                timestamp_ms(edge.timestamps.updated_at),
                edge.sync.visibility.as_str(),
                edge.sync.fidelity.as_str(),
                edge.sync.sync_state.as_str(),
                edge.sync.sync_version as i64,
                optional_timestamp_ms(edge.sync.deleted_at),
                serde_json::to_string(&edge.sync.metadata)?,
            ],
        )?;
        Ok(())
    }

    pub fn session_edge_exists(&self, edge_id: Uuid) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM session_edges WHERE id = ?1",
                params![edge_id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn upsert_run(&self, run: &Run) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO runs
            (id, history_record_id, session_id, run_type, status, started_at_ms, ended_at_ms, exit_code, cwd, command_preview, input_blob_id, output_blob_id, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
            ON CONFLICT(id) DO UPDATE SET
                history_record_id = excluded.history_record_id,
                session_id = excluded.session_id,
                run_type = excluded.run_type,
                status = excluded.status,
                started_at_ms = excluded.started_at_ms,
                ended_at_ms = excluded.ended_at_ms,
                exit_code = excluded.exit_code,
                cwd = excluded.cwd,
                command_preview = excluded.command_preview,
                input_blob_id = excluded.input_blob_id,
                output_blob_id = excluded.output_blob_id,
                updated_at_ms = excluded.updated_at_ms,
                source_id = excluded.source_id,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                run.id.to_string(),
                optional_uuid_string(run.history_record_id),
                optional_uuid_string(run.session_id),
                run.run_type.as_str(),
                run.status.as_str(),
                timestamp_ms(run.started_at),
                optional_timestamp_ms(run.ended_at),
                run.exit_code,
                run.cwd.as_deref(),
                run.command_preview.as_deref(),
                optional_uuid_string(run.input_blob_id),
                optional_uuid_string(run.output_blob_id),
                timestamp_ms(run.timestamps.created_at),
                timestamp_ms(run.timestamps.updated_at),
                optional_uuid_string(run.source_id),
                run.sync.visibility.as_str(),
                run.sync.fidelity.as_str(),
                run.sync.sync_state.as_str(),
                run.sync.sync_version as i64,
                optional_timestamp_ms(run.sync.deleted_at),
                serde_json::to_string(&run.sync.metadata)?,
            ],
        )?;
        Ok(())
    }

    pub fn insert_run_if_absent(&self, run: &Run) -> Result<bool> {
        let changed = self
            .conn
            .prepare_cached(
                r#"
                INSERT OR IGNORE INTO runs
                (id, history_record_id, session_id, run_type, status, started_at_ms, ended_at_ms, exit_code, cwd, command_preview, input_blob_id, output_blob_id, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
                "#,
            )?
            .execute(params![
                run.id.to_string(),
                optional_uuid_string(run.history_record_id),
                optional_uuid_string(run.session_id),
                run.run_type.as_str(),
                run.status.as_str(),
                timestamp_ms(run.started_at),
                optional_timestamp_ms(run.ended_at),
                run.exit_code,
                run.cwd.as_deref(),
                run.command_preview.as_deref(),
                optional_uuid_string(run.input_blob_id),
                optional_uuid_string(run.output_blob_id),
                timestamp_ms(run.timestamps.created_at),
                timestamp_ms(run.timestamps.updated_at),
                optional_uuid_string(run.source_id),
                run.sync.visibility.as_str(),
                run.sync.fidelity.as_str(),
                run.sync.sync_state.as_str(),
                run.sync.sync_version as i64,
                optional_timestamp_ms(run.sync.deleted_at),
                serde_json::to_string(&run.sync.metadata)?,
            ])?;
        Ok(changed > 0)
    }

    pub fn get_run(&self, id: Uuid) -> Result<Run> {
        self.conn
            .query_row(
                run_select_sql("WHERE id = ?1").as_str(),
                params![id.to_string()],
                run_from_row,
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))
    }

    pub fn runs_for_session(&self, session_id: Uuid) -> Result<Vec<Run>> {
        let mut stmt = self
            .conn
            .prepare(run_select_sql("WHERE session_id = ?1 ORDER BY started_at_ms, id").as_str())?;
        let rows = stmt.query_map(params![session_id.to_string()], run_from_row)?;
        collect_rows(rows)
    }

    pub fn runs_for_record(&self, record_id: Uuid) -> Result<Vec<Run>> {
        let mut stmt = self.conn.prepare(
            run_select_sql(
                r#"
                WHERE history_record_id = ?1
                   OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                ORDER BY started_at_ms, id
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], run_from_row)?;
        collect_rows(rows)
    }

    pub fn runs_for_records(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, Vec<Run>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.relations_for_records(ids, run_select_sql(""), "FROM runs", "runs.history_record_id = requested.record_id OR runs.session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)", "runs.started_at_ms, runs.id", 21, run_from_row, &[])
    }

    pub fn search_runs_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchRunRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        self.relations_for_records(ids, search_run_select_sql(""), "FROM runs", "runs.history_record_id = requested.record_id OR runs.session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)", "runs.started_at_ms, runs.id", 9, search_run_from_row, &[])
    }

    fn list_runs(&self) -> Result<Vec<Run>> {
        let mut stmt = self
            .conn
            .prepare(run_select_sql("ORDER BY started_at_ms, id").as_str())?;
        let rows = stmt.query_map([], run_from_row)?;
        collect_rows(rows)
    }

    pub fn provider_event_dedupe_key(
        provider: CaptureProvider,
        external_session_id: &str,
        provider_index: u64,
        payload_hash: &str,
    ) -> String {
        format!(
            "provider:{}:{}:{}:{}",
            provider.as_str(),
            external_session_id,
            provider_index,
            payload_hash
        )
    }

    pub fn upsert_event(&self, event: &Event) -> Result<Uuid> {
        // Dedupe probe, id-existence probe, base upsert, and projection
        // maintenance all run as one atomic write unit: the write
        // transaction takes the write lock before the probes in autocommit
        // mode and nests inside existing capture-harness batches. An event
        // id proven absent by the indexed primary-key probe takes the
        // insert-only projection path (skipping the full-scan FTS DELETE);
        // an existing id keeps the delete + insert path, which also removes
        // the projection row when the new preview is blank.
        with_write_transaction(&self.conn, "upsert_event", || {
            let event_id = if let Some(dedupe_key) = &event.dedupe_key {
                reject_provider_event_hash_conflict(&self.conn, dedupe_key)?;
                if let Some(existing_id) = self
                    .conn
                    .query_row(
                        "SELECT id FROM events WHERE dedupe_key = ?1",
                        params![dedupe_key],
                        |row| parse_uuid(row.get::<_, String>(0)?),
                    )
                    .optional()?
                {
                    return Ok(existing_id);
                }
                event.id
            } else {
                event.id
            };

            let existed = event_row_exists(&self.conn, event_id)?;
            self.conn.execute(
                r#"
                INSERT INTO events
                (id, seq, history_record_id, session_id, run_id, event_type, role, occurred_at_ms, capture_source_id, payload_json, payload_blob_id, dedupe_key, visibility, redaction_state, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                ON CONFLICT(id) DO UPDATE SET
                    seq = excluded.seq,
                    history_record_id = excluded.history_record_id,
                    session_id = excluded.session_id,
                    run_id = excluded.run_id,
                    event_type = excluded.event_type,
                    role = excluded.role,
                    occurred_at_ms = excluded.occurred_at_ms,
                    capture_source_id = excluded.capture_source_id,
                    payload_json = excluded.payload_json,
                    payload_blob_id = excluded.payload_blob_id,
                    dedupe_key = excluded.dedupe_key,
                    visibility = excluded.visibility,
                    redaction_state = excluded.redaction_state,
                    fidelity = excluded.fidelity,
                    sync_state = excluded.sync_state,
                    sync_version = excluded.sync_version,
                    deleted_at_ms = excluded.deleted_at_ms,
                    metadata_json = excluded.metadata_json
                "#,
                params![
                    event_id.to_string(),
                    event.seq as i64,
                    optional_uuid_string(event.history_record_id),
                    optional_uuid_string(event.session_id),
                    optional_uuid_string(event.run_id),
                    event.event_type.as_str(),
                    event.role.map(|role| role.as_str()),
                    timestamp_ms(event.occurred_at),
                    optional_uuid_string(event.capture_source_id),
                    serde_json::to_string(&event.payload)?,
                    optional_uuid_string(event.payload_blob_id),
                    event.dedupe_key.as_deref(),
                    event.sync.visibility.as_str(),
                    event.redaction_state.as_str(),
                    event.sync.fidelity.as_str(),
                    event.sync.sync_state.as_str(),
                    event.sync.sync_version as i64,
                    optional_timestamp_ms(event.sync.deleted_at),
                    serde_json::to_string(&event.sync.metadata)?,
                ],
            )?;
            if existed {
                upsert_event_search_projection_for_event(&self.conn, event_id, event)?;
            } else {
                insert_event_search_projection_for_event_id(&self.conn, event_id, event)?;
            }
            if let Some(dedupe_key) = &event.dedupe_key {
                return self.event_id_by_dedupe_key(dedupe_key);
            }
            Ok(event_id)
        })
    }

    pub fn insert_event_if_absent(&self, event: &Event) -> Result<bool> {
        // Same insert-only projection contract as before; the shared write
        // transaction additionally makes base insert + projection atomic
        // without changing the return value or dedupe semantics.
        with_write_transaction(&self.conn, "insert_event_if_absent", || {
            let changed = self
                .conn
                .prepare_cached(
                    r#"
                    INSERT OR IGNORE INTO events
                    (id, seq, history_record_id, session_id, run_id, event_type, role, occurred_at_ms, capture_source_id, payload_json, payload_blob_id, dedupe_key, visibility, redaction_state, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                    "#,
                )?
                .execute(params![
                    event.id.to_string(),
                    event.seq as i64,
                    optional_uuid_string(event.history_record_id),
                    optional_uuid_string(event.session_id),
                    optional_uuid_string(event.run_id),
                    event.event_type.as_str(),
                    event.role.map(|role| role.as_str()),
                    timestamp_ms(event.occurred_at),
                    optional_uuid_string(event.capture_source_id),
                    serde_json::to_string(&event.payload)?,
                    optional_uuid_string(event.payload_blob_id),
                    event.dedupe_key.as_deref(),
                    event.sync.visibility.as_str(),
                    event.redaction_state.as_str(),
                    event.sync.fidelity.as_str(),
                    event.sync.sync_state.as_str(),
                    event.sync.sync_version as i64,
                    optional_timestamp_ms(event.sync.deleted_at),
                    serde_json::to_string(&event.sync.metadata)?,
                ])?;
            if changed == 0 {
                if let Some(dedupe_key) = &event.dedupe_key {
                    reject_provider_event_hash_conflict(&self.conn, dedupe_key)?;
                }
            }
            if changed > 0 {
                insert_event_search_projection_for_event(&self.conn, event)?;
            }
            Ok(changed > 0)
        })
    }

    pub fn event_id_by_dedupe_key(&self, dedupe_key: &str) -> Result<Uuid> {
        self.conn
            .query_row(
                "SELECT id FROM events WHERE dedupe_key = ?1",
                params![dedupe_key],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .map_err(StoreError::from)
    }

    pub fn get_event(&self, id: Uuid) -> Result<Event> {
        self.conn
            .query_row(
                event_select_sql("WHERE id = ?1").as_str(),
                params![id.to_string()],
                event_from_row,
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))
    }

    pub fn resolve_event_by_id_prefix(
        &self,
        prefix: &CtxIdPrefix,
    ) -> Result<IdPrefixResolution<Event>> {
        match self.resolve_id_prefix("events", prefix)? {
            IdPrefixResolution::Found(id) => self.get_event(id).map(IdPrefixResolution::Found),
            IdPrefixResolution::NotFound => Ok(IdPrefixResolution::NotFound),
            IdPrefixResolution::Ambiguous(ambiguity) => {
                Ok(IdPrefixResolution::Ambiguous(ambiguity))
            }
        }
    }

    pub fn events_for_session(&self, session_id: Uuid) -> Result<Vec<Event>> {
        let mut stmt = self.conn.prepare(
            event_select_sql("WHERE session_id = ?1 ORDER BY seq, occurred_at_ms").as_str(),
        )?;
        let rows = stmt.query_map(params![session_id.to_string()], event_from_row)?;
        collect_rows(rows)
    }

    pub fn event_count_for_session(&self, session_id: Uuid) -> Result<usize> {
        self.selected_event_count_for_session(session_id, SelectedEventMode::Log)
    }

    pub fn events_for_session_after(
        &self,
        session_id: Uuid,
        after: Option<(u64, Uuid)>,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.selected_events_for_session_after(session_id, SelectedEventMode::Log, after, limit)
    }

    /// Count exactly the events selected by `mode`.
    pub fn selected_event_count_for_session(
        &self,
        session_id: Uuid,
        mode: SelectedEventMode,
    ) -> Result<usize> {
        let sql = format!(
            "SELECT COUNT(*) FROM events AS e WHERE e.session_id = ?1 AND ({})",
            selected_event_predicate(mode)
        );
        let count = self
            .conn
            .query_row(&sql, params![session_id.to_string()], |row| {
                row.get::<_, i64>(0)
            })?;
        usize::try_from(count).map_err(|_| StoreError::NumericOutOfRange {
            field: "selected event count",
        })
    }

    /// Read a bounded keyset page over the events selected by `mode`.
    ///
    /// Selection is applied before `LIMIT`, so pages are over selected
    /// transcript records rather than raw event rows.
    pub fn selected_events_for_session_after(
        &self,
        session_id: Uuid,
        mode: SelectedEventMode,
        after: Option<(u64, Uuid)>,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let limit = i64::try_from(limit.min(MAX_BOUNDED_EVENT_READ)).map_err(|_| {
            StoreError::NumericOutOfRange {
                field: "event page limit",
            }
        })?;
        let (seq, id) = match after {
            Some((seq, id)) => (
                i64::try_from(seq).map_err(|_| StoreError::NumericOutOfRange {
                    field: "event cursor sequence",
                })?,
                id.to_string(),
            ),
            None => (-1, String::new()),
        };
        let tail = format!(
            "AS e WHERE e.session_id = ?1 AND ({}) AND (e.seq, e.id) > (?2, ?3) ORDER BY e.seq, e.id LIMIT ?4",
            selected_event_predicate(mode)
        );
        let mut stmt = self.conn.prepare(event_select_sql(&tail).as_str())?;
        let rows = stmt.query_map(
            params![session_id.to_string(), seq, id, limit],
            event_from_row,
        )?;
        collect_rows(rows)
    }

    /// Return the zero-based selected position for an exact event cursor.
    ///
    /// Uses the same session/mode selection predicate and `(seq, id)` ordering
    /// as `selected_events_for_session_after`, so callers can validate cursor
    /// semantics without duplicating store-layer transcript selection rules.
    pub fn selected_event_cursor_position(
        &self,
        session_id: Uuid,
        mode: SelectedEventMode,
        cursor: (u64, Uuid),
    ) -> Result<Option<usize>> {
        let seq = i64::try_from(cursor.0).map_err(|_| StoreError::NumericOutOfRange {
            field: "event cursor sequence",
        })?;
        let id = cursor.1.to_string();
        let selected = selected_event_predicate(mode);
        let sql = format!(
            r#"
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1 FROM events AS e
                    WHERE e.session_id = ?1
                      AND e.seq = ?2
                      AND e.id = ?3
                      AND ({selected})
                ) THEN (
                    SELECT COUNT(*) FROM events AS e
                    WHERE e.session_id = ?1
                      AND ({selected})
                      AND (e.seq, e.id) < (?2, ?3)
                )
                ELSE NULL
            END
            "#
        );
        let position: Option<i64> =
            self.conn
                .query_row(&sql, params![session_id.to_string(), seq, id], |row| {
                    row.get(0)
                })?;
        position
            .map(|position| {
                usize::try_from(position).map_err(|_| StoreError::NumericOutOfRange {
                    field: "selected event cursor position",
                })
            })
            .transpose()
    }

    pub fn event_window_bounded(
        &self,
        event_id: Uuid,
        before: usize,
        after: usize,
    ) -> Result<Vec<Event>> {
        let event = self.get_event(event_id)?;
        let Some(session_id) = event.session_id else {
            return Ok(vec![event]);
        };
        let before = i64::try_from(before.min(MAX_BOUNDED_EVENT_READ)).map_err(|_| {
            StoreError::NumericOutOfRange {
                field: "event window before",
            }
        })?;
        let after = i64::try_from(after.min(MAX_BOUNDED_EVENT_READ)).map_err(|_| {
            StoreError::NumericOutOfRange {
                field: "event window after",
            }
        })?;
        let event_seq = i64::try_from(event.seq).map_err(|_| StoreError::NumericOutOfRange {
            field: "event sequence",
        })?;
        let mut before_stmt = self.conn.prepare(
            event_select_sql(
                "WHERE session_id = ?1 AND (seq, id) < (?2, ?3) ORDER BY seq DESC, id DESC LIMIT ?4",
            )
            .as_str(),
        )?;
        let before_rows = before_stmt.query_map(
            params![
                session_id.to_string(),
                event_seq,
                event.id.to_string(),
                before
            ],
            event_from_row,
        )?;
        let mut events = collect_rows(before_rows)?;
        events.reverse();
        events.push(event.clone());
        let mut after_stmt = self.conn.prepare(
            event_select_sql(
                "WHERE session_id = ?1 AND (seq, id) > (?2, ?3) ORDER BY seq, id LIMIT ?4",
            )
            .as_str(),
        )?;
        let after_rows = after_stmt.query_map(
            params![
                session_id.to_string(),
                event_seq,
                event.id.to_string(),
                after
            ],
            event_from_row,
        )?;
        events.extend(collect_rows(after_rows)?);
        Ok(events)
    }

    pub fn snapshot_fingerprint(&self) -> Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(b"ctx-store-physical-snapshot-v1\0");
        for pragma in [
            "user_version",
            "schema_version",
            "data_version",
            "page_count",
            "freelist_count",
        ] {
            let value: i64 = self
                .conn
                .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))?;
            hash_tagged_bytes(&mut hasher, pragma.as_bytes());
            hasher.update(value.to_be_bytes());
        }
        fingerprint_file_stable(&self.path, b"main", &mut hasher)?;
        fingerprint_file_stable(&self.wal_path(), b"wal", &mut hasher)?;
        let mut shm_path = self.path.as_os_str().to_os_string();
        shm_path.push("-shm");
        fingerprint_shm_stable(Path::new(&shm_path), &mut hasher)?;
        Ok(hex_digest(hasher.finalize().as_slice()))
    }

    pub fn events_for_record(&self, record_id: Uuid) -> Result<Vec<Event>> {
        let mut stmt = self.conn.prepare(
            event_select_sql(
                r#"
                WHERE history_record_id = ?1
                   OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                   OR run_id IN (
                        SELECT id FROM runs
                        WHERE history_record_id = ?1
                           OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                   )
                ORDER BY seq, occurred_at_ms
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], event_from_row)?;
        collect_rows(rows)
    }

    pub fn events_for_records(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, Vec<Event>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.relations_for_records(ids, event_select_sql(""), "FROM events", "events.history_record_id = requested.record_id OR events.session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id) OR events.run_id IN (SELECT id FROM runs WHERE history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id))", "events.seq, events.occurred_at_ms", 19, event_from_row, &[])
    }

    pub fn search_events_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchEventRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        self.relations_for_records(ids, search_event_select_sql(""), "FROM events", "events.history_record_id = requested.record_id OR events.session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id) OR events.run_id IN (SELECT id FROM runs WHERE history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id))", "events.seq, events.occurred_at_ms", 12, search_event_from_row, &[])
    }

    fn list_events(&self) -> Result<Vec<Event>> {
        let mut stmt = self
            .conn
            .prepare(event_select_sql("ORDER BY seq, occurred_at_ms, id").as_str())?;
        let rows = stmt.query_map([], event_from_row)?;
        collect_rows(rows)
    }

    pub fn upsert_artifact(&self, artifact: &Artifact) -> Result<Uuid> {
        self.conn.execute(
            r#"
            INSERT INTO artifacts
            (id, kind, blob_hash, blob_path, byte_size, media_type, preview_text, redaction_state, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
            ON CONFLICT DO UPDATE SET
                blob_path = excluded.blob_path,
                byte_size = excluded.byte_size,
                media_type = excluded.media_type,
                preview_text = excluded.preview_text,
                redaction_state = excluded.redaction_state,
                updated_at_ms = excluded.updated_at_ms,
                source_id = excluded.source_id,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                artifact.id.to_string(),
                artifact.kind.as_str(),
                artifact.blob_hash.as_str(),
                artifact.blob_path.as_str(),
                artifact.byte_size as i64,
                artifact.media_type.as_deref(),
                artifact.preview_text.as_deref(),
                artifact.redaction_state.as_str(),
                timestamp_ms(artifact.timestamps.created_at),
                timestamp_ms(artifact.timestamps.updated_at),
                optional_uuid_string(artifact.source_id),
                artifact.sync.visibility.as_str(),
                artifact.sync.fidelity.as_str(),
                artifact.sync.sync_state.as_str(),
                artifact.sync.sync_version as i64,
                optional_timestamp_ms(artifact.sync.deleted_at),
                serde_json::to_string(&artifact.sync.metadata)?,
            ],
        )?;
        self.conn
            .query_row(
                "SELECT id FROM artifacts WHERE blob_hash = ?1 AND kind = ?2",
                params![artifact.blob_hash.as_str(), artifact.kind.as_str()],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .map_err(StoreError::from)
    }

    fn list_artifacts(&self) -> Result<Vec<Artifact>> {
        let mut stmt = self
            .conn
            .prepare(artifact_select_sql("ORDER BY updated_at_ms, id").as_str())?;
        let rows = stmt.query_map([], artifact_from_row)?;
        collect_rows(rows)
    }

    pub fn upsert_vcs_workspace(&self, workspace: &VcsWorkspace) -> Result<Uuid> {
        self.conn.execute(
            r#"
            INSERT INTO vcs_workspaces
            (id, kind, root_path, repo_fingerprint, primary_remote_url_normalized, host, owner, name, monorepo_subpath, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
            ON CONFLICT(kind, repo_fingerprint) DO UPDATE SET
                root_path = excluded.root_path,
                primary_remote_url_normalized = excluded.primary_remote_url_normalized,
                host = excluded.host,
                owner = excluded.owner,
                name = excluded.name,
                monorepo_subpath = excluded.monorepo_subpath,
                updated_at_ms = excluded.updated_at_ms,
                source_id = excluded.source_id,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                workspace.id.to_string(),
                workspace.kind.as_str(),
                workspace.root_path.as_str(),
                workspace.repo_fingerprint.as_str(),
                workspace.primary_remote_url_normalized.as_deref(),
                workspace.host.as_str(),
                workspace.owner.as_deref(),
                workspace.name.as_deref(),
                workspace.monorepo_subpath.as_deref(),
                timestamp_ms(workspace.timestamps.created_at),
                timestamp_ms(workspace.timestamps.updated_at),
                optional_uuid_string(workspace.source_id),
                workspace.sync.visibility.as_str(),
                workspace.sync.fidelity.as_str(),
                workspace.sync.sync_state.as_str(),
                workspace.sync.sync_version as i64,
                optional_timestamp_ms(workspace.sync.deleted_at),
                serde_json::to_string(&workspace.sync.metadata)?,
            ],
        )?;
        self.conn
            .query_row(
                "SELECT id FROM vcs_workspaces WHERE kind = ?1 AND repo_fingerprint = ?2",
                params![workspace.kind.as_str(), workspace.repo_fingerprint.as_str()],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .map_err(StoreError::from)
    }

    pub fn get_or_create_local_device(&self) -> Result<LocalDeviceIdentity> {
        if let Some(device) = self.local_device()? {
            return Ok(device);
        }
        let now = utc_now();
        let device = LocalDeviceIdentity {
            id: new_id(),
            stable_device_id: format!("ctx-device-{}", new_id().simple()),
            created_at: now,
            updated_at: now,
        };
        self.conn.execute(
            r#"
            INSERT INTO local_devices
            (id, stable_device_id, created_at_ms, updated_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?3, '{}')
            "#,
            params![
                device.id.to_string(),
                device.stable_device_id.as_str(),
                timestamp_ms(now),
            ],
        )?;
        Ok(device)
    }

    pub fn register_local_workspace(
        &self,
        root_path: impl AsRef<Path>,
        repo_fingerprint: &str,
        vcs_workspace_id: Option<Uuid>,
    ) -> Result<LocalWorkspaceIdentity> {
        let device = self.get_or_create_local_device()?;
        let root = root_path.as_ref();
        let root_path_hash = sha256_hex(root.display().to_string().as_bytes());
        let display_root = root.display().to_string();
        let now = utc_now();
        let id = new_id();
        self.conn.execute(
            r#"
            INSERT INTO local_workspaces
            (
                id, device_id, vcs_workspace_id, repo_fingerprint, root_path_hash,
                display_root, created_at_ms, updated_at_ms, metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, '{}')
            ON CONFLICT(device_id, repo_fingerprint, root_path_hash) DO UPDATE SET
                vcs_workspace_id = COALESCE(excluded.vcs_workspace_id, local_workspaces.vcs_workspace_id),
                display_root = excluded.display_root,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![
                id.to_string(),
                device.id.to_string(),
                optional_uuid_string(vcs_workspace_id),
                repo_fingerprint,
                root_path_hash,
                display_root,
                timestamp_ms(now),
            ],
        )?;
        self.conn
            .query_row(
                r#"
                SELECT id, device_id, vcs_workspace_id, repo_fingerprint, root_path_hash,
                       display_root, created_at_ms, updated_at_ms
                FROM local_workspaces
                WHERE device_id = ?1 AND repo_fingerprint = ?2 AND root_path_hash = ?3
                "#,
                params![device.id.to_string(), repo_fingerprint, root_path_hash],
                local_workspace_from_row,
            )
            .map_err(StoreError::from)
    }

    pub fn local_device(&self) -> Result<Option<LocalDeviceIdentity>> {
        self.conn
            .query_row(
                "SELECT id, stable_device_id, created_at_ms, updated_at_ms FROM local_devices ORDER BY created_at_ms, id LIMIT 1",
                [],
                local_device_from_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn list_vcs_workspaces(&self) -> Result<Vec<VcsWorkspace>> {
        let mut stmt = self
            .conn
            .prepare(vcs_workspace_select_sql("ORDER BY updated_at_ms, id").as_str())?;
        let rows = stmt.query_map([], vcs_workspace_from_row)?;
        collect_rows(rows)
    }

    pub fn upsert_vcs_change(&self, change: &VcsChange) -> Result<Uuid> {
        self.conn.execute(
            r#"
            INSERT INTO vcs_changes
            (id, vcs_workspace_id, kind, change_id, parent_change_ids_json, branch_or_bookmark, tree_hash, author_time_ms, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
            ON CONFLICT(vcs_workspace_id, kind, change_id) DO UPDATE SET
                parent_change_ids_json = excluded.parent_change_ids_json,
                branch_or_bookmark = excluded.branch_or_bookmark,
                tree_hash = excluded.tree_hash,
                author_time_ms = excluded.author_time_ms,
                confidence = excluded.confidence,
                updated_at_ms = excluded.updated_at_ms,
                source_id = excluded.source_id,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                change.id.to_string(),
                change.vcs_workspace_id.to_string(),
                change.kind.as_str(),
                change.change_id.as_str(),
                serde_json::to_string(&change.parent_change_ids)?,
                change.branch_or_bookmark.as_deref(),
                change.tree_hash.as_deref(),
                optional_timestamp_ms(change.author_time),
                change.confidence.as_str(),
                timestamp_ms(change.timestamps.created_at),
                timestamp_ms(change.timestamps.updated_at),
                optional_uuid_string(change.source_id),
                change.sync.visibility.as_str(),
                change.sync.fidelity.as_str(),
                change.sync.sync_state.as_str(),
                change.sync.sync_version as i64,
                optional_timestamp_ms(change.sync.deleted_at),
                serde_json::to_string(&change.sync.metadata)?,
            ],
        )?;
        self.conn
            .query_row(
                "SELECT id FROM vcs_changes WHERE vcs_workspace_id = ?1 AND kind = ?2 AND change_id = ?3",
                params![change.vcs_workspace_id.to_string(), change.kind.as_str(), change.change_id.as_str()],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .map_err(StoreError::from)
    }

    fn list_vcs_changes(&self) -> Result<Vec<VcsChange>> {
        let mut stmt = self
            .conn
            .prepare(vcs_change_select_sql("ORDER BY updated_at_ms, id").as_str())?;
        let rows = stmt.query_map([], vcs_change_from_row)?;
        collect_rows(rows)
    }

    pub fn upsert_summary(&self, summary: &Summary) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO summaries
            (id, history_record_id, session_id, kind, model_or_source, text, citations_json, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(id) DO UPDATE SET
                history_record_id = excluded.history_record_id,
                session_id = excluded.session_id,
                kind = excluded.kind,
                model_or_source = excluded.model_or_source,
                text = excluded.text,
                citations_json = excluded.citations_json,
                updated_at_ms = excluded.updated_at_ms,
                source_id = excluded.source_id,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                summary.id.to_string(),
                optional_uuid_string(summary.history_record_id),
                optional_uuid_string(summary.session_id),
                summary.kind.as_str(),
                summary.model_or_source.as_deref(),
                summary.text.as_str(),
                serde_json::to_string(&summary.citations)?,
                timestamp_ms(summary.timestamps.created_at),
                timestamp_ms(summary.timestamps.updated_at),
                optional_uuid_string(summary.source_id),
                summary.sync.visibility.as_str(),
                summary.sync.fidelity.as_str(),
                summary.sync.sync_state.as_str(),
                summary.sync.sync_version as i64,
                optional_timestamp_ms(summary.sync.deleted_at),
                serde_json::to_string(&summary.sync.metadata)?,
            ],
        )?;
        Ok(())
    }

    fn list_summaries(&self) -> Result<Vec<Summary>> {
        let mut stmt = self
            .conn
            .prepare(summary_select_sql("ORDER BY updated_at_ms, id").as_str())?;
        let rows = stmt.query_map([], summary_from_row)?;
        collect_rows(rows)
    }

    pub fn upsert_file_touched(&self, file: &FileTouched) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO files_touched
            (id, history_record_id, run_id, event_id, vcs_workspace_id, path, change_kind, old_path, line_count_delta, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
            ON CONFLICT(id) DO UPDATE SET
                history_record_id = excluded.history_record_id,
                run_id = excluded.run_id,
                event_id = excluded.event_id,
                vcs_workspace_id = excluded.vcs_workspace_id,
                path = excluded.path,
                change_kind = excluded.change_kind,
                old_path = excluded.old_path,
                line_count_delta = excluded.line_count_delta,
                confidence = excluded.confidence,
                updated_at_ms = excluded.updated_at_ms,
                source_id = excluded.source_id,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                file.id.to_string(),
                optional_uuid_string(file.history_record_id),
                optional_uuid_string(file.run_id),
                optional_uuid_string(file.event_id),
                optional_uuid_string(file.vcs_workspace_id),
                file.path.as_str(),
                file.change_kind.map(|kind| kind.as_str()),
                file.old_path.as_deref(),
                file.line_count_delta,
                file.confidence.as_str(),
                timestamp_ms(file.timestamps.created_at),
                timestamp_ms(file.timestamps.updated_at),
                optional_uuid_string(file.source_id),
                file.sync.visibility.as_str(),
                file.sync.fidelity.as_str(),
                file.sync.sync_state.as_str(),
                file.sync.sync_version as i64,
                optional_timestamp_ms(file.sync.deleted_at),
                serde_json::to_string(&file.sync.metadata)?,
            ],
        )?;
        Ok(())
    }

    fn list_files_touched(&self) -> Result<Vec<FileTouched>> {
        let mut stmt = self
            .conn
            .prepare(file_touched_select_sql("ORDER BY updated_at_ms, id").as_str())?;
        let rows = stmt.query_map([], file_touched_from_row)?;
        collect_rows(rows)
    }

    pub fn artifacts_for_record(&self, record_id: Uuid) -> Result<Vec<Artifact>> {
        let mut stmt = self.conn.prepare(
            artifact_select_sql(
                r#"
                WHERE id IN (
                    SELECT transcript_blob_id
                    FROM sessions
                    WHERE history_record_id = ?1 AND transcript_blob_id IS NOT NULL
                    UNION
                    SELECT input_blob_id
                    FROM runs
                    WHERE (history_record_id = ?1
                       OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1))
                       AND input_blob_id IS NOT NULL
                    UNION
                    SELECT output_blob_id
                    FROM runs
                    WHERE (history_record_id = ?1
                       OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1))
                       AND output_blob_id IS NOT NULL
                    UNION
                    SELECT payload_blob_id
                    FROM events
                    WHERE (history_record_id = ?1
                       OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1))
                       AND payload_blob_id IS NOT NULL
                    UNION
                    SELECT target_id
                    FROM history_record_links
                    WHERE history_record_id = ?1 AND target_type = 'artifact'
                )
                ORDER BY updated_at_ms DESC, id
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], artifact_from_row)?;
        collect_rows(rows)
    }

    pub fn artifacts_for_records(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, Vec<Artifact>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.relations_for_records(ids, artifact_select_sql(""), "FROM artifacts", r#"artifacts.id IN (
            SELECT transcript_blob_id FROM sessions WHERE history_record_id = requested.record_id AND transcript_blob_id IS NOT NULL
            UNION SELECT input_blob_id FROM runs WHERE (history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) AND input_blob_id IS NOT NULL
            UNION SELECT output_blob_id FROM runs WHERE (history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) AND output_blob_id IS NOT NULL
            UNION SELECT payload_blob_id FROM events WHERE (history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) AND payload_blob_id IS NOT NULL
            UNION SELECT target_id FROM history_record_links WHERE history_record_id = requested.record_id AND target_type = 'artifact')"#, "artifacts.updated_at_ms DESC, artifacts.id", 17, artifact_from_row, &[])
    }

    pub fn search_artifacts_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchArtifactRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        self.relations_for_records(ids, search_artifact_select_sql(""), "FROM artifacts", r#"artifacts.id IN (
            SELECT transcript_blob_id FROM sessions WHERE history_record_id = requested.record_id AND transcript_blob_id IS NOT NULL
            UNION SELECT input_blob_id FROM runs WHERE (history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) AND input_blob_id IS NOT NULL
            UNION SELECT output_blob_id FROM runs WHERE (history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) AND output_blob_id IS NOT NULL
            UNION SELECT payload_blob_id FROM events WHERE (history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) AND payload_blob_id IS NOT NULL
            UNION SELECT target_id FROM history_record_links WHERE history_record_id = requested.record_id AND target_type = 'artifact')"#, "artifacts.updated_at_ms DESC, artifacts.id", 7, search_artifact_from_row, &[])
    }

    pub fn vcs_changes_for_record(&self, record_id: Uuid) -> Result<Vec<VcsChange>> {
        let mut stmt = self.conn.prepare(
            vcs_change_select_sql(
                r#"
                WHERE id IN (
                    SELECT target_id
                    FROM history_record_links
                    WHERE history_record_id = ?1 AND target_type = 'vcs_change'
                )
                ORDER BY updated_at_ms DESC, id
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], vcs_change_from_row)?;
        collect_rows(rows)
    }

    pub fn vcs_changes_for_records(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, Vec<VcsChange>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.relations_for_records(ids, vcs_change_select_sql(""), "FROM vcs_changes", "vcs_changes.id IN (SELECT target_id FROM history_record_links WHERE history_record_id = requested.record_id AND target_type = 'vcs_change')", "vcs_changes.updated_at_ms DESC, vcs_changes.id", 18, vcs_change_from_row, &[])
    }

    pub fn search_vcs_changes_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchVcsChangeRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        self.relations_for_records(ids, search_vcs_change_select_sql(""), "FROM vcs_changes", "vcs_changes.id IN (SELECT target_id FROM history_record_links WHERE history_record_id = requested.record_id AND target_type = 'vcs_change')", "vcs_changes.updated_at_ms DESC, vcs_changes.id", 9, search_vcs_change_from_row, &[])
    }

    pub fn summaries_for_record(&self, record_id: Uuid) -> Result<Vec<Summary>> {
        let mut stmt = self.conn.prepare(
            summary_select_sql(
                r#"
                WHERE history_record_id = ?1
                   OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                ORDER BY updated_at_ms DESC, id
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], summary_from_row)?;
        collect_rows(rows)
    }

    pub fn summaries_for_records(&self, ids: &[Uuid]) -> Result<BTreeMap<Uuid, Vec<Summary>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.relations_for_records(ids, summary_select_sql(""), "FROM summaries", "summaries.history_record_id = requested.record_id OR summaries.session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)", "summaries.updated_at_ms DESC, summaries.id", 16, summary_from_row, &[])
    }

    pub fn search_summaries_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchSummaryRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        self.relations_for_records(ids, search_summary_select_sql(""), "FROM summaries", "summaries.history_record_id = requested.record_id OR summaries.session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)", "summaries.updated_at_ms DESC, summaries.id", 4, search_summary_from_row, &[])
    }

    pub fn files_touched_for_record(&self, record_id: Uuid) -> Result<Vec<FileTouched>> {
        let mut stmt = self.conn.prepare(
            file_touched_select_sql(
                r#"
                WHERE history_record_id = ?1
                   OR run_id IN (
                        SELECT id FROM runs
                        WHERE history_record_id = ?1
                           OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                   )
                   OR event_id IN (
                        SELECT id FROM events
                        WHERE history_record_id = ?1
                           OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                   )
                ORDER BY updated_at_ms DESC, id
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![record_id.to_string()], file_touched_from_row)?;
        collect_rows(rows)
    }

    pub fn files_touched_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<FileTouched>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        self.files_touched_for_records_inner(ids, None)
    }

    pub fn files_touched_for_records_matching(
        &self,
        ids: &[Uuid],
        file: &str,
    ) -> Result<BTreeMap<Uuid, Vec<FileTouched>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(false);
        let Some((exact, suffix)) = file_touch_match_values(file) else {
            return Ok(empty_relation_map(ids));
        };
        self.files_touched_for_records_inner(ids, Some((exact, suffix)))
    }

    pub fn search_files_touched_for_records(
        &self,
        ids: &[Uuid],
    ) -> Result<BTreeMap<Uuid, Vec<SearchFileTouchedRow>>> {
        self.search_files_touched_for_records_inner(ids, None)
    }

    pub fn search_files_touched_for_records_matching(
        &self,
        ids: &[Uuid],
        file: &str,
    ) -> Result<BTreeMap<Uuid, Vec<SearchFileTouchedRow>>> {
        let Some((exact, suffix)) = file_touch_match_values(file) else {
            return Ok(empty_relation_map(ids));
        };
        self.search_files_touched_for_records_inner(ids, Some((exact, suffix)))
    }

    fn search_files_touched_for_records_inner(
        &self,
        ids: &[Uuid],
        file: Option<(String, String)>,
    ) -> Result<BTreeMap<Uuid, Vec<SearchFileTouchedRow>>> {
        #[cfg(feature = "test-utils")]
        self.increment_search_hydration_loader(true);
        let mut extra = Vec::new();
        let file_predicate = if let Some((exact, suffix)) = file {
            extra = vec![
                SqlValue::Text(exact.clone()),
                SqlValue::Text(exact),
                SqlValue::Text(suffix.clone()),
                SqlValue::Text(suffix),
            ];
            " AND (files_touched.path = ? OR files_touched.old_path = ? OR files_touched.path LIKE ? ESCAPE '\\' OR files_touched.old_path LIKE ? ESCAPE '\\')"
        } else {
            ""
        };
        self.relations_for_records(ids, search_file_touched_select_sql(""), "FROM files_touched", &format!("(files_touched.history_record_id = requested.record_id OR files_touched.run_id IN (SELECT id FROM runs WHERE history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) OR files_touched.event_id IN (SELECT id FROM events WHERE history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id))){file_predicate}"), "files_touched.updated_at_ms DESC, files_touched.id", 7, search_file_touched_from_row, &extra)
    }

    fn files_touched_for_records_inner(
        &self,
        ids: &[Uuid],
        file: Option<(String, String)>,
    ) -> Result<BTreeMap<Uuid, Vec<FileTouched>>> {
        let mut extra = Vec::new();
        let file_predicate = if let Some((exact, suffix)) = file {
            extra = vec![
                SqlValue::Text(exact.clone()),
                SqlValue::Text(exact),
                SqlValue::Text(suffix.clone()),
                SqlValue::Text(suffix),
            ];
            " AND (files_touched.path = ? OR files_touched.old_path = ? OR files_touched.path LIKE ? ESCAPE '\\' OR files_touched.old_path LIKE ? ESCAPE '\\')"
        } else {
            ""
        };
        self.relations_for_records(ids, file_touched_select_sql(""), "FROM files_touched", &format!("(files_touched.history_record_id = requested.record_id OR files_touched.run_id IN (SELECT id FROM runs WHERE history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id)) OR files_touched.event_id IN (SELECT id FROM events WHERE history_record_id = requested.record_id OR session_id IN (SELECT id FROM sessions WHERE history_record_id = requested.record_id))){file_predicate}"), "files_touched.updated_at_ms DESC, files_touched.id", 19, file_touched_from_row, &extra)
    }

    #[allow(clippy::too_many_arguments)]
    fn relations_for_records<T>(
        &self,
        ids: &[Uuid],
        select: String,
        from: &str,
        predicate: &str,
        order: &str,
        columns: usize,
        parse: fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
        extra: &[SqlValue],
    ) -> Result<BTreeMap<Uuid, Vec<T>>> {
        let mut grouped = empty_relation_map(ids);
        for chunk in distinct_uuid_chunks(ids) {
            #[cfg(feature = "test-utils")]
            self.relation_batch_executions
                .set(self.relation_batch_executions.get().saturating_add(1));
            let requested = vec!["(?)"; chunk.len()].join(",");
            let projection = select.replacen(
                from,
                &format!(", requested.record_id {from} JOIN requested ON ({predicate})"),
                1,
            );
            let sql = format!("WITH requested(record_id) AS (VALUES {requested}) {projection} ORDER BY requested.record_id, {order}");
            let mut values = uuid_value_vec(&chunk);
            values.extend_from_slice(extra);
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), |row| {
                Ok((parse_uuid(row.get::<_, String>(columns)?)?, parse(row)?))
            })?;
            for row in rows {
                let (id, entity) = row?;
                grouped.entry(id).or_default().push(entity);
            }
        }
        Ok(grouped)
    }

    pub fn files_touched_for_record_matching(
        &self,
        record_id: Uuid,
        file: &str,
    ) -> Result<Vec<FileTouched>> {
        let Some((exact, suffix)) = file_touch_match_values(file) else {
            return Ok(Vec::new());
        };
        let mut stmt = self.conn.prepare(
            file_touched_select_sql(
                r#"
                WHERE (
                    history_record_id = ?1
                    OR run_id IN (
                         SELECT id FROM runs
                         WHERE history_record_id = ?1
                            OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                    )
                    OR event_id IN (
                         SELECT id FROM events
                         WHERE history_record_id = ?1
                            OR session_id IN (SELECT id FROM sessions WHERE history_record_id = ?1)
                    )
                )
                AND (
                    path = ?2
                    OR old_path = ?2
                    OR path LIKE ?3 ESCAPE '\'
                    OR old_path LIKE ?3 ESCAPE '\'
                )
                ORDER BY updated_at_ms DESC, id
                "#,
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(
            params![record_id.to_string(), exact, suffix],
            file_touched_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn file_touch_scope(&self, file: &str) -> Result<FileTouchScope> {
        let Some((exact, suffix)) = file_touch_match_values(file) else {
            return Ok(FileTouchScope::default());
        };
        let mut scope = FileTouchScope::default();
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                COALESCE(
                    ft.history_record_id,
                    e.history_record_id,
                    r.history_record_id,
                    event_session.history_record_id,
                    run_session.history_record_id,
                    source_session.history_record_id
                ),
                COALESCE(e.session_id, r.session_id, source_session.id),
                ft.run_id,
                ft.event_id,
                ft.source_id
            FROM files_touched ft
            LEFT JOIN events e ON e.id = ft.event_id
            LEFT JOIN runs r ON r.id = ft.run_id
            LEFT JOIN sessions event_session ON event_session.id = e.session_id
            LEFT JOIN sessions run_session ON run_session.id = r.session_id
            LEFT JOIN sessions source_session ON source_session.capture_source_id = ft.source_id
            WHERE ft.path = ?1
               OR ft.old_path = ?1
               OR ft.path LIKE ?2 ESCAPE '\'
               OR ft.old_path LIKE ?2 ESCAPE '\'
            "#,
        )?;
        let rows = stmt.query_map(params![exact, suffix], |row| {
            Ok((
                parse_optional_uuid(row.get(0)?)?,
                parse_optional_uuid(row.get(1)?)?,
                parse_optional_uuid(row.get(2)?)?,
                parse_optional_uuid(row.get(3)?)?,
                parse_optional_uuid(row.get(4)?)?,
            ))
        })?;
        for row in rows {
            let (record_id, session_id, run_id, event_id, source_id) = row?;
            if let Some(id) = record_id {
                scope.history_record_ids.insert(id);
            }
            if let Some(id) = session_id {
                scope.session_ids.insert(id);
            }
            if let Some(id) = run_id {
                scope.run_ids.insert(id);
            }
            if let Some(id) = event_id {
                scope.event_ids.insert(id);
            }
            if let Some(id) = source_id {
                scope.source_ids.insert(id);
            }
        }
        Ok(scope)
    }

    pub fn upsert_history_record_link(&self, link: &HistoryRecordLink) -> Result<Uuid> {
        self.conn.execute(
            r#"
            INSERT INTO history_record_links
            (id, history_record_id, target_type, target_id, link_type, confidence, source_id, created_at_ms, updated_at_ms, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(history_record_id, target_type, target_id, link_type) DO UPDATE SET
                confidence = excluded.confidence,
                source_id = excluded.source_id,
                updated_at_ms = excluded.updated_at_ms,
                visibility = excluded.visibility,
                fidelity = excluded.fidelity,
                sync_state = excluded.sync_state,
                sync_version = excluded.sync_version,
                deleted_at_ms = excluded.deleted_at_ms,
                metadata_json = excluded.metadata_json
            "#,
            params![
                link.id.to_string(),
                link.history_record_id.to_string(),
                link.target_type.as_str(),
                link.target_id.to_string(),
                link.link_type.as_str(),
                link.confidence.as_str(),
                optional_uuid_string(link.source_id),
                timestamp_ms(link.timestamps.created_at),
                timestamp_ms(link.timestamps.updated_at),
                link.sync.visibility.as_str(),
                link.sync.fidelity.as_str(),
                link.sync.sync_state.as_str(),
                link.sync.sync_version as i64,
                optional_timestamp_ms(link.sync.deleted_at),
                serde_json::to_string(&link.sync.metadata)?,
            ],
        )?;
        self.conn
            .query_row(
                "SELECT id FROM history_record_links WHERE history_record_id = ?1 AND target_type = ?2 AND target_id = ?3 AND link_type = ?4",
                params![
                    link.history_record_id.to_string(),
                    link.target_type.as_str(),
                    link.target_id.to_string(),
                    link.link_type.as_str()
                ],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .map_err(StoreError::from)
    }

    fn list_history_record_links(&self) -> Result<Vec<HistoryRecordLink>> {
        let mut stmt = self
            .conn
            .prepare(history_record_link_select_sql("ORDER BY updated_at_ms, id").as_str())?;
        let rows = stmt.query_map([], history_record_link_from_row)?;
        collect_rows(rows)
    }

    pub fn upsert_sync_cursor(&self, cursor: &SyncCursor) -> Result<Uuid> {
        if let Some(existing) =
            self.get_sync_cursor(cursor.team_id.as_deref(), &cursor.device_id, &cursor.stream)?
        {
            self.conn.execute(
                r#"
                UPDATE sync_cursors
                SET cursor = ?1, last_synced_at_ms = ?2, updated_at_ms = ?3
                WHERE id = ?4
                "#,
                params![
                    cursor.cursor.as_str(),
                    optional_timestamp_ms(cursor.last_synced_at),
                    timestamp_ms(cursor.timestamps.updated_at),
                    existing.id.to_string(),
                ],
            )?;
            return Ok(existing.id);
        }

        self.conn.execute(
            r#"
            INSERT INTO sync_cursors
            (id, team_id, device_id, stream, cursor, last_synced_at_ms, created_at_ms, updated_at_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(team_id, device_id, stream) DO UPDATE SET
                cursor = excluded.cursor,
                last_synced_at_ms = excluded.last_synced_at_ms,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![
                cursor.id.to_string(),
                cursor.team_id.as_deref(),
                cursor.device_id.as_str(),
                cursor.stream.as_str(),
                cursor.cursor.as_str(),
                optional_timestamp_ms(cursor.last_synced_at),
                timestamp_ms(cursor.timestamps.created_at),
                timestamp_ms(cursor.timestamps.updated_at),
            ],
        )?;
        self.conn
            .query_row(
                "SELECT id FROM sync_cursors WHERE team_id IS ?1 AND device_id = ?2 AND stream = ?3",
                params![cursor.team_id.as_deref(), cursor.device_id.as_str(), cursor.stream.as_str()],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .map_err(StoreError::from)
    }

    pub fn get_sync_cursor(
        &self,
        team_id: Option<&str>,
        device_id: &str,
        stream: &str,
    ) -> Result<Option<SyncCursor>> {
        self.conn
            .query_row(
                "SELECT id, team_id, device_id, stream, cursor, last_synced_at_ms, created_at_ms, updated_at_ms FROM sync_cursors WHERE team_id IS ?1 AND device_id = ?2 AND stream = ?3",
                params![team_id, device_id, stream],
                sync_cursor_from_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn insert_record(&self, record: &HistoryRecord) -> Result<()> {
        // The plain INSERT (no conflict clause) fails on a duplicate primary
        // key, so reaching the projection write proves the record id is new
        // and the insert-only projection can skip the full-scan FTS DELETE.
        // The write transaction makes base row + projection one atomic unit
        // both in autocommit mode and nested inside a caller batch.
        with_write_transaction(&self.conn, "insert_record", || {
            let created_at_ms = timestamp_ms(record.created_at);
            let updated_at_ms = timestamp_ms(record.updated_at);
            self.conn.execute(
                r#"
                INSERT INTO history_records
                (
                    id, title, summary, status, started_at_ms, last_activity_at_ms,
                    created_at_ms, updated_at_ms, body, tags_json, kind, workspace,
                    created_at, updated_at
                )
                VALUES (?1, ?2, ?3, 'open', ?4, ?5, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                "#,
                params![
                    record.id.to_string(),
                    record.title,
                    record.body,
                    created_at_ms,
                    updated_at_ms,
                    record.body,
                    serde_json::to_string(&record.tags)?,
                    record.kind,
                    record.workspace,
                    record.created_at.to_rfc3339(),
                    record.updated_at.to_rfc3339(),
                ],
            )?;
            insert_record_search_projection(&self.conn, record)
        })
    }

    pub fn upsert_record(&self, record: &HistoryRecord) -> Result<()> {
        // Probe, base upsert, and projection maintenance must see one
        // consistent snapshot and apply atomically: the write transaction
        // acquires the write lock before the probe in autocommit mode and
        // nests inside existing harness batches. A record proven absent by
        // the indexed primary-key probe takes the insert-only projection
        // path; an existing record keeps the delete + insert path so its
        // old projection row is replaced.
        with_write_transaction(&self.conn, "upsert_record", || {
            let existed = history_record_row_exists(&self.conn, record.id)?;
            self.upsert_record_row(record)?;
            if existed {
                upsert_record_search_projection(&self.conn, record)
            } else {
                insert_record_search_projection(&self.conn, record)
            }
        })
    }

    pub fn upsert_records(&self, records: &[HistoryRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Base rows and projection rows commit in the same immediate
        // transaction: any projection error rolls back the whole batch, and
        // readers never observe base rows without their search projections
        // (previously projections were written after the batch commit).
        // Per-row novelty comes from an indexed primary-key probe inside the
        // transaction, so an id repeated within one batch is new for its
        // first occurrence and existing for later ones.
        self.begin_immediate_batch()?;
        let body = (|| -> Result<()> {
            for record in records {
                let existed = history_record_row_exists(&self.conn, record.id)?;
                self.upsert_record_row(record)?;
                if existed {
                    upsert_record_search_projection(&self.conn, record)?;
                } else {
                    insert_record_search_projection(&self.conn, record)?;
                }
            }
            Ok(())
        })();
        if let Err(err) = body {
            let _ = self.rollback_batch();
            return Err(err);
        }
        if let Err(err) = self.commit_batch() {
            let _ = self.rollback_batch();
            return Err(err);
        }
        Ok(())
    }

    fn upsert_record_row(&self, record: &HistoryRecord) -> Result<()> {
        let created_at_ms = timestamp_ms(record.created_at);
        let updated_at_ms = timestamp_ms(record.updated_at);
        self.conn.execute(
            r#"
            INSERT INTO history_records
            (
                id, title, summary, status, started_at_ms, last_activity_at_ms,
                created_at_ms, updated_at_ms, body, tags_json, kind, workspace,
                created_at, updated_at
            )
            VALUES (?1, ?2, ?3, 'open', ?4, ?5, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                summary = excluded.summary,
                status = excluded.status,
                started_at_ms = excluded.started_at_ms,
                last_activity_at_ms = excluded.last_activity_at_ms,
                created_at_ms = excluded.created_at_ms,
                updated_at_ms = excluded.updated_at_ms,
                body = excluded.body,
                tags_json = excluded.tags_json,
                kind = excluded.kind,
                workspace = excluded.workspace,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
            params![
                record.id.to_string(),
                record.title,
                record.body,
                created_at_ms,
                updated_at_ms,
                record.body,
                serde_json::to_string(&record.tags)?,
                record.kind,
                record.workspace,
                record.created_at.to_rfc3339(),
                record.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_record(&self, id: Uuid) -> Result<HistoryRecord> {
        self.conn
            .query_row(
                record_select_sql("WHERE id = ?1").as_str(),
                params![id.to_string()],
                record_from_row,
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))
    }

    pub fn list_records(&self, limit: usize) -> Result<Vec<HistoryRecord>> {
        self.list_records_page(limit, 0)
    }

    pub fn list_records_page(&self, limit: usize, offset: usize) -> Result<Vec<HistoryRecord>> {
        self.record_list_page_executions
            .set(self.record_list_page_executions.get().saturating_add(1));
        let mut stmt = self.conn.prepare(
            record_select_sql("ORDER BY created_at DESC, id LIMIT ?1 OFFSET ?2").as_str(),
        )?;
        let rows = stmt.query_map(params![limit as i64, offset as i64], record_from_row)?;
        collect_rows(rows)
    }

    /// Internal, non-contractual test instrumentation: number of record list
    /// page statements executed by this store handle. Tests assert on this
    /// counter to prove fallback scans stay page-bounded; production callers
    /// must not depend on it as a public behavioral or telemetry contract.
    #[doc(hidden)]
    pub fn record_list_page_executions(&self) -> u64 {
        self.record_list_page_executions.get()
    }

    /// Store-handle-local test instrumentation for ranked record FTS statements.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn record_search_page_executions(&self) -> u64 {
        self.record_search_page_executions.get()
    }

    pub fn search_records(&self, query: &str, limit: usize) -> Result<Vec<HistoryRecord>> {
        self.search_records_page(query, limit, 0)
    }

    pub fn search_records_plan(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
    ) -> Result<Vec<HistoryRecord>> {
        self.search_records_plan_page(plan, limit, 0)
    }

    /// Plan-aware ranked record search. When the FTS projection exists this
    /// is a single indexed query. Without it (degraded store: the
    /// `ctx_history_search` table was dropped or never built), a bounded
    /// Rust-side fallback scans newest-first record pages and matches record
    /// sections literally. The fallback reads at most
    /// [`RECORD_FALLBACK_SCAN_MAX_PAGES`] pages of `max(limit * 20, 100)`
    /// records per call: within that window results and `offset` skipping are
    /// deterministic (records ordered by `created_at DESC, id`), but matches
    /// older than the window are missed. Repair via `refresh_search_index` /
    /// reindexing restores exact search.
    pub fn search_records_plan_page(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<HistoryRecord>> {
        if plan.fts_match_query().is_none() {
            return Ok(Vec::new());
        }
        if let Some(records) = self.search_records_fts_plan(plan, limit, offset)? {
            return Ok(records);
        }
        let like_terms = plan
            .clauses
            .iter()
            .flat_map(|clause| clause.terms.iter())
            .collect::<Vec<_>>();
        if like_terms.is_empty() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        let mut source_offset = 0usize;
        let mut matched_seen = 0usize;
        let mut pages_scanned = 0usize;
        let page_size = limit.saturating_mul(20).max(100);
        loop {
            pages_scanned = pages_scanned.saturating_add(1);
            let page = self.list_records_page(page_size, source_offset)?;
            let page_len = page.len();
            for record in page {
                if record_sections_match_plan(plan, &record) {
                    if matched_seen < offset {
                        matched_seen += 1;
                        continue;
                    }
                    records.push(record);
                    if records.len() >= limit {
                        return Ok(records);
                    }
                }
            }
            if page_len < page_size || pages_scanned >= RECORD_FALLBACK_SCAN_MAX_PAGES {
                break;
            }
            source_offset = source_offset.saturating_add(page_size);
        }
        Ok(records)
    }

    pub fn has_event_search_index(&self) -> Result<bool> {
        table_exists(&self.conn, "event_search")
    }

    pub fn search_records_page(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<HistoryRecord>> {
        let plan = SearchQueryPlan::new(SearchMatchMode::All, [query]);
        if plan.fts_match_query().is_none() {
            return Ok(Vec::new());
        }
        if let Some(records) = self.search_records_fts_plan(&plan, limit, offset)? {
            return Ok(records);
        }
        let like = format!("%{}%", query);
        let mut stmt = self.conn.prepare(
            record_select_sql(
                "WHERE title LIKE ?1 OR body LIKE ?1 OR tags_json LIKE ?1 ORDER BY created_at DESC, id LIMIT ?2 OFFSET ?3",
            )
            .as_str(),
        )?;
        let rows = stmt.query_map(params![like, limit as i64, offset as i64], record_from_row)?;
        collect_rows(rows)
    }

    fn search_records_fts_plan(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
        offset: usize,
    ) -> Result<Option<Vec<HistoryRecord>>> {
        let Some(hits) = self.search_record_hits_fts_plan(plan, limit, offset)? else {
            return Ok(None);
        };
        let mut records = Vec::with_capacity(hits.len());
        for hit in hits {
            records.push(self.get_record(hit.record_id)?);
        }
        Ok(Some(records))
    }

    /// Executes one ranked record FTS statement and returns its bounded ID
    /// materialization without hydrating records. `None` denotes a degraded
    /// store without the record FTS projection.
    pub fn search_record_hits_fts_plan(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
        offset: usize,
    ) -> Result<Option<Vec<RecordSearchHit>>> {
        if !table_exists(&self.conn, "ctx_history_search")? {
            return Ok(None);
        }
        let Some(match_query) = plan.fts_match_query() else {
            return Ok(Some(Vec::new()));
        };
        let has_event_search = table_exists(&self.conn, "event_search")?;
        let has_artifact_search = table_exists(&self.conn, "artifact_search")?;
        let sql = if has_event_search && has_artifact_search {
            r#"
            WITH matches(record_id, score) AS (
                SELECT record_id, bm25(ctx_history_search)
                FROM ctx_history_search
                WHERE ctx_history_search MATCH ?1
                UNION ALL
                SELECT history_record_id, bm25(event_search)
                FROM event_search
                WHERE event_search MATCH ?1 AND history_record_id IS NOT NULL
                UNION ALL
                SELECT history_record_id, bm25(artifact_search)
                FROM artifact_search
                WHERE artifact_search MATCH ?1 AND history_record_id IS NOT NULL
            )
            SELECT record_id, MIN(score) AS score
            FROM matches
            WHERE record_id IS NOT NULL
            GROUP BY record_id
            ORDER BY score, record_id
            LIMIT ?2 OFFSET ?3
            "#
        } else {
            r#"
            SELECT record_id, bm25(ctx_history_search) AS score
            FROM ctx_history_search
            WHERE ctx_history_search MATCH ?1
            ORDER BY score, record_id
            LIMIT ?2 OFFSET ?3
            "#
        };
        let mut stmt = self.conn.prepare(sql)?;
        #[cfg(feature = "test-utils")]
        self.record_search_page_executions
            .set(self.record_search_page_executions.get().saturating_add(1));
        let rows = stmt.query_map(params![match_query, limit as i64, offset as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
        })?;
        let mut hits = Vec::new();
        for row in rows {
            let (record_id, score) = row?;
            hits.push(RecordSearchHit {
                record_id: parse_uuid(record_id)?,
                score,
            });
        }
        Ok(Some(hits))
    }

    pub fn max_events_per_history_record(&self) -> Result<i64> {
        let max_events = self.conn.query_row(
            r#"
            SELECT COALESCE(MAX(event_count), 0)
            FROM (
                SELECT COUNT(*) AS event_count
                FROM events
                GROUP BY history_record_id
            )
            "#,
            [],
            |row| row.get(0),
        )?;
        Ok(max_events)
    }

    pub fn has_at_least_events(&self, threshold: i64) -> Result<bool> {
        if threshold <= 0 {
            return Ok(true);
        }
        let exists = self.conn.query_row(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM events
                LIMIT 1 OFFSET ?1
            )
            "#,
            params![threshold - 1],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }

    pub fn has_provider_data(&self, provider: CaptureProvider) -> Result<bool> {
        let exists = self.conn.query_row(
            r#"
            SELECT
                EXISTS(
                    SELECT 1
                    FROM sessions
                    WHERE provider = ?1
                    LIMIT 1
                )
                OR EXISTS(
                    SELECT 1
                    FROM capture_sources
                    WHERE provider = ?1
                    LIMIT 1
                )
            "#,
            params![provider.as_str()],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }

    pub fn search_event_hits(&self, query: &str, limit: usize) -> Result<Vec<EventSearchHit>> {
        self.search_event_hits_page(query, limit, 0)
    }

    /// Internal, non-contractual test instrumentation: number of ranked
    /// event-search page statements successfully prepared by this store
    /// handle (both unfiltered and filtered SQL shapes). Tests and benchmarks
    /// assert on this counter instead of timing; production callers must not
    /// depend on it as a public behavioral or telemetry contract.
    #[doc(hidden)]
    pub fn event_search_page_executions(&self) -> u64 {
        self.event_search_page_executions.get()
    }

    #[doc(hidden)]
    pub fn event_search_rows_hydrated(&self) -> u64 {
        self.event_search_rows_hydrated.get()
    }

    /// Statement count for the bounded relation loaders. Intended for focused
    /// regression tests and evidence collection; unlike connection tracing it
    /// cannot observe unrelated statements on a shared store.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn relation_batch_executions(&self) -> u64 {
        self.relation_batch_executions.get()
    }

    #[cfg(feature = "test-utils")]
    fn increment_search_hydration_loader(&self, narrow: bool) {
        let mut counts = self.search_hydration_loader_executions.get();
        counts[usize::from(narrow)] = counts[usize::from(narrow)].saturating_add(1);
        self.search_hydration_loader_executions.set(counts);
    }

    /// Full and narrow relation-loader calls since the last reset.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn search_hydration_loader_executions(&self) -> [u64; 2] {
        self.search_hydration_loader_executions.get()
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn reset_search_hydration_loader_executions(&self) {
        self.search_hydration_loader_executions.set([0; 2]);
    }

    pub fn search_event_hits_plan_page(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EventSearchHit>> {
        self.search_event_hits_page_inner(plan, limit, offset)
    }

    pub fn search_event_hits_page(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EventSearchHit>> {
        let plan = SearchQueryPlan::new(SearchMatchMode::All, [query]);
        self.search_event_hits_page_inner(&plan, limit, offset)
    }

    fn search_event_hits_page_inner(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EventSearchHit>> {
        if !table_exists(&self.conn, "event_search")? {
            return Ok(Vec::new());
        }
        let Some(match_query) = plan.fts_match_query() else {
            return Ok(Vec::new());
        };
        let mut stmt = self.conn.prepare(SEARCH_EVENT_HITS_PAGE_SQL)?;
        self.event_search_page_executions
            .set(self.event_search_page_executions.get().saturating_add(1));
        let rows = stmt.query_map(
            params![match_query, limit.max(1) as i64, offset as i64],
            event_search_hit_from_row,
        )?;
        let rows = collect_rows(rows)?;
        self.event_search_rows_hydrated.set(
            self.event_search_rows_hydrated
                .get()
                .saturating_add(rows.len() as u64),
        );
        Ok(rows)
    }

    /// Ranked event-search page with the exact-semantics filters of
    /// `EventSearchSqlFilters` applied inside the candidate CTE, before
    /// LIMIT/OFFSET. Ordering, hydration, and pagination semantics are
    /// identical to `search_event_hits_page` restricted to matching rows:
    /// every page is an exact slice of the unfiltered hit stream filtered by
    /// the equivalent Rust predicates. Empty filters delegate to the
    /// unfiltered query so plain searches keep the narrow phase-one shape.
    pub fn search_event_hits_page_filtered(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        filters: &EventSearchSqlFilters,
    ) -> Result<Vec<EventSearchHit>> {
        let plan = SearchQueryPlan::new(SearchMatchMode::All, [query]);
        self.search_event_hits_plan_page_filtered(&plan, limit, offset, filters)
    }

    /// Plan-aware variant of `search_event_hits_page_filtered`: the FTS MATCH
    /// expression is derived from the literal-token `SearchQueryPlan`
    /// (all/any/phrase) instead of an implicit AND-of-words query. The match
    /// expression is only ever a bound `?1` parameter, so both filtered SQL
    /// shapes (and their EXPLAIN plans) are byte-identical across match modes.
    pub fn search_event_hits_plan_page_filtered(
        &self,
        plan: &SearchQueryPlan,
        limit: usize,
        offset: usize,
        filters: &EventSearchSqlFilters,
    ) -> Result<Vec<EventSearchHit>> {
        if filters.is_empty() {
            return self.search_event_hits_page_inner(plan, limit, offset);
        }
        if !table_exists(&self.conn, "event_search")? {
            return Ok(Vec::new());
        }
        let Some(match_query) = plan.fts_match_query() else {
            return Ok(Vec::new());
        };
        // The provider fallback chain is the only predicate that needs the
        // three capture_sources joins; skip them when provider is unfiltered.
        let sql = if filters.provider.is_some() {
            SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL
        } else {
            SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL
        };
        let scope_mode = match filters.agent_scope {
            None => 0_i64,
            Some(EventSearchAgentScope::PrimaryOrSessionless) => 1,
            Some(EventSearchAgentScope::PrimaryOnly) => 2,
        };
        let mut stmt = self.conn.prepare(sql)?;
        self.event_search_page_executions
            .set(self.event_search_page_executions.get().saturating_add(1));
        let rows = stmt.query_map(
            params![
                match_query,
                limit.max(1) as i64,
                offset as i64,
                filters.session_id.map(|id| id.to_string()),
                filters.provider.map(CaptureProvider::as_str),
                filters.since.map(event_search_since_threshold_ms),
                filters.event_type.map(EventType::as_str),
                scope_mode,
                event_role_mask(&filters.roles),
                event_role_mask(&filters.exclude_roles),
                i64::from(filters.exclude_tool_noise),
                filters.file_scope.as_ref().map(file_scope_json),
            ],
            event_search_hit_from_row,
        )?;
        let rows = collect_rows(rows)?;
        self.event_search_rows_hydrated.set(
            self.event_search_rows_hydrated
                .get()
                .saturating_add(rows.len() as u64),
        );
        Ok(rows)
    }

    /// Scans one bounded ranked event statement in logical batches. The
    /// callback returns `false` to stop row hydration early; statement state
    /// is connection/invocation scoped and is dropped before this returns.
    /// The callback must not re-enter this `Store`: the live SQLite cursor
    /// retains the connection until callback processing completes.
    pub fn scan_event_hits_plan_filtered<F>(
        &self,
        plan: &SearchQueryPlan,
        page_size: usize,
        max_pages: usize,
        filters: &EventSearchSqlFilters,
        mut consume: F,
    ) -> Result<()>
    where
        F: FnMut(&[EventSearchHit]) -> bool,
    {
        if !table_exists(&self.conn, "event_search")? {
            return Ok(());
        }
        let Some(match_query) = plan.fts_match_query() else {
            return Ok(());
        };
        let page_size = page_size.max(1);
        let limit = page_size.saturating_mul(max_pages.max(1));
        let mut stmt;
        let mut rows = if filters.is_empty() {
            stmt = self.conn.prepare(SEARCH_EVENT_HITS_PAGE_SQL)?;
            stmt.query(params![match_query, limit as i64, 0_i64])?
        } else {
            let sql = if filters.provider.is_some() {
                SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL
            } else {
                SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL
            };
            let scope_mode = match filters.agent_scope {
                None => 0_i64,
                Some(EventSearchAgentScope::PrimaryOrSessionless) => 1,
                Some(EventSearchAgentScope::PrimaryOnly) => 2,
            };
            stmt = self.conn.prepare(sql)?;
            stmt.query(params![
                match_query,
                limit as i64,
                0_i64,
                filters.session_id.map(|id| id.to_string()),
                filters.provider.map(CaptureProvider::as_str),
                filters.since.map(event_search_since_threshold_ms),
                filters.event_type.map(EventType::as_str),
                scope_mode,
                event_role_mask(&filters.roles),
                event_role_mask(&filters.exclude_roles),
                i64::from(filters.exclude_tool_noise),
                filters.file_scope.as_ref().map(file_scope_json),
            ])?
        };
        self.event_search_page_executions
            .set(self.event_search_page_executions.get().saturating_add(1));
        let mut batch = Vec::with_capacity(page_size);
        while let Some(row) = rows.next()? {
            batch.push(event_search_hit_from_row(row)?);
            if batch.len() == page_size {
                self.event_search_rows_hydrated.set(
                    self.event_search_rows_hydrated
                        .get()
                        .saturating_add(batch.len() as u64),
                );
                if !consume(&batch) {
                    return Ok(());
                }
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.event_search_rows_hydrated.set(
                self.event_search_rows_hydrated
                    .get()
                    .saturating_add(batch.len() as u64),
            );
            consume(&batch);
        }
        Ok(())
    }

    pub fn export_archive(&self) -> Result<SessionHistoryArchive> {
        Ok(SessionHistoryArchive {
            schema_version: 2,
            version: 2,
            records: self.list_records(usize::MAX)?,
            capture_sources: self.list_capture_sources()?,
            sessions: self.list_sessions()?,
            runs: self.list_runs()?,
            events: self.list_events()?,
            artifact_records: self.list_artifacts()?,
            vcs_workspaces: self.list_vcs_workspaces()?,
            vcs_changes: self.list_vcs_changes()?,
            history_record_links: self.list_history_record_links()?,
            summaries: self.list_summaries()?,
            files_touched: self.list_files_touched()?,
        })
    }

    pub fn import_archive(
        &mut self,
        archive: &SessionHistoryArchive,
        overwrite: bool,
    ) -> Result<()> {
        validate_archive_version(archive)?;
        reject_archive_event_internal_conflicts(archive)?;
        let blob_dir = self.object_dir.clone();
        let tx = self.conn.transaction()?;
        reject_import_invariant_conflicts(&tx, archive)?;
        if !overwrite {
            reject_import_conflicts(&tx, archive)?;
        }
        let mut blob_guard = BlobWriteGuard::default();
        for record in &archive.records {
            upsert_record_tx(&tx, record, None)?;
        }
        import_rich_archive_entities_tx(&tx, &blob_dir, archive, &mut blob_guard)?;
        tx.commit()?;
        blob_guard.commit();
        self.rebuild_search_projection()?;
        Ok(())
    }

    pub fn import_archive_from_capture_source(
        &mut self,
        archive: &SessionHistoryArchive,
        source_id: Uuid,
        source: &CaptureSourceDescriptor,
        occurred_at: DateTime<Utc>,
        fidelity: Fidelity,
        overwrite: bool,
    ) -> Result<()> {
        validate_archive_version(archive)?;
        reject_archive_event_internal_conflicts(archive)?;
        let blob_dir = self.object_dir.clone();
        let tx = self.conn.transaction()?;
        reject_import_invariant_conflicts(&tx, archive)?;
        if !overwrite {
            reject_capture_source_import_conflict(&tx, source_id)?;
            reject_import_conflicts(&tx, archive)?;
        }
        let mut blob_guard = BlobWriteGuard::default();
        upsert_capture_source_tx(&tx, source_id, source, occurred_at, fidelity)?;
        for record in &archive.records {
            upsert_record_tx(&tx, record, Some(source_id))?;
        }
        import_rich_archive_entities_tx(&tx, &blob_dir, archive, &mut blob_guard)?;
        tx.commit()?;
        blob_guard.commit();
        self.rebuild_search_projection()?;
        Ok(())
    }

    pub fn validate(&self) -> Result<Vec<String>> {
        let integrity: String = self
            .conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        let foreign_key_failures = count_foreign_key_failures(&self.conn)?;

        let mut findings = Vec::new();
        if integrity != "ok" {
            findings.push(format!("sqlite integrity_check returned {integrity}"));
        }
        if foreign_key_failures > 0 {
            findings.push(format!(
                "{foreign_key_failures} foreign key violations detected"
            ));
        }
        Ok(findings)
    }

    fn rebuild_search_projection(&self) -> Result<()> {
        rebuild_search_projection(&self.conn)
    }

    fn ensure_search_projection_initialized(&self) -> Result<()> {
        ensure_search_projection_initialized(&self.conn)
    }

    fn normalize_legacy_blob_paths(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE artifacts SET blob_path = 'objects/' || substr(blob_path, 7) WHERE blob_path LIKE 'blobs/%'",
            [],
        )?;
        Ok(())
    }
}

fn file_scope_json(scope: &FileTouchScope) -> String {
    serde_json::to_string(&serde_json::json!({
        "history_record_ids": scope.history_record_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "session_ids": scope.session_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "run_ids": scope.run_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "event_ids": scope.event_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
    }))
    .expect("UUID file scope is JSON serializable")
}

// Two-phase ranked event search. Phase one ranks FTS candidates on a narrow
// projection (event_search rowid, bm25 score, occurred_at_ms/seq/event_id tie
// keys) and applies LIMIT/OFFSET before anything wide is touched. Phase two
// re-joins only the selected page rows against the wide tables
// (runs/sessions/capture_sources/history_records) and hydrates
// payload/metadata. The outer ORDER BY replays the exact inner sort keys
// (carried bm25 score plus the same tie-break columns, ending in the unique
// event_id) so the page order is identical to the pre-refactor single-pass
// query. The subquery's LIMIT prevents SQLite from flattening it into the
// outer join, which keeps wide hydration bounded to the selected page; the
// plan shape is asserted in `search_order_tests`.
//
// All page-query shapes share the same phase-two hydration text through this
// macro, so filtered pages hydrate byte-identically to unfiltered ones. The
// filtered candidate CTEs below add exact-semantics predicates (see
// `EventSearchSqlFilters`) before LIMIT/OFFSET; their extra joins are indexed
// primary-key lookups per FTS candidate and every predicate is written
// against the same COALESCE fallback chain that phase two hydrates into the
// corresponding `EventSearchHit` field.
macro_rules! event_hits_page_sql {
    ($($ranked_page_cte:literal),+ $(,)?) => {
        concat!(
            "\n    WITH ranked_page AS (\n",
            $($ranked_page_cte),+,
            "\n    )",
            r#"
    SELECT event_search.event_id,
           COALESCE(e.history_record_id, event_search.history_record_id, s.history_record_id, rs.history_record_id),
           COALESCE(e.session_id, event_search.session_id, s.id, rs.id),
           e.run_id,
           e.seq,
           e.event_type,
           e.role,
           e.occurred_at_ms,
           event_search.safe_preview_text,
           ranked_page.score,
           COALESCE(s.provider, rs.provider, event_source.provider, session_source.provider, run_source.provider),
           COALESCE(s.external_session_id, rs.external_session_id),
           COALESCE(s.parent_session_id, rs.parent_session_id),
           COALESCE(s.root_session_id, rs.root_session_id),
           COALESCE(s.agent_type, rs.agent_type),
           COALESCE(s.is_primary, rs.is_primary),
           COALESCE(event_source.cwd, session_source.cwd, run_source.cwd),
           COALESCE(event_source.raw_source_path, session_source.raw_source_path, run_source.raw_source_path),
           e.payload_json,
           COALESCE(event_source.metadata_json, session_source.metadata_json, run_source.metadata_json),
           wr.title,
           wr.kind,
           wr.workspace
    FROM ranked_page
    JOIN event_search ON event_search.rowid = ranked_page.search_rowid
    JOIN events e ON e.id = event_search.event_id
    LEFT JOIN runs r ON r.id = e.run_id
    LEFT JOIN sessions s ON s.id = COALESCE(e.session_id, event_search.session_id)
    LEFT JOIN sessions rs ON rs.id = r.session_id
    LEFT JOIN capture_sources event_source ON event_source.id = e.capture_source_id
    LEFT JOIN capture_sources session_source ON session_source.id = COALESCE(s.capture_source_id, rs.capture_source_id)
    LEFT JOIN capture_sources run_source ON run_source.id = r.source_id
    LEFT JOIN history_records wr ON wr.id = COALESCE(e.history_record_id, event_search.history_record_id, s.history_record_id, rs.history_record_id, r.history_record_id)
    ORDER BY ranked_page.score, e.occurred_at_ms DESC, e.seq DESC, event_search.event_id
    "#
        )
    };
}

// Compose both filtered candidate CTEs from one readable predicate suffix.
// The caller supplies only the provider-specific joins and predicate; session,
// since, event_type, agent scope, ordering, and pagination therefore cannot
// drift between the scoped and provider shapes.
macro_rules! filtered_event_hits_page_sql {
    () => {
        filtered_event_hits_page_sql!(@compose r#""#, r#""#)
    };
    (provider) => {
        filtered_event_hits_page_sql!(
            @compose
            r#"        LEFT JOIN capture_sources event_source ON event_source.id = e.capture_source_id
        LEFT JOIN capture_sources session_source ON session_source.id = COALESCE(s.capture_source_id, rs.capture_source_id)
        LEFT JOIN capture_sources run_source ON run_source.id = r.source_id
"#,
            r#"          AND (?5 IS NULL OR COALESCE(s.provider, rs.provider, event_source.provider, session_source.provider, run_source.provider) = ?5)
"#
        )
    };
    (@compose $provider_joins:literal, $provider_predicate:literal) => {
        event_hits_page_sql!(
            r#"        SELECT event_search.rowid AS search_rowid,
               bm25(event_search) AS score
        FROM event_search
        JOIN events e ON e.id = event_search.event_id
        LEFT JOIN runs r ON r.id = e.run_id
        LEFT JOIN sessions s ON s.id = COALESCE(e.session_id, event_search.session_id)
        LEFT JOIN sessions rs ON rs.id = r.session_id
"#,
            $provider_joins,
            r#"        WHERE event_search MATCH ?1
          AND (?4 IS NULL OR COALESCE(e.session_id, event_search.session_id, s.id, rs.id) = ?4)
"#,
            $provider_predicate,
            r#"          AND (?6 IS NULL OR e.occurred_at_ms >= ?6)
          AND (?7 IS NULL OR e.event_type = ?7)
          AND (?8 = 0
               OR COALESCE(s.is_primary, rs.is_primary) <> 0
               OR COALESCE(s.agent_type, rs.agent_type) = 'primary'
               OR (?8 = 1
                   AND COALESCE(s.is_primary, rs.is_primary) IS NULL
                   AND COALESCE(s.agent_type, rs.agent_type) IS NULL))
          AND (?9 = 0 OR (CASE e.role
               WHEN 'user' THEN 1 WHEN 'assistant' THEN 2 WHEN 'system' THEN 4
               WHEN 'tool' THEN 8 WHEN 'unknown' THEN 16 ELSE 0 END) & ?9 <> 0)
          AND (?10 = 0 OR (CASE e.role
               WHEN 'user' THEN 1 WHEN 'assistant' THEN 2 WHEN 'system' THEN 4
               WHEN 'tool' THEN 8 WHEN 'unknown' THEN 16 ELSE 0 END) & ?10 = 0)
          AND (?11 = 0 OR e.event_type NOT IN
               ('tool_call', 'tool_output', 'command_started', 'command_output',
                'command_finished'))
          AND (?12 IS NULL OR
               e.id IN (SELECT value FROM json_each(?12, '$.event_ids')) OR
               e.run_id IN (SELECT value FROM json_each(?12, '$.run_ids')) OR
               COALESCE(e.session_id, event_search.session_id, s.id, rs.id)
                   IN (SELECT value FROM json_each(?12, '$.session_ids')) OR
               COALESCE(e.history_record_id, event_search.history_record_id,
                        s.history_record_id, rs.history_record_id)
                   IN (SELECT value FROM json_each(?12, '$.history_record_ids')))
        ORDER BY bm25(event_search), e.occurred_at_ms DESC, e.seq DESC, event_search.event_id
        LIMIT ?2 OFFSET ?3"#,
        )
    };
}

const SEARCH_EVENT_HITS_PAGE_SQL: &str = event_hits_page_sql!(
    r#"        SELECT event_search.rowid AS search_rowid,
               bm25(event_search) AS score
        FROM event_search
        JOIN events e ON e.id = event_search.event_id
        WHERE event_search MATCH ?1
        ORDER BY bm25(event_search), e.occurred_at_ms DESC, e.seq DESC, event_search.event_id
        LIMIT ?2 OFFSET ?3"#
);

// Filtered candidate selection without the provider predicate: only the
// narrow events row plus the session identity chain (runs -> sessions) is
// needed to evaluate session/since/event_type/agent-scope, so the three
// capture_sources joins are skipped. ?5 (provider) is intentionally unused in
// this shape; `search_event_hits_page_filtered` only chooses it when the
// provider filter is absent.
const SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL: &str = filtered_event_hits_page_sql!();

// Filtered candidate selection including the provider predicate, which needs
// the full five-way provider fallback chain and therefore the three
// capture_sources joins (same join conditions as phase two).
const SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL: &str = filtered_event_hits_page_sql!(provider);

/// Smallest stored `occurred_at_ms` value satisfying `occurred_at >= since`.
/// Stored event timestamps are whole milliseconds, so a `since` with
/// sub-millisecond precision must ceil to the next representable millisecond;
/// a `since` that is exactly on a millisecond boundary keeps events at that
/// exact millisecond (>= is inclusive).
fn event_search_since_threshold_ms(since: DateTime<Utc>) -> i64 {
    let floor_ms = since.timestamp_millis();
    if DateTime::<Utc>::from_timestamp_millis(floor_ms) == Some(since) {
        floor_ms
    } else {
        floor_ms.saturating_add(1)
    }
}

/// Bit assigned to each `EventRole` variant in the role bitmask predicates of
/// the filtered page SQL. The CASE expression in
/// `filtered_event_hits_page_sql!` must map every variant string to exactly
/// this bit (asserted by `role_mask_sql_case_covers_event_role_domain`); a
/// NULL role falls through to the CASE ELSE arm (no bit), replicating the
/// Rust `Option<EventRole>` include/exclude semantics.
fn event_role_bit(role: EventRole) -> i64 {
    match role {
        EventRole::User => 1,
        EventRole::Assistant => 2,
        EventRole::System => 4,
        EventRole::Tool => 8,
        EventRole::Unknown => 16,
    }
}

fn event_role_mask(roles: &[EventRole]) -> i64 {
    roles
        .iter()
        .fold(0_i64, |mask, role| mask | event_role_bit(*role))
}

fn event_search_hit_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventSearchHit> {
    let payload_json = row.get::<_, String>(18)?;
    let source_metadata_json = row.get::<_, Option<String>>(19)?;
    let source_identity = event_search_source_identity(source_metadata_json.as_deref())?;
    Ok(EventSearchHit {
        event_id: parse_uuid(row.get::<_, String>(0)?)?,
        history_record_id: parse_optional_uuid(row.get(1)?)?,
        session_id: parse_optional_uuid(row.get(2)?)?,
        run_id: parse_optional_uuid(row.get(3)?)?,
        seq: row.get::<_, i64>(4)? as u64,
        event_type: parse_text_enum::<EventType>(row.get::<_, String>(5)?)?,
        role: parse_optional_text_enum::<EventRole>(row.get(6)?)?,
        occurred_at: ms_to_time(row.get(7)?)?,
        preview: row.get(8)?,
        score: row.get(9)?,
        provider: parse_optional_text_enum::<CaptureProvider>(row.get(10)?)?,
        session_external_session_id: row.get(11)?,
        history_source: source_identity.history_source,
        history_source_plugin: source_identity.history_source_plugin,
        provider_key: source_identity.provider_key,
        source_id: source_identity.source_id,
        source_format: source_identity.source_format,
        session_parent_session_id: parse_optional_uuid(row.get(12)?)?,
        session_root_session_id: parse_optional_uuid(row.get(13)?)?,
        agent_type: parse_optional_text_enum::<AgentType>(row.get(14)?)?,
        session_is_primary: row.get::<_, Option<i64>>(15)?.map(|value| value != 0),
        cwd: row.get(16)?,
        raw_source_path: row.get(17)?,
        cursor: event_search_cursor(&payload_json, source_metadata_json.as_deref())?,
        record_title: row.get(20)?,
        record_kind: row.get(21)?,
        record_workspace: row.get(22)?,
        tool_names: event_tool_names_from_payload(&payload_json),
    })
}

fn event_tool_names_from_payload(payload_json: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(payload_json) else {
        return Vec::new();
    };
    let mut names = Vec::new();
    collect_tool_names(&value, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect_tool_names(value: &serde_json::Value, names: &mut Vec<String>) {
    let Some(object) = value.as_object() else {
        return;
    };
    for key in ["tool", "name", "executable", "command"] {
        if let Some(text) = object.get(key).and_then(|value| value.as_str()) {
            if let Some(name) = executable_name(text) {
                names.push(name);
            }
        }
    }
    if let Some(body) = object.get("body") {
        collect_tool_names(body, names);
    }
}

fn executable_name(text: &str) -> Option<String> {
    let first = text
        .split_whitespace()
        .next()?
        .trim_matches(|c: char| c == '"' || c == '\'' || c == '`' || c == '[' || c == ']');
    let name = std::path::Path::new(first)
        .file_name()?
        .to_str()?
        .to_ascii_lowercase();
    (!name.is_empty()).then_some(name)
}

fn configure_connection(conn: &Connection, busy_timeout: Duration) -> Result<()> {
    conn.busy_timeout(busy_timeout)?;
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -32768;
        PRAGMA wal_autocheckpoint = 10000;
        "#,
    )?;
    Ok(())
}

fn configure_read_only_connection(conn: &Connection, busy_timeout: Duration) -> Result<()> {
    conn.busy_timeout(busy_timeout)?;
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -32768;
        PRAGMA query_only = ON;
        "#,
    )?;
    Ok(())
}

fn validate_raw_sql_options(options: &RawSqlOptions) -> Result<()> {
    validate_raw_sql_usize("max_rows", options.max_rows, 1, RAW_SQL_MAX_ROWS_CAP)?;
    validate_raw_sql_usize(
        "max_columns",
        options.max_columns,
        1,
        RAW_SQL_MAX_COLUMNS_CAP,
    )?;
    validate_raw_sql_usize(
        "max_value_bytes",
        options.max_value_bytes,
        1,
        RAW_SQL_MAX_VALUE_BYTES_CAP,
    )?;
    validate_raw_sql_usize(
        "max_sql_bytes",
        options.max_sql_bytes,
        1,
        RAW_SQL_MAX_SQL_BYTES_CAP,
    )?;
    let timeout_ms = duration_ms(options.timeout);
    if timeout_ms == 0 || options.timeout > RAW_SQL_MAX_TIMEOUT {
        return Err(StoreError::RawSqlLimitOutOfRange {
            field: "timeout_ms",
            value: usize::try_from(timeout_ms).unwrap_or(usize::MAX),
            min: 1,
            max: usize::try_from(duration_ms(RAW_SQL_MAX_TIMEOUT)).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

fn validate_raw_sql_statement_bytes(sql: &str, options: &RawSqlOptions) -> Result<()> {
    validate_raw_sql_usize("sql_bytes", sql.len(), 1, options.max_sql_bytes)
}

struct RawSqlLimitGuard<'a> {
    conn: &'a Connection,
    length: i32,
    sql_length: i32,
    column: i32,
}

impl<'a> RawSqlLimitGuard<'a> {
    fn apply(conn: &'a Connection, options: &RawSqlOptions) -> Result<Self> {
        let length_limit = raw_sql_length_limit(options)?;
        let sql_length_limit = i32::try_from(options.max_sql_bytes).map_err(|_| {
            StoreError::RawSqlLimitOutOfRange {
                field: "max_sql_bytes",
                value: options.max_sql_bytes,
                min: 1,
                max: RAW_SQL_MAX_SQL_BYTES_CAP,
            }
        })?;
        let column_limit =
            i32::try_from(options.max_columns).map_err(|_| StoreError::RawSqlLimitOutOfRange {
                field: "max_columns",
                value: options.max_columns,
                min: 1,
                max: RAW_SQL_MAX_COLUMNS_CAP,
            })?;
        let guard = Self {
            conn,
            length: conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, length_limit),
            sql_length: conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, sql_length_limit),
            column: conn.set_limit(Limit::SQLITE_LIMIT_COLUMN, column_limit),
        };
        Ok(guard)
    }
}

impl Drop for RawSqlLimitGuard<'_> {
    fn drop(&mut self) {
        self.conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, self.length);
        self.conn
            .set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, self.sql_length);
        self.conn.set_limit(Limit::SQLITE_LIMIT_COLUMN, self.column);
    }
}

fn raw_sql_length_limit(options: &RawSqlOptions) -> Result<i32> {
    let bytes = options
        .max_value_bytes
        .saturating_add(RAW_SQL_VALUE_LENGTH_MARGIN_BYTES);
    let bytes = bytes.max(RAW_SQL_MIN_SQLITE_LENGTH_LIMIT_BYTES);
    i32::try_from(bytes).map_err(|_| StoreError::RawSqlLimitOutOfRange {
        field: "max_value_bytes",
        value: options.max_value_bytes,
        min: 1,
        max: RAW_SQL_MAX_VALUE_BYTES_CAP,
    })
}

fn validate_raw_sql_usize(field: &'static str, value: usize, min: usize, max: usize) -> Result<()> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(StoreError::RawSqlLimitOutOfRange {
            field,
            value,
            min,
            max,
        })
    }
}

fn reject_sql_tail(conn: &Connection, sql: &str) -> Result<()> {
    let c_sql = CString::new(sql).map_err(|_| StoreError::RawSqlInteriorNul)?;
    let mut stmt = ptr::null_mut();
    let mut tail: *const c_char = ptr::null();
    let rc =
        unsafe { ffi::sqlite3_prepare_v2(conn.handle(), c_sql.as_ptr(), -1, &mut stmt, &mut tail) };
    if !stmt.is_null() {
        unsafe {
            ffi::sqlite3_finalize(stmt);
        }
    }
    if rc != ffi::SQLITE_OK || tail.is_null() {
        return Ok(());
    }

    let start = c_sql.as_ptr() as usize;
    let tail_offset = (tail as usize).saturating_sub(start);
    let sql_bytes = c_sql.as_bytes();
    if tail_offset < sql_bytes.len() && sql_tail_has_statement(&sql[tail_offset..]) {
        return Err(StoreError::Sql(rusqlite::Error::MultipleStatement));
    }
    Ok(())
}

fn sql_tail_has_statement(mut tail: &str) -> bool {
    loop {
        let trimmed = tail.trim_start();
        if trimmed.is_empty() {
            return false;
        }
        if let Some(rest) = trimmed.strip_prefix("--") {
            if let Some(newline) = rest.find('\n') {
                tail = &rest[newline + 1..];
                continue;
            }
            return false;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            if let Some(end) = rest.find("*/") {
                tail = &rest[end + 2..];
                continue;
            }
            return true;
        }
        return true;
    }
}

fn raw_sql_value(value: ValueRef<'_>, max_value_bytes: usize) -> RawSqlValue {
    match value {
        ValueRef::Null => RawSqlValue::Null,
        ValueRef::Integer(value) => RawSqlValue::Integer(value),
        ValueRef::Real(value) => RawSqlValue::Real(value),
        ValueRef::Text(bytes) => {
            let truncated = bytes.len() > max_value_bytes;
            let preview = if truncated {
                String::from_utf8_lossy(&bytes[..max_value_bytes]).into_owned()
            } else {
                String::from_utf8_lossy(bytes).into_owned()
            };
            RawSqlValue::Text {
                value: preview,
                bytes: bytes.len(),
                truncated,
            }
        }
        ValueRef::Blob(bytes) => {
            let truncated = bytes.len() > max_value_bytes;
            let preview_len = bytes.len().min(max_value_bytes);
            RawSqlValue::Blob {
                bytes: bytes.len(),
                preview_hex: hex_preview(&bytes[..preview_len]),
                truncated,
            }
        }
    }
}

fn hex_preview(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn reject_unsupported_schema_at(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if user_version != SCHEMA_VERSION && !schema_version_is_migratable(user_version) {
        return Err(StoreError::UnsupportedSchemaVersion(user_version));
    }
    Ok(())
}

fn migrate_legacy_history_layout(data_root: &Path) -> Result<bool> {
    let legacy_dir = data_root.join(LEGACY_HISTORY_DIR_NAME);
    if !legacy_dir.is_dir() {
        return Ok(false);
    }

    let mut moves = Vec::new();
    push_legacy_move(
        &mut moves,
        legacy_dir.join("work.sqlite"),
        data_root.join("work.sqlite"),
    );
    push_legacy_move(
        &mut moves,
        legacy_dir.join("config.toml"),
        data_root.join("config.toml"),
    );
    push_legacy_move(&mut moves, legacy_dir.join("logs"), data_root.join("logs"));
    push_legacy_move(
        &mut moves,
        legacy_dir.join("device.json"),
        data_root.join("device.json"),
    );

    let object_candidates = [
        legacy_dir.join(OBJECTS_DIR),
        legacy_dir.join(LEGACY_BLOBS_DIR),
    ];
    let spool_candidates = [
        legacy_dir.join(SPOOL_DIR),
        legacy_dir.join(LEGACY_INBOX_DIR),
    ];
    if multiple_existing_paths(&object_candidates) || multiple_existing_paths(&spool_candidates) {
        return Ok(false);
    }

    if let Some(object_source) = unique_existing_path(&object_candidates) {
        push_legacy_move(&mut moves, object_source, data_root.join(OBJECTS_DIR));
    }

    if let Some(spool_source) = unique_existing_path(&spool_candidates) {
        push_legacy_move(&mut moves, spool_source, data_root.join(SPOOL_DIR));
    }

    if moves.is_empty() || moves.iter().any(|(_, dest)| dest.exists()) {
        return Ok(false);
    }

    for (source, dest) in moves {
        fs::rename(source, dest)?;
    }
    let _ = fs::remove_dir(&legacy_dir);
    Ok(true)
}

fn push_legacy_move(moves: &mut Vec<(PathBuf, PathBuf)>, source: PathBuf, dest: PathBuf) {
    if source.exists() {
        moves.push((source, dest));
    }
}

fn unique_existing_path(paths: &[PathBuf]) -> Option<PathBuf> {
    let mut existing = paths.iter().filter(|path| path.exists());
    let first = existing.next()?.clone();
    if existing.next().is_some() {
        return None;
    }
    Some(first)
}

fn multiple_existing_paths(paths: &[PathBuf]) -> bool {
    paths.iter().filter(|path| path.exists()).take(2).count() > 1
}

fn object_relative_path(hash: &str) -> String {
    let shard = &hash[..2];
    format!("{OBJECTS_DIR}/{shard}/{hash}")
}

/// Runs `body` as one atomic write unit on `conn`, choosing the transaction
/// shape by context.
///
/// In autocommit mode this takes `BEGIN IMMEDIATE` *before* the body runs,
/// so read probes and the writes they guard hold the write lock together
/// from the start. A bare SAVEPOINT here would open a deferred transaction:
/// the probe would read under a shared snapshot and the later write would
/// have to upgrade mid-body, which under WAL can fail immediately with
/// SQLITE_BUSY_SNAPSHOT (bypassing the busy timeout) when another
/// connection commits in between — and would let the probe's answer go
/// stale before the write. With BEGIN IMMEDIATE the lock acquisition waits
/// under the configured busy timeout up front and probe answers stay true
/// for the writes that depend on them. COMMIT is the success point; on body
/// or COMMIT failure a best-effort ROLLBACK runs and the original
/// body/commit error is the one surfaced.
///
/// Inside an already-open caller transaction (migration transactions,
/// capture-harness batches) the caller already holds the write lock, so
/// this nests as a named SAVEPOINT with the cleanup semantics of the
/// projection-rebuild hardening:
/// - On body failure, ROLLBACK TO rewinds the body but keeps the savepoint
///   on the stack; the RELEASE then drops it, leaving the enclosing
///   transaction open and usable. The cleanup is best-effort and the
///   original body error is always the one surfaced.
/// - On success, if the RELEASE itself fails, a best-effort attempt
///   restores/drops the savepoint before surfacing that exact release
///   error.
///
/// `name` must be a trusted static identifier: it is interpolated into the
/// SAVEPOINT statements verbatim.
fn with_write_transaction<T>(
    conn: &Connection,
    name: &'static str,
    body: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if conn.is_autocommit() {
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        return match body() {
            Ok(value) => match conn.execute_batch("COMMIT;") {
                Ok(()) => Ok(value),
                Err(commit_err) => {
                    let _ = conn.execute_batch("ROLLBACK;");
                    Err(StoreError::Sql(commit_err))
                }
            },
            Err(err) => {
                let _ = conn.execute_batch("ROLLBACK;");
                Err(err)
            }
        };
    }
    conn.execute_batch(&format!("SAVEPOINT {name};"))?;
    match body() {
        Ok(value) => match conn.execute_batch(&format!("RELEASE SAVEPOINT {name};")) {
            Ok(()) => Ok(value),
            Err(release_err) => {
                let _ = conn.execute_batch(&format!(
                    "ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name};"
                ));
                Err(StoreError::Sql(release_err))
            }
        },
        Err(err) => {
            let _ = conn.execute_batch(&format!(
                "ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name};"
            ));
            Err(err)
        }
    }
}

/// Atomic full rebuild of every FTS search projection from the base tables.
///
/// Callers invoke this both from autocommit mode
/// (`Store::refresh_search_index`, post-import archive rebuilds,
/// `ensure_search_projection_initialized`) and from inside already-open
/// migration transactions (`migrate_to_v11`/`migrate_to_v12`).
/// [`with_write_transaction`] covers both: an immediate transaction in
/// autocommit mode and a nested savepoint otherwise, so in every context
/// the rebuild commits as one unit — no per-row autocommit overhead — and
/// any failure rolls back to the previous complete projection instead of
/// leaving it empty or partial.
fn rebuild_search_projection(conn: &Connection) -> Result<()> {
    with_write_transaction(conn, "rebuild_search_projection", || {
        rebuild_search_projection_body(conn)
    })
}

/// Projection rebuild statements, identical contents and order to the
/// pre-savepoint implementation. Only [`rebuild_search_projection`] (and the
/// retained rebuild benchmark) may call this directly.
fn rebuild_search_projection_body(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "ctx_history_search")? {
        return Ok(());
    }

    // Projections and rowid maps are cleared and repopulated in lockstep so
    // the rebuilt maps describe exactly the rebuilt FTS rows. The map
    // tables may be absent while rebuilds run inside pre-v1000 migration
    // transactions (v11/v12); the probes keep those paths map-free.
    conn.execute("DELETE FROM ctx_history_search", [])?;
    if RECORD_SEARCH_ROWID_MAP.is_present(conn)? {
        conn.execute("DELETE FROM record_search_rowids", [])?;
    }
    if EVENT_SEARCH_ROWID_MAP.is_present(conn)? {
        conn.execute("DELETE FROM event_search_rowids", [])?;
    }
    let has_event_search = table_exists(conn, "event_search")?;
    if has_event_search {
        conn.execute("DELETE FROM event_search", [])?;
        populate_event_search_projection(conn)?;
    }
    if table_exists(conn, "artifact_search")? {
        conn.execute("DELETE FROM artifact_search", [])?;
    }

    let records = {
        let mut stmt = conn.prepare(record_select_sql("ORDER BY created_at DESC").as_str())?;
        let rows = stmt.query_map([], record_from_row)?;
        collect_rows(rows)?
    };

    let mut insert_record_search = conn.prepare(
        r#"
        INSERT INTO ctx_history_search
        (record_id, title, summary, primary_user_text, decision_text, context_text, tag_text)
        VALUES (?1, ?2, ?3, ?4, '', ?5, ?6)
        "#,
    )?;
    for record in records {
        insert_record_search.execute(params![
            record.id.to_string(),
            local_preview(&record.title, 512),
            local_preview(&record.body, 2048),
            local_preview(&record.body, 2048),
            "",
            local_preview(&record.tags.join(" "), 1024),
        ])?;
        let search_rowid = conn.last_insert_rowid();
        RECORD_SEARCH_ROWID_MAP.store_entry(conn, &record.id.to_string(), search_rowid)?;
    }

    Ok(())
}

/// Maintenance contract for one durable FTS rowid map (fork schema v1000).
///
/// Exact invariants, in force for every write path that touches a search
/// projection:
///
/// - **Caches only.** Query and search code never reads the maps; no search
///   result ever depends on their contents. Dropping or corrupting a map
///   changes write-path cost, never output.
/// - **Same-transaction maintenance.** A map entry is written inside the
///   same write transaction as the FTS row whose freshly SQLite-assigned
///   rowid (captured from `last_insert_rowid()` immediately after the FTS
///   INSERT) it stores, and only after all previous projection rows for
///   that id were removed — by a verified point delete or by the legacy
///   full-scan delete-all. Within supported writer history — every writer
///   since v1000 maintains the maps, and older binaries refuse to newly
///   open the schema — a verified map entry therefore implies exactly one
///   projection row for its id. This scoping matters: a pre-upgrade
///   process that opened the store before the migration ran can keep
///   writing until it restarts, and external SQL writers are never
///   blocked. Such unsupported writes cannot make a point delete remove
///   another id's row (verification matches the id first), but they can
///   leave duplicate or orphaned projection rows that the maps do not
///   know about; a given id's duplicates collapse on its next healed
///   write, and orphans disappear only on a full projection rebuild (the
///   search-index rebuild that `ctx import` performs when required, or
///   the index reset documented in docs/storage.md). Search reads the FTS
///   tables directly and never trusts the maps, so such rows can at worst
///   surface as extra hits, never as wrong deletions.
/// - **Verify before point delete.** A mapped rowid is trusted only after
///   re-reading the FTS id column at that rowid and matching it against the
///   entity id. Missing, stale, or mismatched entries fall back to the
///   legacy full-scan delete (which also collapses legacy duplicate rows)
///   and self-heal when the row is reinserted.
/// - **Never inferred.** Mapped rowids are stored explicitly; they are
///   never derived from base-table rowids or insertion counting. FTS5
///   content rowids survive VACUUM, so external VACUUM cannot invalidate
///   entries.
/// - **Blank previews.** An event whose preview is blank has neither an FTS
///   row nor a map entry.
/// - **Degrade, never fail.** A missing map table turns writes into the
///   legacy full-scan path without error; `Store::migrate` recreates
///   missing map tables empty on open, and `rebuild_search_projection`
///   clears and repopulates maps in lockstep with the projections.
struct SearchRowidMapSpec {
    map_table: &'static str,
    lookup_sql: &'static str,
    store_sql: &'static str,
    remove_sql: &'static str,
    verify_sql: &'static str,
    point_delete_sql: &'static str,
    full_scan_delete_sql: &'static str,
}

const RECORD_SEARCH_ROWID_MAP: SearchRowidMapSpec = SearchRowidMapSpec {
    map_table: "record_search_rowids",
    lookup_sql: "SELECT search_rowid FROM record_search_rowids WHERE record_id = ?1",
    // OR REPLACE also evicts a stale entry from another id that still
    // claims this UNIQUE search_rowid: the rowid was just assigned by the
    // FTS insert, so any other claim is necessarily stale and the evicted
    // id simply heals on its own next write.
    store_sql:
        "INSERT OR REPLACE INTO record_search_rowids (record_id, search_rowid) VALUES (?1, ?2)",
    remove_sql: "DELETE FROM record_search_rowids WHERE record_id = ?1",
    verify_sql: "SELECT record_id FROM ctx_history_search WHERE rowid = ?1",
    point_delete_sql: "DELETE FROM ctx_history_search WHERE rowid = ?1",
    full_scan_delete_sql: "DELETE FROM ctx_history_search WHERE record_id = ?1",
};

const EVENT_SEARCH_ROWID_MAP: SearchRowidMapSpec = SearchRowidMapSpec {
    map_table: "event_search_rowids",
    lookup_sql: "SELECT search_rowid FROM event_search_rowids WHERE event_id = ?1",
    store_sql:
        "INSERT OR REPLACE INTO event_search_rowids (event_id, search_rowid) VALUES (?1, ?2)",
    remove_sql: "DELETE FROM event_search_rowids WHERE event_id = ?1",
    verify_sql: "SELECT event_id FROM event_search WHERE rowid = ?1",
    point_delete_sql: "DELETE FROM event_search WHERE rowid = ?1",
    full_scan_delete_sql: "DELETE FROM event_search WHERE event_id = ?1",
};

impl SearchRowidMapSpec {
    /// Existence probe for the map table, used by the rebuild path (once
    /// per rebuild). Per-row write paths avoid this probe: they attempt the
    /// map statement directly and treat a missing table as a degrade signal
    /// via [`Self::is_missing_map_table_error`].
    fn is_present(&self, conn: &Connection) -> Result<bool> {
        Ok(conn
            .prepare_cached("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")?
            .query_row(params![self.map_table], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// True when `err` is SQLite's "no such table" for this map table,
    /// i.e. the map has been dropped externally. Write paths degrade to the
    /// legacy full-scan behavior instead of failing (mirroring the
    /// `is_missing_fts_module` tolerance for the FTS tables themselves);
    /// `Store::migrate` recreates the table empty on the next open.
    fn is_missing_map_table_error(&self, err: &rusqlite::Error) -> bool {
        matches!(
            err,
            rusqlite::Error::SqliteFailure(error, Some(message))
                if error.extended_code == rusqlite::ffi::SQLITE_ERROR
                    && *message == format!("no such table: {}", self.map_table)
        )
    }

    /// Records `search_rowid` as the projection rowid for `id`. Callers
    /// must pass the value of `conn.last_insert_rowid()` captured
    /// immediately after the FTS INSERT, inside the same transaction. A
    /// missing map table is a silent no-op.
    fn store_entry(&self, conn: &Connection, id: &str, search_rowid: i64) -> Result<()> {
        let result = conn
            .prepare_cached(self.store_sql)
            .and_then(|mut stmt| stmt.execute(params![id, search_rowid]));
        match result {
            Ok(_) => Ok(()),
            Err(err) if self.is_missing_map_table_error(&err) => Ok(()),
            Err(err) => Err(StoreError::Sql(err)),
        }
    }

    /// Removes the map entry for `id`. A missing map table is a silent
    /// no-op.
    fn remove_entry(&self, conn: &Connection, id: &str) -> Result<()> {
        let result = conn
            .prepare_cached(self.remove_sql)
            .and_then(|mut stmt| stmt.execute(params![id]));
        match result {
            Ok(_) => Ok(()),
            Err(err) if self.is_missing_map_table_error(&err) => Ok(()),
            Err(err) => Err(StoreError::Sql(err)),
        }
    }

    /// Mapped rowid for `id`, if any. A missing map table reads as "no
    /// entry", which sends callers down the legacy full-scan path.
    fn mapped_rowid(&self, conn: &Connection, id: &str) -> Result<Option<i64>> {
        let result = conn
            .prepare_cached(self.lookup_sql)
            .and_then(|mut stmt| stmt.query_row(params![id], |row| row.get(0)).optional());
        match result {
            Ok(rowid) => Ok(rowid),
            Err(err) if self.is_missing_map_table_error(&err) => Ok(None),
            Err(err) => Err(StoreError::Sql(err)),
        }
    }

    /// Point lookup of the FTS id column at `search_rowid`. A map entry is
    /// used for deletion only when this returns true.
    fn fts_row_holds_id(&self, conn: &Connection, search_rowid: i64, id: &str) -> Result<bool> {
        let found: Option<Option<String>> = conn
            .prepare_cached(self.verify_sql)?
            .query_row(params![search_rowid], |row| row.get(0))
            .optional()?;
        Ok(matches!(found, Some(Some(existing)) if existing == id))
    }

    /// Removes every projection row for `id` plus its map entry. A verified
    /// map hit is one rowid point delete (constant work regardless of index
    /// size); anything else — no map table, no entry, stale or hijacked
    /// entry — falls back to the legacy full-scan DELETE on the UNINDEXED
    /// id column, which also removes legacy duplicate rows. The caller's
    /// reinsert then re-maps the id (self-healing).
    fn delete_projection_rows(&self, conn: &Connection, id: &str) -> Result<()> {
        if let Some(search_rowid) = self.mapped_rowid(conn, id)? {
            if self.fts_row_holds_id(conn, search_rowid, id)? {
                conn.prepare_cached(self.point_delete_sql)?
                    .execute(params![search_rowid])?;
                self.remove_entry(conn, id)?;
                return Ok(());
            }
        }
        self.remove_entry(conn, id)?;
        conn.prepare_cached(self.full_scan_delete_sql)?
            .execute(params![id])?;
        Ok(())
    }
}

/// Delete-then-insert projection maintenance for a history record that may
/// already be projected. The delete goes through
/// [`SearchRowidMapSpec::delete_projection_rows`]: a verified rowid-map hit
/// is a point delete, and only unmapped or unverifiable ids pay the legacy
/// O(index size) full scan (once — the reinsert re-maps them). Write paths
/// that can prove the base row is newly inserted call
/// [`insert_record_search_projection`] instead and skip the delete
/// entirely.
fn upsert_record_search_projection(conn: &Connection, record: &HistoryRecord) -> Result<()> {
    if !table_exists(conn, "ctx_history_search")? {
        return Ok(());
    }
    RECORD_SEARCH_ROWID_MAP.delete_projection_rows(conn, &record.id.to_string())?;
    insert_record_search_projection(conn, record)
}

/// Insert-only projection write for a history record proven absent from the
/// projection, mirroring the event-side insert/upsert projection split.
/// Novelty must be proven against the base table in the same transaction —
/// a successful plain `INSERT` on the primary key, or an indexed
/// pre-existence probe — never assumed from UUID uniqueness. Callers are
/// responsible for wrapping base write + projection write atomically. The
/// SQLite-assigned FTS rowid is captured immediately and stored in the
/// rowid map so later updates become point operations.
fn insert_record_search_projection(conn: &Connection, record: &HistoryRecord) -> Result<()> {
    if !table_exists(conn, "ctx_history_search")? {
        return Ok(());
    }
    conn.prepare_cached(
        r#"
        INSERT INTO ctx_history_search
        (record_id, title, summary, primary_user_text, decision_text, context_text, tag_text)
        VALUES (?1, ?2, ?3, ?4, '', ?5, ?6)
        "#,
    )?
    .execute(params![
        record.id.to_string(),
        local_preview(&record.title, 512),
        local_preview(&record.body, 2048),
        local_preview(&record.body, 2048),
        "",
        local_preview(&record.tags.join(" "), 1024),
    ])?;
    let search_rowid = conn.last_insert_rowid();
    RECORD_SEARCH_ROWID_MAP.store_entry(conn, &record.id.to_string(), search_rowid)?;
    Ok(())
}

/// Indexed pre-existence probe on the `history_records` primary key. Runs
/// inside the caller's transaction so the answer stays true for the
/// projection decision that follows it.
fn history_record_row_exists(conn: &Connection, id: Uuid) -> Result<bool> {
    let exists: i64 = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM history_records WHERE id = ?1)")?
        .query_row(params![id.to_string()], |row| row.get(0))?;
    Ok(exists != 0)
}

/// Indexed pre-existence probe on the `events` primary key. Runs inside the
/// caller's transaction so the answer stays true for the projection
/// decision that follows it.
fn event_row_exists(conn: &Connection, id: Uuid) -> Result<bool> {
    let exists: i64 = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM events WHERE id = ?1)")?
        .query_row(params![id.to_string()], |row| row.get(0))?;
    Ok(exists != 0)
}

fn ensure_search_projection_initialized(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "ctx_history_search")? {
        return Ok(());
    }

    // Emptiness probes only: a full `COUNT(*)` on an FTS5 table scans the
    // whole content tree, which made every `Store::open` pay O(index size).
    // Short-circuit existence checks decide the identical conservative
    // rebuild question ("is every existing projection empty?") in O(1).
    let projection_has_rows = table_has_rows(conn, "ctx_history_search")?
        || (table_exists(conn, "event_search")? && table_has_rows(conn, "event_search")?)
        || (table_exists(conn, "artifact_search")? && table_has_rows(conn, "artifact_search")?);
    if projection_has_rows {
        return Ok(());
    }

    if table_has_rows(conn, "history_records")?
        || table_has_rows(conn, "events")?
        || linked_artifact_preview_count(conn)? > 0
    {
        rebuild_search_projection(conn)?;
    }

    Ok(())
}

fn table_has_rows(conn: &Connection, table: &str) -> Result<bool> {
    match table {
        "artifacts" | "artifact_search" | "events" | "event_search" | "history_records"
        | "ctx_history_search" => {}
        _ => unreachable!("invalid table {table}"),
    }
    let sql = format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)");
    let has_rows: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
    Ok(has_rows != 0)
}

fn fixed_count(conn: &Connection, sql: &'static str) -> Result<u64> {
    let count: i64 = conn.query_row(sql, [], |row| row.get(0))?;
    u64::try_from(count).map_err(|_| StoreError::NumericOutOfRange {
        field: "profile count",
    })
}

fn linked_artifact_preview_count(conn: &Connection) -> Result<i64> {
    let _ = conn;
    Ok(0)
}

fn populate_event_search_projection(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT e.id,
               COALESCE(e.history_record_id, r.history_record_id, s.history_record_id, rs.history_record_id),
               e.session_id,
               e.role,
               e.event_type,
               e.payload_json,
               e.redaction_state
        FROM events e
        LEFT JOIN runs r ON r.id = e.run_id
        LEFT JOIN sessions s ON s.id = e.session_id
        LEFT JOIN sessions rs ON rs.id = r.session_id
        ORDER BY e.occurred_at_ms, e.seq, e.id
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?;
    let mut insert_event_search = conn.prepare(
        r#"
        INSERT INTO event_search
        (event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
    )?;
    for row in rows {
        let (
            event_id,
            history_record_id,
            session_id,
            role,
            event_type,
            payload_json,
            redaction_state,
        ) = row?;
        let preview = event_search_preview(&payload_json, &redaction_state)?;
        if preview.trim().is_empty() {
            continue;
        }
        insert_event_search.execute(params![
            event_id,
            history_record_id,
            session_id,
            role,
            preview,
            event_type
        ])?;
        let search_rowid = conn.last_insert_rowid();
        EVENT_SEARCH_ROWID_MAP.store_entry(conn, &event_id, search_rowid)?;
    }
    Ok(())
}

fn insert_event_search_projection_for_event(conn: &Connection, event: &Event) -> Result<()> {
    insert_event_search_projection_for_event_id(conn, event.id, event)
}

/// Delete-then-insert projection maintenance for an event that may already
/// be projected (including removing the row entirely when the new preview
/// is blank — a blank preview leaves neither an FTS row nor a map entry).
/// The delete goes through [`SearchRowidMapSpec::delete_projection_rows`]:
/// a verified rowid-map hit is a point delete, and only unmapped or
/// unverifiable ids pay the legacy O(index size) full scan (once — the
/// reinsert re-maps them). Write paths that can prove the event id is new
/// call [`insert_event_search_projection_for_event_id`] instead.
fn upsert_event_search_projection_for_event(
    conn: &Connection,
    event_id: Uuid,
    event: &Event,
) -> Result<()> {
    if !table_exists(conn, "event_search")? {
        return Ok(());
    }
    EVENT_SEARCH_ROWID_MAP.delete_projection_rows(conn, &event_id.to_string())?;
    insert_event_search_projection_for_event_id(conn, event_id, event)
}

fn insert_event_search_projection_for_event_id(
    conn: &Connection,
    event_id: Uuid,
    event: &Event,
) -> Result<()> {
    if !table_exists(conn, "event_search")? {
        return Ok(());
    }
    let preview = event_search_preview_from_payload(&event.payload, event.redaction_state);
    if preview.trim().is_empty() {
        return Ok(());
    }
    conn.prepare_cached(
        r#"
        INSERT INTO event_search
        (event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
    )?
    .execute(params![
        event_id.to_string(),
        optional_uuid_string(event.history_record_id),
        optional_uuid_string(event.session_id),
        event.role.map(|role| role.as_str()),
        preview,
        event.event_type.as_str(),
    ])?;
    let search_rowid = conn.last_insert_rowid();
    EVENT_SEARCH_ROWID_MAP.store_entry(conn, &event_id.to_string(), search_rowid)?;
    Ok(())
}

fn event_search_preview(payload_json: &str, redaction_state: &str) -> Result<String> {
    if redaction_state == RedactionState::Raw.as_str() {
        return Ok("raw event payload withheld".to_owned());
    }
    let payload: serde_json::Value = serde_json::from_str(payload_json)?;
    Ok(event_search_preview_from_payload(
        &payload,
        parse_text_enum::<RedactionState>(redaction_state.to_owned())?,
    ))
}

fn event_search_preview_from_payload(
    payload: &serde_json::Value,
    redaction_state: RedactionState,
) -> String {
    if redaction_state == RedactionState::Raw {
        return "raw event payload withheld".to_owned();
    }
    let preview = event_payload_preview(payload)
        .or_else(|| {
            if payload.is_object() || payload.is_array() {
                Some(payload.to_string())
            } else {
                None
            }
        })
        .unwrap_or_default();
    local_preview(&preview, 2048)
}

fn local_preview(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

fn event_payload_preview(payload: &serde_json::Value) -> Option<String> {
    if let Some(body) = payload.get("body") {
        if let Some(preview) = event_value_preview(body) {
            return Some(preview);
        }
    }
    event_value_preview(payload)
}

fn event_value_preview(value: &serde_json::Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return non_blank(value);
    }
    let object = value.as_object()?;
    for key in [
        "text",
        "preview",
        "summary",
        "command",
        "output_preview",
        "output",
        "message",
    ] {
        if let Some(value) = object.get(key).and_then(event_preview_fragment) {
            return Some(value);
        }
    }
    let structured = ["tool", "name", "arguments_preview", "status"]
        .into_iter()
        .filter_map(|key| {
            object
                .get(key)
                .and_then(event_preview_fragment)
                .map(|value| format!("{key}: {value}"))
        })
        .collect::<Vec<_>>();
    if structured.is_empty() {
        None
    } else {
        Some(structured.join(" | "))
    }
}

fn event_preview_fragment(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => non_blank(value),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

fn non_blank(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn file_touch_match_values(file: &str) -> Option<(String, String)> {
    let exact = file.trim();
    if exact.is_empty() {
        return None;
    }
    let suffix = exact.trim_start_matches(['/', '\\']);
    Some((
        exact.to_owned(),
        format!("%/{}", escape_like_pattern(suffix)),
    ))
}

fn escape_like_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn migrate_to_v1(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        ensure_columns(conn, "history_records", HISTORY_RECORD_COLUMNS)?;
        backfill_legacy_tables(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 1;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v2(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        ensure_columns(conn, "history_records", HISTORY_RECORD_COLUMNS)?;
        backfill_legacy_tables(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 2;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v3(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        ensure_columns(conn, "history_records", HISTORY_RECORD_COLUMNS)?;
        backfill_legacy_tables(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 3;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v4(conn: &Connection) -> Result<()> {
    let foreign_keys_enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    conn.execute_batch("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        rebuild_capture_sources_provider_check(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 4;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Err(err)
        }
    }
}

fn migrate_to_v5(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        ensure_columns(
            conn,
            "catalog_sessions",
            CATALOG_SESSION_IMPORT_STATE_COLUMNS,
        )?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 5;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v6(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        ensure_columns(
            conn,
            "catalog_sessions",
            CATALOG_SESSION_IMPORT_STATE_COLUMNS,
        )?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 6;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v7(conn: &Connection) -> Result<()> {
    let foreign_keys_enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    conn.execute_batch("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        rebuild_capture_sources_provider_check(conn)?;
        rebuild_catalog_sessions_provider_check(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 7;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Err(err)
        }
    }
}

fn migrate_to_v8(conn: &Connection) -> Result<()> {
    let foreign_keys_enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    conn.execute_batch("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        drop_legacy_history_record_indexes(conn)?;
        rename_table_if_exists(conn, "work_record_links", "history_record_links")?;
        rename_table_if_exists(conn, "work_record_tags", "history_record_tags")?;
        rename_table_if_exists(conn, "work_records", "history_records")?;
        for table in ["sessions", "runs", "events", "summaries", "files_touched"] {
            rename_column_if_exists(conn, table, "work_record_id", "history_record_id")?;
        }
        rename_column_if_exists(
            conn,
            "history_record_links",
            "work_record_id",
            "history_record_id",
        )?;
        rename_column_if_exists(
            conn,
            "history_record_tags",
            "work_record_id",
            "history_record_id",
        )?;
        rewrite_history_table_names(conn, "sync_outbox", "local_table")?;
        rewrite_history_table_names(conn, "audit_log", "target_table")?;
        drop_fts_table_if_column_exists(conn, "event_search", "work_record_id")?;
        drop_fts_table_if_column_exists(conn, "artifact_search", "work_record_id")?;
        conn.execute_batch(CREATE_TABLES_SQL)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 8;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Err(err)
        }
    }
}

fn migrate_to_v9(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 9;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v10(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 10;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v11(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        rebuild_search_projection(conn)?;
        conn.execute_batch("PRAGMA user_version = 11;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v12(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        invalidate_provider_import_indexes(conn)?;
        rebuild_search_projection(conn)?;
        conn.execute_batch("PRAGMA user_version = 12;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v13(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        create_stable_sql_views(conn)?;
        conn.execute_batch("PRAGMA user_version = 13;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn migrate_to_v14(conn: &Connection) -> Result<()> {
    let foreign_keys_enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    conn.execute_batch("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        ensure_columns(
            conn,
            "catalog_sessions",
            CATALOG_SESSION_IMPORT_STATE_COLUMNS,
        )?;
        rebuild_capture_sources_provider_check(conn)?;
        rebuild_catalog_sessions_provider_check(conn)?;
        rebuild_source_import_files_provider_check(conn)?;
        backfill_catalog_session_import_checkpoints(conn)?;
        create_stable_sql_views(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 14;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Err(err)
        }
    }
}

fn migrate_to_v15(conn: &Connection) -> Result<()> {
    let foreign_keys_enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    conn.execute_batch("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        if stable_sql_views_exist(conn)? {
            drop_stable_sql_views(conn)?;
        }
        rebuild_capture_sources_provider_check(conn)?;
        rebuild_catalog_sessions_provider_check(conn)?;
        rebuild_source_import_files_provider_check(conn)?;
        conn.execute_batch(INDEXES_SQL)?;
        create_stable_sql_views(conn)?;
        conn.execute_batch("PRAGMA user_version = 15;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            if foreign_keys_enabled != 0 {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            }
            Err(err)
        }
    }
}

/// First fork schema divergence: v15 → v1000 (upstream chain ends at v15;
/// fork versions start at 1000 per docs/fork-plan.md decision 9).
///
/// Creates the persistent FTS rowid map tables and nothing else. There is
/// deliberately no FTS rebuild and no map backfill: existing search
/// projections stay byte-for-byte intact (rowids included) and the maps
/// start empty. Every map entry is created lazily by the first
/// post-migration write that touches its row — the legacy full-scan delete
/// runs once per updated row, then the stored rowid makes later updates
/// point operations. Rolling back to an older binary is not supported once
/// a store reaches v1000 (older binaries refuse to open the version); the
/// data itself is unchanged by this migration, so unsupported external
/// recovery is possible by dropping the two map tables and resetting
/// `PRAGMA user_version` to 15 in one transaction (exact steps in
/// docs/storage.md), or by rebuilding the index from provider history.
fn migrate_to_v1000(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(SEARCH_ROWID_MAP_TABLES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 1000;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

/// Fork schema v1000 → v1001: bounded pagination keyset indexes.
///
/// Creates the two covering indexes in [`V1001_INDEXES_SQL`] and nothing
/// else. The v1000 rowid maps and every FTS projection are untouched — no
/// rebuild, no backfill, no rowid churn — so a v1000 store upgrades in
/// place. Unsupported external recovery mirrors v1000's: drop the two
/// indexes and reset `PRAGMA user_version` to 1000 in one transaction
/// (exact steps in docs/storage.md).
fn migrate_to_v1001(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(V1001_INDEXES_SQL)?;
        conn.execute_batch("PRAGMA user_version = 1001;")?;
        Ok(())
    })();

    match migration {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
                return Err(StoreError::Sql(rollback_err));
            }
            Err(err)
        }
    }
}

fn create_stable_sql_views(conn: &Connection) -> Result<()> {
    conn.execute_batch(STABLE_SQL_VIEWS_SQL)?;
    Ok(())
}

fn drop_stable_sql_views(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        DROP VIEW IF EXISTS ctx_sessions;
        DROP VIEW IF EXISTS ctx_events;
        DROP VIEW IF EXISTS ctx_files_touched;
        DROP VIEW IF EXISTS ctx_sources;
        "#,
    )?;
    Ok(())
}

fn stable_sql_views_exist(conn: &Connection) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = 'ctx_sessions'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn invalidate_provider_import_indexes(conn: &Connection) -> Result<()> {
    if table_exists(conn, "catalog_sessions")? {
        conn.execute(
            r#"
            UPDATE catalog_sessions
            SET indexed_at_ms = NULL,
                indexed_file_size_bytes = NULL,
                indexed_file_modified_at_ms = NULL,
                indexed_status = 'pending',
                indexed_error = NULL,
                indexed_event_count = NULL
            WHERE indexed_status = 'indexed'
            "#,
            [],
        )?;
    }
    if table_exists(conn, "source_import_files")? {
        conn.execute(
            r#"
            UPDATE source_import_files
            SET indexed_at_ms = NULL,
                indexed_file_size_bytes = NULL,
                indexed_file_modified_at_ms = NULL,
                indexed_status = 'pending',
                indexed_error = NULL
            WHERE indexed_status = 'indexed'
            "#,
            [],
        )?;
    }
    Ok(())
}

fn backfill_catalog_session_import_checkpoints(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "catalog_sessions")? {
        return Ok(());
    }
    conn.execute(
        r#"
        UPDATE catalog_sessions
        SET last_imported_at_ms = indexed_at_ms,
            last_imported_file_size_bytes = indexed_file_size_bytes,
            last_imported_file_modified_at_ms = indexed_file_modified_at_ms,
            last_imported_event_count = indexed_event_count
        WHERE last_imported_file_size_bytes IS NULL
          AND indexed_file_size_bytes IS NOT NULL
        "#,
        [],
    )?;
    Ok(())
}

fn drop_legacy_history_record_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        DROP INDEX IF EXISTS idx_work_records_primary_vcs_workspace_id;
        DROP INDEX IF EXISTS idx_work_records_source_id;
        DROP INDEX IF EXISTS idx_work_records_last_activity_at_ms;
        DROP INDEX IF EXISTS idx_work_records_created_at;
        DROP INDEX IF EXISTS idx_sessions_work_record_id;
        DROP INDEX IF EXISTS idx_runs_work_record_started_at_ms;
        DROP INDEX IF EXISTS idx_runs_work_record_id;
        DROP INDEX IF EXISTS idx_events_work_record_occurred_at_ms;
        DROP INDEX IF EXISTS idx_events_work_record_id;
        DROP INDEX IF EXISTS idx_work_record_links_work_record_id;
        DROP INDEX IF EXISTS idx_work_record_links_source_id;
        DROP INDEX IF EXISTS idx_summaries_work_record_id;
        DROP INDEX IF EXISTS idx_files_touched_work_record_id;
        DROP INDEX IF EXISTS idx_work_record_tags_tag_id;
        DROP INDEX IF EXISTS idx_work_record_tags_source_id;
        "#,
    )?;
    Ok(())
}

fn rename_table_if_exists(conn: &Connection, old: &str, new: &str) -> Result<()> {
    if table_exists(conn, old)? && !table_exists(conn, new)? {
        conn.execute(&format!("ALTER TABLE {old} RENAME TO {new}"), [])?;
    }
    Ok(())
}

fn rename_column_if_exists(conn: &Connection, table: &str, old: &str, new: &str) -> Result<()> {
    if table_exists(conn, table)?
        && table_has_column(conn, table, old)?
        && !table_has_column(conn, table, new)?
    {
        conn.execute(
            &format!("ALTER TABLE {table} RENAME COLUMN {old} TO {new}"),
            [],
        )?;
    }
    Ok(())
}

fn rewrite_history_table_names(conn: &Connection, table: &str, column: &str) -> Result<()> {
    if !table_exists(conn, table)? || !table_has_column(conn, table, column)? {
        return Ok(());
    }
    conn.execute(
        &format!(
            "UPDATE {table}
             SET {column} = CASE {column}
                WHEN 'work_records' THEN 'history_records'
                WHEN 'work_record_links' THEN 'history_record_links'
                WHEN 'work_record_tags' THEN 'history_record_tags'
                ELSE {column}
             END
             WHERE {column} IN ('work_records', 'work_record_links', 'work_record_tags')"
        ),
        [],
    )?;
    Ok(())
}

fn drop_fts_table_if_column_exists(conn: &Connection, table: &str, column: &str) -> Result<()> {
    if table_exists(conn, table)? && table_has_column(conn, table, column)? {
        conn.execute(&format!("DROP TABLE {table}"), [])?;
    }
    Ok(())
}

fn rebuild_capture_sources_provider_check(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "capture_sources")? {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        return Ok(());
    }

    let recreate_views = stable_sql_views_exist(conn)?;
    if recreate_views {
        drop_stable_sql_views(conn)?;
    }
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS capture_sources_new;
        CREATE TABLE capture_sources_new (
            id TEXT PRIMARY KEY NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('provider_import', 'provider_hook', 'direct_cli', 'manual')),
            provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'pi', 'opencode', 'antigravity', 'gemini', 'cursor', 'copilot_cli', 'factory_ai_droid', 'openclaw', 'hermes', 'nanoclaw', 'astrbot', 'shell', 'git', 'jj', 'gh', 'custom', 'unknown')),
            machine_id TEXT NOT NULL,
            process_id INTEGER,
            cwd TEXT,
            raw_source_path TEXT,
            external_session_id TEXT,
            started_at_ms INTEGER NOT NULL,
            ended_at_ms INTEGER,
            fidelity TEXT NOT NULL CHECK (fidelity IN ('full', 'partial', 'imported', 'inferred', 'summary_only')),
            visibility TEXT NOT NULL DEFAULT 'local_only' CHECK (visibility IN ('local_only', 'reportable', 'sync_metadata', 'sync_full', 'withheld')),
            sync_state TEXT NOT NULL DEFAULT 'local_only' CHECK (sync_state IN ('local_only', 'pending', 'synced', 'failed', 'withheld')),
            sync_version INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        INSERT INTO capture_sources_new
        (id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json)
        SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json
        FROM capture_sources;
        DROP TABLE capture_sources;
        ALTER TABLE capture_sources_new RENAME TO capture_sources;
        "#,
    )?;
    if recreate_views {
        create_stable_sql_views(conn)?;
    }
    Ok(())
}

fn rebuild_catalog_sessions_provider_check(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "catalog_sessions")? {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        return Ok(());
    }

    let recreate_views = stable_sql_views_exist(conn)?;
    if recreate_views {
        drop_stable_sql_views(conn)?;
    }
    ensure_columns(
        conn,
        "catalog_sessions",
        CATALOG_SESSION_IMPORT_STATE_COLUMNS,
    )?;
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS catalog_sessions_new;
        CREATE TABLE catalog_sessions_new (
            source_path TEXT PRIMARY KEY NOT NULL,
            provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'pi', 'opencode', 'antigravity', 'gemini', 'cursor', 'copilot_cli', 'factory_ai_droid', 'openclaw', 'hermes', 'nanoclaw', 'astrbot', 'shell', 'git', 'jj', 'gh', 'custom', 'unknown')),
            source_format TEXT NOT NULL,
            source_root TEXT NOT NULL,
            external_session_id TEXT,
            parent_external_session_id TEXT,
            agent_type TEXT NOT NULL CHECK (agent_type IN ('primary', 'subagent', 'agent_team_member', 'reviewer', 'implementer', 'unknown')),
            role_hint TEXT,
            external_agent_id TEXT,
            cwd TEXT,
            session_started_at_ms INTEGER,
            file_size_bytes INTEGER NOT NULL,
            file_modified_at_ms INTEGER NOT NULL,
            cataloged_at_ms INTEGER NOT NULL,
            is_stale INTEGER NOT NULL DEFAULT 0,
            indexed_at_ms INTEGER,
            indexed_file_size_bytes INTEGER,
            indexed_file_modified_at_ms INTEGER,
            indexed_status TEXT NOT NULL DEFAULT 'pending' CHECK (indexed_status IN ('pending', 'indexed', 'failed')),
            indexed_error TEXT,
            indexed_event_count INTEGER,
            last_imported_at_ms INTEGER,
            last_imported_file_size_bytes INTEGER,
            last_imported_file_modified_at_ms INTEGER,
            last_imported_file_sha256 TEXT,
            last_imported_event_count INTEGER,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        INSERT INTO catalog_sessions_new
        (source_path, provider, source_format, source_root, external_session_id, parent_external_session_id, agent_type, role_hint, external_agent_id, cwd, session_started_at_ms, file_size_bytes, file_modified_at_ms, cataloged_at_ms, is_stale, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms, indexed_status, indexed_error, indexed_event_count, last_imported_at_ms, last_imported_file_size_bytes, last_imported_file_modified_at_ms, last_imported_file_sha256, last_imported_event_count, metadata_json)
        SELECT source_path, provider, source_format, source_root, external_session_id, parent_external_session_id, agent_type, role_hint, external_agent_id, cwd, session_started_at_ms, file_size_bytes, file_modified_at_ms, cataloged_at_ms, is_stale, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms, indexed_status, indexed_error, indexed_event_count, last_imported_at_ms, last_imported_file_size_bytes, last_imported_file_modified_at_ms, last_imported_file_sha256, last_imported_event_count, metadata_json
        FROM catalog_sessions;
        DROP TABLE catalog_sessions;
        ALTER TABLE catalog_sessions_new RENAME TO catalog_sessions;
        "#,
    )?;
    if recreate_views {
        create_stable_sql_views(conn)?;
    }
    Ok(())
}

fn rebuild_source_import_files_provider_check(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "source_import_files")? {
        conn.execute_batch(CREATE_TABLES_SQL)?;
        return Ok(());
    }

    let recreate_views = stable_sql_views_exist(conn)?;
    if recreate_views {
        drop_stable_sql_views(conn)?;
    }
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS source_import_files_new;
        CREATE TABLE source_import_files_new (
            provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'pi', 'opencode', 'antigravity', 'gemini', 'cursor', 'copilot_cli', 'factory_ai_droid', 'openclaw', 'hermes', 'nanoclaw', 'astrbot', 'shell', 'git', 'jj', 'gh', 'custom', 'unknown')),
            source_format TEXT NOT NULL,
            source_root TEXT NOT NULL,
            source_path TEXT NOT NULL,
            file_size_bytes INTEGER NOT NULL,
            file_modified_at_ms INTEGER NOT NULL,
            observed_at_ms INTEGER NOT NULL,
            is_stale INTEGER NOT NULL DEFAULT 0,
            indexed_at_ms INTEGER,
            indexed_file_size_bytes INTEGER,
            indexed_file_modified_at_ms INTEGER,
            indexed_status TEXT NOT NULL DEFAULT 'pending' CHECK (indexed_status IN ('pending', 'indexed', 'failed')),
            indexed_error TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (provider, source_root, source_path)
        );
        INSERT INTO source_import_files_new
        (provider, source_format, source_root, source_path, file_size_bytes, file_modified_at_ms, observed_at_ms, is_stale, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms, indexed_status, indexed_error, metadata_json)
        SELECT provider, source_format, source_root, source_path, file_size_bytes, file_modified_at_ms, observed_at_ms, is_stale, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms, indexed_status, indexed_error, metadata_json
        FROM source_import_files;
        DROP TABLE source_import_files;
        ALTER TABLE source_import_files_new RENAME TO source_import_files;
        "#,
    )?;
    if recreate_views {
        create_stable_sql_views(conn)?;
    }
    Ok(())
}

fn create_fts_tables_if_supported(conn: &Connection) -> Result<()> {
    match conn.execute_batch(FTS_TABLES_SQL) {
        Ok(()) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(error, message))
            if is_missing_fts_module(error.extended_code, message.as_deref()) =>
        {
            Ok(())
        }
        Err(err) => Err(StoreError::Sql(err)),
    }
}

fn is_missing_fts_module(extended_code: i32, message: Option<&str>) -> bool {
    extended_code == rusqlite::ffi::SQLITE_ERROR
        && message
            .map(|value| value.contains("no such module: fts5"))
            .unwrap_or(false)
}

struct ColumnSpec {
    name: &'static str,
    definition: &'static str,
}

fn ensure_columns(conn: &Connection, table: &str, columns: &[ColumnSpec]) -> Result<()> {
    for column in columns {
        if !table_has_column(conn, table, column.name)? {
            let sql = format!("ALTER TABLE {table} ADD COLUMN {}", column.definition);
            conn.execute(&sql, [])?;
        }
    }
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let sql = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn reject_provider_event_hash_conflict(conn: &Connection, dedupe_key: &str) -> Result<()> {
    let Some((provider, external_session_id, provider_index, _new_hash)) =
        parse_provider_event_dedupe_key(dedupe_key)
    else {
        return Ok(());
    };
    let prefix = provider_event_dedupe_key_prefix(&provider, &external_session_id, provider_index);
    let upper_bound = provider_event_dedupe_key_upper_bound(&prefix);
    let mut stmt = conn.prepare(
        "SELECT dedupe_key FROM events
         WHERE dedupe_key >= ?1 AND dedupe_key < ?2
         ORDER BY dedupe_key",
    )?;
    let rows = stmt.query_map(params![prefix, upper_bound], |row| row.get::<_, String>(0))?;
    reject_provider_event_hash_conflict_from_rows(dedupe_key, rows)
}

fn reject_provider_event_hash_conflict_tx(tx: &Transaction<'_>, dedupe_key: &str) -> Result<()> {
    let Some((provider, external_session_id, provider_index, _new_hash)) =
        parse_provider_event_dedupe_key(dedupe_key)
    else {
        return Ok(());
    };
    let prefix = provider_event_dedupe_key_prefix(&provider, &external_session_id, provider_index);
    let upper_bound = provider_event_dedupe_key_upper_bound(&prefix);
    let mut stmt = tx.prepare(
        "SELECT dedupe_key FROM events
         WHERE dedupe_key >= ?1 AND dedupe_key < ?2
         ORDER BY dedupe_key",
    )?;
    let rows = stmt.query_map(params![prefix, upper_bound], |row| row.get::<_, String>(0))?;
    reject_provider_event_hash_conflict_from_rows(dedupe_key, rows)
}

fn reject_provider_event_hash_conflict_from_rows(
    dedupe_key: &str,
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<String>>,
) -> Result<()> {
    let Some((provider, external_session_id, provider_index, new_hash)) =
        parse_provider_event_dedupe_key(dedupe_key)
    else {
        return Ok(());
    };
    for row in rows {
        let existing_key = row?;
        let Some((existing_provider, existing_session_id, existing_index, existing_hash)) =
            parse_provider_event_dedupe_key(&existing_key)
        else {
            continue;
        };
        if existing_provider == provider
            && existing_session_id == external_session_id
            && existing_index == provider_index
            && existing_hash != new_hash
        {
            return Err(StoreError::ProviderEventConflict {
                provider,
                external_session_id,
                provider_index,
                existing_hash,
                new_hash,
            });
        }
    }
    Ok(())
}

fn provider_event_dedupe_key_prefix(
    provider: &str,
    external_session_id: &str,
    provider_index: u64,
) -> String {
    format!("provider:{provider}:{external_session_id}:{provider_index}:")
}

fn provider_event_dedupe_key_upper_bound(prefix: &str) -> String {
    let mut upper_bound = prefix.to_owned();
    upper_bound.push(char::MAX);
    upper_bound
}

fn parse_provider_event_dedupe_key(dedupe_key: &str) -> Option<(String, String, u64, String)> {
    let mut parts = dedupe_key.splitn(5, ':');
    let prefix = parts.next()?;
    if prefix != "provider" {
        return None;
    }
    let provider = parts.next()?.to_owned();
    let external_session_id = parts.next()?.to_owned();
    let provider_index = parts.next()?.parse().ok()?;
    let payload_hash = parts.next()?.to_owned();
    if provider.is_empty() || external_session_id.is_empty() || payload_hash.is_empty() {
        None
    } else {
        Some((provider, external_session_id, provider_index, payload_hash))
    }
}

fn record_sections_match_plan(plan: &SearchQueryPlan, record: &HistoryRecord) -> bool {
    plan.matches_text(&record.title)
        || plan.matches_text(&record.body)
        || record.tags.iter().any(|tag| plan.matches_text(tag))
        || record
            .workspace
            .as_deref()
            .is_some_and(|workspace| plan.matches_text(workspace))
}

fn backfill_legacy_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        UPDATE history_records
        SET summary = body
        WHERE summary IS NULL;

        UPDATE history_records
        SET created_at_ms = COALESCE(CAST(strftime('%s', created_at) AS INTEGER) * 1000, created_at_ms)
        WHERE created_at_ms = 0 AND created_at IS NOT NULL;

        UPDATE history_records
        SET updated_at_ms = COALESCE(CAST(strftime('%s', updated_at) AS INTEGER) * 1000, updated_at_ms)
        WHERE updated_at_ms = 0 AND updated_at IS NOT NULL;

        UPDATE history_records
        SET started_at_ms = created_at_ms
        WHERE started_at_ms IS NULL AND created_at_ms != 0;

        UPDATE history_records
        SET last_activity_at_ms = CASE
            WHEN updated_at_ms != 0 THEN updated_at_ms
            WHEN created_at_ms != 0 THEN created_at_ms
            ELSE last_activity_at_ms
        END
        WHERE last_activity_at_ms = 0;
        "#,
    )?;
    Ok(())
}

fn count_foreign_key_failures(conn: &Connection) -> Result<i64> {
    let mut stmt = conn.prepare("PRAGMA foreign_key_check")?;
    let mut rows = stmt.query([])?;
    let mut count = 0;
    while rows.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

fn timestamp_ms(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

fn capped_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn nonnegative_i64_to_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

fn time_ms(value: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(value).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut value, "{byte:02x}");
    }
    value
}

fn ensure_regular_blob_file(id: Uuid, path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(StoreError::ArchiveArtifactNonRegularFile {
            id,
            path: path.to_path_buf(),
        })
    }
}

#[derive(Debug, Default)]
struct BlobWriteGuard {
    created_paths: Vec<PathBuf>,
    committed: bool,
}

impl BlobWriteGuard {
    fn commit(&mut self) {
        self.committed = true;
        self.created_paths.clear();
    }
}

impl Drop for BlobWriteGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for path in self.created_paths.iter().rev() {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(unix)]
fn restrict_private_dir(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_private_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_private_file(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_private_file(_path: &Path) -> Result<()> {
    Ok(())
}

pub fn validate_archive_version(archive: &SessionHistoryArchive) -> Result<()> {
    if matches!((archive.schema_version, archive.version), (1, 1) | (2, 2)) {
        Ok(())
    } else {
        Err(StoreError::UnsupportedArchiveVersion(
            archive.schema_version.max(archive.version),
        ))
    }
}

fn reject_import_conflicts(tx: &Transaction<'_>, archive: &SessionHistoryArchive) -> Result<()> {
    for record in &archive.records {
        if row_exists(tx, "history_records", record.id)? {
            return Err(StoreError::ImportConflict {
                kind: "record",
                id: record.id,
            });
        }
    }
    reject_rich_import_conflicts(tx, archive)?;
    Ok(())
}

fn reject_capture_source_import_conflict(tx: &Transaction<'_>, source_id: Uuid) -> Result<()> {
    if row_exists(tx, "capture_sources", source_id)? {
        return Err(StoreError::ImportConflict {
            kind: "capture_source",
            id: source_id,
        });
    }
    Ok(())
}

fn reject_import_invariant_conflicts(
    tx: &Transaction<'_>,
    archive: &SessionHistoryArchive,
) -> Result<()> {
    if archive.schema_version < 2 && archive.version < 2 {
        return Ok(());
    }

    for event in &archive.events {
        if let Some(dedupe_key) = &event.dedupe_key {
            reject_provider_event_hash_conflict_tx(tx, dedupe_key)?;
        }
    }
    Ok(())
}

fn row_exists(tx: &Transaction<'_>, table: &str, id: Uuid) -> Result<bool> {
    let sql = format!("SELECT 1 FROM {table} WHERE id = ?1");
    Ok(tx
        .query_row(&sql, params![id.to_string()], |_| Ok(()))
        .optional()?
        .is_some())
}

fn reject_rich_import_conflicts(
    tx: &Transaction<'_>,
    archive: &SessionHistoryArchive,
) -> Result<()> {
    if archive.schema_version < 2 && archive.version < 2 {
        return Ok(());
    }

    for source in &archive.capture_sources {
        reject_entity_conflict(
            existing_capture_source_by_id(tx, source.id)?,
            source,
            "capture_source",
            source.id,
        )?;
        if let Some(external_session_id) = &source.descriptor.external_session_id {
            reject_entity_conflict(
                existing_capture_source_by_external_session(
                    tx,
                    source.descriptor.provider,
                    external_session_id,
                )?,
                source,
                "capture_source",
                source.id,
            )?;
        }
    }
    for workspace in &archive.vcs_workspaces {
        reject_entity_conflict(
            existing_vcs_workspace_by_id(tx, workspace.id)?,
            workspace,
            "vcs_workspace",
            workspace.id,
        )?;
        reject_entity_conflict(
            existing_vcs_workspace_by_identity(tx, workspace)?,
            workspace,
            "vcs_workspace",
            workspace.id,
        )?;
    }
    for artifact in &archive.artifact_records {
        reject_entity_conflict(
            existing_artifact_by_id(tx, artifact.id)?,
            artifact,
            "artifact",
            artifact.id,
        )?;
        reject_entity_conflict(
            existing_artifact_by_identity(tx, artifact)?,
            artifact,
            "artifact",
            artifact.id,
        )?;
    }
    for session in &archive.sessions {
        reject_entity_conflict(
            existing_session_by_id(tx, session.id)?,
            session,
            "session",
            session.id,
        )?;
        if let Some(external_session_id) = &session.external_session_id {
            reject_entity_conflict(
                existing_session_by_external_session(tx, session.provider, external_session_id)?,
                session,
                "session",
                session.id,
            )?;
        }
    }
    for run in &archive.runs {
        reject_entity_conflict(existing_run_by_id(tx, run.id)?, run, "run", run.id)?;
    }
    for event in &archive.events {
        reject_entity_conflict(
            existing_event_by_id(tx, event.id)?,
            event,
            "event",
            event.id,
        )?;
        reject_entity_conflict(
            existing_event_by_seq(tx, event.seq)?,
            event,
            "event",
            event.id,
        )?;
        if let Some(dedupe_key) = &event.dedupe_key {
            reject_provider_event_hash_conflict_tx(tx, dedupe_key)?;
            reject_entity_conflict(
                existing_event_by_dedupe_key(tx, dedupe_key)?,
                event,
                "event",
                event.id,
            )?;
        }
    }
    for change in &archive.vcs_changes {
        reject_entity_conflict(
            existing_vcs_change_by_id(tx, change.id)?,
            change,
            "vcs_change",
            change.id,
        )?;
        reject_entity_conflict(
            existing_vcs_change_by_identity(tx, change)?,
            change,
            "vcs_change",
            change.id,
        )?;
    }
    for summary in &archive.summaries {
        reject_entity_conflict(
            existing_summary_by_id(tx, summary.id)?,
            summary,
            "summary",
            summary.id,
        )?;
    }
    for file in &archive.files_touched {
        reject_entity_conflict(
            existing_file_touched_by_id(tx, file.id)?,
            file,
            "file_touched",
            file.id,
        )?;
    }
    for link in &archive.history_record_links {
        reject_entity_conflict(
            existing_history_record_link_by_id(tx, link.id)?,
            link,
            "history_record_link",
            link.id,
        )?;
        reject_entity_conflict(
            existing_history_record_link_by_identity(tx, link)?,
            link,
            "history_record_link",
            link.id,
        )?;
    }
    Ok(())
}

fn reject_archive_event_internal_conflicts(archive: &SessionHistoryArchive) -> Result<()> {
    let mut seen_seq: HashMap<u64, &Event> = HashMap::new();
    let mut seen_provider_events: HashMap<(String, String, u64), String> = HashMap::new();

    for event in &archive.events {
        if let Some(existing) = seen_seq.insert(event.seq, event) {
            if existing != event {
                return Err(StoreError::ImportConflict {
                    kind: "event",
                    id: event.id,
                });
            }
        }

        let Some(dedupe_key) = &event.dedupe_key else {
            continue;
        };
        let Some((provider, external_session_id, provider_index, payload_hash)) =
            parse_provider_event_dedupe_key(dedupe_key)
        else {
            continue;
        };
        let key = (provider, external_session_id, provider_index);
        if let Some(existing_hash) = seen_provider_events.get(&key) {
            if existing_hash != &payload_hash {
                return Err(StoreError::ProviderEventConflict {
                    provider: key.0,
                    external_session_id: key.1,
                    provider_index: key.2,
                    existing_hash: existing_hash.clone(),
                    new_hash: payload_hash,
                });
            }
        } else {
            seen_provider_events.insert(key, payload_hash);
        }
    }

    Ok(())
}

fn reject_entity_conflict<T: PartialEq>(
    existing: Option<T>,
    incoming: &T,
    kind: &'static str,
    id: Uuid,
) -> Result<()> {
    if let Some(existing) = existing {
        if existing != *incoming {
            return Err(StoreError::ImportConflict { kind, id });
        }
    }
    Ok(())
}

fn existing_capture_source_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<CaptureSource>> {
    tx.query_row(
        "SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources WHERE id = ?1",
        params![id.to_string()],
        capture_source_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_capture_source_by_external_session(
    tx: &Transaction<'_>,
    provider: CaptureProvider,
    external_session_id: &str,
) -> Result<Option<CaptureSource>> {
    tx.query_row(
        "SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources WHERE provider = ?1 AND external_session_id = ?2 ORDER BY started_at_ms DESC LIMIT 1",
        params![provider.as_str(), external_session_id],
        capture_source_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_session_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<Session>> {
    tx.query_row(
        session_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        session_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_session_by_external_session(
    tx: &Transaction<'_>,
    provider: CaptureProvider,
    external_session_id: &str,
) -> Result<Option<Session>> {
    tx.query_row(
        session_select_sql(
            "WHERE provider = ?1 AND external_session_id = ?2 ORDER BY started_at_ms DESC LIMIT 1",
        )
        .as_str(),
        params![provider.as_str(), external_session_id],
        session_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_run_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<Run>> {
    tx.query_row(
        run_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        run_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_event_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<Event>> {
    tx.query_row(
        event_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        event_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_event_by_dedupe_key(tx: &Transaction<'_>, dedupe_key: &str) -> Result<Option<Event>> {
    tx.query_row(
        event_select_sql("WHERE dedupe_key = ?1").as_str(),
        params![dedupe_key],
        event_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_event_by_seq(tx: &Transaction<'_>, seq: u64) -> Result<Option<Event>> {
    tx.query_row(
        event_select_sql("WHERE seq = ?1").as_str(),
        params![seq as i64],
        event_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_artifact_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<Artifact>> {
    tx.query_row(
        artifact_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        artifact_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_artifact_by_hash_kind(
    tx: &Transaction<'_>,
    blob_hash: &str,
    kind: ArtifactKind,
) -> Result<Option<Artifact>> {
    tx.query_row(
        artifact_select_sql("WHERE blob_hash = ?1 AND kind = ?2").as_str(),
        params![blob_hash, kind.as_str()],
        artifact_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_artifact_by_identity(
    tx: &Transaction<'_>,
    artifact: &Artifact,
) -> Result<Option<Artifact>> {
    existing_artifact_by_hash_kind(tx, &artifact.blob_hash, artifact.kind)
}

fn existing_vcs_workspace_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<VcsWorkspace>> {
    tx.query_row(
        vcs_workspace_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        vcs_workspace_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_vcs_workspace_by_identity(
    tx: &Transaction<'_>,
    workspace: &VcsWorkspace,
) -> Result<Option<VcsWorkspace>> {
    tx.query_row(
        vcs_workspace_select_sql("WHERE kind = ?1 AND repo_fingerprint = ?2").as_str(),
        params![workspace.kind.as_str(), workspace.repo_fingerprint.as_str()],
        vcs_workspace_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_vcs_change_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<VcsChange>> {
    tx.query_row(
        vcs_change_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        vcs_change_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_vcs_change_by_identity(
    tx: &Transaction<'_>,
    change: &VcsChange,
) -> Result<Option<VcsChange>> {
    tx.query_row(
        vcs_change_select_sql("WHERE vcs_workspace_id = ?1 AND kind = ?2 AND change_id = ?3")
            .as_str(),
        params![
            change.vcs_workspace_id.to_string(),
            change.kind.as_str(),
            change.change_id.as_str()
        ],
        vcs_change_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_summary_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<Summary>> {
    tx.query_row(
        summary_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        summary_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_file_touched_by_id(tx: &Transaction<'_>, id: Uuid) -> Result<Option<FileTouched>> {
    tx.query_row(
        file_touched_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        file_touched_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_history_record_link_by_id(
    tx: &Transaction<'_>,
    id: Uuid,
) -> Result<Option<HistoryRecordLink>> {
    tx.query_row(
        history_record_link_select_sql("WHERE id = ?1").as_str(),
        params![id.to_string()],
        history_record_link_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

fn existing_history_record_link_by_identity(
    tx: &Transaction<'_>,
    link: &HistoryRecordLink,
) -> Result<Option<HistoryRecordLink>> {
    tx.query_row(
        history_record_link_select_sql(
            "WHERE history_record_id = ?1 AND target_type = ?2 AND target_id = ?3 AND link_type = ?4",
        )
        .as_str(),
        params![
            link.history_record_id.to_string(),
            link.target_type.as_str(),
            link.target_id.to_string(),
            link.link_type.as_str()
        ],
        history_record_link_from_row,
    )
    .optional()
    .map_err(StoreError::from)
}

#[cfg(test)]
mod archive_validation_tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-archive-validation-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn artifact(id: Uuid, blob_hash: String, byte_size: u64) -> Artifact {
        Artifact {
            id,
            kind: ArtifactKind::Markdown,
            blob_path: object_relative_path(&blob_hash),
            blob_hash,
            byte_size,
            media_type: Some("text/markdown".into()),
            preview_text: Some("synthetic local preview blob".into()),
            redaction_state: RedactionState::LocalPreview,
            timestamps: EntityTimestamps {
                created_at: fixed_time(),
                updated_at: fixed_time(),
            },
            source_id: None,
            sync: SyncMetadata {
                visibility: Visibility::LocalOnly,
                fidelity: Fidelity::Imported,
                sync_state: SyncState::LocalOnly,
                sync_version: 0,
                deleted_at: None,
                metadata: serde_json::json!({}),
            },
        }
    }

    fn write_blob(blob_dir: &Path, blob_hash: &str, content: &[u8]) {
        let path = blob_dir.join(&blob_hash[..2]).join(blob_hash);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn assert_artifact_error(
        error: StoreError,
        matches_expected: impl FnOnce(&StoreError) -> bool,
    ) {
        assert!(
            matches_expected(&error),
            "unexpected archive artifact validation error: {error:?}"
        );
    }

    #[test]
    fn archive_blob_validation_fails_closed_when_blob_is_missing() {
        let temp = tempdir();
        let content = b"missing synthetic blob";
        let artifact = artifact(new_id(), sha256_hex(content), content.len() as u64);

        let error = validate_archive_artifact_record_blob(temp.path(), &artifact).unwrap_err();
        assert_artifact_error(
            error,
            |error| matches!(error, StoreError::ArchiveArtifactMissingContent { id } if *id == artifact.id),
        );
    }

    #[test]
    fn archive_blob_validation_fails_closed_when_hash_differs() {
        let temp = tempdir();
        let stored_content = b"stored bytes";
        let expected_content = b"expected bytes";
        let artifact = artifact(
            new_id(),
            sha256_hex(expected_content),
            stored_content.len() as u64,
        );
        write_blob(temp.path(), &artifact.blob_hash, stored_content);

        let error = validate_archive_artifact_record_blob(temp.path(), &artifact).unwrap_err();
        assert_artifact_error(
            error,
            |error| matches!(error, StoreError::ArchiveArtifactHashMismatch { id } if *id == artifact.id),
        );
    }

    #[test]
    fn archive_blob_validation_fails_closed_when_byte_size_differs() {
        let temp = tempdir();
        let content = b"size checked bytes";
        let artifact = artifact(new_id(), sha256_hex(content), content.len() as u64 + 1);
        write_blob(temp.path(), &artifact.blob_hash, content);

        let error = validate_archive_artifact_record_blob(temp.path(), &artifact).unwrap_err();
        assert_artifact_error(
            error,
            |error| matches!(error, StoreError::ArchiveArtifactSizeMismatch { id } if *id == artifact.id),
        );
    }

    #[test]
    fn archive_blob_validation_fails_closed_when_blob_path_mismatches_hash() {
        let temp = tempdir();
        let content = b"path checked bytes";
        let mut artifact = artifact(new_id(), sha256_hex(content), content.len() as u64);
        artifact.blob_path = "objects/ff/not-the-recorded-hash".into();
        write_blob(temp.path(), &artifact.blob_hash, content);

        let error = validate_archive_artifact_record_blob(temp.path(), &artifact).unwrap_err();
        assert_artifact_error(
            error,
            |error| matches!(error, StoreError::ArchiveArtifactPathMismatch { id } if *id == artifact.id),
        );
    }

    #[test]
    fn archive_blob_validation_fails_closed_when_blob_is_not_regular_file() {
        let temp = tempdir();
        let content = b"directory at blob path";
        let artifact = artifact(new_id(), sha256_hex(content), content.len() as u64);
        let path = temp
            .path()
            .join(&artifact.blob_hash[..2])
            .join(&artifact.blob_hash);
        fs::create_dir_all(&path).unwrap();

        let error = validate_archive_artifact_record_blob(temp.path(), &artifact).unwrap_err();
        assert_artifact_error(
            error,
            |error| matches!(error, StoreError::ArchiveArtifactNonRegularFile { id, .. } if *id == artifact.id),
        );
    }

    #[test]
    fn archive_version_validation_rejects_future_version() {
        let archive = SessionHistoryArchive {
            schema_version: 3,
            version: 3,
            ..SessionHistoryArchive::default()
        };

        let error = validate_archive_version(&archive).unwrap_err();
        assert!(matches!(
            error,
            StoreError::UnsupportedArchiveVersion(version) if version == 3
        ));
    }
}

fn expected_archive_blob_path(id: Uuid, blob_hash: &str) -> Result<String> {
    if blob_hash.get(..2).is_none() {
        return Err(StoreError::ArchiveArtifactPathMismatch { id });
    }
    Ok(object_relative_path(blob_hash))
}

fn validate_archive_artifact_record_blobs(
    blob_dir: &Path,
    archive: &SessionHistoryArchive,
) -> Result<()> {
    for artifact in &archive.artifact_records {
        validate_archive_artifact_record_blob(blob_dir, artifact)?;
    }
    Ok(())
}

fn validate_archive_artifact_record_blob(blob_dir: &Path, artifact: &Artifact) -> Result<()> {
    let expected_path = expected_archive_blob_path(artifact.id, &artifact.blob_hash)?;
    let legacy_path = {
        let shard = &artifact.blob_hash[..2];
        format!("{LEGACY_BLOBS_DIR}/{shard}/{}", artifact.blob_hash)
    };
    if artifact.blob_path != expected_path && artifact.blob_path != legacy_path {
        return Err(StoreError::ArchiveArtifactPathMismatch { id: artifact.id });
    }

    let absolute_path = blob_dir
        .join(&artifact.blob_hash[..2])
        .join(&artifact.blob_hash);
    if !absolute_path.exists() {
        return Err(StoreError::ArchiveArtifactMissingContent { id: artifact.id });
    }
    ensure_regular_blob_file(artifact.id, &absolute_path)?;
    let content = fs::read(&absolute_path)?;
    let hash = sha256_hex(&content);
    if hash != artifact.blob_hash {
        return Err(StoreError::ArchiveArtifactHashMismatch { id: artifact.id });
    }
    if content.len() as u64 != artifact.byte_size {
        return Err(StoreError::ArchiveArtifactSizeMismatch { id: artifact.id });
    }
    Ok(())
}

fn upsert_capture_source_tx(
    tx: &Transaction<'_>,
    source_id: Uuid,
    source: &CaptureSourceDescriptor,
    occurred_at: DateTime<Utc>,
    fidelity: Fidelity,
) -> Result<()> {
    let occurred_at_ms = timestamp_ms(occurred_at);
    tx.execute(
        r#"
        INSERT INTO capture_sources
        (
            id, kind, provider, machine_id, process_id, cwd, raw_source_path,
            external_session_id, started_at_ms, ended_at_ms, fidelity,
            visibility, sync_state, sync_version, metadata_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, 'local_only', 'local_only', 0, '{}')
        ON CONFLICT(id) DO UPDATE SET
            kind = excluded.kind,
            provider = excluded.provider,
            machine_id = excluded.machine_id,
            process_id = excluded.process_id,
            cwd = excluded.cwd,
            raw_source_path = excluded.raw_source_path,
            external_session_id = excluded.external_session_id,
            started_at_ms = excluded.started_at_ms,
            fidelity = excluded.fidelity
        "#,
        params![
            source_id.to_string(),
            source.kind.as_str(),
            source.provider.as_str(),
            source.machine_id.as_str(),
            source.process_id.map(i64::from),
            source.cwd.as_deref(),
            source.raw_source_path.as_deref(),
            source.external_session_id.as_deref(),
            occurred_at_ms,
            fidelity.as_str(),
        ],
    )?;
    Ok(())
}

fn import_rich_archive_entities_tx(
    tx: &Transaction<'_>,
    blob_dir: &Path,
    archive: &SessionHistoryArchive,
    _blob_guard: &mut BlobWriteGuard,
) -> Result<()> {
    if archive.schema_version < 2 && archive.version < 2 {
        return Ok(());
    }

    validate_archive_artifact_record_blobs(blob_dir, archive)?;

    for source in &archive.capture_sources {
        upsert_imported_capture_source_tx(tx, source)?;
    }
    for workspace in &archive.vcs_workspaces {
        upsert_vcs_workspace_tx(tx, workspace)?;
    }
    for artifact in &archive.artifact_records {
        upsert_artifact_tx(tx, artifact)?;
    }
    for session in &archive.sessions {
        upsert_session_tx(tx, session)?;
    }
    for run in &archive.runs {
        upsert_run_tx(tx, run)?;
    }
    for event in &archive.events {
        upsert_event_tx(tx, event)?;
    }
    for change in &archive.vcs_changes {
        upsert_vcs_change_tx(tx, change)?;
    }
    for summary in &archive.summaries {
        upsert_summary_tx(tx, summary)?;
    }
    for file in &archive.files_touched {
        upsert_file_touched_tx(tx, file)?;
    }
    for link in &archive.history_record_links {
        upsert_history_record_link_tx(tx, link)?;
    }
    Ok(())
}

fn upsert_imported_capture_source_tx(tx: &Transaction<'_>, source: &CaptureSource) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO capture_sources
        (id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
        ON CONFLICT(id) DO UPDATE SET
            kind = excluded.kind,
            provider = excluded.provider,
            machine_id = excluded.machine_id,
            process_id = excluded.process_id,
            cwd = excluded.cwd,
            raw_source_path = excluded.raw_source_path,
            external_session_id = excluded.external_session_id,
            started_at_ms = excluded.started_at_ms,
            ended_at_ms = excluded.ended_at_ms,
            fidelity = excluded.fidelity,
            visibility = excluded.visibility,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            metadata_json = excluded.metadata_json
        "#,
        params![
            source.id.to_string(),
            source.descriptor.kind.as_str(),
            source.descriptor.provider.as_str(),
            source.descriptor.machine_id.as_str(),
            source.descriptor.process_id.map(i64::from),
            source.descriptor.cwd.as_deref(),
            source.descriptor.raw_source_path.as_deref(),
            source.descriptor.external_session_id.as_deref(),
            timestamp_ms(source.started_at),
            optional_timestamp_ms(source.ended_at),
            source.sync.fidelity.as_str(),
            source.sync.visibility.as_str(),
            source.sync.sync_state.as_str(),
            source.sync.sync_version as i64,
            serde_json::to_string(&source.sync.metadata)?,
        ],
    )?;
    Ok(())
}

fn upsert_session_tx(tx: &Transaction<'_>, session: &Session) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO sessions
        (id, history_record_id, parent_session_id, root_session_id, capture_source_id, provider, external_session_id, external_agent_id, agent_type, role_hint, is_primary, status, fidelity, transcript_blob_id, started_at_ms, ended_at_ms, created_at_ms, updated_at_ms, visibility, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
        ON CONFLICT(id) DO UPDATE SET
            history_record_id = excluded.history_record_id,
            parent_session_id = excluded.parent_session_id,
            root_session_id = excluded.root_session_id,
            capture_source_id = excluded.capture_source_id,
            provider = excluded.provider,
            external_session_id = excluded.external_session_id,
            external_agent_id = excluded.external_agent_id,
            agent_type = excluded.agent_type,
            role_hint = excluded.role_hint,
            is_primary = excluded.is_primary,
            status = excluded.status,
            fidelity = excluded.fidelity,
            transcript_blob_id = excluded.transcript_blob_id,
            started_at_ms = excluded.started_at_ms,
            ended_at_ms = excluded.ended_at_ms,
            updated_at_ms = excluded.updated_at_ms,
            visibility = excluded.visibility,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            session.id.to_string(),
            optional_uuid_string(session.history_record_id),
            optional_uuid_string(session.parent_session_id),
            optional_uuid_string(session.root_session_id),
            optional_uuid_string(session.capture_source_id),
            session.provider.as_str(),
            session.external_session_id.as_deref(),
            session.external_agent_id.as_deref(),
            session.agent_type.as_str(),
            session.role_hint.as_deref(),
            session.is_primary as i64,
            session.status.as_str(),
            session.sync.fidelity.as_str(),
            optional_uuid_string(session.transcript_blob_id),
            timestamp_ms(session.started_at),
            optional_timestamp_ms(session.ended_at),
            timestamp_ms(session.timestamps.created_at),
            timestamp_ms(session.timestamps.updated_at),
            session.sync.visibility.as_str(),
            session.sync.sync_state.as_str(),
            session.sync.sync_version as i64,
            optional_timestamp_ms(session.sync.deleted_at),
            serde_json::to_string(&session.sync.metadata)?,
        ],
    )?;
    Ok(())
}

fn upsert_run_tx(tx: &Transaction<'_>, run: &Run) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO runs
        (id, history_record_id, session_id, run_type, status, started_at_ms, ended_at_ms, exit_code, cwd, command_preview, input_blob_id, output_blob_id, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
        ON CONFLICT(id) DO UPDATE SET
            history_record_id = excluded.history_record_id,
            session_id = excluded.session_id,
            run_type = excluded.run_type,
            status = excluded.status,
            started_at_ms = excluded.started_at_ms,
            ended_at_ms = excluded.ended_at_ms,
            exit_code = excluded.exit_code,
            cwd = excluded.cwd,
            command_preview = excluded.command_preview,
            input_blob_id = excluded.input_blob_id,
            output_blob_id = excluded.output_blob_id,
            updated_at_ms = excluded.updated_at_ms,
            source_id = excluded.source_id,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            run.id.to_string(),
            optional_uuid_string(run.history_record_id),
            optional_uuid_string(run.session_id),
            run.run_type.as_str(),
            run.status.as_str(),
            timestamp_ms(run.started_at),
            optional_timestamp_ms(run.ended_at),
            run.exit_code,
            run.cwd.as_deref(),
            run.command_preview.as_deref(),
            optional_uuid_string(run.input_blob_id),
            optional_uuid_string(run.output_blob_id),
            timestamp_ms(run.timestamps.created_at),
            timestamp_ms(run.timestamps.updated_at),
            optional_uuid_string(run.source_id),
            run.sync.visibility.as_str(),
            run.sync.fidelity.as_str(),
            run.sync.sync_state.as_str(),
            run.sync.sync_version as i64,
            optional_timestamp_ms(run.sync.deleted_at),
            serde_json::to_string(&run.sync.metadata)?,
        ],
    )?;
    Ok(())
}

fn upsert_event_tx(tx: &Transaction<'_>, event: &Event) -> Result<Uuid> {
    let event_id = if let Some(dedupe_key) = &event.dedupe_key {
        if let Some(existing) = tx
            .query_row(
                "SELECT id FROM events WHERE dedupe_key = ?1",
                params![dedupe_key],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .optional()?
        {
            existing
        } else {
            event.id
        }
    } else {
        event.id
    };

    tx.execute(
        r#"
        INSERT INTO events
        (id, seq, history_record_id, session_id, run_id, event_type, role, occurred_at_ms, capture_source_id, payload_json, payload_blob_id, dedupe_key, visibility, redaction_state, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
        ON CONFLICT(id) DO UPDATE SET
            seq = excluded.seq,
            history_record_id = excluded.history_record_id,
            session_id = excluded.session_id,
            run_id = excluded.run_id,
            event_type = excluded.event_type,
            role = excluded.role,
            occurred_at_ms = excluded.occurred_at_ms,
            capture_source_id = excluded.capture_source_id,
            payload_json = excluded.payload_json,
            payload_blob_id = excluded.payload_blob_id,
            dedupe_key = excluded.dedupe_key,
            visibility = excluded.visibility,
            redaction_state = excluded.redaction_state,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            event_id.to_string(),
            event.seq as i64,
            optional_uuid_string(event.history_record_id),
            optional_uuid_string(event.session_id),
            optional_uuid_string(event.run_id),
            event.event_type.as_str(),
            event.role.map(|role| role.as_str()),
            timestamp_ms(event.occurred_at),
            optional_uuid_string(event.capture_source_id),
            serde_json::to_string(&event.payload)?,
            optional_uuid_string(event.payload_blob_id),
            event.dedupe_key.as_deref(),
            event.sync.visibility.as_str(),
            event.redaction_state.as_str(),
            event.sync.fidelity.as_str(),
            event.sync.sync_state.as_str(),
            event.sync.sync_version as i64,
            optional_timestamp_ms(event.sync.deleted_at),
            serde_json::to_string(&event.sync.metadata)?,
        ],
    )?;
    Ok(event_id)
}

fn upsert_artifact_tx(tx: &Transaction<'_>, artifact: &Artifact) -> Result<Uuid> {
    tx.execute(
        r#"
        INSERT INTO artifacts
        (id, kind, blob_hash, blob_path, byte_size, media_type, preview_text, redaction_state, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
        ON CONFLICT DO UPDATE SET
            blob_path = excluded.blob_path,
            byte_size = excluded.byte_size,
            media_type = excluded.media_type,
            preview_text = excluded.preview_text,
            redaction_state = excluded.redaction_state,
            updated_at_ms = excluded.updated_at_ms,
            source_id = excluded.source_id,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            artifact.id.to_string(),
            artifact.kind.as_str(),
            artifact.blob_hash.as_str(),
            artifact.blob_path.as_str(),
            artifact.byte_size as i64,
            artifact.media_type.as_deref(),
            artifact.preview_text.as_deref(),
            artifact.redaction_state.as_str(),
            timestamp_ms(artifact.timestamps.created_at),
            timestamp_ms(artifact.timestamps.updated_at),
            optional_uuid_string(artifact.source_id),
            artifact.sync.visibility.as_str(),
            artifact.sync.fidelity.as_str(),
            artifact.sync.sync_state.as_str(),
            artifact.sync.sync_version as i64,
            optional_timestamp_ms(artifact.sync.deleted_at),
            serde_json::to_string(&artifact.sync.metadata)?,
        ],
    )?;
    tx.query_row(
        "SELECT id FROM artifacts WHERE blob_hash = ?1 AND kind = ?2",
        params![artifact.blob_hash.as_str(), artifact.kind.as_str()],
        |row| parse_uuid(row.get::<_, String>(0)?),
    )
    .map_err(StoreError::from)
}

fn upsert_vcs_workspace_tx(tx: &Transaction<'_>, workspace: &VcsWorkspace) -> Result<Uuid> {
    tx.execute(
        r#"
        INSERT INTO vcs_workspaces
        (id, kind, root_path, repo_fingerprint, primary_remote_url_normalized, host, owner, name, monorepo_subpath, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
        ON CONFLICT(kind, repo_fingerprint) DO UPDATE SET
            root_path = excluded.root_path,
            primary_remote_url_normalized = excluded.primary_remote_url_normalized,
            host = excluded.host,
            owner = excluded.owner,
            name = excluded.name,
            monorepo_subpath = excluded.monorepo_subpath,
            updated_at_ms = excluded.updated_at_ms,
            source_id = excluded.source_id,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            workspace.id.to_string(),
            workspace.kind.as_str(),
            workspace.root_path.as_str(),
            workspace.repo_fingerprint.as_str(),
            workspace.primary_remote_url_normalized.as_deref(),
            workspace.host.as_str(),
            workspace.owner.as_deref(),
            workspace.name.as_deref(),
            workspace.monorepo_subpath.as_deref(),
            timestamp_ms(workspace.timestamps.created_at),
            timestamp_ms(workspace.timestamps.updated_at),
            optional_uuid_string(workspace.source_id),
            workspace.sync.visibility.as_str(),
            workspace.sync.fidelity.as_str(),
            workspace.sync.sync_state.as_str(),
            workspace.sync.sync_version as i64,
            optional_timestamp_ms(workspace.sync.deleted_at),
            serde_json::to_string(&workspace.sync.metadata)?,
        ],
    )?;
    tx.query_row(
        "SELECT id FROM vcs_workspaces WHERE kind = ?1 AND repo_fingerprint = ?2",
        params![workspace.kind.as_str(), workspace.repo_fingerprint.as_str()],
        |row| parse_uuid(row.get::<_, String>(0)?),
    )
    .map_err(StoreError::from)
}

fn upsert_vcs_change_tx(tx: &Transaction<'_>, change: &VcsChange) -> Result<Uuid> {
    tx.execute(
        r#"
        INSERT INTO vcs_changes
        (id, vcs_workspace_id, kind, change_id, parent_change_ids_json, branch_or_bookmark, tree_hash, author_time_ms, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
        ON CONFLICT(vcs_workspace_id, kind, change_id) DO UPDATE SET
            parent_change_ids_json = excluded.parent_change_ids_json,
            branch_or_bookmark = excluded.branch_or_bookmark,
            tree_hash = excluded.tree_hash,
            author_time_ms = excluded.author_time_ms,
            confidence = excluded.confidence,
            updated_at_ms = excluded.updated_at_ms,
            source_id = excluded.source_id,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            change.id.to_string(),
            change.vcs_workspace_id.to_string(),
            change.kind.as_str(),
            change.change_id.as_str(),
            serde_json::to_string(&change.parent_change_ids)?,
            change.branch_or_bookmark.as_deref(),
            change.tree_hash.as_deref(),
            optional_timestamp_ms(change.author_time),
            change.confidence.as_str(),
            timestamp_ms(change.timestamps.created_at),
            timestamp_ms(change.timestamps.updated_at),
            optional_uuid_string(change.source_id),
            change.sync.visibility.as_str(),
            change.sync.fidelity.as_str(),
            change.sync.sync_state.as_str(),
            change.sync.sync_version as i64,
            optional_timestamp_ms(change.sync.deleted_at),
            serde_json::to_string(&change.sync.metadata)?,
        ],
    )?;
    tx.query_row(
        "SELECT id FROM vcs_changes WHERE vcs_workspace_id = ?1 AND kind = ?2 AND change_id = ?3",
        params![
            change.vcs_workspace_id.to_string(),
            change.kind.as_str(),
            change.change_id.as_str()
        ],
        |row| parse_uuid(row.get::<_, String>(0)?),
    )
    .map_err(StoreError::from)
}

fn upsert_record_tx(
    tx: &Transaction<'_>,
    record: &HistoryRecord,
    source_id: Option<Uuid>,
) -> Result<()> {
    let created_at_ms = timestamp_ms(record.created_at);
    let updated_at_ms = timestamp_ms(record.updated_at);
    tx.execute(
        r#"
        INSERT INTO history_records
        (
            id, title, summary, status, started_at_ms, last_activity_at_ms,
            created_at_ms, updated_at_ms, source_id, body, tags_json, kind,
            workspace, created_at, updated_at
        )
        VALUES (?1, ?2, ?3, 'open', ?4, ?5, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            summary = excluded.summary,
            status = excluded.status,
            started_at_ms = excluded.started_at_ms,
            last_activity_at_ms = excluded.last_activity_at_ms,
            created_at_ms = excluded.created_at_ms,
            updated_at_ms = excluded.updated_at_ms,
            source_id = COALESCE(excluded.source_id, history_records.source_id),
            body = excluded.body,
            tags_json = excluded.tags_json,
            kind = excluded.kind,
            workspace = excluded.workspace,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        "#,
        params![
            record.id.to_string(),
            record.title,
            record.body,
            created_at_ms,
            updated_at_ms,
            source_id.map(|id| id.to_string()),
            record.body,
            serde_json::to_string(&record.tags)?,
            record.kind,
            record.workspace,
            record.created_at.to_rfc3339(),
            record.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn upsert_summary_tx(tx: &Transaction<'_>, summary: &Summary) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO summaries
        (id, history_record_id, session_id, kind, model_or_source, text, citations_json, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
        ON CONFLICT(id) DO UPDATE SET
            history_record_id = excluded.history_record_id,
            session_id = excluded.session_id,
            kind = excluded.kind,
            model_or_source = excluded.model_or_source,
            text = excluded.text,
            citations_json = excluded.citations_json,
            updated_at_ms = excluded.updated_at_ms,
            source_id = excluded.source_id,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            summary.id.to_string(),
            optional_uuid_string(summary.history_record_id),
            optional_uuid_string(summary.session_id),
            summary.kind.as_str(),
            summary.model_or_source.as_deref(),
            summary.text.as_str(),
            serde_json::to_string(&summary.citations)?,
            timestamp_ms(summary.timestamps.created_at),
            timestamp_ms(summary.timestamps.updated_at),
            optional_uuid_string(summary.source_id),
            summary.sync.visibility.as_str(),
            summary.sync.fidelity.as_str(),
            summary.sync.sync_state.as_str(),
            summary.sync.sync_version as i64,
            optional_timestamp_ms(summary.sync.deleted_at),
            serde_json::to_string(&summary.sync.metadata)?,
        ],
    )?;
    Ok(())
}

fn upsert_file_touched_tx(tx: &Transaction<'_>, file: &FileTouched) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO files_touched
        (id, history_record_id, run_id, event_id, vcs_workspace_id, path, change_kind, old_path, line_count_delta, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
        ON CONFLICT(id) DO UPDATE SET
            history_record_id = excluded.history_record_id,
            run_id = excluded.run_id,
            event_id = excluded.event_id,
            vcs_workspace_id = excluded.vcs_workspace_id,
            path = excluded.path,
            change_kind = excluded.change_kind,
            old_path = excluded.old_path,
            line_count_delta = excluded.line_count_delta,
            confidence = excluded.confidence,
            updated_at_ms = excluded.updated_at_ms,
            source_id = excluded.source_id,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            file.id.to_string(),
            optional_uuid_string(file.history_record_id),
            optional_uuid_string(file.run_id),
            optional_uuid_string(file.event_id),
            optional_uuid_string(file.vcs_workspace_id),
            file.path.as_str(),
            file.change_kind.map(|kind| kind.as_str()),
            file.old_path.as_deref(),
            file.line_count_delta,
            file.confidence.as_str(),
            timestamp_ms(file.timestamps.created_at),
            timestamp_ms(file.timestamps.updated_at),
            optional_uuid_string(file.source_id),
            file.sync.visibility.as_str(),
            file.sync.fidelity.as_str(),
            file.sync.sync_state.as_str(),
            file.sync.sync_version as i64,
            optional_timestamp_ms(file.sync.deleted_at),
            serde_json::to_string(&file.sync.metadata)?,
        ],
    )?;
    Ok(())
}

fn upsert_history_record_link_tx(tx: &Transaction<'_>, link: &HistoryRecordLink) -> Result<Uuid> {
    tx.execute(
        r#"
        INSERT INTO history_record_links
        (id, history_record_id, target_type, target_id, link_type, confidence, source_id, created_at_ms, updated_at_ms, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
        ON CONFLICT(history_record_id, target_type, target_id, link_type) DO UPDATE SET
            confidence = excluded.confidence,
            source_id = excluded.source_id,
            updated_at_ms = excluded.updated_at_ms,
            visibility = excluded.visibility,
            fidelity = excluded.fidelity,
            sync_state = excluded.sync_state,
            sync_version = excluded.sync_version,
            deleted_at_ms = excluded.deleted_at_ms,
            metadata_json = excluded.metadata_json
        "#,
        params![
            link.id.to_string(),
            link.history_record_id.to_string(),
            link.target_type.as_str(),
            link.target_id.to_string(),
            link.link_type.as_str(),
            link.confidence.as_str(),
            optional_uuid_string(link.source_id),
            timestamp_ms(link.timestamps.created_at),
            timestamp_ms(link.timestamps.updated_at),
            link.sync.visibility.as_str(),
            link.sync.fidelity.as_str(),
            link.sync.sync_state.as_str(),
            link.sync.sync_version as i64,
            optional_timestamp_ms(link.sync.deleted_at),
            serde_json::to_string(&link.sync.metadata)?,
        ],
    )?;
    tx.query_row(
        "SELECT id FROM history_record_links WHERE history_record_id = ?1 AND target_type = ?2 AND target_id = ?3 AND link_type = ?4",
        params![
            link.history_record_id.to_string(),
            link.target_type.as_str(),
            link.target_id.to_string(),
            link.link_type.as_str()
        ],
        |row| parse_uuid(row.get::<_, String>(0)?),
    )
    .map_err(StoreError::from)
}

fn capture_source_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CaptureSource> {
    Ok(CaptureSource {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        descriptor: CaptureSourceDescriptor {
            kind: parse_text_enum::<ctx_history_core::CaptureSourceKind>(row.get::<_, String>(1)?)?,
            provider: parse_text_enum::<CaptureProvider>(row.get::<_, String>(2)?)?,
            machine_id: row.get(3)?,
            process_id: row.get::<_, Option<i64>>(4)?.map(|value| value as u32),
            cwd: row.get(5)?,
            raw_source_path: row.get(6)?,
            external_session_id: row.get(7)?,
        },
        started_at: ms_to_time(row.get(8)?)?,
        ended_at: optional_ms_to_time(row.get(9)?)?,
        sync: SyncMetadata {
            fidelity: parse_text_enum::<Fidelity>(row.get::<_, String>(10)?)?,
            visibility: parse_text_enum::<Visibility>(row.get::<_, String>(11)?)?,
            sync_state: parse_text_enum::<SyncState>(row.get::<_, String>(12)?)?,
            sync_version: row.get::<_, i64>(13)? as u64,
            deleted_at: None,
            metadata: parse_json(row.get::<_, String>(14)?)?,
        },
    })
}

fn search_capture_source_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SearchCaptureSourceRow> {
    Ok(SearchCaptureSourceRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        provider: parse_text_enum::<CaptureProvider>(row.get::<_, String>(1)?)?,
        cwd: row.get(2)?,
        raw_source_path: row.get(3)?,
        external_session_id: row.get(4)?,
        metadata: parse_json(row.get::<_, String>(5)?)?,
    })
}

fn catalog_session_select_sql(tail: &str) -> String {
    format!(
        "SELECT source_path, provider, source_format, source_root, external_session_id, parent_external_session_id, agent_type, role_hint, external_agent_id, cwd, session_started_at_ms, file_size_bytes, file_modified_at_ms, cataloged_at_ms, metadata_json FROM catalog_sessions {tail}"
    )
}

fn source_import_file_select_sql(tail: &str) -> String {
    format!(
        "SELECT provider, source_format, source_root, source_path, file_size_bytes, file_modified_at_ms, observed_at_ms, metadata_json FROM source_import_files {tail}"
    )
}

fn source_import_file_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SourceImportFile> {
    Ok(SourceImportFile {
        provider: parse_text_enum::<CaptureProvider>(row.get::<_, String>(0)?)?,
        source_format: row.get(1)?,
        source_root: row.get(2)?,
        source_path: row.get(3)?,
        file_size_bytes: nonnegative_i64_to_u64(row.get(4)?)?,
        file_modified_at_ms: row.get(5)?,
        observed_at_ms: row.get(6)?,
        metadata: parse_json(row.get::<_, String>(7)?)?,
    })
}

fn catalog_session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CatalogSession> {
    Ok(CatalogSession {
        source_path: row.get(0)?,
        provider: parse_text_enum::<CaptureProvider>(row.get::<_, String>(1)?)?,
        source_format: row.get(2)?,
        source_root: row.get(3)?,
        external_session_id: row.get(4)?,
        parent_external_session_id: row.get(5)?,
        agent_type: parse_text_enum::<AgentType>(row.get::<_, String>(6)?)?,
        role_hint: row.get(7)?,
        external_agent_id: row.get(8)?,
        cwd: row.get(9)?,
        session_started_at_ms: row.get(10)?,
        file_size_bytes: nonnegative_i64_to_u64(row.get(11)?)?,
        file_modified_at_ms: row.get(12)?,
        cataloged_at_ms: row.get(13)?,
        metadata: parse_json(row.get::<_, String>(14)?)?,
    })
}

fn catalog_pending_import_condition_sql(alias: &str) -> String {
    format!(
        r#"
        (
            {alias}.indexed_status != 'indexed'
            OR {alias}.indexed_file_size_bytes IS NULL
            OR {alias}.indexed_file_modified_at_ms IS NULL
            OR {alias}.indexed_file_size_bytes != {alias}.file_size_bytes
            OR {alias}.indexed_file_modified_at_ms != {alias}.file_modified_at_ms
            OR NOT EXISTS (
                SELECT 1
                FROM sessions AS session
                WHERE session.provider = {alias}.provider
                  AND {alias}.external_session_id IS NOT NULL
                  AND session.external_session_id = {alias}.external_session_id
                LIMIT 1
            )
        )
        "#
    )
}

fn catalog_indexed_count_sql() -> String {
    r#"
    SELECT COUNT(*)
    FROM catalog_sessions AS catalog
    WHERE catalog.is_stale = 0
      AND catalog.indexed_status = 'indexed'
      AND catalog.indexed_file_size_bytes = catalog.file_size_bytes
      AND catalog.indexed_file_modified_at_ms = catalog.file_modified_at_ms
      AND EXISTS (
        SELECT 1
        FROM sessions AS session
        WHERE session.provider = catalog.provider
          AND catalog.external_session_id IS NOT NULL
          AND session.external_session_id = catalog.external_session_id
        LIMIT 1
      )
    "#
    .to_owned()
}

fn distinct_uuid_chunks(ids: &[Uuid]) -> Vec<Vec<Uuid>> {
    let distinct = ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    distinct
        .chunks(BATCH_RECORD_ID_CHUNK_SIZE)
        .map(<[Uuid]>::to_vec)
        .collect()
}

fn uuid_value_vec(ids: &[Uuid]) -> Vec<SqlValue> {
    ids.iter()
        .map(|id| SqlValue::Text(id.to_string()))
        .collect()
}

fn uuid_values(ids: &[Uuid]) -> impl rusqlite::Params + '_ {
    params_from_iter(ids.iter().map(Uuid::to_string))
}

fn sql_placeholders(count: usize) -> String {
    vec!["?"; count].join(",")
}

fn empty_relation_map<T>(ids: &[Uuid]) -> BTreeMap<Uuid, Vec<T>> {
    ids.iter().copied().map(|id| (id, Vec::new())).collect()
}

fn session_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, history_record_id, parent_session_id, root_session_id, capture_source_id, provider, external_session_id, external_agent_id, agent_type, role_hint, is_primary, status, fidelity, transcript_blob_id, started_at_ms, ended_at_ms, created_at_ms, updated_at_ms, visibility, sync_state, sync_version, deleted_at_ms, metadata_json FROM sessions {tail}"
    )
}

fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Session> {
    Ok(Session {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        history_record_id: parse_optional_uuid(row.get(1)?)?,
        parent_session_id: parse_optional_uuid(row.get(2)?)?,
        root_session_id: parse_optional_uuid(row.get(3)?)?,
        capture_source_id: parse_optional_uuid(row.get(4)?)?,
        provider: parse_text_enum::<CaptureProvider>(row.get::<_, String>(5)?)?,
        external_session_id: row.get(6)?,
        external_agent_id: row.get(7)?,
        agent_type: parse_text_enum::<AgentType>(row.get::<_, String>(8)?)?,
        role_hint: row.get(9)?,
        is_primary: row.get::<_, i64>(10)? != 0,
        status: parse_text_enum::<SessionStatus>(row.get::<_, String>(11)?)?,
        transcript_blob_id: parse_optional_uuid(row.get(13)?)?,
        started_at: ms_to_time(row.get(14)?)?,
        ended_at: optional_ms_to_time(row.get(15)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(16)?)?,
            updated_at: ms_to_time(row.get(17)?)?,
        },
        sync: sync_metadata_from_row(row, 18, 12, 19, 20, 21, 22)?,
    })
}

fn search_session_select_sql(tail: &str) -> String {
    format!("SELECT id, parent_session_id, root_session_id, capture_source_id, provider, external_session_id, external_agent_id, agent_type, role_hint, is_primary, status, started_at_ms, ended_at_ms, metadata_json FROM sessions {tail}")
}

fn search_session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchSessionRow> {
    Ok(SearchSessionRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        parent_session_id: parse_optional_uuid(row.get(1)?)?,
        root_session_id: parse_optional_uuid(row.get(2)?)?,
        capture_source_id: parse_optional_uuid(row.get(3)?)?,
        provider: parse_text_enum::<CaptureProvider>(row.get::<_, String>(4)?)?,
        external_session_id: row.get(5)?,
        external_agent_id: row.get(6)?,
        agent_type: parse_text_enum::<AgentType>(row.get::<_, String>(7)?)?,
        role_hint: row.get(8)?,
        is_primary: row.get::<_, i64>(9)? != 0,
        status: parse_text_enum::<SessionStatus>(row.get::<_, String>(10)?)?,
        started_at: ms_to_time(row.get(11)?)?,
        ended_at: optional_ms_to_time(row.get(12)?)?,
        metadata: parse_json(row.get::<_, String>(13)?)?,
    })
}

fn run_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, history_record_id, session_id, run_type, status, started_at_ms, ended_at_ms, exit_code, cwd, command_preview, input_blob_id, output_blob_id, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM runs {tail}"
    )
}

fn run_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Run> {
    Ok(Run {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        history_record_id: parse_optional_uuid(row.get(1)?)?,
        session_id: parse_optional_uuid(row.get(2)?)?,
        run_type: parse_text_enum::<RunType>(row.get::<_, String>(3)?)?,
        status: parse_text_enum::<RunStatus>(row.get::<_, String>(4)?)?,
        started_at: ms_to_time(row.get(5)?)?,
        ended_at: optional_ms_to_time(row.get(6)?)?,
        exit_code: row.get(7)?,
        cwd: row.get(8)?,
        command_preview: row.get(9)?,
        input_blob_id: parse_optional_uuid(row.get(10)?)?,
        output_blob_id: parse_optional_uuid(row.get(11)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(12)?)?,
            updated_at: ms_to_time(row.get(13)?)?,
        },
        source_id: parse_optional_uuid(row.get(14)?)?,
        sync: sync_metadata_from_row(row, 15, 16, 17, 18, 19, 20)?,
    })
}

fn search_run_select_sql(tail: &str) -> String {
    format!("SELECT id, session_id, run_type, status, started_at_ms, exit_code, cwd, command_preview, source_id FROM runs {tail}")
}

fn search_run_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchRunRow> {
    Ok(SearchRunRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        session_id: parse_optional_uuid(row.get(1)?)?,
        run_type: parse_text_enum::<RunType>(row.get::<_, String>(2)?)?,
        status: parse_text_enum::<RunStatus>(row.get::<_, String>(3)?)?,
        started_at: ms_to_time(row.get(4)?)?,
        exit_code: row.get(5)?,
        cwd: row.get(6)?,
        command_preview: row.get(7)?,
        source_id: parse_optional_uuid(row.get(8)?)?,
    })
}

fn event_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, seq, history_record_id, session_id, run_id, event_type, role, occurred_at_ms, capture_source_id, payload_json, payload_blob_id, dedupe_key, visibility, redaction_state, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM events {tail}"
    )
}

fn selected_event_predicate(mode: SelectedEventMode) -> &'static str {
    match mode {
        SelectedEventMode::Log => "1 = 1",
        SelectedEventMode::Full => {
            "e.event_type = 'message' AND e.role IN ('user', 'assistant', 'system')"
        }
        SelectedEventMode::Lite => {
            r#"e.event_type = 'message' AND (
                e.role = 'user'
                OR (
                    e.role = 'assistant'
                    AND COALESCE((
                        SELECT next.role
                        FROM events AS next
                        WHERE next.session_id = e.session_id
                          AND next.event_type = 'message'
                          AND next.role IN ('user', 'assistant')
                          AND (next.seq, next.id) > (e.seq, e.id)
                        ORDER BY next.seq, next.id
                        LIMIT 1
                    ), 'user') = 'user'
                )
            )"#
        }
    }
}

const FINGERPRINT_SAMPLE_BYTES: usize = 4096;

fn fingerprint_shm_stable(path: &Path, hasher: &mut Sha256) -> Result<()> {
    hash_tagged_bytes(hasher, b"shm");
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            hasher.update([0]);
            return Ok(());
        }
        Err(err) => return Err(StoreError::Io(err)),
    };
    hasher.update([1]);
    hasher.update(metadata.len().to_be_bytes());
    // The WAL-index header and nBackfill checkpoint counter occupy the first
    // 100 bytes. Bytes after that include reader marks and lock bytes which a
    // read-only open legitimately mutates; hashing those would invalidate a
    // continuation merely because the previous CLI process exited.
    let mut header = [0_u8; 100];
    let mut file = fs::File::open(path)?;
    let read = file.read(&mut header)?;
    hash_tagged_bytes(hasher, &header[..read]);
    Ok(())
}

fn fingerprint_file_stable(path: &Path, role: &[u8], hasher: &mut Sha256) -> Result<()> {
    hash_tagged_bytes(hasher, role);
    let before = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            hasher.update([0]);
            return Ok(());
        }
        Err(err) => return Err(StoreError::Io(err)),
    };
    hasher.update([1]);
    hash_file_metadata(hasher, &before);

    let mut file = fs::File::open(path)?;
    let mut sample = vec![0_u8; FINGERPRINT_SAMPLE_BYTES];
    let first_read = file.read(&mut sample)?;
    hash_tagged_bytes(hasher, &sample[..first_read]);
    if before.len() > FINGERPRINT_SAMPLE_BYTES as u64 {
        let tail_start = before.len().saturating_sub(FINGERPRINT_SAMPLE_BYTES as u64);
        file.seek(SeekFrom::Start(tail_start))?;
        let tail_read = file.read(&mut sample)?;
        hash_tagged_bytes(hasher, &sample[..tail_read]);
    } else {
        hash_tagged_bytes(hasher, &[]);
    }

    // A concurrent append/rewrite must not produce a silently hybrid sample.
    // Hashing both observations is conservative and guarantees a subsequent
    // before/after query check sees a different physical state.
    let after = fs::metadata(path)?;
    hash_file_metadata(hasher, &after);
    Ok(())
}

fn hash_file_metadata(hasher: &mut Sha256, metadata: &fs::Metadata) {
    hasher.update(metadata.len().to_be_bytes());
    match metadata.modified().ok().and_then(|value| {
        value
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|duration| (duration.as_secs(), duration.subsec_nanos()))
    }) {
        Some((seconds, nanos)) => {
            hasher.update([1]);
            hasher.update(seconds.to_be_bytes());
            hasher.update(nanos.to_be_bytes());
        }
        None => hasher.update([0]),
    }
}

fn hash_tagged_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Event> {
    Ok(Event {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        seq: row.get::<_, i64>(1)? as u64,
        history_record_id: parse_optional_uuid(row.get(2)?)?,
        session_id: parse_optional_uuid(row.get(3)?)?,
        run_id: parse_optional_uuid(row.get(4)?)?,
        event_type: parse_text_enum::<EventType>(row.get::<_, String>(5)?)?,
        role: row
            .get::<_, Option<String>>(6)?
            .map(parse_text_enum::<EventRole>)
            .transpose()?,
        occurred_at: ms_to_time(row.get(7)?)?,
        capture_source_id: parse_optional_uuid(row.get(8)?)?,
        payload: parse_json(row.get::<_, String>(9)?)?,
        payload_blob_id: parse_optional_uuid(row.get(10)?)?,
        dedupe_key: row.get(11)?,
        redaction_state: parse_text_enum::<RedactionState>(row.get::<_, String>(13)?)?,
        sync: sync_metadata_from_row(row, 12, 14, 15, 16, 17, 18)?,
    })
}

fn search_event_select_sql(tail: &str) -> String {
    format!("SELECT id, seq, session_id, run_id, event_type, role, occurred_at_ms, capture_source_id, payload_json, dedupe_key, redaction_state, metadata_json FROM events {tail}")
}

fn search_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchEventRow> {
    Ok(SearchEventRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        seq: row.get::<_, i64>(1)? as u64,
        session_id: parse_optional_uuid(row.get(2)?)?,
        run_id: parse_optional_uuid(row.get(3)?)?,
        event_type: parse_text_enum::<EventType>(row.get::<_, String>(4)?)?,
        role: row
            .get::<_, Option<String>>(5)?
            .map(parse_text_enum::<EventRole>)
            .transpose()?,
        occurred_at: ms_to_time(row.get(6)?)?,
        capture_source_id: parse_optional_uuid(row.get(7)?)?,
        payload: parse_json(row.get::<_, String>(8)?)?,
        dedupe_key: row.get(9)?,
        redaction_state: parse_text_enum::<RedactionState>(row.get::<_, String>(10)?)?,
        metadata: parse_json(row.get::<_, String>(11)?)?,
    })
}

fn artifact_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, kind, blob_hash, blob_path, byte_size, media_type, preview_text, redaction_state, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM artifacts {tail}"
    )
}

fn artifact_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Artifact> {
    Ok(Artifact {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        kind: parse_text_enum::<ArtifactKind>(row.get::<_, String>(1)?)?,
        blob_hash: row.get(2)?,
        blob_path: row.get(3)?,
        byte_size: row.get::<_, i64>(4)? as u64,
        media_type: row.get(5)?,
        preview_text: row.get(6)?,
        redaction_state: parse_text_enum::<RedactionState>(row.get::<_, String>(7)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(8)?)?,
            updated_at: ms_to_time(row.get(9)?)?,
        },
        source_id: parse_optional_uuid(row.get(10)?)?,
        sync: sync_metadata_from_row(row, 11, 12, 13, 14, 15, 16)?,
    })
}

fn search_artifact_select_sql(tail: &str) -> String {
    format!("SELECT id, kind, blob_path, media_type, preview_text, updated_at_ms, source_id FROM artifacts {tail}")
}

fn search_artifact_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchArtifactRow> {
    Ok(SearchArtifactRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        kind: parse_text_enum::<ArtifactKind>(row.get::<_, String>(1)?)?,
        blob_path: row.get(2)?,
        media_type: row.get(3)?,
        preview_text: row.get(4)?,
        updated_at: ms_to_time(row.get(5)?)?,
        source_id: parse_optional_uuid(row.get(6)?)?,
    })
}

fn vcs_workspace_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, kind, root_path, repo_fingerprint, primary_remote_url_normalized, host, owner, name, monorepo_subpath, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM vcs_workspaces {tail}"
    )
}

fn vcs_workspace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<VcsWorkspace> {
    Ok(VcsWorkspace {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        kind: parse_text_enum::<ctx_history_core::VcsKind>(row.get::<_, String>(1)?)?,
        root_path: row.get(2)?,
        repo_fingerprint: row.get(3)?,
        primary_remote_url_normalized: row.get(4)?,
        host: parse_text_enum::<ctx_history_core::VcsHost>(row.get::<_, String>(5)?)?,
        owner: row.get(6)?,
        name: row.get(7)?,
        monorepo_subpath: row.get(8)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(9)?)?,
            updated_at: ms_to_time(row.get(10)?)?,
        },
        source_id: parse_optional_uuid(row.get(11)?)?,
        sync: sync_metadata_from_row(row, 12, 13, 14, 15, 16, 17)?,
    })
}

fn vcs_change_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, vcs_workspace_id, kind, change_id, parent_change_ids_json, branch_or_bookmark, tree_hash, author_time_ms, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM vcs_changes {tail}"
    )
}

fn vcs_change_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<VcsChange> {
    Ok(VcsChange {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        vcs_workspace_id: parse_uuid(row.get::<_, String>(1)?)?,
        kind: parse_text_enum::<ctx_history_core::VcsChangeKind>(row.get::<_, String>(2)?)?,
        change_id: row.get(3)?,
        parent_change_ids: serde_json::from_str(&row.get::<_, String>(4)?)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        branch_or_bookmark: row.get(5)?,
        tree_hash: row.get(6)?,
        author_time: optional_ms_to_time(row.get(7)?)?,
        confidence: parse_text_enum::<ctx_history_core::Confidence>(row.get::<_, String>(8)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(9)?)?,
            updated_at: ms_to_time(row.get(10)?)?,
        },
        source_id: parse_optional_uuid(row.get(11)?)?,
        sync: sync_metadata_from_row(row, 12, 13, 14, 15, 16, 17)?,
    })
}

fn search_vcs_change_select_sql(tail: &str) -> String {
    format!("SELECT id, kind, change_id, parent_change_ids_json, branch_or_bookmark, tree_hash, author_time_ms, updated_at_ms, source_id FROM vcs_changes {tail}")
}

fn search_vcs_change_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchVcsChangeRow> {
    Ok(SearchVcsChangeRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        kind: parse_text_enum::<ctx_history_core::VcsChangeKind>(row.get::<_, String>(1)?)?,
        change_id: row.get(2)?,
        parent_change_ids: serde_json::from_str(&row.get::<_, String>(3)?)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        branch_or_bookmark: row.get(4)?,
        tree_hash: row.get(5)?,
        author_time: optional_ms_to_time(row.get(6)?)?,
        updated_at: ms_to_time(row.get(7)?)?,
        source_id: parse_optional_uuid(row.get(8)?)?,
    })
}

fn summary_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, history_record_id, session_id, kind, model_or_source, text, citations_json, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM summaries {tail}"
    )
}

fn summary_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Summary> {
    Ok(Summary {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        history_record_id: parse_optional_uuid(row.get(1)?)?,
        session_id: parse_optional_uuid(row.get(2)?)?,
        kind: parse_text_enum::<ctx_history_core::SummaryKind>(row.get::<_, String>(3)?)?,
        model_or_source: row.get(4)?,
        text: row.get(5)?,
        citations: serde_json::from_str(&row.get::<_, String>(6)?)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(7)?)?,
            updated_at: ms_to_time(row.get(8)?)?,
        },
        source_id: parse_optional_uuid(row.get(9)?)?,
        sync: sync_metadata_from_row(row, 10, 11, 12, 13, 14, 15)?,
    })
}

fn search_summary_select_sql(tail: &str) -> String {
    format!("SELECT id, text, updated_at_ms, source_id FROM summaries {tail}")
}

fn search_summary_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchSummaryRow> {
    Ok(SearchSummaryRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        text: row.get(1)?,
        updated_at: ms_to_time(row.get(2)?)?,
        source_id: parse_optional_uuid(row.get(3)?)?,
    })
}

fn file_touched_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, history_record_id, run_id, event_id, vcs_workspace_id, path, change_kind, old_path, line_count_delta, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM files_touched {tail}"
    )
}

fn file_touched_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileTouched> {
    Ok(FileTouched {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        history_record_id: parse_optional_uuid(row.get(1)?)?,
        run_id: parse_optional_uuid(row.get(2)?)?,
        event_id: parse_optional_uuid(row.get(3)?)?,
        vcs_workspace_id: parse_optional_uuid(row.get(4)?)?,
        path: row.get(5)?,
        change_kind: row
            .get::<_, Option<String>>(6)?
            .map(parse_text_enum::<ctx_history_core::FileChangeKind>)
            .transpose()?,
        old_path: row.get(7)?,
        line_count_delta: row.get(8)?,
        confidence: parse_text_enum::<ctx_history_core::Confidence>(row.get::<_, String>(9)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(10)?)?,
            updated_at: ms_to_time(row.get(11)?)?,
        },
        source_id: parse_optional_uuid(row.get(12)?)?,
        sync: sync_metadata_from_row(row, 13, 14, 15, 16, 17, 18)?,
    })
}

fn search_file_touched_select_sql(tail: &str) -> String {
    format!("SELECT id, event_id, path, change_kind, old_path, updated_at_ms, source_id FROM files_touched {tail}")
}

fn search_file_touched_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchFileTouchedRow> {
    Ok(SearchFileTouchedRow {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        event_id: parse_optional_uuid(row.get(1)?)?,
        path: row.get(2)?,
        change_kind: row
            .get::<_, Option<String>>(3)?
            .map(parse_text_enum::<ctx_history_core::FileChangeKind>)
            .transpose()?,
        old_path: row.get(4)?,
        updated_at: ms_to_time(row.get(5)?)?,
        source_id: parse_optional_uuid(row.get(6)?)?,
    })
}

fn history_record_link_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, history_record_id, target_type, target_id, link_type, confidence, source_id, created_at_ms, updated_at_ms, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM history_record_links {tail}"
    )
}

fn history_record_link_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryRecordLink> {
    Ok(HistoryRecordLink {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        history_record_id: parse_uuid(row.get::<_, String>(1)?)?,
        target_type: parse_text_enum::<ctx_history_core::HistoryRecordLinkTargetType>(
            row.get::<_, String>(2)?,
        )?,
        target_id: parse_uuid(row.get::<_, String>(3)?)?,
        link_type: parse_text_enum::<ctx_history_core::HistoryRecordLinkType>(
            row.get::<_, String>(4)?,
        )?,
        confidence: parse_text_enum::<ctx_history_core::Confidence>(row.get::<_, String>(5)?)?,
        source_id: parse_optional_uuid(row.get(6)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(7)?)?,
            updated_at: ms_to_time(row.get(8)?)?,
        },
        sync: sync_metadata_from_row(row, 9, 10, 11, 12, 13, 14)?,
    })
}

fn sync_cursor_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SyncCursor> {
    Ok(SyncCursor {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        team_id: row.get(1)?,
        device_id: row.get(2)?,
        stream: row.get(3)?,
        cursor: row.get(4)?,
        last_synced_at: optional_ms_to_time(row.get(5)?)?,
        timestamps: EntityTimestamps {
            created_at: ms_to_time(row.get(6)?)?,
            updated_at: ms_to_time(row.get(7)?)?,
        },
    })
}

fn sync_metadata_from_row(
    row: &rusqlite::Row<'_>,
    visibility_index: usize,
    fidelity_index: usize,
    sync_state_index: usize,
    sync_version_index: usize,
    deleted_at_index: usize,
    metadata_index: usize,
) -> rusqlite::Result<SyncMetadata> {
    Ok(SyncMetadata {
        visibility: parse_text_enum::<Visibility>(row.get::<_, String>(visibility_index)?)?,
        fidelity: parse_text_enum::<Fidelity>(row.get::<_, String>(fidelity_index)?)?,
        sync_state: parse_text_enum::<SyncState>(row.get::<_, String>(sync_state_index)?)?,
        sync_version: row.get::<_, i64>(sync_version_index)? as u64,
        deleted_at: optional_ms_to_time(row.get(deleted_at_index)?)?,
        metadata: parse_json(row.get::<_, String>(metadata_index)?)?,
    })
}

fn optional_uuid_string(id: Option<Uuid>) -> Option<String> {
    id.map(|id| id.to_string())
}

fn optional_timestamp_ms(value: Option<DateTime<Utc>>) -> Option<i64> {
    value.map(timestamp_ms)
}

fn ms_to_time(value: i64) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value).ok_or_else(|| {
        rusqlite::Error::ToSqlConversionFailure(format!("invalid timestamp millis: {value}").into())
    })
}

fn optional_ms_to_time(value: Option<i64>) -> rusqlite::Result<Option<DateTime<Utc>>> {
    value.map(ms_to_time).transpose()
}

fn parse_optional_uuid(value: Option<String>) -> rusqlite::Result<Option<Uuid>> {
    value.map(parse_uuid).transpose()
}

fn parse_json(value: String) -> rusqlite::Result<serde_json::Value> {
    serde_json::from_str(&value)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

fn record_select_sql(tail: &str) -> String {
    format!(
        "SELECT id, title, body, tags_json, kind, workspace, created_at, updated_at FROM history_records {tail}"
    )
}

fn record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryRecord> {
    let tags_json: String = row.get(3)?;
    Ok(HistoryRecord {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        title: row.get(1)?,
        body: row.get(2)?,
        tags: serde_json::from_str(&tags_json)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        kind: row.get(4)?,
        workspace: row.get(5)?,
        created_at: parse_time(row.get::<_, String>(6)?)?,
        updated_at: parse_time(row.get::<_, String>(7)?)?,
    })
}

fn local_device_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LocalDeviceIdentity> {
    Ok(LocalDeviceIdentity {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        stable_device_id: row.get(1)?,
        created_at: time_ms(row.get(2)?),
        updated_at: time_ms(row.get(3)?),
    })
}

fn local_workspace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LocalWorkspaceIdentity> {
    let vcs_workspace_id: Option<String> = row.get(2)?;
    Ok(LocalWorkspaceIdentity {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        device_id: parse_uuid(row.get::<_, String>(1)?)?,
        vcs_workspace_id: vcs_workspace_id
            .map(parse_uuid)
            .transpose()
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        repo_fingerprint: row.get(3)?,
        root_path_hash: row.get(4)?,
        display_root: row.get(5)?,
        created_at: time_ms(row.get(6)?),
        updated_at: time_ms(row.get(7)?),
    })
}

fn parse_uuid(value: String) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(&value).map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

fn parse_time(value: String) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

fn parse_text_enum<T>(value: String) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .parse()
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

fn parse_optional_text_enum<T>(value: Option<String>) -> rusqlite::Result<Option<T>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value.map(parse_text_enum).transpose()
}

fn event_search_cursor(
    payload_json: &str,
    source_metadata_json: Option<&str>,
) -> rusqlite::Result<Option<String>> {
    let payload: serde_json::Value = serde_json::from_str(payload_json)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    if let Some(cursor) = payload.get("cursor").and_then(|value| value.as_str()) {
        return Ok(Some(cursor.to_owned()));
    }
    if let Some(cursor) = payload
        .get("body")
        .and_then(|body| body.get("cursor"))
        .and_then(|value| value.as_str())
    {
        return Ok(Some(cursor.to_owned()));
    }

    let Some(source_metadata_json) = source_metadata_json else {
        return Ok(None);
    };
    let metadata: serde_json::Value = serde_json::from_str(source_metadata_json)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    Ok(metadata
        .get("cursor")
        .and_then(|cursor| cursor.get("after"))
        .and_then(|after| after.get("cursor"))
        .and_then(|value| value.as_str())
        .map(str::to_owned))
}

#[derive(Default)]
struct EventSearchSourceIdentity {
    history_source: Option<String>,
    history_source_plugin: Option<String>,
    provider_key: Option<String>,
    source_id: Option<String>,
    source_format: Option<String>,
}

fn event_search_source_identity(
    source_metadata_json: Option<&str>,
) -> rusqlite::Result<EventSearchSourceIdentity> {
    let Some(source_metadata_json) = source_metadata_json else {
        return Ok(EventSearchSourceIdentity::default());
    };
    let metadata: serde_json::Value = serde_json::from_str(source_metadata_json)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    let source_metadata = metadata
        .get("source_metadata")
        .and_then(serde_json::Value::as_object);
    let plugin = source_metadata
        .and_then(|metadata| metadata.get("ctx_history_plugin"))
        .or_else(|| metadata.get("ctx_history_plugin"))
        .and_then(serde_json::Value::as_object);
    let custom = source_metadata
        .and_then(|metadata| metadata.get("ctx_history_jsonl_v1"))
        .or_else(|| metadata.get("ctx_history_jsonl_v1"))
        .and_then(serde_json::Value::as_object);
    let plugin_name = plugin
        .and_then(|plugin| plugin.get("plugin_name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let plugin_source_id = plugin
        .and_then(|plugin| plugin.get("plugin_source_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let history_source = plugin
        .and_then(|plugin| plugin.get("history_source"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            plugin_name
                .as_deref()
                .zip(plugin_source_id.as_deref())
                .map(|(plugin_name, source_id)| format!("{plugin_name}/{source_id}"))
        });
    let provider_key = custom
        .and_then(|custom| custom.get("provider_key"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let source_id = custom
        .and_then(|custom| custom.get("source_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let source_format = custom
        .and_then(|custom| custom.get("source_format"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            source_metadata
                .and_then(|metadata| metadata.get("source_format"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            metadata
                .get("source_format")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_owned);
    Ok(EventSearchSourceIdentity {
        history_source,
        history_source_plugin: plugin_name,
        provider_key,
        source_id,
        source_format,
    })
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    let mut values = Vec::new();
    for row in rows {
        values.push(row?);
    }
    Ok(values)
}

fn common_prefix_len(left: &str, right: &str) -> usize {
    left.as_bytes()
        .iter()
        .zip(right.as_bytes())
        .take_while(|(left, right)| left == right)
        .count()
}

#[cfg(test)]
mod search_order_tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-search-order-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn sqlite_profile_metadata_reports_runtime_settings() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let metadata = store.sqlite_profile_metadata().unwrap();
        assert!(!metadata.version.is_empty());
        assert!(!metadata.journal_mode.is_empty());
        assert!(metadata.page_size > 0);
        assert!(metadata.user_version >= 0);
    }

    #[test]
    fn profile_table_counts_are_exact_internal_instrumentation() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let empty = store.profile_table_counts().unwrap();
        assert_eq!(empty.records, 0);
        assert_eq!(empty.record_fts, 0);
        assert_eq!(empty.events, 0);
        assert_eq!(empty.event_fts, 0);

        let record = stable_tie_record(42);
        store.insert_record(&record).unwrap();
        let counts = store.profile_table_counts().unwrap();
        assert_eq!(counts.records, 1);
        assert_eq!(counts.record_fts, 1);
        assert_eq!(counts.events, 0);
        assert_eq!(counts.event_fts, 0);

        let raw = store
            .raw_sql_query("SELECT COUNT(*) FROM history_records", Default::default())
            .unwrap();
        assert_eq!(raw.limits.timeout_ms, 10_000);
        assert!(store
            .raw_sql_query("DELETE FROM history_records", Default::default())
            .is_err());
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn local_preview_event(seq: u64, text: &str, redaction_state: RedactionState) -> Event {
        Event {
            id: new_id(),
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({ "text": text }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state,
            sync: sync_metadata(),
        }
    }

    #[test]
    fn indexed_history_item_count_uses_sessions_and_events() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        for (idx, session_id) in [
            "018f45d0-0000-7000-8000-000000050001",
            "018f45d0-0000-7000-8000-000000050002",
        ]
        .into_iter()
        .enumerate()
        {
            store
                .conn
                .execute(
                    r#"
                    INSERT INTO sessions
                    (id, provider, external_session_id, agent_type, is_primary, status, fidelity,
                     started_at_ms, created_at_ms, updated_at_ms)
                    VALUES (?1, 'codex', ?2, 'primary', 1, 'imported', 'full', 1, 1, 1)
                    "#,
                    params![session_id, format!("external-session-{idx}")],
                )
                .unwrap();
        }

        for (seq, event_id, session_id) in [
            (
                1_i64,
                "018f45d0-0000-7000-8000-000000060001",
                "018f45d0-0000-7000-8000-000000050001",
            ),
            (
                2_i64,
                "018f45d0-0000-7000-8000-000000060002",
                "018f45d0-0000-7000-8000-000000050001",
            ),
            (
                3_i64,
                "018f45d0-0000-7000-8000-000000060003",
                "018f45d0-0000-7000-8000-000000050002",
            ),
        ] {
            store
                .conn
                .execute(
                    r#"
                    INSERT INTO events
                    (id, seq, session_id, event_type, role, occurred_at_ms, payload_json)
                    VALUES (?1, ?2, ?3, 'message', 'user', 1, '{}')
                    "#,
                    params![event_id, seq, session_id],
                )
                .unwrap();
        }

        assert_eq!(store.indexed_history_item_count().unwrap(), 5);
    }

    #[test]
    fn capture_source_count_uses_aggregate_count() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        for index in 1..=3 {
            store
                .conn
                .execute(
                    r#"
                    INSERT INTO capture_sources
                    (id, kind, provider, machine_id, started_at_ms, fidelity)
                    VALUES (?1, 'provider_import', 'codex', 'test-machine', ?2, 'full')
                    "#,
                    params![
                        format!("018f45d0-0000-7000-8000-000000070{index:03}"),
                        i64::from(index),
                    ],
                )
                .unwrap();
        }

        assert_eq!(store.capture_source_count().unwrap(), 3);
    }

    fn stable_tie_record(index: u16) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            "Stable tie title",
            "stabletie exact equal body for deterministic fts ranking",
            vec!["stabletie".into()],
            "task",
            None,
        );
        record.id =
            Uuid::parse_str(&format!("018f45d0-0000-7000-8000-000000010{index:03}")).unwrap();
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        record
    }

    fn assert_search_order(store: &Store, expected: &[Uuid]) {
        let actual = store
            .search_records("stabletie", 10)
            .unwrap()
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn search_query_plan_literals_modes_and_unicode61_punctuation_match_fts() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let one = local_preview_event(
            1,
            "Alpha beta write_to_file OR NOT title:body star*",
            RedactionState::SafePreview,
        );
        let two = local_preview_event(
            2,
            "beta alpha write to file or not title body star",
            RedactionState::SafePreview,
        );
        let three = local_preview_event(3, "alpha only", RedactionState::SafePreview);
        for event in [&one, &two, &three] {
            store.upsert_event(event).unwrap();
        }
        store.refresh_search_index().unwrap();

        let all = SearchQueryPlan::new(SearchMatchMode::All, ["alpha beta"]);
        let all_ids = store
            .search_event_hits_plan_page(&all, 10, 0)
            .unwrap()
            .into_iter()
            .map(|hit| hit.event_id)
            .collect::<Vec<_>>();
        assert!(all_ids.contains(&one.id));
        assert!(all_ids.contains(&two.id));
        assert!(!all_ids.contains(&three.id));

        let phrase = SearchQueryPlan::new(SearchMatchMode::Phrase, ["write_to_file"]);
        let phrase_ids = store
            .search_event_hits_plan_page(&phrase, 10, 0)
            .unwrap()
            .into_iter()
            .map(|hit| hit.event_id)
            .collect::<Vec<_>>();
        assert!(phrase_ids.contains(&one.id));
        assert!(phrase_ids.contains(&two.id));

        let any = SearchQueryPlan::new(SearchMatchMode::Any, ["gamma alpha"]);
        let any_ids = store
            .search_event_hits_plan_page(&any, 10, 0)
            .unwrap()
            .into_iter()
            .map(|hit| hit.event_id)
            .collect::<Vec<_>>();
        assert!(any_ids.contains(&three.id));

        let operators = SearchQueryPlan::new(SearchMatchMode::All, ["OR NOT title:body star*"]);
        let operator_ids = store
            .search_event_hits_plan_page(&operators, 10, 0)
            .unwrap()
            .into_iter()
            .map(|hit| hit.event_id)
            .collect::<Vec<_>>();
        assert!(operator_ids.contains(&one.id));
        assert!(operator_ids.contains(&two.id));
    }

    #[test]
    fn search_records_plan_no_fts_scans_past_early_non_matches_and_offsets_matches() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .conn
            .execute("DROP TABLE ctx_history_search", [])
            .unwrap();
        for index in 0..430 {
            let body = if index == 425 {
                "deep fallback needle"
            } else {
                "ordinary"
            };
            let mut record =
                HistoryRecord::new(format!("record {index}"), body, Vec::new(), "note", None);
            record.id =
                Uuid::parse_str(&format!("018f45d0-0000-7000-8000-00000003{index:04x}")).unwrap();
            record.created_at = fixed_time() + chrono::Duration::seconds(index as i64);
            record.updated_at = record.created_at;
            store.insert_record(&record).unwrap();
        }
        let plan = SearchQueryPlan::new(SearchMatchMode::All, ["deep fallback needle"]);
        let hits = store.search_records_plan_page(&plan, 1, 0).unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].body.contains("deep fallback needle"));
        assert!(store
            .search_records_plan_page(&plan, 1, 1)
            .unwrap()
            .is_empty());
    }

    /// The degraded no-FTS fallback must do bounded database work per call:
    /// at most `RECORD_FALLBACK_SCAN_MAX_PAGES` record list pages, proven via
    /// the page-execution counter instead of timing. Within the scan window
    /// results and offset skipping stay deterministic; matches older than the
    /// window are missed (documented degraded behavior), never scanned for
    /// with unbounded O(table) work.
    #[test]
    fn search_records_plan_no_fts_scan_work_is_page_bounded() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .conn
            .execute("DROP TABLE ctx_history_search", [])
            .unwrap();
        // limit 1 => page size 100; budget = 20 pages = the 2,000 newest
        // records. 2,150 records total leaves 150 older than the window.
        let total = 2_150_usize;
        store.begin_immediate_batch().unwrap();
        for index in 0..total {
            let body = match index {
                0 => "beyondbudgetneedle oldest record",
                2_105 => "windowedneedle second",
                2_145 => "windowedneedle first",
                _ => "ordinary",
            };
            let mut record =
                HistoryRecord::new(format!("record {index}"), body, Vec::new(), "note", None);
            record.id =
                Uuid::parse_str(&format!("018f45d0-0000-7000-8000-00000004{index:04x}")).unwrap();
            record.created_at = fixed_time() + chrono::Duration::seconds(index as i64);
            record.updated_at = record.created_at;
            store.insert_record(&record).unwrap();
        }
        store.commit_batch().unwrap();

        // A query with no match in the window stops at the page budget
        // instead of walking all 22 pages of the table.
        let beyond = SearchQueryPlan::new(SearchMatchMode::All, ["beyondbudgetneedle"]);
        let before = store.record_list_page_executions();
        let hits = store.search_records_plan_page(&beyond, 1, 0).unwrap();
        assert_eq!(
            store.record_list_page_executions() - before,
            RECORD_FALLBACK_SCAN_MAX_PAGES as u64,
            "no-FTS fallback must stop at the page budget"
        );
        assert!(
            hits.is_empty(),
            "a match older than the scan window is missed in degraded no-FTS mode"
        );
        assert!(store
            .list_records(usize::MAX)
            .unwrap()
            .iter()
            .any(|record| record.body.contains("beyondbudgetneedle")));

        // Within the window, matches return early with deterministic offset
        // skipping over the newest-first ordering.
        let windowed = SearchQueryPlan::new(SearchMatchMode::All, ["windowedneedle"]);
        let before = store.record_list_page_executions();
        let first = store.search_records_plan_page(&windowed, 1, 0).unwrap();
        assert_eq!(store.record_list_page_executions() - before, 1);
        assert_eq!(first.len(), 1);
        assert!(first[0].body.contains("windowedneedle first"));
        let second = store.search_records_plan_page(&windowed, 1, 1).unwrap();
        assert_eq!(second.len(), 1);
        assert!(second[0].body.contains("windowedneedle second"));
        assert!(store
            .search_records_plan_page(&windowed, 1, 2)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn search_records_plan_no_fts_does_not_match_across_record_sections() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .conn
            .execute("DROP TABLE ctx_history_search", [])
            .unwrap();
        for index in 0..25 {
            let mut record = HistoryRecord::new(
                format!("alpha decoy {index}"),
                "beta decoy",
                Vec::new(),
                "note",
                None,
            );
            record.created_at = fixed_time() + chrono::Duration::seconds(index);
            record.updated_at = record.created_at;
            store.insert_record(&record).unwrap();
        }
        let mut valid =
            HistoryRecord::new("valid", "alpha beta same section", Vec::new(), "note", None);
        valid.created_at = fixed_time() + chrono::Duration::seconds(100);
        valid.updated_at = valid.created_at;
        store.insert_record(&valid).unwrap();
        let plan = SearchQueryPlan::new(SearchMatchMode::All, ["alpha beta"]);
        let hits = store.search_records_plan_page(&plan, 1, 0).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, valid.id);
    }

    #[test]
    fn search_records_equal_fts_scores_use_record_id_across_refresh_and_reopen() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        for index in [4, 1, 3, 2] {
            store.insert_record(&stable_tie_record(index)).unwrap();
        }

        let expected = vec![
            stable_tie_record(1).id,
            stable_tie_record(2).id,
            stable_tie_record(3).id,
            stable_tie_record(4).id,
        ];
        assert_search_order(&store, &expected);

        store.upsert_record(&stable_tie_record(3)).unwrap();
        assert_search_order(&store, &expected);

        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_search_order(&reopened, &expected);
    }

    #[test]
    fn search_records_empty_or_no_token_query_returns_empty() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let record = stable_tie_record(1);
        store.insert_record(&record).unwrap();

        assert!(store.search_records("", 10).unwrap().is_empty());
        assert!(store.search_records("!!!", 10).unwrap().is_empty());
        assert!(store.search_records("---", 10).unwrap().is_empty());
        assert!(store.search_records("___", 10).unwrap().is_empty());
        assert!(store.search_records_page("", 10, 0).unwrap().is_empty());
    }

    #[test]
    fn event_search_local_preview_preserves_private_text_but_raw_is_withheld() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let local_event = local_preview_event(
            1,
            "cwd=/home/example/private token=ghp_1234567890abcdef",
            RedactionState::LocalPreview,
        );
        let raw_event = local_preview_event(
            2,
            "raw cwd=/home/example/private token=ghp_1234567890abcdef",
            RedactionState::Raw,
        );

        store.upsert_event(&local_event).unwrap();
        store.upsert_event(&raw_event).unwrap();

        let local_preview: String = store
            .conn
            .query_row(
                "SELECT safe_preview_text FROM event_search WHERE event_id = ?1",
                [local_event.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(local_preview.contains("/home/example/private"));
        assert!(local_preview.contains("ghp_1234567890abcdef"));

        let raw_preview: String = store
            .conn
            .query_row(
                "SELECT safe_preview_text FROM event_search WHERE event_id = ?1",
                [raw_event.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(raw_preview, "raw event payload withheld");
    }

    #[test]
    fn upsert_record_updates_record_search_without_rebuilding_event_search() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO event_search
                (event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
                VALUES ('sentinel-event', NULL, NULL, 'user', 'preserve-event-search-row', 'message')
                "#,
                [],
            )
            .unwrap();

        let record = stable_tie_record(5);
        store.upsert_record(&record).unwrap();

        let sentinel_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event_search WHERE event_id = 'sentinel-event'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sentinel_count, 1);
        assert_search_order(&store, &[record.id]);
    }

    fn tie_event(id: &str, seq: u64, occurred_at: DateTime<Utc>, text: &str) -> Event {
        Event {
            id: Uuid::parse_str(id).unwrap(),
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at,
            capture_source_id: None,
            payload: serde_json::json!({ "text": text }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    // Test-only snapshot of the pre-optimization single-phase query. Keep it
    // independent from SEARCH_EVENT_HITS_PAGE_SQL: this is the behavioral
    // oracle for ordering, pagination, fallback joins, and hydrated contents.
    const LEGACY_SEARCH_EVENT_HITS_PAGE_SQL: &str = r#"
        SELECT event_search.event_id,
               COALESCE(e.history_record_id, event_search.history_record_id, s.history_record_id, rs.history_record_id),
               COALESCE(e.session_id, event_search.session_id, s.id, rs.id),
               e.run_id,
               e.seq,
               e.event_type,
               e.role,
               e.occurred_at_ms,
               event_search.safe_preview_text,
               bm25(event_search),
               COALESCE(s.provider, rs.provider, event_source.provider, session_source.provider, run_source.provider),
               COALESCE(s.external_session_id, rs.external_session_id),
               COALESCE(s.parent_session_id, rs.parent_session_id),
               COALESCE(s.root_session_id, rs.root_session_id),
               COALESCE(s.agent_type, rs.agent_type),
               COALESCE(s.is_primary, rs.is_primary),
               COALESCE(event_source.cwd, session_source.cwd, run_source.cwd),
               COALESCE(event_source.raw_source_path, session_source.raw_source_path, run_source.raw_source_path),
               e.payload_json,
               COALESCE(event_source.metadata_json, session_source.metadata_json, run_source.metadata_json),
               wr.title,
               wr.kind,
               wr.workspace
        FROM event_search
        JOIN events e ON e.id = event_search.event_id
        LEFT JOIN runs r ON r.id = e.run_id
        LEFT JOIN sessions s ON s.id = COALESCE(e.session_id, event_search.session_id)
        LEFT JOIN sessions rs ON rs.id = r.session_id
        LEFT JOIN capture_sources event_source ON event_source.id = e.capture_source_id
        LEFT JOIN capture_sources session_source ON session_source.id = COALESCE(s.capture_source_id, rs.capture_source_id)
        LEFT JOIN capture_sources run_source ON run_source.id = r.source_id
        LEFT JOIN history_records wr ON wr.id = COALESCE(e.history_record_id, event_search.history_record_id, s.history_record_id, rs.history_record_id, r.history_record_id)
        WHERE event_search MATCH ?1
        ORDER BY bm25(event_search), e.occurred_at_ms DESC, e.seq DESC, event_search.event_id
        LIMIT ?2 OFFSET ?3
        "#;

    fn legacy_event_hits_page(
        store: &Store,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Vec<EventSearchHit> {
        let mut stmt = store
            .conn
            .prepare(LEGACY_SEARCH_EVENT_HITS_PAGE_SQL)
            .unwrap();
        let rows = stmt
            .query_map(params![query, limit.max(1) as i64, offset as i64], |row| {
                let payload_json = row.get::<_, String>(18)?;
                let source_metadata_json = row.get::<_, Option<String>>(19)?;
                let source_identity =
                    event_search_source_identity(source_metadata_json.as_deref())?;
                Ok(EventSearchHit {
                    event_id: parse_uuid(row.get::<_, String>(0)?)?,
                    history_record_id: parse_optional_uuid(row.get(1)?)?,
                    session_id: parse_optional_uuid(row.get(2)?)?,
                    run_id: parse_optional_uuid(row.get(3)?)?,
                    seq: row.get::<_, i64>(4)? as u64,
                    event_type: parse_text_enum::<EventType>(row.get::<_, String>(5)?)?,
                    role: parse_optional_text_enum::<EventRole>(row.get(6)?)?,
                    occurred_at: ms_to_time(row.get(7)?)?,
                    preview: row.get(8)?,
                    score: row.get(9)?,
                    provider: parse_optional_text_enum::<CaptureProvider>(row.get(10)?)?,
                    session_external_session_id: row.get(11)?,
                    history_source: source_identity.history_source,
                    history_source_plugin: source_identity.history_source_plugin,
                    provider_key: source_identity.provider_key,
                    source_id: source_identity.source_id,
                    source_format: source_identity.source_format,
                    session_parent_session_id: parse_optional_uuid(row.get(12)?)?,
                    session_root_session_id: parse_optional_uuid(row.get(13)?)?,
                    agent_type: parse_optional_text_enum::<AgentType>(row.get(14)?)?,
                    session_is_primary: row.get::<_, Option<i64>>(15)?.map(|value| value != 0),
                    cwd: row.get(16)?,
                    raw_source_path: row.get(17)?,
                    cursor: event_search_cursor(&payload_json, source_metadata_json.as_deref())?,
                    record_title: row.get(20)?,
                    record_kind: row.get(21)?,
                    record_workspace: row.get(22)?,
                    tool_names: event_tool_names_from_payload(&payload_json),
                })
            })
            .unwrap();
        collect_rows(rows).unwrap()
    }

    /// Exercises every reachable tie-break level of the ranked event page:
    /// bm25 score, then occurred_at DESC, then seq DESC. (`events.seq` is
    /// UNIQUE, so the trailing event_id tie key can never be reached through
    /// real rows; it stays in the ORDER BY purely as a determinism guard.)
    /// Also proves paged reads are exact slices of the full ordering with
    /// identical hydrated contents (wide-join fields included).
    #[test]
    fn event_hits_page_two_phase_order_and_hydration_equivalence_under_ties() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        let record_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000d0001").unwrap();
        let mut record = HistoryRecord::new(
            "Hydration record title",
            "hydration record body",
            vec!["pagetie-test".into()],
            "task",
            Some("/workspace/pagetie".into()),
        );
        record.id = record_id;
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        store.insert_record(&record).unwrap();

        let session_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000d0002").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO sessions
                (id, history_record_id, provider, external_session_id, agent_type, is_primary,
                 status, fidelity, started_at_ms, created_at_ms, updated_at_ms)
                VALUES (?1, ?2, 'codex', 'external-pagetie-session', 'primary', 1,
                        'imported', 'full', 1, 1, 1)
                "#,
                params![session_id.to_string(), record_id.to_string()],
            )
            .unwrap();
        let source_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000d0003").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO capture_sources
                (id, kind, provider, machine_id, cwd, raw_source_path, started_at_ms, fidelity,
                 metadata_json)
                VALUES (?1, 'provider_import', 'codex', 'test-machine', '/workspace/pagetie',
                        '/workspace/pagetie/transcript.jsonl', 1, 'full',
                        '{"source_metadata":{"ctx_history_plugin":{"plugin_name":"fixture-plugin","plugin_source_id":"fixture-source","history_source":"fixture/history"},"ctx_history_jsonl_v1":{"provider_key":"fixture-provider","source_id":"fixture-id","source_format":"ctx-history-jsonl-v1"}},"cursor":{"after":{"cursor":"metadata-cursor"}}}')
                "#,
                params![source_id.to_string()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE sessions SET capture_source_id = ?1 WHERE id = ?2",
                params![source_id.to_string(), session_id.to_string()],
            )
            .unwrap();

        // A run-only relationship exercises the rs/run_source fallback path
        // independently of the direct event/session/source path above.
        let run_source_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000d0004").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO capture_sources
                (id, kind, provider, machine_id, cwd, raw_source_path, started_at_ms, fidelity,
                 metadata_json)
                VALUES (?1, 'provider_import', 'claude', 'run-machine', '/workspace/run',
                        '/workspace/run/transcript.jsonl', 1, 'full', '{}')
                "#,
                params![run_source_id.to_string()],
            )
            .unwrap();
        let run_session_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000d0005").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO sessions
                (id, history_record_id, capture_source_id, provider, external_session_id,
                 agent_type, is_primary, status, fidelity, started_at_ms, created_at_ms, updated_at_ms)
                VALUES (?1, ?2, ?3, 'claude', 'external-run-session', 'subagent', 0,
                        'imported', 'full', 1, 1, 1)
                "#,
                params![
                    run_session_id.to_string(),
                    record_id.to_string(),
                    run_source_id.to_string()
                ],
            )
            .unwrap();
        let run_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000d0006").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO runs
                (id, history_record_id, session_id, run_type, status, started_at_ms,
                 created_at_ms, updated_at_ms, source_id)
                VALUES (?1, ?2, ?3, 'agent_turn', 'succeeded', 1, 1, 1, ?4)
                "#,
                params![
                    run_id.to_string(),
                    record_id.to_string(),
                    run_session_id.to_string(),
                    run_source_id.to_string()
                ],
            )
            .unwrap();

        let older = fixed_time();
        let newer = fixed_time() + chrono::Duration::seconds(1);
        // Better bm25 group (tf=2, same doc length as the tf=1 group) at the
        // OLDER timestamp: score must dominate recency.
        let mut hydrated = tie_event(
            "018f45d0-0000-7000-8000-0000000e0001",
            5,
            older,
            "pagetie pagetie",
        );
        hydrated.history_record_id = Some(record_id);
        hydrated.session_id = Some(session_id);
        hydrated.capture_source_id = Some(source_id);
        hydrated.payload = serde_json::json!({
            "cursor": "payload-cursor",
            "tool": "/usr/local/bin/FixtureTool --flag",
            "body": { "text": "pagetie pagetie" }
        });
        // Same score and timestamp as `hydrated`, lower seq: seq DESC decides.
        let score_tie_lower_seq = tie_event(
            "018f45d0-0000-7000-8000-0000000e0002",
            4,
            older,
            "pagetie pagetie",
        );
        // tf=1 group: identical bm25 within the group, so ordering falls
        // through to occurred_at DESC, then seq DESC.
        let mut newer_seq7 = tie_event(
            "018f45d0-0000-7000-8000-0000000e0103",
            7,
            newer,
            "pagetie filler",
        );
        newer_seq7.run_id = Some(run_id);
        let newer_seq2 = tie_event(
            "018f45d0-0000-7000-8000-0000000e0104",
            2,
            newer,
            "pagetie filler",
        );
        // Highest seq of all, but the older timestamp must lose to recency.
        let older_seq9 = tie_event(
            "018f45d0-0000-7000-8000-0000000e0105",
            9,
            older,
            "pagetie filler",
        );
        for event in [
            &newer_seq2,
            &score_tie_lower_seq,
            &older_seq9,
            &hydrated,
            &newer_seq7,
        ] {
            store.upsert_event(event).unwrap();
        }

        // The base event has no record/session IDs; the projection does.
        // This directly exercises event_search-vs-events fallback fields.
        store
            .conn
            .execute(
                r#"
                UPDATE event_search
                SET history_record_id = ?1, session_id = ?2
                WHERE event_id = ?3
                "#,
                params![
                    record_id.to_string(),
                    session_id.to_string(),
                    score_tie_lower_seq.id.to_string()
                ],
            )
            .unwrap();

        let expected = vec![
            hydrated.id,
            score_tie_lower_seq.id,
            newer_seq7.id,
            newer_seq2.id,
            older_seq9.id,
        ];
        let full = store.search_event_hits_page("pagetie", 50, 0).unwrap();
        assert_eq!(
            full.iter().map(|hit| hit.event_id).collect::<Vec<_>>(),
            expected
        );

        // bm25 sanity: the tf=2 group scores strictly better (more negative)
        // and scores are identical inside each tie group.
        assert_eq!(full[0].score, full[1].score);
        assert!(full[1].score < full[2].score);
        for hit in &full[3..] {
            assert_eq!(hit.score, full[2].score);
        }

        // Wide payload/metadata hydration for the top hit survives the
        // two-phase page, including derived cursor/source fields.
        let top = &full[0];
        assert_eq!(top.preview, "pagetie pagetie");
        assert_eq!(top.history_record_id, Some(record_id));
        assert_eq!(top.session_id, Some(session_id));
        assert_eq!(top.provider, Some(CaptureProvider::Codex));
        assert_eq!(
            top.session_external_session_id.as_deref(),
            Some("external-pagetie-session")
        );
        assert_eq!(top.cwd.as_deref(), Some("/workspace/pagetie"));
        assert_eq!(
            top.raw_source_path.as_deref(),
            Some("/workspace/pagetie/transcript.jsonl")
        );
        assert_eq!(top.record_title.as_deref(), Some("Hydration record title"));
        assert_eq!(top.record_kind.as_deref(), Some("task"));
        assert_eq!(top.record_workspace.as_deref(), Some("/workspace/pagetie"));
        assert_eq!(top.cursor.as_deref(), Some("payload-cursor"));
        assert_eq!(top.history_source.as_deref(), Some("fixture/history"));
        assert_eq!(top.history_source_plugin.as_deref(), Some("fixture-plugin"));
        assert_eq!(top.provider_key.as_deref(), Some("fixture-provider"));
        assert_eq!(top.source_id.as_deref(), Some("fixture-id"));
        assert_eq!(top.source_format.as_deref(), Some("ctx-history-jsonl-v1"));

        // Projection-only IDs win when the base event IDs are NULL.
        let projection_fallback = &full[1];
        assert_eq!(projection_fallback.history_record_id, Some(record_id));
        assert_eq!(projection_fallback.session_id, Some(session_id));
        assert_eq!(projection_fallback.provider, Some(CaptureProvider::Codex));
        assert_eq!(
            projection_fallback.record_title.as_deref(),
            Some("Hydration record title")
        );

        // With no event/projection session, the run's session and source
        // supply all wide fallback fields.
        let run_fallback = &full[2];
        assert_eq!(run_fallback.run_id, Some(run_id));
        assert_eq!(run_fallback.session_id, Some(run_session_id));
        assert_eq!(run_fallback.provider, Some(CaptureProvider::Claude));
        assert_eq!(
            run_fallback.session_external_session_id.as_deref(),
            Some("external-run-session")
        );
        assert_eq!(run_fallback.cwd.as_deref(), Some("/workspace/run"));
        assert_eq!(
            run_fallback.raw_source_path.as_deref(),
            Some("/workspace/run/transcript.jsonl")
        );
        assert_eq!(
            run_fallback.record_title.as_deref(),
            Some("Hydration record title")
        );

        // Differential oracle: every page returned by the production API is
        // byte-for-byte equivalent at the EventSearchHit field level to the
        // legacy single-phase SQL, including scores and ordered hydration.
        for limit in [0, 1, 2, 3, expected.len(), expected.len() + 2] {
            for offset in [
                0,
                1,
                2,
                expected.len() - 1,
                expected.len(),
                expected.len() + 2,
            ] {
                let legacy = legacy_event_hits_page(&store, "pagetie", limit, offset);
                let page = store
                    .search_event_hits_page("pagetie", limit, offset)
                    .unwrap();
                assert_eq!(page, legacy, "limit={limit} offset={offset}");
            }
        }
    }

    /// Asserts the two-phase shape of the ranked event page query: the
    /// LIMIT/OFFSET candidate selection subquery touches only the FTS index
    /// and the narrow `events` sort keys, while the wide hydration joins
    /// (runs/sessions/capture_sources/history_records) are indexed lookups
    /// driven by the already-limited page rows.
    #[test]
    fn event_hits_page_query_plan_bounds_wide_hydration_to_ranked_page() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let mut stmt = store
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {SEARCH_EVENT_HITS_PAGE_SQL}"))
            .unwrap();
        let rows = stmt
            .query_map(params!["pagetie", 3_i64, 0_i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let plan_text = rows
            .iter()
            .map(|(id, parent, detail)| format!("{id} {parent} {detail}"))
            .collect::<Vec<_>>()
            .join("\n");

        // Exactly one subquery node hosts the ranked candidate page.
        let subquery_roots = rows
            .iter()
            .filter(|(_, _, detail)| {
                detail.starts_with("CO-ROUTINE") || detail.starts_with("MATERIALIZE")
            })
            .map(|(id, _, _)| *id)
            .collect::<Vec<_>>();
        assert_eq!(subquery_roots.len(), 1, "plan:\n{plan_text}");
        let mut inside = std::collections::HashSet::from([subquery_roots[0]]);
        loop {
            let before = inside.len();
            for (id, parent, _) in &rows {
                if inside.contains(parent) {
                    inside.insert(*id);
                }
            }
            if inside.len() == before {
                break;
            }
        }

        fn table_access(detail: &str) -> Option<(&str, &str)> {
            let mut parts = detail.split_whitespace();
            let op = parts.next()?;
            if op != "SCAN" && op != "SEARCH" {
                return None;
            }
            Some((op, parts.next()?))
        }

        let mut inner_tables = std::collections::HashSet::new();
        let mut outer = Vec::new();
        for (id, _, detail) in &rows {
            let Some((op, table)) = table_access(detail) else {
                continue;
            };
            if inside.contains(id) {
                inner_tables.insert(table.to_owned());
            } else {
                outer.push((op.to_owned(), table.to_owned()));
            }
        }

        // Candidate selection reads only the FTS index plus the narrow
        // events sort keys; nothing wide is joined before LIMIT/OFFSET.
        assert_eq!(
            inner_tables,
            std::collections::HashSet::from(["event_search".to_owned(), "e".to_owned()]),
            "plan:\n{plan_text}"
        );

        // Hydration is driven by the limited page and every wide join is an
        // indexed SEARCH (per selected row), never a table SCAN.
        assert!(
            outer.iter().any(|(_, table)| table == "ranked_page"),
            "plan:\n{plan_text}"
        );
        for wide in [
            "r",
            "s",
            "rs",
            "event_source",
            "session_source",
            "run_source",
            "wr",
        ] {
            assert!(
                outer
                    .iter()
                    .any(|(op, table)| op == "SEARCH" && table == wide),
                "missing indexed page-bounded lookup for {wide}; plan:\n{plan_text}"
            );
            assert!(
                !outer
                    .iter()
                    .any(|(op, table)| op == "SCAN" && table == wide),
                "wide table {wide} must not be scanned; plan:\n{plan_text}"
            );
        }
        assert!(
            outer
                .iter()
                .any(|(op, table)| op == "SEARCH" && table == "e"),
            "plan:\n{plan_text}"
        );
    }

    #[test]
    fn since_threshold_ceils_sub_millisecond_since_to_next_millisecond() {
        let exact = DateTime::<Utc>::from_timestamp_millis(1_750_000_000_123).unwrap();
        assert_eq!(event_search_since_threshold_ms(exact), 1_750_000_000_123);

        // Any sub-millisecond remainder must round up: an event stored at the
        // floored millisecond is strictly before `since` and must be excluded.
        for nanos in [1, 500_000, 999_999] {
            let fractional = exact + chrono::Duration::nanoseconds(nanos);
            assert_eq!(
                event_search_since_threshold_ms(fractional),
                1_750_000_000_124,
                "nanos={nanos}"
            );
        }

        let pre_epoch = DateTime::<Utc>::from_timestamp_millis(-1_001).unwrap();
        assert_eq!(event_search_since_threshold_ms(pre_epoch), -1_001);
        assert_eq!(
            event_search_since_threshold_ms(pre_epoch + chrono::Duration::microseconds(1)),
            -1_000
        );
    }

    #[test]
    fn filtered_event_hits_page_sql_shapes_share_exact_hydration_phase() {
        fn phases(sql: &str) -> (&str, &str) {
            sql.split_once("\n    )")
                .expect("ranked_page CTE terminator")
        }
        let (_, unfiltered_hydration) = phases(SEARCH_EVENT_HITS_PAGE_SQL);
        for filtered in [
            SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL,
            SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL,
        ] {
            let (cte, hydration) = phases(filtered);
            assert_eq!(hydration, unfiltered_hydration);
            // Both filtered CTEs must keep the exact unfiltered sort keys and
            // page clamp so filtered pages are slices of the same ordering.
            assert!(cte.contains(
                "ORDER BY bm25(event_search), e.occurred_at_ms DESC, e.seq DESC, event_search.event_id"
            ));
            assert!(cte.contains("LIMIT ?2 OFFSET ?3"));
        }
    }

    /// The role bitmask CASE in the filtered SQL must stay a total, exact
    /// mapping of the `EventRole` domain: one distinct bit per variant string
    /// (matching `event_role_bit`), covering exactly the values the schema
    /// CHECK constraint admits, with NULL falling to the ELSE arm. The
    /// tool-noise NOT IN list must likewise name exactly the event types the
    /// Rust `event_hit_is_excluded_tool_noise` predicate excludes.
    #[test]
    fn role_mask_sql_case_covers_event_role_domain() {
        let variants = [
            EventRole::User,
            EventRole::Assistant,
            EventRole::System,
            EventRole::Tool,
            EventRole::Unknown,
        ];
        assert_eq!(
            variants.map(EventRole::as_str).to_vec(),
            EventRole::variants().to_vec(),
            "bitmask coverage must track the EventRole domain"
        );
        let mut seen_bits = 0_i64;
        for role in variants {
            let bit = event_role_bit(role);
            assert_eq!(bit.count_ones(), 1, "{role:?} must map to a single bit");
            assert_eq!(seen_bits & bit, 0, "{role:?} bit must be distinct");
            seen_bits |= bit;
            for sql in [
                SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL,
                SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL,
            ] {
                assert_eq!(
                    sql.matches(&format!("WHEN '{}' THEN {bit}", role.as_str()))
                        .count(),
                    2,
                    "include and exclude CASE arms must both map {role:?} to {bit}"
                );
            }
        }
        assert_eq!(
            event_role_mask(&variants),
            seen_bits,
            "mask must OR every variant bit"
        );
        assert_eq!(event_role_mask(&[]), 0, "empty set must disable the filter");

        // The schema CHECK constraint admits exactly the enum domain (plus
        // NULL), so the CASE mapping is total over reachable rows.
        let schema: String = pushdown_corpus()
            .store
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        for role in EventRole::variants() {
            assert!(
                schema.contains(&format!("'{role}'")),
                "events.role CHECK must admit '{role}'"
            );
        }

        let noise_types = [
            EventType::ToolCall,
            EventType::ToolOutput,
            EventType::CommandStarted,
            EventType::CommandOutput,
            EventType::CommandFinished,
        ];
        let expected_list = format!(
            "('{}', '{}', '{}', '{}',\n                '{}')",
            noise_types[0].as_str(),
            noise_types[1].as_str(),
            noise_types[2].as_str(),
            noise_types[3].as_str(),
            noise_types[4].as_str(),
        );
        for sql in [
            SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL,
            SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL,
        ] {
            assert!(
                sql.contains(&expected_list),
                "tool-noise NOT IN list must name exactly the Rust-excluded event types"
            );
        }
    }

    /// Hydrated hits expose payload-derived executable names for the
    /// Rust-side `exclude_tool_name` filter: structured `tool` keys and
    /// `command` strings normalize to lowercase basenames.
    #[test]
    fn event_search_hits_hydrate_tool_names_from_payload() {
        let corpus = pushdown_corpus();
        let hits = corpus
            .store
            .search_event_hits_page("pushfilter", 100, 0)
            .unwrap();
        let by_id = |suffix: &str| {
            hits.iter()
                .find(|hit| hit.event_id.to_string().ends_with(suffix))
                .unwrap()
        };
        assert_eq!(by_id("0f0049").tool_names, vec!["shell".to_owned()]);
        assert_eq!(by_id("0f004a").tool_names, vec!["ctx".to_owned()]);
        assert!(by_id("0f0041").tool_names.is_empty());
    }

    /// Executable-name extraction feeding `tool_names`: quoted commands,
    /// absolute paths, and mixed case normalize to a lowercase basename;
    /// nested `body` objects are searched; broken payloads yield nothing.
    #[test]
    fn event_tool_names_normalize_paths_quotes_case_and_nesting() {
        assert_eq!(
            event_tool_names_from_payload(r#"{"command":"/usr/local/bin/CTX search foo"}"#),
            vec!["ctx".to_owned()]
        );
        assert_eq!(
            event_tool_names_from_payload(r#"{"tool":"'Shell'","command":"`git` status"}"#),
            vec!["git".to_owned(), "shell".to_owned()]
        );
        assert_eq!(
            event_tool_names_from_payload(r#"{"body":{"executable":"[node]"}}"#),
            vec!["node".to_owned()]
        );
        assert!(event_tool_names_from_payload(r#"{"output":"ctx search"}"#).is_empty());
        assert!(event_tool_names_from_payload("not json").is_empty());
        assert!(event_tool_names_from_payload(r#"{"command":"   "}"#).is_empty());
    }

    /// Corpus exercising every fallback chain the pushed-down predicates
    /// touch: direct sessions (primary/subagent/unknown), a run-only session
    /// chain, provider via event-level capture source only, a fully
    /// sessionless row, an event whose session identity exists only in
    /// the event_search projection, and role-varied rows (user, tool with
    /// structured tool/command payload keys, NULL role, system) for the
    /// role and tool-noise pushdown predicates. Timestamps straddle a
    /// millisecond boundary for the fractional `since` cases.
    struct PushdownCorpus {
        store: Store,
        _temp: tempfile::TempDir,
        base: DateTime<Utc>,
        s_primary: Uuid,
        s_subagent: Uuid,
        s_run: Uuid,
        projection_session: Uuid,
    }

    fn pushdown_corpus() -> PushdownCorpus {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let base = fixed_time();

        let record_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000f0001").unwrap();
        let mut record = HistoryRecord::new(
            "Pushdown record",
            "pushdown record body",
            vec!["pushdown-test".into()],
            "task",
            Some("/workspace/pushdown".into()),
        );
        record.id = record_id;
        record.created_at = base;
        record.updated_at = base;
        store.insert_record(&record).unwrap();

        let insert_session =
            |id: &str, provider: &str, agent_type: &str, is_primary: i64| -> Uuid {
                let session_id = Uuid::parse_str(id).unwrap();
                store
                    .conn
                    .execute(
                        r#"
                        INSERT INTO sessions
                        (id, history_record_id, provider, external_session_id, agent_type,
                         is_primary, status, fidelity, started_at_ms, created_at_ms, updated_at_ms)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'imported', 'full', 1, 1, 1)
                        "#,
                        params![
                            session_id.to_string(),
                            record_id.to_string(),
                            provider,
                            format!("external-{id}"),
                            agent_type,
                            is_primary
                        ],
                    )
                    .unwrap();
                session_id
            };
        let s_primary = insert_session(
            "018f45d0-0000-7000-8000-0000000f0011",
            "codex",
            "primary",
            1,
        );
        let s_subagent = insert_session(
            "018f45d0-0000-7000-8000-0000000f0012",
            "claude",
            "subagent",
            0,
        );
        let s_unknown = insert_session(
            "018f45d0-0000-7000-8000-0000000f0013",
            "opencode",
            "unknown",
            0,
        );
        let s_run = insert_session(
            "018f45d0-0000-7000-8000-0000000f0014",
            "claude",
            "subagent",
            0,
        );

        let event_source_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000f0021").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO capture_sources
                (id, kind, provider, machine_id, cwd, raw_source_path, started_at_ms, fidelity,
                 metadata_json)
                VALUES (?1, 'provider_import', 'gemini', 'test-machine', '/workspace/pushdown',
                        '/workspace/pushdown/gemini.jsonl', 1, 'full', '{}')
                "#,
                params![event_source_id.to_string()],
            )
            .unwrap();
        let run_source_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000f0022").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO capture_sources
                (id, kind, provider, machine_id, cwd, raw_source_path, started_at_ms, fidelity,
                 metadata_json)
                VALUES (?1, 'provider_import', 'cursor', 'run-machine', '/workspace/run',
                        '/workspace/run/transcript.jsonl', 1, 'full', '{}')
                "#,
                params![run_source_id.to_string()],
            )
            .unwrap();
        let run_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000f0031").unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO runs
                (id, history_record_id, session_id, run_type, status, started_at_ms,
                 created_at_ms, updated_at_ms, source_id)
                VALUES (?1, ?2, ?3, 'agent_turn', 'succeeded', 1, 1, 1, ?4)
                "#,
                params![
                    run_id.to_string(),
                    record_id.to_string(),
                    s_run.to_string(),
                    run_source_id.to_string()
                ],
            )
            .unwrap();

        let event = |id: &str,
                     seq: u64,
                     session: Option<Uuid>,
                     run: Option<Uuid>,
                     source: Option<Uuid>,
                     event_type: EventType,
                     at: DateTime<Utc>,
                     text: &str| Event {
            id: Uuid::parse_str(id).unwrap(),
            seq,
            history_record_id: Some(record_id),
            session_id: session,
            run_id: run,
            event_type,
            role: Some(EventRole::Assistant),
            occurred_at: at,
            capture_source_id: source,
            payload: serde_json::json!({ "text": text }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        let plus_ms = |ms: i64| base + chrono::Duration::milliseconds(ms);
        let events = [
            event(
                "018f45d0-0000-7000-8000-0000000f0041",
                1,
                Some(s_primary),
                None,
                None,
                EventType::Message,
                plus_ms(0),
                "pushfilter pushfilter",
            ),
            event(
                "018f45d0-0000-7000-8000-0000000f0042",
                2,
                Some(s_subagent),
                None,
                None,
                EventType::Message,
                plus_ms(1),
                "pushfilter subagent",
            ),
            event(
                "018f45d0-0000-7000-8000-0000000f0043",
                3,
                Some(s_unknown),
                None,
                None,
                EventType::ToolCall,
                plus_ms(0),
                "pushfilter unknown-agent",
            ),
            // Session identity only through the run chain (rs fallback).
            event(
                "018f45d0-0000-7000-8000-0000000f0044",
                4,
                None,
                Some(run_id),
                None,
                EventType::Message,
                plus_ms(2),
                "pushfilter run-chained",
            ),
            // Provider only through the event-level capture source.
            event(
                "018f45d0-0000-7000-8000-0000000f0045",
                5,
                None,
                None,
                Some(event_source_id),
                EventType::ToolCall,
                plus_ms(0),
                "pushfilter source-only",
            ),
            // Fully sessionless: no session, run, or source identity.
            event(
                "018f45d0-0000-7000-8000-0000000f0046",
                6,
                None,
                None,
                None,
                EventType::Message,
                plus_ms(1),
                "pushfilter sessionless",
            ),
            // Session identity only in the event_search projection row.
            event(
                "018f45d0-0000-7000-8000-0000000f0047",
                7,
                None,
                None,
                None,
                EventType::Message,
                plus_ms(0),
                "pushfilter projection-fallback",
            ),
        ];
        for event in &events {
            store.upsert_event(event).unwrap();
        }
        // Role-varied rows for the role/tool-noise pushdown axes: an explicit
        // user message, tool-role tool/command events carrying structured
        // tool/executable payload keys, a NULL-role message (include sets
        // must reject it, exclude sets must keep it), and a system-role
        // command output on the subagent session.
        let mut role_user = event(
            "018f45d0-0000-7000-8000-0000000f0048",
            8,
            Some(s_primary),
            None,
            None,
            EventType::Message,
            plus_ms(0),
            "pushfilter role-user",
        );
        role_user.role = Some(EventRole::User);
        let mut tool_shell = event(
            "018f45d0-0000-7000-8000-0000000f0049",
            9,
            Some(s_primary),
            None,
            None,
            EventType::ToolOutput,
            plus_ms(0),
            "pushfilter tool-shell",
        );
        tool_shell.role = Some(EventRole::Tool);
        tool_shell.payload =
            serde_json::json!({ "text": "pushfilter tool-shell", "tool": "shell" });
        let mut command_ctx = event(
            "018f45d0-0000-7000-8000-0000000f004a",
            10,
            Some(s_primary),
            None,
            None,
            EventType::CommandStarted,
            plus_ms(0),
            "pushfilter command-ctx",
        );
        command_ctx.role = Some(EventRole::Tool);
        command_ctx.payload = serde_json::json!({
            "text": "pushfilter command-ctx",
            "command": "/usr/bin/CTX search pushfilter",
        });
        let mut role_null = event(
            "018f45d0-0000-7000-8000-0000000f004b",
            11,
            Some(s_primary),
            None,
            None,
            EventType::Message,
            plus_ms(0),
            "pushfilter role-null",
        );
        role_null.role = None;
        let mut system_output = event(
            "018f45d0-0000-7000-8000-0000000f004c",
            12,
            Some(s_subagent),
            None,
            None,
            EventType::CommandOutput,
            plus_ms(0),
            "pushfilter system-output",
        );
        system_output.role = Some(EventRole::System);
        for event in [
            &role_user,
            &tool_shell,
            &command_ctx,
            &role_null,
            &system_output,
        ] {
            store.upsert_event(event).unwrap();
        }
        let projection_session = s_primary;
        store
            .conn
            .execute(
                "UPDATE event_search SET session_id = ?1 WHERE event_id = ?2",
                params![
                    projection_session.to_string(),
                    "018f45d0-0000-7000-8000-0000000f0047"
                ],
            )
            .unwrap();

        PushdownCorpus {
            store,
            _temp: temp,
            base,
            s_primary,
            s_subagent,
            s_run,
            projection_session,
        }
    }

    /// Test-side oracle that mirrors the pushed-down subset of
    /// `ctx-history-search::event_hit_matches_filters` (session, provider,
    /// since, event_type, role include/exclude, tool-noise event types) and
    /// `event_hit_matches_agent_scope` (primary / primary-or-sessionless)
    /// over hydrated hits. The role and tool-noise arms are copied verbatim
    /// from the search crate's `role_matches` /
    /// `event_hit_is_excluded_tool_noise` Rust predicates so the differential
    /// proves SQL pushdown equivalence against the real filter semantics.
    fn oracle_matches(hit: &EventSearchHit, filters: &EventSearchSqlFilters) -> bool {
        let primary =
            hit.session_is_primary == Some(true) || hit.agent_type == Some(AgentType::Primary);
        let sessionless = hit.session_is_primary.is_none() && hit.agent_type.is_none();
        let tool_noise = matches!(
            hit.event_type,
            EventType::ToolCall
                | EventType::ToolOutput
                | EventType::CommandStarted
                | EventType::CommandOutput
                | EventType::CommandFinished
        );
        filters
            .session_id
            .is_none_or(|id| hit.session_id == Some(id))
            && filters
                .provider
                .is_none_or(|provider| hit.provider == Some(provider))
            && filters.since.is_none_or(|since| hit.occurred_at >= since)
            && filters
                .event_type
                .is_none_or(|event_type| hit.event_type == event_type)
            && match filters.agent_scope {
                None => true,
                Some(EventSearchAgentScope::PrimaryOrSessionless) => primary || sessionless,
                Some(EventSearchAgentScope::PrimaryOnly) => primary,
            }
            && (filters.roles.is_empty()
                || hit.role.is_some_and(|role| filters.roles.contains(&role)))
            && !hit
                .role
                .is_some_and(|role| filters.exclude_roles.contains(&role))
            && !(filters.exclude_tool_noise && tool_noise)
    }

    /// Differential oracle: for every filter combination, the filtered SQL
    /// page equals the unfiltered ranked stream filtered in Rust by the
    /// equivalent hit-level predicates and sliced by limit/offset — the exact
    /// contract `fast_event_search_packet` relies on when it pushes filters
    /// down while keeping `event_hit_matches_filters` as final authority.
    #[test]
    fn filtered_event_hits_page_equals_rust_filtered_unfiltered_stream() {
        let corpus = pushdown_corpus();
        let store = &corpus.store;
        let query = "pushfilter";

        let full = store.search_event_hits_page(query, 100, 0).unwrap();
        assert_eq!(full.len(), 12, "corpus must index all events");

        let exact_since = corpus.base;
        let fractional_since = corpus.base + chrono::Duration::microseconds(500);
        let next_ms_since = corpus.base + chrono::Duration::milliseconds(1);
        let sessions = [
            None,
            Some(corpus.s_primary),
            Some(corpus.s_subagent),
            Some(corpus.s_run),
        ];
        let providers = [
            None,
            Some(CaptureProvider::Codex),
            Some(CaptureProvider::Claude),
            Some(CaptureProvider::Gemini),
        ];
        let sinces = [
            None,
            Some(exact_since),
            Some(fractional_since),
            Some(next_ms_since),
        ];
        let event_types = [None, Some(EventType::ToolCall)];
        let scopes = [
            None,
            Some(EventSearchAgentScope::PrimaryOrSessionless),
            Some(EventSearchAgentScope::PrimaryOnly),
        ];
        let role_sets: [Vec<EventRole>; 4] = [
            Vec::new(),
            vec![EventRole::User],
            vec![EventRole::User, EventRole::Assistant],
            vec![EventRole::Tool],
        ];
        let exclude_role_sets: [Vec<EventRole>; 3] = [
            Vec::new(),
            vec![EventRole::Tool],
            vec![EventRole::Assistant, EventRole::System],
        ];
        let noise_flags = [false, true];

        let mut combos = Vec::new();
        for session_id in sessions {
            for provider in providers {
                for since in sinces {
                    for event_type in event_types {
                        for agent_scope in scopes {
                            for roles in &role_sets {
                                for exclude_roles in &exclude_role_sets {
                                    for exclude_tool_noise in noise_flags {
                                        combos.push(EventSearchSqlFilters {
                                            session_id,
                                            provider,
                                            since,
                                            event_type,
                                            agent_scope,
                                            roles: roles.clone(),
                                            exclude_roles: exclude_roles.clone(),
                                            exclude_tool_noise,
                                            file_scope: None,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut nonempty_filtered_combos = 0;
        for filters in &combos {
            let expected: Vec<EventSearchHit> = full
                .iter()
                .filter(|hit| oracle_matches(hit, filters))
                .cloned()
                .collect();
            if !filters.is_empty() && !expected.is_empty() {
                nonempty_filtered_combos += 1;
            }
            let actual = store
                .search_event_hits_page_filtered(query, 100, 0, filters)
                .unwrap();
            assert_eq!(actual, expected, "filters={filters:?}");
        }
        assert!(nonempty_filtered_combos > 40, "corpus must not be vacuous");

        // Fractional-since ceiling has observable effect: events exactly at
        // the base millisecond pass `since = base` but fail `since = base +
        // 500µs`, while events at the next millisecond pass both.
        let at_exact = store
            .search_event_hits_page_filtered(
                query,
                100,
                0,
                &EventSearchSqlFilters {
                    since: Some(exact_since),
                    ..EventSearchSqlFilters::default()
                },
            )
            .unwrap();
        let at_fractional = store
            .search_event_hits_page_filtered(
                query,
                100,
                0,
                &EventSearchSqlFilters {
                    since: Some(fractional_since),
                    ..EventSearchSqlFilters::default()
                },
            )
            .unwrap();
        assert_eq!(at_exact.len(), 12);
        assert_eq!(at_fractional.len(), 3);
        assert!(at_fractional
            .iter()
            .all(|hit| hit.occurred_at >= fractional_since));

        // The projection-only session identity is honored by session pushdown.
        let projection = store
            .search_event_hits_page_filtered(
                query,
                100,
                0,
                &EventSearchSqlFilters {
                    session_id: Some(corpus.projection_session),
                    ..EventSearchSqlFilters::default()
                },
            )
            .unwrap();
        assert!(projection.iter().any(|hit| {
            hit.event_id == Uuid::parse_str("018f45d0-0000-7000-8000-0000000f0047").unwrap()
        }));

        // Limit/offset paging over filtered results is an exact slice of the
        // filtered ordering, replicating the unfiltered API's `limit.max(1)`
        // clamp and out-of-range behavior.
        let paged_filters = [
            EventSearchSqlFilters::default(),
            EventSearchSqlFilters {
                agent_scope: Some(EventSearchAgentScope::PrimaryOrSessionless),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                provider: Some(CaptureProvider::Claude),
                agent_scope: Some(EventSearchAgentScope::PrimaryOnly),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                session_id: Some(corpus.s_subagent),
                since: Some(fractional_since),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                event_type: Some(EventType::Message),
                since: Some(exact_since),
                agent_scope: Some(EventSearchAgentScope::PrimaryOrSessionless),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                roles: vec![EventRole::User, EventRole::Assistant],
                agent_scope: Some(EventSearchAgentScope::PrimaryOrSessionless),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                exclude_roles: vec![EventRole::Tool],
                exclude_tool_noise: true,
                ..EventSearchSqlFilters::default()
            },
        ];
        for filters in &paged_filters {
            let expected_all: Vec<EventSearchHit> = full
                .iter()
                .filter(|hit| oracle_matches(hit, filters))
                .cloned()
                .collect();
            for limit in [0_usize, 1, 2, 3, expected_all.len(), expected_all.len() + 2] {
                for offset in [0_usize, 1, 2, expected_all.len(), expected_all.len() + 2] {
                    let expected: Vec<EventSearchHit> = expected_all
                        .iter()
                        .skip(offset)
                        .take(limit.max(1))
                        .cloned()
                        .collect();
                    let actual = store
                        .search_event_hits_page_filtered(query, limit, offset, filters)
                        .unwrap();
                    assert_eq!(
                        actual, expected,
                        "filters={filters:?} limit={limit} offset={offset}"
                    );
                }
            }
        }

        // Empty filters delegate to the unfiltered two-phase query.
        assert_eq!(
            store
                .search_event_hits_page_filtered(query, 3, 1, &EventSearchSqlFilters::default())
                .unwrap(),
            store.search_event_hits_page(query, 3, 1).unwrap()
        );
    }

    /// Differential oracle across match modes: for all/any/phrase plans, the
    /// filtered SQL page equals the unfiltered plan-ranked stream filtered in
    /// Rust and sliced by limit/offset. The MATCH expression is only ever a
    /// bound `?1` parameter, so every mode runs through the same prepared SQL
    /// constants whose EXPLAIN shapes are asserted by
    /// `event_hits_page_query_plan_bounds_wide_hydration_to_ranked_page` and
    /// `filtered_event_hits_page_query_plan_keeps_two_phase_shape`; the page
    /// execution counter below proves plan calls take those same statements.
    #[test]
    fn filtered_event_hits_plan_page_modes_equal_rust_filtered_plan_stream() {
        let corpus = pushdown_corpus();
        let store = &corpus.store;

        let plans = [
            SearchQueryPlan::new(SearchMatchMode::All, ["pushfilter"]),
            SearchQueryPlan::new(SearchMatchMode::All, ["subagent pushfilter"]),
            SearchQueryPlan::new(SearchMatchMode::Any, ["subagent sessionless qzzqx"]),
            SearchQueryPlan::new(SearchMatchMode::Phrase, ["pushfilter subagent"]),
            SearchQueryPlan::new(SearchMatchMode::Phrase, ["subagent pushfilter"]),
            // Operator-looking input stays literal in every mode.
            SearchQueryPlan::new(SearchMatchMode::All, ["pushfilter OR subagent"]),
        ];
        let filter_shapes = [
            EventSearchSqlFilters::default(),
            EventSearchSqlFilters {
                agent_scope: Some(EventSearchAgentScope::PrimaryOrSessionless),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                provider: Some(CaptureProvider::Claude),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                session_id: Some(corpus.s_subagent),
                event_type: Some(EventType::Message),
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                roles: vec![EventRole::User],
                ..EventSearchSqlFilters::default()
            },
            EventSearchSqlFilters {
                exclude_roles: vec![EventRole::Assistant],
                exclude_tool_noise: true,
                ..EventSearchSqlFilters::default()
            },
        ];

        let mut nonempty_mode_combos = 0;
        for plan in &plans {
            let full = store.search_event_hits_plan_page(plan, 100, 0).unwrap();
            for filters in &filter_shapes {
                let expected_all: Vec<EventSearchHit> = full
                    .iter()
                    .filter(|hit| oracle_matches(hit, filters))
                    .cloned()
                    .collect();
                if !expected_all.is_empty() {
                    nonempty_mode_combos += 1;
                }
                for (limit, offset) in [(100_usize, 0_usize), (2, 0), (2, 1), (1, 2)] {
                    let expected: Vec<EventSearchHit> = expected_all
                        .iter()
                        .skip(offset)
                        .take(limit.max(1))
                        .cloned()
                        .collect();
                    let before = store.event_search_page_executions();
                    let actual = store
                        .search_event_hits_plan_page_filtered(plan, limit, offset, filters)
                        .unwrap();
                    assert_eq!(
                        store.event_search_page_executions() - before,
                        1,
                        "plan-filtered pages must run one ranked page statement"
                    );
                    assert_eq!(
                        actual, expected,
                        "plan={plan:?} filters={filters:?} limit={limit} offset={offset}"
                    );
                }
            }
        }
        assert!(nonempty_mode_combos > 8, "mode corpus must not be vacuous");

        // Mode semantics are visible in the SQL stream itself: `all` requires
        // both tokens, `phrase` additionally requires adjacency/order, and
        // reversed phrase order matches nothing.
        let all_hits = store
            .search_event_hits_plan_page(
                &SearchQueryPlan::new(SearchMatchMode::All, ["subagent pushfilter"]),
                100,
                0,
            )
            .unwrap();
        assert!(!all_hits.is_empty());
        assert!(all_hits
            .iter()
            .all(|hit| hit.preview.contains("pushfilter subagent")));
        let phrase_hits = store
            .search_event_hits_plan_page(
                &SearchQueryPlan::new(SearchMatchMode::Phrase, ["pushfilter subagent"]),
                100,
                0,
            )
            .unwrap();
        assert_eq!(
            phrase_hits
                .iter()
                .map(|hit| hit.event_id)
                .collect::<Vec<_>>(),
            all_hits.iter().map(|hit| hit.event_id).collect::<Vec<_>>()
        );
        assert!(store
            .search_event_hits_plan_page(
                &SearchQueryPlan::new(SearchMatchMode::Phrase, ["subagent pushfilter"]),
                100,
                0,
            )
            .unwrap()
            .is_empty());
        let any_hits = store
            .search_event_hits_plan_page(
                &SearchQueryPlan::new(SearchMatchMode::Any, ["subagent sessionless qzzqx"]),
                100,
                0,
            )
            .unwrap();
        assert!(any_hits.len() > phrase_hits.len());
    }

    /// The filtered shapes must preserve the two-phase plan: candidate
    /// selection (FTS index + narrow keys + the predicate joins) happens
    /// inside the ranked_page subquery with LIMIT/OFFSET, every predicate
    /// join is an indexed SEARCH (never a table SCAN), and history_records
    /// hydration stays outside, bounded to the selected page.
    #[test]
    fn filtered_event_hits_page_query_plan_keeps_two_phase_shape() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        for (sql, inner_expected) in [
            (
                SEARCH_EVENT_HITS_PAGE_SCOPED_FILTERED_SQL,
                vec!["event_search", "e", "r", "s", "rs", "json_each"],
            ),
            (
                SEARCH_EVENT_HITS_PAGE_PROVIDER_FILTERED_SQL,
                vec![
                    "event_search",
                    "e",
                    "r",
                    "s",
                    "rs",
                    "event_source",
                    "session_source",
                    "run_source",
                    "json_each",
                ],
            ),
        ] {
            let mut stmt = store
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let rows = stmt
                .query_map(
                    params![
                        "pushfilter",
                        3_i64,
                        0_i64,
                        Some("018f45d0-0000-7000-8000-0000000f0011"),
                        Some("codex"),
                        Some(1_i64),
                        Some("message"),
                        1_i64,
                        3_i64,
                        8_i64,
                        1_i64,
                        None::<String>
                    ],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            let plan_text = rows
                .iter()
                .map(|(id, parent, detail)| format!("{id} {parent} {detail}"))
                .collect::<Vec<_>>()
                .join("\n");

            let subquery_roots = rows
                .iter()
                .filter(|(_, _, detail)| {
                    detail.starts_with("CO-ROUTINE") || detail.starts_with("MATERIALIZE")
                })
                .map(|(id, _, _)| *id)
                .collect::<Vec<_>>();
            assert_eq!(subquery_roots.len(), 1, "plan:\n{plan_text}");
            let mut inside = std::collections::HashSet::from([subquery_roots[0]]);
            loop {
                let before = inside.len();
                for (id, parent, _) in &rows {
                    if inside.contains(parent) {
                        inside.insert(*id);
                    }
                }
                if inside.len() == before {
                    break;
                }
            }

            fn table_access(detail: &str) -> Option<(&str, &str)> {
                let mut parts = detail.split_whitespace();
                let op = parts.next()?;
                if op != "SCAN" && op != "SEARCH" {
                    return None;
                }
                Some((op, parts.next()?))
            }

            let mut inner = Vec::new();
            let mut outer = Vec::new();
            for (id, _, detail) in &rows {
                let Some((op, table)) = table_access(detail) else {
                    continue;
                };
                if inside.contains(id) {
                    inner.push((op.to_owned(), table.to_owned()));
                } else {
                    outer.push((op.to_owned(), table.to_owned()));
                }
            }

            // Candidate selection touches exactly the FTS index, the narrow
            // events keys, and the predicate joins — nothing else (in
            // particular, no history_records) before LIMIT/OFFSET.
            assert_eq!(
                inner
                    .iter()
                    .map(|(_, table)| table.clone())
                    .collect::<std::collections::HashSet<_>>(),
                inner_expected
                    .iter()
                    .map(|table| (*table).to_owned())
                    .collect::<std::collections::HashSet<_>>(),
                "plan:\n{plan_text}"
            );
            // Every base-table predicate join inside the CTE is an indexed
            // SEARCH. SCAN is limited to the FTS driver and bounded json_each
            // virtual tables carrying the bound file-scope IDs.
            for (op, table) in &inner {
                if table != "event_search" && table != "json_each" {
                    assert_eq!(
                        op, "SEARCH",
                        "inner join on {table} must be indexed; plan:\n{plan_text}"
                    );
                }
            }

            // Hydration stays outside, driven by the limited page.
            assert!(
                outer.iter().any(|(_, table)| table == "ranked_page"),
                "plan:\n{plan_text}"
            );
            for wide in [
                "r",
                "s",
                "rs",
                "event_source",
                "session_source",
                "run_source",
                "wr",
            ] {
                assert!(
                    outer
                        .iter()
                        .any(|(op, table)| op == "SEARCH" && table == wide),
                    "missing indexed page-bounded lookup for {wide}; plan:\n{plan_text}"
                );
                assert!(
                    !outer
                        .iter()
                        .any(|(op, table)| op == "SCAN" && table == wide),
                    "wide table {wide} must not be scanned; plan:\n{plan_text}"
                );
            }
            assert!(
                !inner.iter().any(|(_, table)| table == "wr"),
                "record hydration must stay page-bounded; plan:\n{plan_text}"
            );
        }
    }
}

#[cfg(test)]
mod projection_probe_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-projection-probe-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn probe_event(seq: u64) -> Event {
        Event {
            id: new_id(),
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({ "text": format!("probe event body {seq:05}") }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    fn probe_record() -> HistoryRecord {
        let mut record = HistoryRecord::new(
            "Probe record title",
            "probe record body",
            vec!["probe".into()],
            "task",
            None,
        );
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        record
    }

    fn projection_count(store: &Store, table: &str) -> i64 {
        store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    #[test]
    fn event_search_projection_needs_backfill_state_combinations() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        // events empty + projection empty -> no backfill.
        assert!(!store.event_search_projection_needs_backfill().unwrap());

        // Orphan projection row while events is empty -> no backfill.
        store
            .conn
            .execute(
                r#"
                INSERT INTO event_search
                (event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
                VALUES ('orphan', NULL, NULL, 'user', 'orphan preview', 'message')
                "#,
                [],
            )
            .unwrap();
        assert!(!store.event_search_projection_needs_backfill().unwrap());
        store.conn.execute("DELETE FROM event_search", []).unwrap();

        // events nonempty + projection nonempty -> no backfill.
        store.upsert_event(&probe_event(1)).unwrap();
        assert!(!store.event_search_projection_needs_backfill().unwrap());

        // events nonempty + projection empty -> backfill required.
        store.conn.execute("DELETE FROM event_search", []).unwrap();
        assert!(store.event_search_projection_needs_backfill().unwrap());

        // Missing event_search table -> never claims backfill.
        store.conn.execute("DROP TABLE event_search", []).unwrap();
        assert!(!store.event_search_projection_needs_backfill().unwrap());
    }

    #[test]
    fn ensure_search_projection_initialized_state_combinations() {
        // Everything empty: no rebuild, projections stay empty.
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(projection_count(&store, "ctx_history_search"), 0);
        assert_eq!(projection_count(&store, "event_search"), 0);

        // Base rows present, every projection empty: rebuild repopulates.
        let record = probe_record();
        store.insert_record(&record).unwrap();
        store.upsert_event(&probe_event(1)).unwrap();
        for table in ["ctx_history_search", "event_search", "artifact_search"] {
            store
                .conn
                .execute(&format!("DELETE FROM {table}"), [])
                .unwrap();
        }
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(projection_count(&store, "ctx_history_search"), 1);
        assert_eq!(projection_count(&store, "event_search"), 1);

        // Any single nonempty projection short-circuits: event_search kept
        // its row, ctx_history_search emptied -> conservative skip.
        store
            .conn
            .execute("DELETE FROM ctx_history_search", [])
            .unwrap();
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(
            projection_count(&store, "ctx_history_search"),
            0,
            "nonempty event_search must skip the rebuild"
        );

        // Only ctx_history_search nonempty -> same conservative skip.
        store.refresh_search_index().unwrap();
        store.conn.execute("DELETE FROM event_search", []).unwrap();
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(
            projection_count(&store, "event_search"),
            0,
            "nonempty ctx_history_search must skip the rebuild"
        );

        // Only artifact_search nonempty -> same conservative skip.
        for table in ["ctx_history_search", "event_search"] {
            store
                .conn
                .execute(&format!("DELETE FROM {table}"), [])
                .unwrap();
        }
        store
            .conn
            .execute(
                r#"
                INSERT INTO artifact_search
                (artifact_id, history_record_id, safe_preview_text)
                VALUES ('sentinel-artifact', NULL, 'sentinel preview')
                "#,
                [],
            )
            .unwrap();
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(projection_count(&store, "ctx_history_search"), 0);
        assert_eq!(projection_count(&store, "event_search"), 0);
        store
            .conn
            .execute("DELETE FROM artifact_search", [])
            .unwrap();

        // Events only (no records): rebuild fills event_search.
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(projection_count(&store, "event_search"), 1);

        // Optional projections missing entirely: probe still rebuilds
        // ctx_history_search from base rows without touching dropped tables.
        for table in ["event_search", "artifact_search"] {
            store
                .conn
                .execute(&format!("DROP TABLE {table}"), [])
                .unwrap();
        }
        store
            .conn
            .execute("DELETE FROM ctx_history_search", [])
            .unwrap();
        store.ensure_search_projection_initialized().unwrap();
        assert_eq!(projection_count(&store, "ctx_history_search"), 1);

        // ctx_history_search missing: whole probe is a no-op.
        store
            .conn
            .execute("DROP TABLE ctx_history_search", [])
            .unwrap();
        store.ensure_search_projection_initialized().unwrap();
    }

    /// Deterministic bounded-work evidence: counts VDBE operations via the
    /// SQLite progress handler (granularity 1 opcode) for both startup
    /// probes on a small and a 20x larger indexed corpus. Existence probes
    /// must not scale with projection size; the previous full FTS
    /// `COUNT(*)` probes stepped the whole event_search content tree and
    /// fail this bound.
    #[test]
    fn projection_probes_do_bounded_work_independent_of_index_size() {
        fn probe_ops(event_count: u64) -> (usize, usize) {
            let temp = tempdir();
            let store = Store::open(temp.path().join("work.sqlite")).unwrap();
            for seq in 1..=event_count {
                store.upsert_event(&probe_event(seq)).unwrap();
            }
            assert_eq!(projection_count(&store, "event_search"), event_count as i64);

            let counter = Arc::new(AtomicUsize::new(0));
            let handler_counter = Arc::clone(&counter);
            store.conn.progress_handler(
                1,
                Some(move || {
                    handler_counter.fetch_add(1, Ordering::Relaxed);
                    false
                }),
            );

            counter.store(0, Ordering::Relaxed);
            assert!(!store.event_search_projection_needs_backfill().unwrap());
            let backfill_ops = counter.load(Ordering::Relaxed);

            counter.store(0, Ordering::Relaxed);
            store.ensure_search_projection_initialized().unwrap();
            let ensure_ops = counter.load(Ordering::Relaxed);

            store.conn.progress_handler(0, None::<fn() -> bool>);
            (backfill_ops, ensure_ops)
        }

        let (small_backfill, small_ensure) = probe_ops(30);
        let (large_backfill, large_ensure) = probe_ops(600);

        let slack = 16;
        assert!(
            large_backfill <= small_backfill + slack,
            "needs_backfill probe work scaled with index size: {small_backfill} ops at 30 events vs {large_backfill} ops at 600 events"
        );
        assert!(
            large_ensure <= small_ensure + slack,
            "ensure_search_projection_initialized probe work scaled with index size: {small_ensure} ops at 30 events vs {large_ensure} ops at 600 events"
        );
    }
}

#[cfg(test)]
mod search_maintenance_tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-search-maintenance-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn probe_event(seq: u64) -> Event {
        Event {
            id: new_id(),
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({ "text": format!("maintenance probe body {seq:05}") }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    /// Exact contents of the FTS5 segment b-tree shadow table. Two equal
    /// snapshots mean the maintenance pass rewrote nothing on disk.
    fn fts_data_snapshot(store: &Store, table: &str) -> Vec<(i64, Vec<u8>)> {
        let mut stmt = store
            .conn
            .prepare(&format!("SELECT id, block FROM {table}_data ORDER BY id"))
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    fn match_event_ids(store: &Store, query: &str) -> Vec<String> {
        let mut stmt = store
            .conn
            .prepare(
                "SELECT event_id FROM event_search WHERE event_search MATCH ?1 ORDER BY event_id",
            )
            .unwrap();
        let rows = stmt
            .query_map(params![query], |row| row.get::<_, String>(0))
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    fn assert_fts_integrity(store: &Store, table: &str) {
        store
            .conn
            .execute(
                &format!("INSERT INTO {table}({table}) VALUES ('integrity-check')"),
                [],
            )
            .unwrap();
    }

    fn total_changes(store: &Store) -> i64 {
        store
            .conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap()
    }

    fn bulk_insert_events(store: &Store, count: u64) {
        store.conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        for seq in 1..=count {
            store.upsert_event(&probe_event(seq)).unwrap();
        }
        store.conn.execute_batch("COMMIT;").unwrap();
    }

    #[test]
    fn bounded_merge_budget_is_positive_and_negative_is_unrepresentable() {
        // FTS5 interprets a negative `merge` argument as "merge the whole
        // index towards one segment" (unbounded, optimize-like). The budget
        // is a NonZeroU16, so neither zero nor a negative value can be
        // expressed, and the method takes no user input that could alter it.
        assert_eq!(Store::SEARCH_INDEX_MERGE_PAGES.get(), 256);
        assert!(i64::from(Store::SEARCH_INDEX_MERGE_PAGES.get()) > 0);
    }

    #[test]
    fn tiny_increment_fixture_has_no_eligible_positive_merge_work() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        for seq in 1..=20 {
            store.upsert_event(&probe_event(seq)).unwrap();
        }
        store.optimize_search_index().unwrap();

        // Tiny increment: one autocommit insert -> one small level-0 segment.
        store.upsert_event(&probe_event(21)).unwrap();

        let hits_before = match_event_ids(&store, "maintenance");
        assert_eq!(hits_before.len(), 21);
        let segments_before = fts_data_snapshot(&store, "event_search");

        store.merge_search_index_bounded().unwrap();

        // In this deterministic fixture, one sub-`usermerge` level-0 segment
        // is not eligible for a positive merge, so the segment b-tree is
        // byte-identical. Real tiny increments will usually behave this way,
        // but may encounter eligible fragmentation left by earlier imports.
        let segments_after = fts_data_snapshot(&store, "event_search");
        assert_eq!(
            segments_before, segments_after,
            "this optimized-baseline fixture must have no eligible merge work"
        );
        assert_eq!(match_event_ids(&store, "maintenance"), hits_before);
        assert_fts_integrity(&store, "event_search");
    }

    #[test]
    fn bounded_merge_compacts_fragmented_segments_and_preserves_matches() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        // Each autocommit upsert flushes its own level-0 segment, mirroring
        // repeated small imports; 12 segments exceed the FTS5 `usermerge`
        // eligibility threshold (default 4).
        for seq in 1..=12 {
            store.upsert_event(&probe_event(seq)).unwrap();
        }
        // artifact_search is intentionally not populated by current store
        // write paths. Seed a representative row directly so maintenance
        // identity/integrity coverage includes this optional projection.
        store
            .conn
            .execute(
                "INSERT INTO artifact_search
                 (artifact_id, history_record_id, safe_preview_text)
                 VALUES ('artifact-maintenance-sentinel', NULL, 'maintenance artifact sentinel')",
                [],
            )
            .unwrap();

        let hits_before = match_event_ids(&store, "maintenance");
        assert_eq!(hits_before.len(), 12);
        let exact_before = match_event_ids(&store, "\"maintenance probe body 00007\"");
        assert_eq!(exact_before.len(), 1);
        let artifact_before: String = store
            .conn
            .query_row(
                "SELECT artifact_id FROM artifact_search
                 WHERE artifact_search MATCH 'maintenance'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let segments_before = fts_data_snapshot(&store, "event_search");

        store.merge_search_index_bounded().unwrap();

        // Eligible fragmentation performs real, bounded compaction work.
        let segments_after = fts_data_snapshot(&store, "event_search");
        assert_ne!(
            segments_before, segments_after,
            "bounded merge must compact eligible fragmented segments"
        );
        assert!(
            segments_after.len() <= segments_before.len(),
            "compaction must not grow the segment b-tree: {} -> {}",
            segments_before.len(),
            segments_after.len()
        );

        // MATCH identities and index integrity are unchanged.
        assert_eq!(match_event_ids(&store, "maintenance"), hits_before);
        assert_eq!(
            match_event_ids(&store, "\"maintenance probe body 00007\""),
            exact_before
        );
        let artifact_after: String = store
            .conn
            .query_row(
                "SELECT artifact_id FROM artifact_search
                 WHERE artifact_search MATCH 'maintenance'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(artifact_after, artifact_before);
        assert_fts_integrity(&store, "event_search");
        assert_fts_integrity(&store, "ctx_history_search");
        assert_fts_integrity(&store, "artifact_search");
    }

    /// Deterministic cheap-request evidence via `total_changes()`, which
    /// counts every row FTS5 rewrites in its shadow tables on this
    /// connection. After a tiny increment on an optimized baseline, the
    /// positive merge request usually has no eligible work, while a full
    /// `optimize` rewrites the whole index. This is not a saturated-merge
    /// proof or a strict upper bound on SQLite's page writes.
    #[test]
    fn tiny_increment_merge_request_stays_cheap_as_index_grows() {
        fn maintenance_changes(event_count: u64, full_optimize: bool) -> i64 {
            let temp = tempdir();
            let store = Store::open(temp.path().join("work.sqlite")).unwrap();
            bulk_insert_events(&store, event_count);
            store.optimize_search_index().unwrap();
            store.upsert_event(&probe_event(event_count + 1)).unwrap();

            let before = total_changes(&store);
            if full_optimize {
                store.optimize_search_index().unwrap();
            } else {
                store.merge_search_index_bounded().unwrap();
            }
            total_changes(&store) - before
        }

        let small_merge = maintenance_changes(30, false);
        let large_merge = maintenance_changes(600, false);
        let large_optimize = maintenance_changes(600, true);

        let slack = 8;
        assert!(
            large_merge <= small_merge + slack,
            "tiny-increment merge request became unexpectedly expensive: {small_merge} shadow-table changes at 30 events vs {large_merge} at 600 events"
        );
        assert!(
            large_optimize > large_merge + slack,
            "full optimize should rewrite the whole index ({large_optimize} changes) while this tiny-increment merge request stays cheap ({large_merge} changes)"
        );
    }

    #[test]
    fn bounded_merge_handles_missing_optional_fts_tables() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        for seq in 1..=5 {
            store.upsert_event(&probe_event(seq)).unwrap();
        }

        // Optional projections may be absent on older stores.
        store
            .conn
            .execute("DROP TABLE artifact_search", [])
            .unwrap();
        store.merge_search_index_bounded().unwrap();

        store.conn.execute("DROP TABLE event_search", []).unwrap();
        store.merge_search_index_bounded().unwrap();

        // Even the record projection missing is tolerated, matching
        // optimize_search_index.
        store
            .conn
            .execute("DROP TABLE ctx_history_search", [])
            .unwrap();
        store.merge_search_index_bounded().unwrap();
    }
}

/// Retained benchmark evidence for the post-import maintenance change. Run
/// explicitly with:
///
/// ```text
/// cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_post_import
/// ```
#[cfg(test)]
mod search_maintenance_benches {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-maintenance-bench-")
            .tempdir_in(root)
            .unwrap()
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn bench_event(seq: u64) -> Event {
        Event {
            id: new_id(),
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            capture_source_id: None,
            payload: serde_json::json!({
                "text": format!(
                    "bench transcript event {seq:07}: the agent inspected the store, \
                     compared segment layouts, and reported deterministic merge \
                     behavior across repeated incremental import batches {seq:07}"
                )
            }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    fn build_corpus(size: u64) -> (tempfile::TempDir, Store, Duration) {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let started = Instant::now();
        store.conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        for seq in 1..=size {
            store.upsert_event(&bench_event(seq)).unwrap();
        }
        store.conn.execute_batch("COMMIT;").unwrap();
        let generation = started.elapsed();
        store.optimize_search_index().unwrap();
        (temp, store, generation)
    }

    #[test]
    #[ignore = "benchmark: cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_post_import"]
    fn bench_post_import_full_optimize_vs_bounded_merge() {
        for &size in &[10_000u64, 50_000] {
            let (_temp_a, optimize_store, gen_a) = build_corpus(size);
            let (_temp_b, merge_store, gen_b) = build_corpus(size);
            println!(
                "corpus {size}: generation {:?} / {:?} (excluded from maintenance timings)",
                gen_a, gen_b
            );

            // One tiny increment, then a single maintenance pass.
            optimize_store.upsert_event(&bench_event(size + 1)).unwrap();
            merge_store.upsert_event(&bench_event(size + 1)).unwrap();
            let started = Instant::now();
            optimize_store.optimize_search_index().unwrap();
            let optimize_once = started.elapsed();
            let started = Instant::now();
            merge_store.merge_search_index_bounded().unwrap();
            let merge_once = started.elapsed();
            println!(
                "corpus {size}: tiny increment -> full optimize {optimize_once:?} vs bounded merge {merge_once:?}"
            );

            // Repeated small increments with maintenance after each, the
            // steady-state import pattern.
            let increments = 100u64;
            let started = Instant::now();
            for step in 0..increments {
                optimize_store
                    .upsert_event(&bench_event(size + 2 + step))
                    .unwrap();
                optimize_store.optimize_search_index().unwrap();
            }
            let optimize_repeated = started.elapsed();
            let started = Instant::now();
            for step in 0..increments {
                merge_store
                    .upsert_event(&bench_event(size + 2 + step))
                    .unwrap();
                merge_store.merge_search_index_bounded().unwrap();
            }
            let merge_repeated = started.elapsed();
            println!(
                "corpus {size}: {increments} increments -> full optimize {optimize_repeated:?} vs bounded merge {merge_repeated:?}"
            );
        }
    }
}

#[cfg(test)]
mod projection_rebuild_atomicity_tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-rebuild-atomicity-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn probe_event(seq: u64) -> Event {
        Event {
            id: new_id(),
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({ "text": format!("rebuild probe body {seq:05}") }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    fn probe_record(title: &str) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            title,
            "rebuild probe record body",
            vec!["rebuild".into()],
            "task",
            None,
        );
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        record
    }

    type EventProjectionRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        String,
    );
    type RecordProjectionRow = (String, String, String, String, String, String, String);
    type ArtifactProjectionRow = (String, Option<String>, String);

    #[derive(Debug, PartialEq, Eq)]
    struct ProjectionSnapshot {
        events: Vec<EventProjectionRow>,
        records: Vec<RecordProjectionRow>,
        artifacts: Vec<ArtifactProjectionRow>,
    }

    /// Full, ordered contents of both populated projections; equality means
    /// the exact previous projection survived, not merely the same counts.
    fn projection_snapshot(store: &Store) -> ProjectionSnapshot {
        let mut events = store.conn.prepare(
            "SELECT event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket
             FROM event_search ORDER BY event_id",
        ).unwrap();
        let events = events
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let mut records = store
            .conn
            .prepare(
                "SELECT record_id, title, summary, primary_user_text, decision_text, context_text, tag_text
                 FROM ctx_history_search ORDER BY record_id",
            )
            .unwrap();
        let records = records
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let mut artifacts = store
            .conn
            .prepare(
                "SELECT artifact_id, history_record_id, safe_preview_text
                 FROM artifact_search ORDER BY artifact_id",
            )
            .unwrap();
        let artifacts = artifacts
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        ProjectionSnapshot {
            events,
            records,
            artifacts,
        }
    }

    fn populated_store(temp: &tempfile::TempDir) -> Store {
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        for seq in 1..=5 {
            store.upsert_event(&probe_event(seq)).unwrap();
        }
        store
            .insert_record(&probe_record("Rebuild record one"))
            .unwrap();
        store
            .insert_record(&probe_record("Rebuild record two"))
            .unwrap();
        store
    }

    fn corrupt_one_record_id(store: &Store) -> String {
        let original: String = store
            .conn
            .query_row(
                "SELECT id FROM history_records ORDER BY created_at DESC, id LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let changed = store
            .conn
            .execute(
                "UPDATE history_records SET id = 'malformed-history-record-id' WHERE id = ?1",
                params![original],
            )
            .unwrap();
        assert_eq!(changed, 1);
        original
    }

    fn match_ids(store: &Store, table: &str, id_column: &str, query: &str) -> Vec<String> {
        let sql =
            format!("SELECT {id_column} FROM {table} WHERE {table} MATCH ?1 ORDER BY {id_column}");
        let mut stmt = store.conn.prepare(&sql).unwrap();
        stmt.query_map(params![query], |row| row.get(0))
            .unwrap()
            .map(|row| row.unwrap())
            .collect()
    }

    fn assert_fts_integrity(store: &Store, table: &str) {
        store
            .conn
            .execute(
                &format!("INSERT INTO {table}({table}) VALUES ('integrity-check')"),
                [],
            )
            .unwrap();
    }

    #[test]
    fn successful_rebuild_reaches_base_and_fts_parity() {
        let temp = tempdir();
        let store = populated_store(&temp);
        // Wreck the projections, then rebuild from base tables.
        store.conn.execute("DELETE FROM event_search", []).unwrap();
        store
            .conn
            .execute("DELETE FROM ctx_history_search", [])
            .unwrap();

        store.refresh_search_index().unwrap();

        let snapshot = projection_snapshot(&store);
        assert_eq!(
            snapshot.events.len(),
            5,
            "every event with preview text projected"
        );
        assert_eq!(snapshot.records.len(), 2, "every history record projected");
        let base_events: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(snapshot.events.len() as i64, base_events);
        let hits: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event_search WHERE event_search MATCH 'rebuild'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hits, 5);
    }

    #[test]
    fn failed_rebuild_restores_previous_complete_projection() {
        let temp = tempdir();
        let store = populated_store(&temp);
        store.refresh_search_index().unwrap();
        store
            .conn
            .execute(
                "INSERT INTO artifact_search
                 (artifact_id, history_record_id, safe_preview_text)
                 VALUES ('artifact-rollback-sentinel', NULL, 'artifact rollback sentinel')",
                [],
            )
            .unwrap();
        let before = projection_snapshot(&store);
        assert_eq!(before.events.len(), 5);
        assert_eq!(before.records.len(), 2);
        assert_eq!(before.artifacts.len(), 1);
        let event_matches = match_ids(&store, "event_search", "event_id", "rebuild");
        let record_matches = match_ids(&store, "ctx_history_search", "record_id", "rebuild");
        let artifact_matches = match_ids(&store, "artifact_search", "artifact_id", "rollback");

        // The malformed record is not read until event_search has been fully
        // repopulated and artifact_search (including its sentinel) deleted.
        // Thus this fails late, after real replacement/deletion work, rather
        // than immediately on the first event row.
        let original_record_id = corrupt_one_record_id(&store);
        let err = store.refresh_search_index().unwrap_err();
        assert!(matches!(err, StoreError::Sql(_)), "unexpected error: {err}");
        assert!(
            err.to_string().contains("invalid character"),
            "original malformed-UUID error was not preserved: {err}"
        );

        // The previous complete projection is back, not empty or partial,
        // and the connection is back in autocommit (no dangling transaction).
        assert_eq!(projection_snapshot(&store), before);
        assert_eq!(
            match_ids(&store, "event_search", "event_id", "rebuild"),
            event_matches
        );
        assert_eq!(
            match_ids(&store, "ctx_history_search", "record_id", "rebuild"),
            record_matches
        );
        assert_eq!(
            match_ids(&store, "artifact_search", "artifact_id", "rollback"),
            artifact_matches
        );
        for table in SEARCH_PROJECTION_FTS_TABLES {
            assert_fts_integrity(&store, table);
        }
        assert!(store.conn.is_autocommit());

        // Repairing the base row makes the same rebuild succeed again.
        store
            .conn
            .execute(
                "UPDATE history_records SET id = ?1 WHERE id = 'malformed-history-record-id'",
                params![original_record_id],
            )
            .unwrap();
        store.refresh_search_index().unwrap();
        assert_eq!(projection_snapshot(&store).events.len(), 5);
    }

    #[test]
    fn rebuild_nests_inside_an_open_transaction() {
        let temp = tempdir();
        let store = populated_store(&temp);
        store.refresh_search_index().unwrap();
        let baseline = projection_snapshot(&store);

        // Success inside an outer transaction: the savepoint must nest (a
        // BEGIN here would fail with "cannot start a transaction within a
        // transaction") and its work must remain part of the outer
        // transaction, so rolling the outer back also rewinds the rebuild.
        store.conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        store.upsert_event(&probe_event(6)).unwrap();
        rebuild_search_projection(&store.conn).unwrap();
        assert!(!store.conn.is_autocommit());
        assert_eq!(projection_snapshot(&store).events.len(), 6);
        store.conn.execute_batch("ROLLBACK;").unwrap();
        assert_eq!(projection_snapshot(&store), baseline);

        // Failure inside an outer transaction: the savepoint rewinds only
        // the rebuild, preserves the original error, and leaves the outer
        // transaction open and usable — mirroring the migration callers.
        store.conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        corrupt_one_record_id(&store);
        let err = rebuild_search_projection(&store.conn).unwrap_err();
        assert!(matches!(err, StoreError::Sql(_)), "unexpected error: {err}");
        assert!(
            err.to_string().contains("invalid character"),
            "original malformed-UUID error was not preserved: {err}"
        );
        assert!(
            !store.conn.is_autocommit(),
            "outer transaction must survive a failed rebuild"
        );
        assert_eq!(projection_snapshot(&store), baseline);
        // The outer transaction is still usable after the failure.
        store
            .conn
            .execute("UPDATE events SET metadata_json = '{}'", [])
            .unwrap();
        store.conn.execute_batch("ROLLBACK;").unwrap();
        assert!(store.conn.is_autocommit());
        assert_eq!(projection_snapshot(&store), baseline);
    }
}

/// Retained benchmark evidence for the atomic projection rebuild. Run
/// explicitly with:
///
/// ```text
/// cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_rebuild
/// ```
#[cfg(test)]
mod projection_rebuild_benches {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-rebuild-bench-")
            .tempdir_in(root)
            .unwrap()
    }

    #[test]
    #[ignore = "benchmark: cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_rebuild"]
    fn bench_rebuild_search_projection_transactional_vs_autocommit() {
        for &size in &[10_000u64, 50_000] {
            let temp = tempdir();
            let store = Store::open(temp.path().join("work.sqlite")).unwrap();

            // Source generation, separated from the rebuild timings: raw
            // base-table rows only; the rebuild itself repopulates the
            // projections from scratch either way.
            let started = Instant::now();
            store.conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
            {
                let mut insert = store
                    .conn
                    .prepare(
                        "INSERT INTO events (id, seq, event_type, role, occurred_at_ms, payload_json)
                         VALUES (?1, ?2, 'message', 'user', ?3, ?4)",
                    )
                    .unwrap();
                for seq in 1..=size {
                    insert
                        .execute(params![
                            new_id().to_string(),
                            seq as i64,
                            1_750_000_000_000_i64 + seq as i64,
                            format!(
                                "{{\"text\": \"bench rebuild event {seq:07}: deterministic \
                                 transcript payload for projection rebuild timing {seq:07}\"}}"
                            ),
                        ])
                        .unwrap();
                }
            }
            store.conn.execute_batch("COMMIT;").unwrap();
            let generation = started.elapsed();
            println!(
                "corpus {size}: base-row generation {generation:?} (excluded from rebuild timings)"
            );

            // Old behavior: bare statement sequence in autocommit mode, one
            // implicit transaction (and WAL commit) per projected row.
            let started = Instant::now();
            rebuild_search_projection_body(&store.conn).unwrap();
            let autocommit = started.elapsed();

            // New behavior: the same statements inside one write transaction.
            let started = Instant::now();
            rebuild_search_projection(&store.conn).unwrap();
            let transactional = started.elapsed();

            println!(
                "corpus {size}: rebuild autocommit (per-row) {autocommit:?} vs transactional (atomic) {transactional:?}"
            );
        }
    }
}

#[cfg(test)]
mod projection_write_path_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, OnceLock};

    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-write-path-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn record_with(id: Uuid, body: &str) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            "Write path record title",
            body,
            vec!["writepath".into()],
            "task",
            None,
        );
        record.id = id;
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        record
    }

    fn event_with(id: Uuid, seq: u64, payload: serde_json::Value) -> Event {
        Event {
            id,
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload,
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    fn text_event(id: Uuid, seq: u64, text: &str) -> Event {
        event_with(id, seq, serde_json::json!({ "text": text }))
    }

    fn count(store: &Store, sql: &str) -> i64 {
        store.conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn record_projection_rows(store: &Store, id: Uuid) -> Vec<String> {
        let mut stmt = store
            .conn
            .prepare("SELECT summary FROM ctx_history_search WHERE record_id = ?1")
            .unwrap();
        let rows = stmt
            .query_map(params![id.to_string()], |row| row.get::<_, String>(0))
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    fn event_projection_rows(store: &Store, id: Uuid) -> Vec<String> {
        let mut stmt = store
            .conn
            .prepare("SELECT safe_preview_text FROM event_search WHERE event_id = ?1")
            .unwrap();
        let rows = stmt
            .query_map(params![id.to_string()], |row| row.get::<_, String>(0))
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    /// Counts VDBE operations for `work` via the SQLite progress handler
    /// (granularity 1 opcode). The full-scan FTS DELETE steps the virtual
    /// table row by row (one VNext per projected row), so its opcode count
    /// scales with index size; the insert-only projection path compiles to
    /// a constant opcode sequence regardless of index size.
    fn vdbe_ops(store: &Store, work: impl FnOnce()) -> usize {
        let counter = Arc::new(AtomicUsize::new(0));
        let handler_counter = Arc::clone(&counter);
        store.conn.progress_handler(
            1,
            Some(move || {
                handler_counter.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        work();
        store.conn.progress_handler(0, None::<fn() -> bool>);
        counter.load(Ordering::Relaxed)
    }

    fn populated_record_store(temp: &tempfile::TempDir, size: u64) -> Store {
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let records = (0..size)
            .map(|index| record_with(new_id(), &format!("seed record body {index:05}")))
            .collect::<Vec<_>>();
        store.upsert_records(&records).unwrap();
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM ctx_history_search"),
            size as i64
        );
        store
    }

    fn populated_event_store(temp: &tempfile::TempDir, size: u64) -> Store {
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store.begin_immediate_batch().unwrap();
        for seq in 1..=size {
            store
                .upsert_event(&text_event(
                    new_id(),
                    seq,
                    &format!("seed event body {seq:05}"),
                ))
                .unwrap();
        }
        store.commit_batch().unwrap();
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM event_search"),
            size as i64
        );
        store
    }

    /// Deterministic bounded-work evidence for #186: writing provably new
    /// records must not pay the O(index size) full-scan FTS DELETE, and —
    /// with the v1000 rowid maps — neither must updates of already-mapped
    /// ids, so the VDBE opcode count of those paths stays flat between a
    /// small and a 20x larger index. Only the one-time healing update of an
    /// unmapped (legacy) id still scans, and its immediate successor is
    /// bounded again.
    #[test]
    fn fresh_record_write_work_stays_bounded_as_index_grows() {
        fn ops_at(size: u64) -> (usize, usize, usize, usize, usize, usize) {
            let temp = tempdir();
            let store = populated_record_store(&temp, size);

            let insert_ops = vdbe_ops(&store, || {
                store
                    .insert_record(&record_with(new_id(), "fresh insert body"))
                    .unwrap();
            });
            let upsert_ops = vdbe_ops(&store, || {
                store
                    .upsert_record(&record_with(new_id(), "fresh upsert body"))
                    .unwrap();
            });
            let batch = (0..3)
                .map(|index| record_with(new_id(), &format!("fresh batch body {index}")))
                .collect::<Vec<_>>();
            let batch_ops = vdbe_ops(&store, || {
                store.upsert_records(&batch).unwrap();
            });
            // Map hit: seeding populated the rowid map, so the existing-id
            // update point-deletes its projection row.
            let map_hit_ops = vdbe_ops(&store, || {
                store
                    .upsert_record(&record_with(batch[0].id, "map-hit update body"))
                    .unwrap();
            });
            // Legacy shape: a v15→v1000 migrated store has projections but
            // an empty map, so the first update per id heals via the
            // full-scan delete...
            store
                .conn
                .execute("DELETE FROM record_search_rowids", [])
                .unwrap();
            let heal_ops = vdbe_ops(&store, || {
                store
                    .upsert_record(&record_with(batch[0].id, "healing update body"))
                    .unwrap();
            });
            // ...and the heal stored the fresh rowid, so the next update of
            // the same id is bounded again.
            let remapped_ops = vdbe_ops(&store, || {
                store
                    .upsert_record(&record_with(batch[0].id, "remapped update body"))
                    .unwrap();
            });
            (
                insert_ops,
                upsert_ops,
                batch_ops,
                map_hit_ops,
                heal_ops,
                remapped_ops,
            )
        }

        let (small_insert, small_upsert, small_batch, small_map_hit, small_heal, small_remapped) =
            ops_at(30);
        let (large_insert, large_upsert, large_batch, large_map_hit, large_heal, large_remapped) =
            ops_at(600);

        let slack = 32;
        assert!(
            large_insert <= small_insert + slack,
            "insert_record work scaled with index size: {small_insert} ops at 30 records vs {large_insert} ops at 600"
        );
        assert!(
            large_upsert <= small_upsert + slack,
            "upsert_record (new id) work scaled with index size: {small_upsert} ops at 30 records vs {large_upsert} ops at 600"
        );
        assert!(
            large_batch <= small_batch + slack,
            "upsert_records (all new ids) work scaled with index size: {small_batch} ops at 30 records vs {large_batch} ops at 600"
        );
        assert!(
            large_map_hit <= small_map_hit + slack,
            "map-hit existing-id upsert work scaled with index size: {small_map_hit} ops at 30 records vs {large_map_hit} ops at 600"
        );
        assert!(
            large_remapped <= small_remapped + slack,
            "post-heal existing-id upsert work scaled with index size: {small_remapped} ops at 30 records vs {large_remapped} ops at 600"
        );
        // The unmapped (legacy) arm still walks the projection exactly once
        // per id: it must scale with the index and dominate the mapped arm
        // at size, while the small-index heal stays in the same regime as
        // the mapped path.
        assert!(
            large_heal > small_heal + 600,
            "healing update unexpectedly stopped scanning the projection: {small_heal} ops at 30 records vs {large_heal} ops at 600"
        );
        assert!(
            large_heal > large_map_hit + 600,
            "healing update should dominate the mapped update at scale: heal {large_heal} ops vs mapped {large_map_hit}"
        );
    }

    /// Event-side twin of the record bound: fresh event ids through both
    /// upsert_event and insert_event_if_absent stay flat as event_search
    /// grows, mapped existing-id updates stay flat too, and only the
    /// one-time heal of an unmapped (legacy) id pays the delete scan.
    #[test]
    fn fresh_event_write_work_stays_bounded_as_index_grows() {
        fn ops_at(size: u64) -> (usize, usize, usize, usize, usize) {
            let temp = tempdir();
            let store = populated_event_store(&temp, size);

            let upsert_id = new_id();
            let upsert_ops = vdbe_ops(&store, || {
                store
                    .upsert_event(&text_event(upsert_id, size + 1, "fresh upsert event"))
                    .unwrap();
            });
            let insert_ops = vdbe_ops(&store, || {
                assert!(store
                    .insert_event_if_absent(&text_event(
                        new_id(),
                        size + 2,
                        "fresh insert-if-absent event"
                    ))
                    .unwrap());
            });
            let map_hit_ops = vdbe_ops(&store, || {
                store
                    .upsert_event(&text_event(upsert_id, size + 1, "map-hit updated event"))
                    .unwrap();
            });
            store
                .conn
                .execute("DELETE FROM event_search_rowids", [])
                .unwrap();
            let heal_ops = vdbe_ops(&store, || {
                store
                    .upsert_event(&text_event(upsert_id, size + 1, "healing updated event"))
                    .unwrap();
            });
            let remapped_ops = vdbe_ops(&store, || {
                store
                    .upsert_event(&text_event(upsert_id, size + 1, "remapped updated event"))
                    .unwrap();
            });
            (upsert_ops, insert_ops, map_hit_ops, heal_ops, remapped_ops)
        }

        let (small_upsert, small_insert, small_map_hit, small_heal, small_remapped) = ops_at(30);
        let (large_upsert, large_insert, large_map_hit, large_heal, large_remapped) = ops_at(600);

        let slack = 32;
        assert!(
            large_upsert <= small_upsert + slack,
            "upsert_event (new id) work scaled with index size: {small_upsert} ops at 30 events vs {large_upsert} ops at 600"
        );
        assert!(
            large_insert <= small_insert + slack,
            "insert_event_if_absent work scaled with index size: {small_insert} ops at 30 events vs {large_insert} ops at 600"
        );
        assert!(
            large_map_hit <= small_map_hit + slack,
            "map-hit existing-id upsert_event work scaled with index size: {small_map_hit} ops at 30 events vs {large_map_hit} ops at 600"
        );
        assert!(
            large_remapped <= small_remapped + slack,
            "post-heal existing-id upsert_event work scaled with index size: {small_remapped} ops at 30 events vs {large_remapped} ops at 600"
        );
        assert!(
            large_heal > small_heal + 600,
            "healing upsert_event unexpectedly stopped scanning the projection: {small_heal} ops at 30 events vs {large_heal} ops at 600"
        );
        assert!(
            large_heal > large_map_hit + 600,
            "healing upsert_event should dominate the mapped update at scale: heal {large_heal} ops vs mapped {large_map_hit}"
        );
    }

    #[test]
    fn same_id_updates_keep_exactly_one_projection_row_with_updated_text() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        let record_id = new_id();
        store
            .insert_record(&record_with(record_id, "original searchable alpha"))
            .unwrap();
        store
            .upsert_record(&record_with(record_id, "revised searchable bravo"))
            .unwrap();
        assert_eq!(
            record_projection_rows(&store, record_id),
            vec!["revised searchable bravo".to_owned()]
        );
        // Batch update of the same id again: still exactly one row.
        store
            .upsert_records(&[record_with(record_id, "batched searchable charlie")])
            .unwrap();
        assert_eq!(
            record_projection_rows(&store, record_id),
            vec!["batched searchable charlie".to_owned()]
        );
        assert_eq!(
            store.search_records("charlie", 10).unwrap()[0].id,
            record_id
        );
        assert!(store.search_records("alpha", 10).unwrap().is_empty());
        assert!(store.search_records("bravo", 10).unwrap().is_empty());

        let event_id = new_id();
        store
            .upsert_event(&text_event(event_id, 1, "original event delta"))
            .unwrap();
        store
            .upsert_event(&text_event(event_id, 1, "revised event echo"))
            .unwrap();
        assert_eq!(
            event_projection_rows(&store, event_id),
            vec!["revised event echo".to_owned()]
        );

        // A batch that repeats one id must not double-project it: the first
        // occurrence is new (insert-only), later ones are existing
        // (delete + insert).
        let repeated = new_id();
        store
            .upsert_records(&[
                record_with(repeated, "repeat one"),
                record_with(repeated, "repeat two"),
            ])
            .unwrap();
        assert_eq!(
            record_projection_rows(&store, repeated),
            vec!["repeat two".to_owned()]
        );
    }

    #[test]
    fn event_preview_blank_transitions_maintain_projection() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let event_id = new_id();

        // Fresh insert with a blank preview projects nothing.
        store
            .upsert_event(&event_with(event_id, 1, serde_json::json!("")))
            .unwrap();
        assert!(event_projection_rows(&store, event_id).is_empty());

        // blank -> nonblank: existing id, projection row appears once.
        store
            .upsert_event(&text_event(event_id, 1, "now searchable foxtrot"))
            .unwrap();
        assert_eq!(
            event_projection_rows(&store, event_id),
            vec!["now searchable foxtrot".to_owned()]
        );

        // nonblank -> blank: the delete + (skipped) insert removes the row.
        store
            .upsert_event(&event_with(event_id, 1, serde_json::json!("")))
            .unwrap();
        assert!(event_projection_rows(&store, event_id).is_empty());

        // blank -> nonblank again: exactly one row returns.
        store
            .upsert_event(&text_event(event_id, 1, "searchable again golf"))
            .unwrap();
        assert_eq!(
            event_projection_rows(&store, event_id),
            vec!["searchable again golf".to_owned()]
        );
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 1);
    }

    /// Signal channel for [`signal_busy_then_retry`]. rusqlite's
    /// `busy_handler` takes a plain fn pointer, so the barrier sender lives
    /// in a static; only the contention test below uses it.
    static CONTENTION_SIGNAL: OnceLock<mpsc::Sender<()>> = OnceLock::new();

    /// Busy handler for the contender connection: report the observed
    /// contention to the test thread, then keep retrying (bounded, with a
    /// tiny backoff, purely as hang protection).
    fn signal_busy_then_retry(attempts: i32) -> bool {
        if let Some(sender) = CONTENTION_SIGNAL.get() {
            let _ = sender.send(());
        }
        std::thread::sleep(Duration::from_millis(1));
        attempts < 60_000
    }

    /// Deterministic two-connection WAL contention: while a writer
    /// connection holds the write lock, an autocommit upsert on a second
    /// connection must wait in `BEGIN IMMEDIATE` — before its existence
    /// probe runs, so the probe can never go stale — and complete once the
    /// writer commits, landing exactly one base row and one projection row.
    ///
    /// Determinism comes from a lock-order barrier, not timing: while the
    /// writer transaction is open, the contender's `BEGIN IMMEDIATE` is
    /// guaranteed to hit SQLITE_BUSY, so the busy-handler signal always
    /// arrives before the test releases the lock. The only timeouts are
    /// generous overall hang guards, never minimum-delay assertions.
    #[test]
    fn contended_autocommit_upsert_waits_for_writer_then_lands_exactly_once() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let writer = Store::open(&path).unwrap();
        let contender = Store::open(&path).unwrap();

        let (busy_tx, busy_rx) = mpsc::channel();
        CONTENTION_SIGNAL.set(busy_tx).ok();
        contender
            .conn
            .busy_handler(Some(signal_busy_then_retry))
            .unwrap();

        let held_id = new_id();
        writer.begin_immediate_batch().unwrap();
        writer
            .insert_record(&record_with(held_id, "writer held body"))
            .unwrap();

        let contended_id = new_id();
        let contended = record_with(contended_id, "contended upsert body");
        let contender_thread = std::thread::spawn(move || {
            let result = contender.upsert_record(&contended);
            (contender, result)
        });

        // Barrier: the contender observed the writer's lock and is waiting.
        busy_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("contender never blocked on the writer's write lock");
        writer.commit_batch().unwrap();

        let (contender, result) = contender_thread.join().unwrap();
        result.unwrap();
        assert!(contender.conn.is_autocommit());

        // Both writes landed exactly once, base and projection in step.
        assert_eq!(count(&writer, "SELECT COUNT(*) FROM history_records"), 2);
        assert_eq!(count(&writer, "SELECT COUNT(*) FROM ctx_history_search"), 2);
        assert_eq!(
            record_projection_rows(&writer, held_id),
            vec!["writer held body".to_owned()]
        );
        assert_eq!(
            record_projection_rows(&writer, contended_id),
            vec!["contended upsert body".to_owned()]
        );
        assert_eq!(
            contender.get_record(contended_id).unwrap().body,
            "contended upsert body"
        );
    }

    /// The write transactions must nest as savepoints inside a
    /// caller-managed transaction: rolling
    /// the outer transaction back rewinds base rows and projections together,
    /// leaving nothing orphaned on either side.
    #[test]
    fn outer_transaction_rollback_rewinds_base_and_projection_together() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        store.begin_immediate_batch().unwrap();
        store
            .insert_record(&record_with(new_id(), "rollback insert body"))
            .unwrap();
        store
            .upsert_record(&record_with(new_id(), "rollback upsert body"))
            .unwrap();
        store
            .upsert_event(&text_event(new_id(), 1, "rollback event body"))
            .unwrap();
        assert!(store
            .insert_event_if_absent(&text_event(new_id(), 2, "rollback if-absent body"))
            .unwrap());
        assert!(!store.conn.is_autocommit());
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 2);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 2);
        store.rollback_batch().unwrap();

        assert!(store.conn.is_autocommit());
        for sql in [
            "SELECT COUNT(*) FROM history_records",
            "SELECT COUNT(*) FROM ctx_history_search",
            "SELECT COUNT(*) FROM events",
            "SELECT COUNT(*) FROM event_search",
        ] {
            assert_eq!(count(&store, sql), 0, "{sql}");
        }

        // The connection stays fully usable in autocommit afterwards.
        let record_id = new_id();
        store
            .insert_record(&record_with(record_id, "post rollback body"))
            .unwrap();
        assert_eq!(record_projection_rows(&store, record_id).len(), 1);
    }

    /// Replaces a search projection FTS table with a plain table whose CHECK
    /// constraint rejects every INSERT while still accepting DELETEs: the
    /// projection write fails after the base write succeeded, which must
    /// roll the base write back too and preserve the original error.
    fn poison_projection_inserts(store: &Store, table: &str, columns: &str) {
        store
            .conn
            .execute_batch(&format!(
                "DROP TABLE {table}; CREATE TABLE {table} ({columns}, CHECK (0 = 1));"
            ))
            .unwrap();
    }

    #[test]
    fn injected_projection_failure_rolls_back_base_and_fts_writes() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let existing_id = new_id();
        store
            .insert_record(&record_with(existing_id, "pre poison body"))
            .unwrap();
        let existing_event = new_id();
        store
            .upsert_event(&text_event(existing_event, 1, "pre poison event"))
            .unwrap();

        poison_projection_inserts(
            &store,
            "ctx_history_search",
            "record_id, title, summary, primary_user_text, decision_text, context_text, tag_text",
        );

        // Fresh insert: base INSERT succeeded inside the write transaction,
        // the
        // projection INSERT fails, and both roll back.
        let fresh = record_with(new_id(), "poisoned insert body");
        let err = store.insert_record(&fresh).unwrap_err();
        assert!(
            err.to_string().contains("CHECK constraint"),
            "original projection error was not preserved: {err}"
        );
        assert!(matches!(
            store.get_record(fresh.id).unwrap_err(),
            StoreError::NotFound(_)
        ));
        assert!(store.conn.is_autocommit());

        // Existing-id upsert: the base row keeps its previous contents.
        let err = store
            .upsert_record(&record_with(existing_id, "poisoned update body"))
            .unwrap_err();
        assert!(err.to_string().contains("CHECK constraint"), "{err}");
        assert_eq!(
            store.get_record(existing_id).unwrap().body,
            "pre poison body"
        );

        // Batch: an all-or-nothing rollback, no partial base rows.
        let batch = vec![
            record_with(new_id(), "poisoned batch one"),
            record_with(new_id(), "poisoned batch two"),
        ];
        let err = store.upsert_records(&batch).unwrap_err();
        assert!(err.to_string().contains("CHECK constraint"), "{err}");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM history_records"), 1);
        assert!(store.conn.is_autocommit());

        poison_projection_inserts(
            &store,
            "event_search",
            "event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket",
        );

        let fresh_event = text_event(new_id(), 2, "poisoned event body");
        let err = store.upsert_event(&fresh_event).unwrap_err();
        assert!(err.to_string().contains("CHECK constraint"), "{err}");
        assert!(matches!(
            store.get_event(fresh_event.id).unwrap_err(),
            StoreError::NotFound(_)
        ));

        let err = store
            .insert_event_if_absent(&text_event(new_id(), 3, "poisoned if-absent body"))
            .unwrap_err();
        assert!(err.to_string().contains("CHECK constraint"), "{err}");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM events"), 1);

        // Existing-id event upsert rolls back to the previous payload.
        let err = store
            .upsert_event(&text_event(existing_event, 1, "poisoned event update"))
            .unwrap_err();
        assert!(err.to_string().contains("CHECK constraint"), "{err}");
        assert_eq!(
            store.get_event(existing_event).unwrap().payload,
            serde_json::json!({ "text": "pre poison event" })
        );
        assert!(store.conn.is_autocommit());
    }

    /// Capture-harness shape: an immediate batch of upsert_record +
    /// insert_event_if_absent, committed, then replayed verbatim. Replays
    /// must be projection no-ops for events and exact one-row replacements
    /// for records, with base/FTS parity throughout.
    #[test]
    fn repeated_harness_batch_replay_keeps_projection_parity() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let record_id = new_id();
        let event_ids = [new_id(), new_id(), new_id()];

        let run_batch = |body: &str| {
            store.begin_immediate_batch().unwrap();
            store.upsert_record(&record_with(record_id, body)).unwrap();
            for (index, event_id) in event_ids.iter().enumerate() {
                store
                    .insert_event_if_absent(&text_event(
                        *event_id,
                        index as u64 + 1,
                        &format!("harness event body {index}"),
                    ))
                    .unwrap();
            }
            store.commit_batch().unwrap();
        };

        run_batch("harness record body v1");
        run_batch("harness record body v1");
        run_batch("harness record body v2");

        assert_eq!(count(&store, "SELECT COUNT(*) FROM history_records"), 1);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 1);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM events"), 3);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 3);
        assert_eq!(
            record_projection_rows(&store, record_id),
            vec!["harness record body v2".to_owned()]
        );
        for (index, event_id) in event_ids.iter().enumerate() {
            assert_eq!(
                event_projection_rows(&store, *event_id),
                vec![format!("harness event body {index}")]
            );
        }
        // Search sees exactly the current contents.
        assert_eq!(store.search_records("v2", 10).unwrap()[0].id, record_id);
        assert!(store.search_records("v1", 10).unwrap().is_empty());
    }
}

#[cfg(test)]
mod search_rowid_map_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-rowid-map-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn record_with(id: Uuid, body: &str) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            "Rowid map record title",
            body,
            vec!["rowidmap".into()],
            "task",
            None,
        );
        record.id = id;
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        record
    }

    fn event_with(id: Uuid, seq: u64, payload: serde_json::Value) -> Event {
        Event {
            id,
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload,
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        }
    }

    fn text_event(id: Uuid, seq: u64, text: &str) -> Event {
        event_with(id, seq, serde_json::json!({ "text": text }))
    }

    fn blank_event(id: Uuid, seq: u64) -> Event {
        event_with(id, seq, serde_json::json!(""))
    }

    fn count(store: &Store, sql: &str) -> i64 {
        store.conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn user_version(store: &Store) -> i64 {
        store
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap()
    }

    fn map_rows(store: &Store, map_table: &str) -> Vec<(String, i64)> {
        let sql = format!("SELECT * FROM {map_table} ORDER BY search_rowid");
        let mut stmt = store.conn.prepare(&sql).unwrap();
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    fn fts_rows(store: &Store, fts_table: &str, id_column: &str) -> Vec<(String, i64)> {
        let sql = format!("SELECT {id_column}, rowid FROM {fts_table} ORDER BY rowid");
        let mut stmt = store.conn.prepare(&sql).unwrap();
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    /// Exact map <-> FTS identity in both directions: every projection row
    /// has a map entry with the same id and rowid, and every map entry
    /// points at a projection row holding its id.
    fn assert_map_fts_identity(store: &Store) {
        for (fts_table, id_column, map_table) in [
            ("ctx_history_search", "record_id", "record_search_rowids"),
            ("event_search", "event_id", "event_search_rowids"),
        ] {
            let unmapped: i64 = store
                .conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM {fts_table} f
                         LEFT JOIN {map_table} m
                           ON m.{id_column} = f.{id_column} AND m.search_rowid = f.rowid
                         WHERE m.{id_column} IS NULL"
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(unmapped, 0, "{fts_table} rows without exact map entries");
            let dangling: i64 = store
                .conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM {map_table} m
                         LEFT JOIN {fts_table} f ON f.rowid = m.search_rowid
                         WHERE f.{id_column} IS NULL OR f.{id_column} != m.{id_column}"
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                dangling, 0,
                "{map_table} entries not backed by {fts_table} rows"
            );
        }
    }

    /// Counts VDBE operations for `work` (same technique as
    /// projection_write_path_tests::vdbe_ops).
    fn vdbe_ops(store: &Store, work: impl FnOnce()) -> usize {
        let counter = Arc::new(AtomicUsize::new(0));
        let handler_counter = Arc::clone(&counter);
        store.conn.progress_handler(
            1,
            Some(move || {
                handler_counter.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        work();
        store.conn.progress_handler(0, None::<fn() -> bool>);
        counter.load(Ordering::Relaxed)
    }

    /// Builds a database exactly as an upstream-chain v15 binary leaves it:
    /// base rows present, FTS projections populated at explicit rowids
    /// (listing an id twice models legacy duplicate projection rows), no map
    /// tables, `user_version` 15.
    fn build_v15_database(
        path: &Path,
        record_rows: &[(Uuid, i64, &str)],
        event_rows: &[(Uuid, i64, u64, &str)],
    ) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(CREATE_TABLES_SQL).unwrap();
        conn.execute_batch(FTS_TABLES_SQL).unwrap();
        conn.execute_batch(INDEXES_SQL).unwrap();
        for (id, fts_rowid, projected_text) in record_rows {
            conn.execute(
                "INSERT OR IGNORE INTO history_records
                 (id, title, last_activity_at_ms, body, created_at, updated_at)
                 VALUES (?1, 'Legacy record title', 0, ?2, '2026-06-23T12:00:00+00:00', '2026-06-23T12:00:00+00:00')",
                params![id.to_string(), format!("legacy base body for {projected_text}")],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO ctx_history_search
                 (rowid, record_id, title, summary, primary_user_text, decision_text, context_text, tag_text)
                 VALUES (?1, ?2, 'Legacy record title', ?3, ?3, '', '', '')",
                params![fts_rowid, id.to_string(), projected_text],
            )
            .unwrap();
        }
        for (id, fts_rowid, seq, projected_text) in event_rows {
            conn.execute(
                "INSERT OR IGNORE INTO events (id, seq, event_type, role, occurred_at_ms, payload_json)
                 VALUES (?1, ?2, 'message', 'user', 0, ?3)",
                params![
                    id.to_string(),
                    *seq as i64,
                    serde_json::json!({ "text": projected_text }).to_string()
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO event_search
                 (rowid, event_id, history_record_id, session_id, role, safe_preview_text, rank_bucket)
                 VALUES (?1, ?2, NULL, NULL, 'user', ?3, 'message')",
                params![fts_rowid, id.to_string(), projected_text],
            )
            .unwrap();
        }
        conn.execute_batch("PRAGMA user_version = 15;").unwrap();
    }

    #[test]
    fn schema_v15_to_v1001_preserves_projections_and_creates_empty_maps() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let record_a = new_id();
        let record_b = new_id();
        let event_e = new_id();
        build_v15_database(
            &path,
            &[
                (record_a, 10, "legacy alpha projection"),
                (record_b, 20, "legacy bravo projection"),
            ],
            &[(event_e, 30, 1, "legacy charlie event")],
        );

        let store = Store::open(&path).unwrap();
        assert_eq!(user_version(&store), 1001);
        assert_eq!(user_version(&store), SCHEMA_VERSION);

        // The maps exist and start empty: no backfill.
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM record_search_rowids"),
            0
        );
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 0);

        // The whole fork chain ran: the v1001 pagination indexes exist too.
        for index in [
            "idx_sessions_provider_external_session_started",
            "idx_events_session_seq_id",
        ] {
            assert!(
                index_exists(&store.conn, index),
                "v15 upgrade did not create {index}"
            );
        }

        // No forced rebuild: the legacy projection rows survive at their
        // original explicit rowids with their original text (a rebuild
        // would renumber them 1..N and re-derive text from base rows).
        assert_eq!(
            fts_rows(&store, "ctx_history_search", "record_id"),
            vec![(record_a.to_string(), 10), (record_b.to_string(), 20)]
        );
        assert_eq!(
            fts_rows(&store, "event_search", "event_id"),
            vec![(event_e.to_string(), 30)]
        );
        let projected: String = store
            .conn
            .query_row(
                "SELECT summary FROM ctx_history_search WHERE rowid = 10",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(projected, "legacy alpha projection");

        // Search serves the intact legacy projections.
        assert_eq!(store.search_records("alpha", 10).unwrap()[0].id, record_a);
        assert_eq!(
            store.search_event_hits("charlie", 10).unwrap()[0].event_id,
            event_e
        );
    }

    #[test]
    fn fresh_database_reaches_v1001_through_the_upstream_chain() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        assert_eq!(user_version(&store), SCHEMA_VERSION);
        assert_eq!(user_version(&store), 1001);
        // The upstream chain ran first: its v13+ stable views exist.
        assert_eq!(
            count(
                &store,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'view' AND name = 'ctx_sessions'"
            ),
            1
        );
        for map_table in ["record_search_rowids", "event_search_rowids"] {
            assert!(table_exists(&store.conn, map_table).unwrap());
        }
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM record_search_rowids"),
            0
        );
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 0);
        // A fresh database receives both fork migrations: the v1000 maps
        // above and the v1001 pagination indexes.
        for index in [
            "idx_sessions_provider_external_session_started",
            "idx_events_session_seq_id",
        ] {
            assert!(
                index_exists(&store.conn, index),
                "fresh database is missing {index}"
            );
        }
    }

    #[test]
    fn non_current_schema_versions_are_rejected_explicitly() {
        let temp = tempdir();

        // Read-only open requires the exact current version: a v15 store
        // must be migrated by a writable open first.
        let v15_path = temp.path().join("v15.sqlite");
        build_v15_database(&v15_path, &[(new_id(), 1, "legacy row")], &[]);
        assert!(matches!(
            Store::open_read_only(&v15_path),
            Err(StoreError::UnsupportedSchemaVersion(15))
        ));

        // A current store opens read-only.
        let current_path = temp.path().join("current.sqlite");
        drop(Store::open(&current_path).unwrap());
        drop(Store::open_read_only(&current_path).unwrap());

        // Versions newer than this binary are refused in both modes: this
        // is exactly how a fork-versioned store looks to an older binary.
        let future_path = temp.path().join("future.sqlite");
        drop(Store::open(&future_path).unwrap());
        Connection::open(&future_path)
            .unwrap()
            .execute_batch("PRAGMA user_version = 1002;")
            .unwrap();
        assert!(matches!(
            Store::open(&future_path),
            Err(StoreError::UnsupportedSchemaVersion(1002))
        ));
        assert!(matches!(
            Store::open_read_only(&future_path),
            Err(StoreError::UnsupportedSchemaVersion(1002))
        ));

        // Read-only open also requires the exact current version for fork
        // schemas: a v1000 (rowid maps only) store must be migrated to
        // v1001 by a writable open first.
        let v1000_path = temp.path().join("v1000.sqlite");
        drop(Store::open(&v1000_path).unwrap());
        Connection::open(&v1000_path)
            .unwrap()
            .execute_batch(
                r#"
                BEGIN IMMEDIATE;
                DROP INDEX idx_sessions_provider_external_session_started;
                DROP INDEX idx_events_session_seq_id;
                PRAGMA user_version = 1000;
                COMMIT;
                "#,
            )
            .unwrap();
        assert!(matches!(
            Store::open_read_only(&v1000_path),
            Err(StoreError::UnsupportedSchemaVersion(1000))
        ));

        // Versions in the (15, 1000) gap could only come from an unreviewed
        // newer upstream chain; they are rejected instead of migrated blind.
        let gap_path = temp.path().join("gap.sqlite");
        build_v15_database(&gap_path, &[], &[]);
        Connection::open(&gap_path)
            .unwrap()
            .execute_batch("PRAGMA user_version = 16;")
            .unwrap();
        assert!(matches!(
            Store::open(&gap_path),
            Err(StoreError::UnsupportedSchemaVersion(16))
        ));
        assert!(matches!(
            Store::open_read_only(&gap_path),
            Err(StoreError::UnsupportedSchemaVersion(16))
        ));
    }

    #[test]
    fn rejected_foreign_schemas_leave_the_database_file_untouched() {
        let temp = tempdir();
        // One gap version (unreviewed upstream chain) and one future
        // version, both in SQLite's default rollback-journal mode: any
        // persistent PRAGMA (journal_mode = WAL) applied before the version
        // gate would show up as mutated bytes, a changed journal mode, or
        // WAL sidecar files.
        for version in [16i64, 999, 1002] {
            let path = temp.path().join(format!("foreign-{version}.sqlite"));
            {
                let conn = Connection::open(&path).unwrap();
                conn.execute_batch(
                    "CREATE TABLE foreign_marker (id INTEGER PRIMARY KEY, note TEXT);",
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO foreign_marker (note) VALUES ('foreign row')",
                    [],
                )
                .unwrap();
                conn.execute_batch(&format!("PRAGMA user_version = {version};"))
                    .unwrap();
            }
            let before = fs::read(&path).unwrap();

            assert!(matches!(
                Store::open(&path),
                Err(StoreError::UnsupportedSchemaVersion(rejected)) if rejected == version
            ));
            assert!(matches!(
                Store::open_read_only(&path),
                Err(StoreError::UnsupportedSchemaVersion(rejected)) if rejected == version
            ));

            let after = fs::read(&path).unwrap();
            assert_eq!(
                before, after,
                "rejected open mutated the database file for version {version}"
            );
            let mut wal_path = path.clone().into_os_string();
            wal_path.push("-wal");
            assert!(
                !PathBuf::from(wal_path).exists(),
                "rejected open created a WAL sidecar for version {version}"
            );

            let conn = Connection::open(&path).unwrap();
            let journal_mode: String = conn
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                journal_mode, "delete",
                "rejected open persisted a journal-mode change for version {version}"
            );
            let user_version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(user_version, version);
            let schema_objects: i64 = conn
                .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                schema_objects, 1,
                "rejected open changed the schema for version {version}"
            );
        }
    }

    /// Mirrors the unsupported external downgrade recipe documented in
    /// docs/storage.md: dropping both map tables and resetting
    /// `user_version` to 15 in one transaction yields a store the upstream
    /// v15 chain owns again (this binary's own read-only gate confirms it
    /// reads as exactly v15), with all base and FTS data intact, and a
    /// later writable open by this binary re-migrates it cleanly.
    #[test]
    fn documented_downgrade_steps_restore_a_v15_shaped_store() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let record_id = new_id();
        let event_id = new_id();
        {
            let store = Store::open(&path).unwrap();
            store
                .insert_record(&record_with(record_id, "downgrade survivor body"))
                .unwrap();
            store
                .upsert_event(&text_event(event_id, 1, "downgrade survivor event"))
                .unwrap();
        }

        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
                BEGIN IMMEDIATE;
                DROP TABLE record_search_rowids;
                DROP TABLE event_search_rowids;
                PRAGMA user_version = 15;
                COMMIT;
                "#,
            )
            .unwrap();
            let user_version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(user_version, 15);
            assert!(!table_exists(&conn, "record_search_rowids").unwrap());
            assert!(!table_exists(&conn, "event_search_rowids").unwrap());
        }

        // This binary's exact-version read-only gate sees a plain v15 store.
        assert!(matches!(
            Store::open_read_only(&path),
            Err(StoreError::UnsupportedSchemaVersion(15))
        ));

        // Re-upgrading re-runs only the fork steps (v1000 maps, then the
        // v1001 pagination indexes): data intact, maps recreated empty,
        // lazy healing resumes.
        let store = Store::open(&path).unwrap();
        assert_eq!(user_version(&store), 1001);
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM record_search_rowids"),
            0
        );
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 0);
        assert_eq!(
            store.search_records("survivor", 10).unwrap()[0].id,
            record_id
        );
        assert_eq!(
            store.search_event_hits("survivor", 10).unwrap()[0].event_id,
            event_id
        );
        store
            .upsert_record(&record_with(record_id, "post downgrade healed body"))
            .unwrap();
        store
            .upsert_event(&text_event(event_id, 1, "post downgrade healed event"))
            .unwrap();
        assert_map_fts_identity(&store);
    }

    fn index_exists(conn: &Connection, index: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
            params![index],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// A brand-new data root and database, or a just-moved known legacy
    /// store, are restricted to 0700/0600 at creation/move time — before the
    /// migration chain runs — not as a post-migration afterthought. The
    /// companion rejection-purity tests prove pre-existing foreign databases
    /// still see no chmod/mkdir before schema validation.
    #[cfg(unix)]
    #[test]
    fn fresh_and_moved_legacy_stores_are_restricted_before_migration() {
        use std::os::unix::fs::PermissionsExt as _;

        // Fresh creation: parent 0700 and database 0600 after open.
        let temp = tempdir();
        let root = temp.path().join("fresh-root");
        let db = root.join("work.sqlite");
        drop(Store::open(&db).unwrap());
        assert_eq!(
            fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&db).unwrap().permissions().mode() & 0o777,
            0o600
        );

        // Moved legacy store: permissions are restricted even when the
        // migration chain fails afterwards, proving the chmod happens at
        // move/creation time rather than only after a successful migrate.
        let temp = tempdir();
        let root = temp.path().join("legacy-root");
        let legacy_dir = root.join(LEGACY_HISTORY_DIR_NAME);
        fs::create_dir_all(&legacy_dir).unwrap();
        let legacy_db = legacy_dir.join("work.sqlite");
        build_v15_database(&legacy_db, &[], &[]);
        // Squat on a v1001 index name so migrate() fails mid-chain.
        let conn = Connection::open(&legacy_db).unwrap();
        conn.execute_batch("CREATE TABLE idx_events_session_seq_id(id TEXT PRIMARY KEY);")
            .unwrap();
        drop(conn);
        fs::set_permissions(&legacy_db, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();

        let db = root.join("work.sqlite");
        assert!(Store::open(&db).is_err(), "blocked migration must fail");
        assert!(db.exists(), "legacy store was not moved into place");
        assert_eq!(
            fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&db).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn schema_v1000_to_v1001_upgrades_in_place_without_rebuilding_maps_or_fts() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let record_id = new_id();
        let event_id = new_id();
        {
            let store = Store::open(&path).unwrap();
            store
                .insert_record(&record_with(record_id, "pagination upgrade body"))
                .unwrap();
            store
                .upsert_event(&text_event(event_id, 1, "pagination upgrade event"))
                .unwrap();
            assert_map_fts_identity(&store);
        }

        // Reshape the store to exactly v1000: populated maps and FTS
        // projections, no pagination indexes.
        let (maps_before, fts_before) = {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
                BEGIN IMMEDIATE;
                DROP INDEX idx_sessions_provider_external_session_started;
                DROP INDEX idx_events_session_seq_id;
                PRAGMA user_version = 1000;
                COMMIT;
                "#,
            )
            .unwrap();
            let maps: Vec<(String, i64)> = conn
                .prepare(
                    "SELECT record_id, search_rowid FROM record_search_rowids ORDER BY record_id",
                )
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let fts: Vec<(i64, String)> = conn
                .prepare("SELECT rowid, record_id FROM ctx_history_search ORDER BY rowid")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(!maps.is_empty(), "v1000 store should have map entries");
            (maps, fts)
        };

        // A writable open migrates v1000 → v1001 in place.
        let store = Store::open(&path).unwrap();
        assert_eq!(user_version(&store), 1001);
        for index in [
            "idx_sessions_provider_external_session_started",
            "idx_events_session_seq_id",
        ] {
            assert!(
                index_exists(&store.conn, index),
                "v1000→v1001 migration did not create {index}"
            );
        }
        // No map rebuild and no FTS rebuild: rows and rowids are identical.
        let maps_after: Vec<(String, i64)> = store
            .conn
            .prepare("SELECT record_id, search_rowid FROM record_search_rowids ORDER BY record_id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let fts_after: Vec<(i64, String)> = store
            .conn
            .prepare("SELECT rowid, record_id FROM ctx_history_search ORDER BY rowid")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(maps_before, maps_after);
        assert_eq!(fts_before, fts_after);
        assert_map_fts_identity(&store);
        assert_eq!(
            store.search_records("pagination", 10).unwrap()[0].id,
            record_id
        );
    }

    #[test]
    fn every_write_path_maps_projection_rows_exactly() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        store
            .insert_record(&record_with(new_id(), "insert_record body"))
            .unwrap();
        store
            .upsert_record(&record_with(new_id(), "upsert_record body"))
            .unwrap();
        let repeated = new_id();
        store
            .upsert_records(&[
                record_with(new_id(), "batch body one"),
                record_with(repeated, "batch repeat first"),
                record_with(repeated, "batch repeat second"),
            ])
            .unwrap();

        let event_id = new_id();
        store
            .upsert_event(&text_event(event_id, 1, "upsert_event body"))
            .unwrap();
        store
            .upsert_event(&text_event(event_id, 1, "upsert_event updated body"))
            .unwrap();
        assert!(store
            .insert_event_if_absent(&text_event(new_id(), 2, "insert_if_absent body"))
            .unwrap());
        let blank_id = new_id();
        store.upsert_event(&blank_event(blank_id, 3)).unwrap();

        // One projection row and one map entry per projected id; the blank
        // event has neither.
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 4);
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM record_search_rowids"),
            4
        );
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 2);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 2);
        assert!(!map_rows(&store, "event_search_rowids")
            .iter()
            .any(|(id, _)| id == &blank_id.to_string()));
        assert_map_fts_identity(&store);

        // The repeated batch id kept its second body.
        assert_eq!(store.search_records("second", 10).unwrap()[0].id, repeated);
        assert!(store.search_records("first", 10).unwrap().is_empty());
    }

    #[test]
    fn legacy_rows_heal_lazily_on_first_update() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let record_a = new_id();
        let record_b = new_id();
        let event_e = new_id();
        // record_a and event_e both carry legacy duplicate projection rows.
        build_v15_database(
            &path,
            &[
                (record_a, 10, "legacy alpha projection"),
                (record_a, 11, "legacy alpha duplicate"),
                (record_b, 20, "legacy bravo projection"),
            ],
            &[
                (event_e, 30, 1, "legacy charlie event"),
                (event_e, 31, 1, "legacy charlie duplicate"),
            ],
        );

        let store = Store::open(&path).unwrap();
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 3);

        // First update of an unmapped id: the legacy full-scan delete
        // removes ALL its rows (both duplicates), the reinsert re-maps it.
        store
            .upsert_record(&record_with(record_a, "healed alpha body"))
            .unwrap();
        let record_projection = fts_rows(&store, "ctx_history_search", "record_id");
        assert_eq!(record_projection.len(), 2);
        assert_eq!(record_projection[0], (record_b.to_string(), 20));
        assert_eq!(record_projection[1].0, record_a.to_string());
        assert_eq!(
            map_rows(&store, "record_search_rowids"),
            vec![(record_a.to_string(), record_projection[1].1)]
        );

        store
            .upsert_event(&text_event(event_e, 1, "healed charlie event"))
            .unwrap();
        let event_projection = fts_rows(&store, "event_search", "event_id");
        assert_eq!(event_projection.len(), 1);
        assert_eq!(event_projection[0].0, event_e.to_string());
        assert_eq!(
            map_rows(&store, "event_search_rowids"),
            vec![(event_e.to_string(), event_projection[0].1)]
        );

        // Untouched legacy rows stay unmapped and searchable; healed rows
        // now satisfy exact identity for their ids.
        assert_eq!(store.search_records("bravo", 10).unwrap()[0].id, record_b);
        assert_eq!(store.search_records("healed", 10).unwrap()[0].id, record_a);
        assert!(store.search_records("duplicate", 10).unwrap().is_empty());

        // Second update of the healed id keeps exactly one row and an exact
        // map (the bounded-work contract is pinned in
        // projection_write_path_tests).
        store
            .upsert_record(&record_with(record_a, "healed alpha body again"))
            .unwrap();
        assert_eq!(
            count(
                &store,
                "SELECT COUNT(*) FROM ctx_history_search WHERE record_id IN (SELECT record_id FROM record_search_rowids)"
            ),
            1
        );
    }

    #[test]
    fn stale_map_entries_fall_back_without_touching_other_rows() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let record_a = new_id();
        let record_b = new_id();
        store
            .insert_record(&record_with(record_a, "alpha original body"))
            .unwrap();
        store
            .insert_record(&record_with(record_b, "bravo original body"))
            .unwrap();
        let rowid_b: i64 = store
            .conn
            .query_row(
                "SELECT search_rowid FROM record_search_rowids WHERE record_id = ?1",
                params![record_b.to_string()],
                |row| row.get(0),
            )
            .unwrap();

        // Corrupt the map: point record_a's entry at record_b's row.
        store
            .conn
            .execute(
                "DELETE FROM record_search_rowids WHERE record_id = ?1",
                params![record_b.to_string()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE record_search_rowids SET search_rowid = ?1 WHERE record_id = ?2",
                params![rowid_b, record_a.to_string()],
            )
            .unwrap();

        // The verified point lookup sees record_b's id at the mapped rowid,
        // rejects the entry, and falls back to the full-scan delete for
        // record_a only. record_b's projection row must survive untouched.
        store
            .upsert_record(&record_with(record_a, "alpha revised body"))
            .unwrap();
        let survivor: String = store
            .conn
            .query_row(
                "SELECT record_id FROM ctx_history_search WHERE rowid = ?1",
                params![rowid_b],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(survivor, record_b.to_string());
        assert_eq!(store.search_records("bravo", 10).unwrap()[0].id, record_b);
        assert_eq!(store.search_records("revised", 10).unwrap()[0].id, record_a);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 2);

        // record_a self-healed to a fresh verified entry; record_b heals on
        // its own next write.
        let healed = map_rows(&store, "record_search_rowids");
        assert_eq!(healed.len(), 1);
        assert_eq!(healed[0].0, record_a.to_string());
        assert_ne!(healed[0].1, rowid_b);

        // A mapped rowid that no longer exists is equally safe.
        store
            .conn
            .execute(
                "UPDATE record_search_rowids SET search_rowid = 999999 WHERE record_id = ?1",
                params![record_a.to_string()],
            )
            .unwrap();
        store
            .upsert_record(&record_with(record_a, "alpha third body"))
            .unwrap();
        assert_eq!(store.search_records("third", 10).unwrap()[0].id, record_a);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 2);
        store
            .upsert_record(&record_with(record_b, "bravo revised body"))
            .unwrap();
        assert_map_fts_identity(&store);
    }

    #[test]
    fn blank_preview_transitions_keep_maps_in_lockstep() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let event_id = new_id();

        // Fresh blank: neither FTS row nor map entry.
        store.upsert_event(&blank_event(event_id, 1)).unwrap();
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 0);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 0);

        // blank -> nonblank: both appear together.
        store
            .upsert_event(&text_event(event_id, 1, "now searchable delta"))
            .unwrap();
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 1);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 1);
        assert_map_fts_identity(&store);

        // nonblank -> blank: both disappear together.
        store.upsert_event(&blank_event(event_id, 1)).unwrap();
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 0);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 0);

        // blank -> nonblank again: exactly one of each returns.
        store
            .upsert_event(&text_event(event_id, 1, "searchable echo again"))
            .unwrap();
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 1);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 1);
        assert_map_fts_identity(&store);
    }

    #[test]
    fn rebuild_clears_and_repopulates_maps_in_lockstep() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        for index in 0..3 {
            store
                .insert_record(&record_with(new_id(), &format!("rebuild body {index}")))
                .unwrap();
        }
        store
            .upsert_event(&text_event(new_id(), 1, "rebuild event one"))
            .unwrap();
        store
            .upsert_event(&text_event(new_id(), 2, "rebuild event two"))
            .unwrap();
        store.upsert_event(&blank_event(new_id(), 3)).unwrap();

        // Scramble the maps arbitrarily; the rebuild must not trust them.
        store
            .conn
            .execute(
                "UPDATE record_search_rowids SET search_rowid = search_rowid + 700",
                [],
            )
            .unwrap();
        store
            .conn
            .execute("DELETE FROM event_search_rowids", [])
            .unwrap();

        store.refresh_search_index().unwrap();
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 3);
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM record_search_rowids"),
            3
        );
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search"), 2);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM event_search_rowids"), 2);
        assert_map_fts_identity(&store);
    }

    #[test]
    fn injected_map_failure_rolls_back_base_fts_and_maps() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let record_id = new_id();
        store
            .insert_record(&record_with(record_id, "original protected body"))
            .unwrap();
        let original_map = map_rows(&store, "record_search_rowids");
        let original_fts = fts_rows(&store, "ctx_history_search", "record_id");

        // Test-only fault injection: production code never installs
        // triggers (projections and maps are maintained manually); this
        // trigger only forces the map INSERT inside the write transaction
        // to fail after base + FTS writes succeeded.
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER injected_map_failure
                 BEFORE INSERT ON record_search_rowids
                 BEGIN SELECT RAISE(ABORT, 'injected map failure'); END;",
            )
            .unwrap();

        let err = store
            .upsert_record(&record_with(record_id, "poisoned update body"))
            .unwrap_err();
        assert!(
            err.to_string().contains("injected map failure"),
            "original injected error was not preserved: {err}"
        );
        assert!(store.conn.is_autocommit());
        assert_eq!(
            store.get_record(record_id).unwrap().body,
            "original protected body"
        );
        assert_eq!(map_rows(&store, "record_search_rowids"), original_map);
        assert_eq!(
            fts_rows(&store, "ctx_history_search", "record_id"),
            original_fts
        );
        assert_map_fts_identity(&store);
        assert_eq!(
            store.search_records("protected", 10).unwrap()[0].id,
            record_id
        );

        // A fresh insert rolls back base + FTS + map together too.
        let fresh = record_with(new_id(), "poisoned fresh body");
        assert!(store.insert_record(&fresh).is_err());
        assert!(matches!(
            store.get_record(fresh.id).unwrap_err(),
            StoreError::NotFound(_)
        ));
        assert_eq!(count(&store, "SELECT COUNT(*) FROM ctx_history_search"), 1);

        store
            .conn
            .execute_batch("DROP TRIGGER injected_map_failure;")
            .unwrap();
        store
            .upsert_record(&record_with(record_id, "recovered update body"))
            .unwrap();
        assert_map_fts_identity(&store);
        assert_eq!(
            store.search_records("recovered", 10).unwrap()[0].id,
            record_id
        );
    }

    #[test]
    fn search_output_is_independent_of_maps() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        let record_a = new_id();
        let record_b = new_id();
        store
            .insert_record(&record_with(record_a, "alpha needle body"))
            .unwrap();
        store
            .insert_record(&record_with(record_b, "bravo needle body"))
            .unwrap();
        store
            .upsert_event(&text_event(new_id(), 1, "event needle one"))
            .unwrap();
        store
            .upsert_event(&text_event(new_id(), 2, "event needle two"))
            .unwrap();

        let baseline_records: Vec<Uuid> = store
            .search_records("needle", 10)
            .unwrap()
            .iter()
            .map(|record| record.id)
            .collect();
        let baseline_events: Vec<Uuid> = store
            .search_event_hits("needle", 10)
            .unwrap()
            .iter()
            .map(|hit| hit.event_id)
            .collect();
        assert_eq!(baseline_records.len(), 2);
        assert_eq!(baseline_events.len(), 2);

        // Corrupt every map entry: search output must not change, because
        // search never reads the maps.
        store
            .conn
            .execute(
                "UPDATE record_search_rowids SET search_rowid = search_rowid + 900",
                [],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE event_search_rowids SET search_rowid = search_rowid + 900",
                [],
            )
            .unwrap();
        assert_eq!(
            store
                .search_records("needle", 10)
                .unwrap()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            baseline_records
        );
        assert_eq!(
            store
                .search_event_hits("needle", 10)
                .unwrap()
                .iter()
                .map(|hit| hit.event_id)
                .collect::<Vec<_>>(),
            baseline_events
        );

        // Drop the map tables entirely: search is still identical, and
        // writes degrade to the legacy full-scan path instead of failing.
        store
            .conn
            .execute_batch("DROP TABLE record_search_rowids; DROP TABLE event_search_rowids;")
            .unwrap();
        assert_eq!(
            store
                .search_records("needle", 10)
                .unwrap()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            baseline_records
        );
        store
            .upsert_record(&record_with(record_a, "alpha needle revised"))
            .unwrap();
        assert_eq!(store.search_records("revised", 10).unwrap()[0].id, record_a);
        drop(store);

        // Reopen recreates the dropped map tables empty; the next write per
        // id heals it back into the map.
        let store = Store::open(&path).unwrap();
        assert_eq!(
            count(&store, "SELECT COUNT(*) FROM record_search_rowids"),
            0
        );
        store
            .upsert_record(&record_with(record_a, "alpha needle healed"))
            .unwrap();
        let healed = map_rows(&store, "record_search_rowids");
        assert_eq!(healed.len(), 1);
        assert_eq!(healed[0].0, record_a.to_string());
        assert_eq!(store.search_records("healed", 10).unwrap()[0].id, record_a);
        assert_eq!(store.search_records("bravo", 10).unwrap()[0].id, record_b);
    }

    #[test]
    fn external_vacuum_preserves_explicit_mapped_rowids() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let records = (0..50)
            .map(|index| record_with(new_id(), &format!("vacuum corpus body {index:03}")))
            .collect::<Vec<_>>();
        store.upsert_records(&records).unwrap();
        store
            .upsert_event(&text_event(new_id(), 1, "vacuum corpus event"))
            .unwrap();
        let before = map_rows(&store, "record_search_rowids");

        store.conn.execute_batch("VACUUM;").unwrap();

        // The maps store explicit FTS rowids and FTS5 content rowids
        // survive VACUUM, so identity holds without any healing...
        assert_eq!(map_rows(&store, "record_search_rowids"), before);
        assert_map_fts_identity(&store);

        // ...and the post-VACUUM update still takes the constant mapped
        // path rather than falling back to the full scan: its VDBE work
        // matches a known map hit, and both stay far below the heal cost.
        let target = records[7].id;
        let mapped_ops = vdbe_ops(&store, || {
            store
                .upsert_record(&record_with(target, "post vacuum mapped update"))
                .unwrap();
        });
        let repeat_ops = vdbe_ops(&store, || {
            store
                .upsert_record(&record_with(target, "post vacuum mapped repeat"))
                .unwrap();
        });
        store
            .conn
            .execute(
                "DELETE FROM record_search_rowids WHERE record_id = ?1",
                params![target.to_string()],
            )
            .unwrap();
        let heal_ops = vdbe_ops(&store, || {
            store
                .upsert_record(&record_with(target, "post vacuum healed update"))
                .unwrap();
        });
        assert!(
            mapped_ops <= repeat_ops + 32,
            "post-VACUUM update fell off the mapped path: {mapped_ops} ops vs mapped repeat {repeat_ops}"
        );
        assert!(
            heal_ops > mapped_ops + 100,
            "heal arm should dominate the mapped arm: heal {heal_ops} vs mapped {mapped_ops}"
        );
        assert_map_fts_identity(&store);
    }
}

/// Retained benchmark evidence for the fresh-row FTS write paths. Run
/// explicitly with:
///
/// ```text
/// cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_fresh_row
/// ```
#[cfg(test)]
mod projection_write_path_benches {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-fresh-row-bench-")
            .tempdir_in(root)
            .unwrap()
    }

    fn bench_record(index: u64, body_tag: &str) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            format!("Bench record {index:07}"),
            format!(
                "bench {body_tag} record {index:07}: deterministic transcript payload for \
                 fresh-row projection timing {index:07}"
            ),
            vec!["bench".into()],
            "task",
            None,
        );
        record.created_at = DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        record.updated_at = record.created_at;
        record
    }

    #[test]
    #[ignore = "benchmark: cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_fresh_row"]
    fn bench_fresh_row_fts_maintenance_vs_full_scan_delete() {
        const FRESH_ROWS: u64 = 200;
        const UPDATE_ROWS: usize = 50;
        for &size in &[10_000u64, 30_000] {
            let temp = tempdir();
            let store = Store::open(temp.path().join("work.sqlite")).unwrap();

            let started = Instant::now();
            let seed = (0..size)
                .map(|index| bench_record(index, "seed"))
                .collect::<Vec<_>>();
            store.upsert_records(&seed).unwrap();
            println!(
                "corpus {size}: seed ingestion {:?} (excluded from arm timings)",
                started.elapsed()
            );

            // Old behavior for provably new rows: delete + insert projection
            // maintenance per row, exactly the statements upsert_record ran
            // before the novelty probe existed, in one immediate batch.
            let fresh_old = (0..FRESH_ROWS)
                .map(|index| bench_record(size + index, "old-arm"))
                .collect::<Vec<_>>();
            let started = Instant::now();
            store.begin_immediate_batch().unwrap();
            for record in &fresh_old {
                store.upsert_record_row(record).unwrap();
                upsert_record_search_projection(&store.conn, record).unwrap();
            }
            store.commit_batch().unwrap();
            let old_arm = started.elapsed();

            // New behavior: the probe proves novelty and the insert-only
            // projection skips the full-scan delete.
            let fresh_new = (0..FRESH_ROWS)
                .map(|index| bench_record(size + FRESH_ROWS + index, "new-arm"))
                .collect::<Vec<_>>();
            let started = Instant::now();
            store.upsert_records(&fresh_new).unwrap();
            let new_arm = started.elapsed();

            println!(
                "corpus {size}: {FRESH_ROWS} fresh rows -> delete+insert {old_arm:?} vs insert-only {new_arm:?}"
            );

            // Update arm, intentionally unchanged: existing ids still pay
            // the O(index size) projection scan per row.
            let updates = seed
                .iter()
                .take(UPDATE_ROWS)
                .map(|record| {
                    let mut updated = record.clone();
                    updated.body = format!("updated {}", record.body);
                    updated
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            store.upsert_records(&updates).unwrap();
            let update_arm = started.elapsed();
            println!(
                "corpus {size}: {UPDATE_ROWS} existing-row updates (unchanged full-scan path) {update_arm:?}"
            );
        }
    }
}

/// Retained benchmark evidence for the durable FTS rowid maps. Run
/// explicitly with:
///
/// ```text
/// cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_rowid_map
/// ```
#[cfg(test)]
mod search_rowid_map_benches {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-rowid-map-bench-")
            .tempdir_in(root)
            .unwrap()
    }

    fn bench_record(index: u64, body_tag: &str) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            format!("Bench record {index:07}"),
            format!(
                "bench {body_tag} record {index:07}: deterministic transcript payload for \
                 rowid map replay timing {index:07}"
            ),
            vec!["bench".into()],
            "task",
            None,
        );
        record.created_at = DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        record.updated_at = record.created_at;
        record
    }

    /// 250 existing-record replays against 30k/100k-row projections.
    /// The first pass runs against an empty map (exactly the state of a
    /// store migrated from v15): every row pays the one-time legacy
    /// full-scan heal. The second pass replays the same rows through their
    /// now-verified map entries. The fresh arm appends 250 new records
    /// through the insert-only path (which now also writes map entries) to
    /// show it does not regress.
    #[test]
    #[ignore = "benchmark: cargo test --release -p ctx-history-store -- --ignored --nocapture --test-threads=1 bench_rowid_map"]
    fn bench_rowid_map_replay_vs_legacy_scan() {
        const REPLAY_ROWS: usize = 250;
        const FRESH_ROWS: u64 = 250;
        for &size in &[30_000u64, 100_000] {
            let temp = tempdir();
            let store = Store::open(temp.path().join("work.sqlite")).unwrap();

            let started = Instant::now();
            let seed = (0..size)
                .map(|index| bench_record(index, "seed"))
                .collect::<Vec<_>>();
            store.upsert_records(&seed).unwrap();
            println!(
                "corpus {size}: seed ingestion {:?} (excluded from arm timings)",
                started.elapsed()
            );

            // v15-migration shape: projections intact, maps empty.
            store
                .conn
                .execute("DELETE FROM record_search_rowids", [])
                .unwrap();

            let step = size as usize / REPLAY_ROWS;
            let replay_ids = seed
                .iter()
                .step_by(step)
                .take(REPLAY_ROWS)
                .map(|record| record.id)
                .collect::<Vec<_>>();

            let heal_pass = replay_ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    let mut record = bench_record(index as u64, "heal-pass");
                    record.id = *id;
                    record
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            store.upsert_records(&heal_pass).unwrap();
            let first_replay = started.elapsed();

            let mapped_pass = replay_ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    let mut record = bench_record(index as u64, "mapped-pass");
                    record.id = *id;
                    record
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            store.upsert_records(&mapped_pass).unwrap();
            let second_replay = started.elapsed();

            println!(
                "corpus {size}: {REPLAY_ROWS} existing-record replay -> first (legacy heal) {first_replay:?} vs second (mapped) {second_replay:?}"
            );

            let fresh = (0..FRESH_ROWS)
                .map(|index| bench_record(size + index, "fresh"))
                .collect::<Vec<_>>();
            let started = Instant::now();
            store.upsert_records(&fresh).unwrap();
            let fresh_elapsed = started.elapsed();
            println!(
                "corpus {size}: {FRESH_ROWS} fresh rows (insert-only + map) {fresh_elapsed:?}"
            );
        }
    }
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    type CatalogSessionCheckpointRow = (
        String,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    );

    fn tempdir() -> tempfile::TempDir {
        let root = std::env::current_dir().unwrap().join("target/test-data");
        fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-store-catalog-")
            .tempdir_in(root)
            .unwrap()
    }

    fn index_exists(conn: &Connection, index: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
            params![index],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn timestamps() -> EntityTimestamps {
        EntityTimestamps {
            created_at: fixed_time(),
            updated_at: fixed_time(),
        }
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn schema_v1001_adds_pagination_indexes_and_read_only_rejects_v15() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        let user_version: i64 = store
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(user_version, 1001);
        let event_plan = store
            .conn
            .prepare("EXPLAIN QUERY PLAN SELECT id FROM events WHERE session_id = ?1 AND (seq, id) > (?2, ?3) ORDER BY seq, id LIMIT ?4")
            .unwrap()
            .query_map(params![Uuid::nil().to_string(), 0_i64, "", 10_i64], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            event_plan.contains("idx_events_session_seq_id"),
            "{event_plan}"
        );
        assert!(
            event_plan.contains("(seq,id)>(?,?)"),
            "keyset plan did not use the composite range: {event_plan}"
        );
        assert!(
            !event_plan.to_ascii_lowercase().contains("scan events"),
            "{event_plan}"
        );
        let provider_plan = store
            .conn
            .prepare("EXPLAIN QUERY PLAN SELECT id FROM sessions WHERE provider = ?1 AND external_session_id = ?2 ORDER BY started_at_ms DESC, id LIMIT ?3")
            .unwrap()
            .query_map(params![CaptureProvider::Codex.as_str(), "abc", 2_i64], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            provider_plan.contains("idx_sessions_provider_external_session_started"),
            "{provider_plan}"
        );
        drop(store);

        let legacy = temp.path().join("legacy.sqlite");
        let conn = Connection::open(&legacy).unwrap();
        conn.execute_batch(CREATE_TABLES_SQL).unwrap();
        // CREATE_TABLES_SQL describes the base-table schema without the fork
        // additions. Drop the fork indexes defensively so this test proves
        // the v1001 migration creates both of them rather than merely
        // observing fresh-schema DDL.
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_events_session_seq_id;
             DROP INDEX IF EXISTS idx_sessions_provider_external_session_started;
             PRAGMA user_version = 15;",
        )
        .unwrap();
        drop(conn);
        let err = match Store::open_read_only(&legacy) {
            Ok(_) => panic!("legacy schema unexpectedly opened read-only"),
            Err(err) => err,
        };
        assert!(matches!(err, StoreError::UnsupportedSchemaVersion(15)));
        let migrated = Store::open(&legacy).unwrap();
        let migrated_version: i64 = migrated
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(migrated_version, 1001);
        for index in [
            "idx_events_session_seq_id",
            "idx_sessions_provider_external_session_started",
        ] {
            assert!(
                index_exists(&migrated.conn, index),
                "migration did not create {index}"
            );
        }
    }

    #[test]
    fn writable_open_rejects_in_between_fork_schema_versions_without_mutation() {
        for version in [16_i64, 999, 1002] {
            let temp = tempdir();
            let db = temp.path().join(format!("v{version}.sqlite"));
            let conn = Connection::open(&db).unwrap();
            conn.execute_batch(CREATE_TABLES_SQL).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA journal_mode = DELETE; PRAGMA user_version = {version};"
            ))
            .unwrap();
            drop(conn);
            let modified_before = fs::metadata(&db).unwrap().modified().unwrap();
            #[cfg(unix)]
            let mode_before = {
                use std::os::unix::fs::PermissionsExt as _;
                fs::metadata(&db).unwrap().permissions().mode()
            };

            let err = match Store::open(&db) {
                Ok(_) => panic!("unsupported schema v{version} unexpectedly opened"),
                Err(err) => err,
            };
            assert!(matches!(err, StoreError::UnsupportedSchemaVersion(v) if v == version));
            assert!(!temp.path().join(OBJECTS_DIR).exists());
            assert!(!temp.path().join(SPOOL_DIR).exists());
            assert_eq!(
                fs::metadata(&db).unwrap().modified().unwrap(),
                modified_before
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                assert_eq!(fs::metadata(&db).unwrap().permissions().mode(), mode_before);
            }

            let conn = Connection::open(&db).unwrap();
            let unchanged_version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(unchanged_version, version);
            let journal_mode: String = conn
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            assert_eq!(journal_mode, "delete");
            assert!(!index_exists(
                &conn,
                "idx_sessions_provider_external_session_started"
            ));
            assert!(!index_exists(&conn, "idx_events_session_seq_id"));
        }
    }

    #[test]
    fn v1001_migration_rolls_back_index_and_version_on_failure() {
        let temp = tempdir();
        let db = temp.path().join("rollback.sqlite");
        // Start from a real v1000-shaped store (maps present, pagination
        // indexes absent) with a table squatting on an index name so the
        // v1001 step fails after the v1000 step has already been committed.
        drop(Store::open(&db).unwrap());
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            r#"
            BEGIN IMMEDIATE;
            DROP INDEX IF EXISTS idx_sessions_provider_external_session_started;
            DROP INDEX IF EXISTS idx_events_session_seq_id;
            CREATE TABLE idx_events_session_seq_id(id TEXT PRIMARY KEY);
            PRAGMA user_version = 1000;
            COMMIT;
            "#,
        )
        .unwrap();
        drop(conn);

        assert!(Store::open(&db).is_err());

        let conn = Connection::open(&db).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1000);
        assert!(!index_exists(
            &conn,
            "idx_sessions_provider_external_session_started"
        ));
        let blocking_table_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'idx_events_session_seq_id')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(blocking_table_exists);
        // The v1000 rowid maps survive the failed v1001 step untouched.
        for map_table in ["record_search_rowids", "event_search_rowids"] {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                    params![map_table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "{map_table} missing after failed v1001 step");
        }
    }

    fn catalog_session(
        source_path: &str,
        external_session_id: &str,
        mtime_ms: i64,
    ) -> CatalogSession {
        CatalogSession {
            provider: CaptureProvider::Codex,
            source_format: "codex_session_jsonl".into(),
            source_root: "/home/user/.codex/sessions".into(),
            source_path: source_path.into(),
            external_session_id: Some(external_session_id.into()),
            parent_external_session_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            external_agent_id: None,
            cwd: Some("/repo".into()),
            session_started_at_ms: Some(mtime_ms),
            file_size_bytes: 42,
            file_modified_at_ms: mtime_ms,
            cataloged_at_ms: mtime_ms,
            metadata: serde_json::json!({"catalog_scope": "session_meta"}),
        }
    }

    fn imported_session(external_session_id: &str) -> Session {
        Session {
            id: new_id(),
            history_record_id: None,
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some(external_session_id.into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        }
    }

    /// `usize` limits saturate to `i64::MAX` instead of `as`-wrapping:
    /// `usize::MAX as i64` is -1, which SQLite's `LIMIT` treats as
    /// unlimited. The result set must stay bounded by the available rows
    /// and small limits must keep their exact semantics.
    #[test]
    fn sessions_by_external_session_limit_saturates_instead_of_wrapping() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        for _ in 0..3 {
            store
                .upsert_session(&imported_session("shared-external-id"))
                .unwrap();
        }
        store
            .upsert_session(&imported_session("other-external-id"))
            .unwrap();

        let all = store
            .sessions_by_external_session(CaptureProvider::Codex, "shared-external-id", usize::MAX)
            .unwrap();
        assert_eq!(all.len(), 3, "usize::MAX must mean 'all rows', bounded");
        let page = store
            .sessions_by_external_session(CaptureProvider::Codex, "shared-external-id", 2)
            .unwrap();
        assert_eq!(page.len(), 2);
        let none = store
            .sessions_by_external_session(CaptureProvider::Codex, "shared-external-id", 0)
            .unwrap();
        assert!(none.is_empty(), "limit 0 must not become unlimited");
    }

    fn imported_event(id: Uuid, seq: u64) -> Event {
        Event {
            id,
            seq,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({"text":"private transcript text"}),
            payload_blob_id: None,
            dedupe_key: Some(format!("event-{seq}")),
            redaction_state: RedactionState::Raw,
            sync: sync_metadata(),
        }
    }

    #[test]
    fn id_prefix_resolution_reports_deterministic_ambiguity_and_unique_matches() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let session_ids = [
            Uuid::parse_str("aaaaaaaa-1100-7000-8000-000000000001").unwrap(),
            Uuid::parse_str("aaaaaaaa-1200-7000-8000-000000000002").unwrap(),
            Uuid::parse_str("aaaaaaaa-2000-7000-8000-000000000003").unwrap(),
        ];
        for (idx, id) in session_ids.into_iter().enumerate() {
            let mut session = imported_session(&format!("session-{idx}"));
            session.id = id;
            store.upsert_session(&session).unwrap();
        }
        let event_ids = [
            Uuid::parse_str("bbbbbbbb-1000-7000-8000-000000000001").unwrap(),
            Uuid::parse_str("bbbbbbbb-1001-7000-8000-000000000002").unwrap(),
            Uuid::parse_str("bbbbbbbb-2000-7000-8000-000000000003").unwrap(),
        ];
        for (idx, id) in event_ids.into_iter().enumerate() {
            store.upsert_event(&imported_event(id, idx as u64)).unwrap();
        }

        let ambiguous_sessions = CtxIdPrefix::parse("AAAAAAAA").unwrap();
        assert_eq!(ambiguous_sessions.canonical(), "aaaaaaaa");
        match store
            .resolve_session_by_id_prefix(&ambiguous_sessions)
            .unwrap()
        {
            IdPrefixResolution::Ambiguous(ambiguity) => {
                assert_eq!(ambiguity.candidate_count, 3);
                assert_eq!(ambiguity.minimum_total_hex_digits, 10);
                assert_eq!(ambiguity.additional_hex_digits, 2);
            }
            other => panic!("expected ambiguous sessions, got {other:?}"),
        }

        let unique_session = CtxIdPrefix::parse("aaaaaaaa11").unwrap();
        assert!(matches!(
            store.resolve_session_by_id_prefix(&unique_session).unwrap(),
            IdPrefixResolution::Found(Session { id, .. }) if id == session_ids[0]
        ));
        let unique_event = CtxIdPrefix::parse("bbbbbbbb-2").unwrap();
        assert!(matches!(
            store.resolve_event_by_id_prefix(&unique_event).unwrap(),
            IdPrefixResolution::Found(Event { id, .. }) if id == event_ids[2]
        ));
        let ambiguous_events = CtxIdPrefix::parse("bbbbbbbb").unwrap();
        match store.resolve_event_by_id_prefix(&ambiguous_events).unwrap() {
            IdPrefixResolution::Ambiguous(ambiguity) => {
                assert_eq!(ambiguity.candidate_count, 3);
                assert_eq!(ambiguity.minimum_total_hex_digits, 12);
                assert_eq!(ambiguity.additional_hex_digits, 4);
            }
            other => panic!("expected ambiguous events, got {other:?}"),
        }
    }

    #[test]
    fn id_prefix_queries_use_index_range_searches() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let session_prefix = CtxIdPrefix::parse("aaaaaaaa").unwrap();
        let event_prefix = CtxIdPrefix::parse("bbbbbbbb").unwrap();

        for (sql, pattern) in [
            (
                "EXPLAIN QUERY PLAN SELECT id FROM sessions WHERE id GLOB ?1 ORDER BY id",
                format!("{}*", session_prefix.canonical()),
            ),
            (
                "EXPLAIN QUERY PLAN SELECT id FROM events WHERE id GLOB ?1 ORDER BY id",
                format!("{}*", event_prefix.canonical()),
            ),
        ] {
            let details = store
                .conn
                .prepare(sql)
                .unwrap()
                .query_map(params![pattern], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            let plan = details.join("\n").to_ascii_uppercase();
            assert!(
                plan.contains("SEARCH"),
                "expected range search plan, got:\n{plan}"
            );
            assert!(
                plan.contains("ID>?") && plan.contains("ID<?"),
                "expected id range bounds, got:\n{plan}"
            );
            assert!(
                !plan.contains("SCAN"),
                "expected no full scan, got:\n{plan}"
            );
        }
    }

    #[test]
    fn catalog_session_upsert_skips_unchanged_rows() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        let session = catalog_session(
            "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
            "codex-session-1",
            cataloged_at_ms,
        );
        store
            .upsert_catalog_sessions(std::slice::from_ref(&session))
            .unwrap();
        let after_insert: i64 = store
            .conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();

        let mut recataloged = session.clone();
        recataloged.cataloged_at_ms += 1_000;
        store
            .upsert_catalog_sessions(std::slice::from_ref(&recataloged))
            .unwrap();
        let after_noop: i64 = store
            .conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(after_noop, after_insert);

        let mut changed = recataloged;
        changed.file_size_bytes += 1;
        changed.cataloged_at_ms += 1_000;
        store
            .upsert_catalog_sessions(std::slice::from_ref(&changed))
            .unwrap();
        let after_changed: i64 = store
            .conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
        assert!(after_changed > after_noop);
    }

    #[test]
    fn search_index_optimize_is_safe_on_initialized_store() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store.optimize_search_index().unwrap();
    }

    #[test]
    fn catalog_sessions_count_indexed_and_stale_rows() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        store
            .upsert_catalog_sessions(&[catalog_session(
                "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
                "codex-session-1",
                cataloged_at_ms,
            )])
            .unwrap();

        let counts = store.catalog_session_counts().unwrap();
        assert_eq!(counts.total, 1);
        assert_eq!(counts.indexed, 0);
        assert_eq!(counts.stale, 0);
        assert_eq!(counts.pending, 1);
        assert_eq!(counts.failed, 0);
        assert_eq!(
            store
                .catalog_source_stale_session_count(
                    CaptureProvider::Codex,
                    "/home/user/.codex/sessions"
                )
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .list_pending_catalog_sessions(CaptureProvider::Codex, "/home/user/.codex/sessions")
                .unwrap()
                .len(),
            1
        );

        store
            .upsert_session(&imported_session("codex-session-1"))
            .unwrap();
        store
            .mark_catalog_source_indexed(
                CaptureProvider::Codex,
                CatalogSourceIndexUpdate {
                    source_root: "/home/user/.codex/sessions",
                    source_path: "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
                    file_size_bytes: 42,
                    file_modified_at_ms: cataloged_at_ms,
                    file_sha256: None,
                    event_count: Some(3),
                    indexed_at_ms: cataloged_at_ms + 10,
                },
            )
            .unwrap();
        let counts = store.catalog_session_counts().unwrap();
        assert_eq!(counts.indexed, 1);
        assert_eq!(counts.pending, 0);

        store
            .mark_catalog_source_stale(
                CaptureProvider::Codex,
                "/home/user/.codex/sessions",
                cataloged_at_ms + 1,
            )
            .unwrap();
        let counts = store.catalog_session_counts().unwrap();
        assert_eq!(counts.total, 0);
        assert_eq!(counts.indexed, 0);
        assert_eq!(counts.stale, 1);
        assert_eq!(counts.pending, 0);
        assert_eq!(
            store
                .catalog_source_stale_session_count(
                    CaptureProvider::Codex,
                    "/home/user/.codex/sessions"
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn catalog_import_planning_requires_current_index_state_and_matching_session() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        store
            .upsert_catalog_sessions(&[catalog_session(
                "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
                "codex-session-1",
                cataloged_at_ms,
            )])
            .unwrap();
        store
            .mark_catalog_source_indexed(
                CaptureProvider::Codex,
                CatalogSourceIndexUpdate {
                    source_root: "/home/user/.codex/sessions",
                    source_path: "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
                    file_size_bytes: 42,
                    file_modified_at_ms: cataloged_at_ms,
                    file_sha256: None,
                    event_count: Some(3),
                    indexed_at_ms: cataloged_at_ms + 10,
                },
            )
            .unwrap();

        let pending = store
            .list_pending_catalog_sessions(CaptureProvider::Codex, "/home/user/.codex/sessions")
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(store.catalog_session_counts().unwrap().indexed, 0);

        store
            .upsert_session(&imported_session("codex-session-1"))
            .unwrap();
        let pending = store
            .list_pending_catalog_sessions(CaptureProvider::Codex, "/home/user/.codex/sessions")
            .unwrap();
        assert!(pending.is_empty());
        let counts = store.catalog_session_counts().unwrap();
        assert_eq!(counts.indexed, 1);
        assert_eq!(counts.pending, 0);
    }

    #[test]
    fn catalog_import_mark_failed_records_error_and_remains_pending() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        store
            .upsert_catalog_sessions(&[catalog_session(
                "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
                "codex-session-1",
                cataloged_at_ms,
            )])
            .unwrap();

        let changed = store
            .mark_catalog_source_failed(
                CaptureProvider::Codex,
                "/home/user/.codex/sessions",
                "/home/user/.codex/sessions/2026/06/24/rollout.jsonl",
                "bad json",
                cataloged_at_ms + 10,
            )
            .unwrap();
        assert_eq!(changed, 1);

        let counts = store.catalog_session_counts().unwrap();
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.pending, 1);
        let (status, error, indexed_at_ms): (String, Option<String>, Option<i64>) = store
            .conn
            .query_row(
                "SELECT indexed_status, indexed_error, indexed_at_ms FROM catalog_sessions WHERE source_path = ?1",
                ["/home/user/.codex/sessions/2026/06/24/rollout.jsonl"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, CatalogIndexedStatus::Failed.as_str());
        assert_eq!(error.as_deref(), Some("bad json"));
        assert_eq!(indexed_at_ms, Some(cataloged_at_ms + 10));
    }

    #[test]
    fn catalog_upsert_clears_completion_metadata_but_preserves_append_checkpoint() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        let source_path = "/home/user/.codex/sessions/2026/06/24/rollout.jsonl";
        store
            .upsert_catalog_sessions(&[catalog_session(
                source_path,
                "codex-session-1",
                cataloged_at_ms,
            )])
            .unwrap();
        store
            .upsert_session(&imported_session("codex-session-1"))
            .unwrap();
        store
            .mark_catalog_source_indexed(
                CaptureProvider::Codex,
                CatalogSourceIndexUpdate {
                    source_root: "/home/user/.codex/sessions",
                    source_path,
                    file_size_bytes: 42,
                    file_modified_at_ms: cataloged_at_ms,
                    file_sha256: None,
                    event_count: Some(3),
                    indexed_at_ms: cataloged_at_ms + 10,
                },
            )
            .unwrap();

        store
            .upsert_catalog_sessions(&[catalog_session(
                source_path,
                "codex-session-1",
                cataloged_at_ms,
            )])
            .unwrap();
        assert_eq!(store.catalog_session_counts().unwrap().indexed, 1);

        let mut changed = catalog_session(source_path, "codex-session-1", cataloged_at_ms + 1);
        changed.file_size_bytes = 43;
        store.upsert_catalog_sessions(&[changed]).unwrap();

        let counts = store.catalog_session_counts().unwrap();
        assert_eq!(counts.indexed, 0);
        assert_eq!(counts.pending, 1);
        let (
            status,
            indexed_at_ms,
            indexed_size,
            indexed_mtime,
            indexed_event_count,
            checkpoint_at_ms,
            checkpoint_size,
            checkpoint_mtime,
            checkpoint_event_count,
        ): CatalogSessionCheckpointRow = store
            .conn
            .query_row(
                "SELECT indexed_status, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms, indexed_event_count, last_imported_at_ms, last_imported_file_size_bytes, last_imported_file_modified_at_ms, last_imported_event_count FROM catalog_sessions WHERE source_path = ?1",
                [source_path],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(status, CatalogIndexedStatus::Pending.as_str());
        assert_eq!(indexed_at_ms, None);
        assert_eq!(indexed_size, None);
        assert_eq!(indexed_mtime, None);
        assert_eq!(indexed_event_count, None);
        assert_eq!(checkpoint_at_ms, Some(cataloged_at_ms + 10));
        assert_eq!(checkpoint_size, Some(42));
        assert_eq!(checkpoint_mtime, Some(cataloged_at_ms));
        assert_eq!(checkpoint_event_count, Some(3));

        let checkpoint = store
            .catalog_source_index_state(
                CaptureProvider::Codex,
                "/home/user/.codex/sessions",
                source_path,
            )
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.last_imported_file_size_bytes, Some(42));
        assert_eq!(
            checkpoint.last_imported_file_modified_at_ms,
            Some(cataloged_at_ms)
        );
        assert_eq!(checkpoint.last_imported_file_sha256, None);
        assert_eq!(checkpoint.last_imported_event_count, Some(3));
        assert_eq!(checkpoint.last_imported_at_ms, Some(cataloged_at_ms + 10));
    }

    #[test]
    fn catalog_upsert_invalidates_checkpoint_for_shrink_and_same_size_change() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        for (source_path, file_size_bytes) in [
            ("/home/user/.codex/sessions/2026/06/24/shrink.jsonl", 41_u64),
            (
                "/home/user/.codex/sessions/2026/06/24/same-size.jsonl",
                42_u64,
            ),
        ] {
            store
                .upsert_catalog_sessions(&[catalog_session(
                    source_path,
                    source_path,
                    cataloged_at_ms,
                )])
                .unwrap();
            store
                .upsert_session(&imported_session(source_path))
                .unwrap();
            store
                .mark_catalog_source_indexed(
                    CaptureProvider::Codex,
                    CatalogSourceIndexUpdate {
                        source_root: "/home/user/.codex/sessions",
                        source_path,
                        file_size_bytes: 42,
                        file_modified_at_ms: cataloged_at_ms,
                        file_sha256: None,
                        event_count: Some(3),
                        indexed_at_ms: cataloged_at_ms + 10,
                    },
                )
                .unwrap();

            let mut changed = catalog_session(source_path, source_path, cataloged_at_ms + 1);
            changed.file_size_bytes = file_size_bytes;
            store.upsert_catalog_sessions(&[changed]).unwrap();

            let (status, indexed_size, checkpoint_size): (String, Option<i64>, Option<i64>) =
                store
                    .conn
                    .query_row(
                        "SELECT indexed_status, indexed_file_size_bytes, last_imported_file_size_bytes FROM catalog_sessions WHERE source_path = ?1",
                        [source_path],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
            assert_eq!(status, CatalogIndexedStatus::Pending.as_str());
            assert_eq!(indexed_size, None);
            assert_eq!(checkpoint_size, None);
        }
    }

    #[test]
    fn catalog_index_checkpoint_event_count_can_be_unknown() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let cataloged_at_ms = timestamp_ms(fixed_time());
        let source_path = "/home/user/.codex/sessions/2026/06/24/unknown-count.jsonl";
        store
            .upsert_catalog_sessions(&[catalog_session(
                source_path,
                "codex-session-unknown-count",
                cataloged_at_ms,
            )])
            .unwrap();
        store
            .mark_catalog_source_indexed(
                CaptureProvider::Codex,
                CatalogSourceIndexUpdate {
                    source_root: "/home/user/.codex/sessions",
                    source_path,
                    file_size_bytes: 42,
                    file_modified_at_ms: cataloged_at_ms,
                    file_sha256: Some("abc123"),
                    event_count: None,
                    indexed_at_ms: cataloged_at_ms + 10,
                },
            )
            .unwrap();

        let checkpoint = store
            .catalog_source_index_state(
                CaptureProvider::Codex,
                "/home/user/.codex/sessions",
                source_path,
            )
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.last_imported_event_count, None);
        assert_eq!(
            checkpoint.last_imported_file_sha256.as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn source_import_manifest_upsert_ignores_observed_at_for_unchanged_files() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let observed_at_ms = timestamp_ms(fixed_time());
        let mut file = SourceImportFile {
            provider: CaptureProvider::Claude,
            source_format: "claude_projects_jsonl_tree".into(),
            source_root: "/home/user/.claude/projects".into(),
            source_path: "/home/user/.claude/projects/session.jsonl".into(),
            file_size_bytes: 42,
            file_modified_at_ms: observed_at_ms,
            observed_at_ms,
            metadata: serde_json::json!({}),
        };
        store
            .upsert_source_import_files(std::slice::from_ref(&file))
            .unwrap();
        store
            .mark_source_import_file_indexed(
                CaptureProvider::Claude,
                SourceImportFileIndexUpdate {
                    source_root: "/home/user/.claude/projects",
                    source_path: "/home/user/.claude/projects/session.jsonl",
                    file_size_bytes: 42,
                    file_modified_at_ms: observed_at_ms,
                    indexed_at_ms: observed_at_ms + 10,
                },
            )
            .unwrap();
        let after_indexed: i64 = store
            .conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();

        file.observed_at_ms += 1_000;
        store
            .upsert_source_import_files(std::slice::from_ref(&file))
            .unwrap();
        let after_noop: i64 = store
            .conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(after_noop, after_indexed);
        assert!(store
            .list_pending_source_import_files(
                CaptureProvider::Claude,
                "/home/user/.claude/projects"
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn source_import_zero_yield_anomaly_count_is_path_free_and_clears_on_indexed() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let file = SourceImportFile {
            provider: CaptureProvider::Codex,
            source_format: "codex_sessions".to_owned(),
            source_root: "secret-root".to_owned(),
            source_path: "secret-path".to_owned(),
            file_size_bytes: 10,
            file_modified_at_ms: 1,
            observed_at_ms: 1,
            metadata: serde_json::Value::Null,
        };
        store
            .upsert_source_import_files(std::slice::from_ref(&file))
            .unwrap();
        let mut file2 = file.clone();
        file2.source_path = "secret-path-2".to_owned();
        store
            .upsert_source_import_files(std::slice::from_ref(&file2))
            .unwrap();
        store
            .mark_source_import_file_failed(
                CaptureProvider::Codex,
                &file.source_root,
                &file.source_path,
                SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE,
                2,
            )
            .unwrap();
        store
            .mark_source_import_file_failed(
                CaptureProvider::Codex,
                &file2.source_root,
                &file2.source_path,
                SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE,
                2,
            )
            .unwrap();
        assert_eq!(store.count_source_import_zero_yield_anomalies().unwrap(), 2);
        store
            .mark_source_import_file_indexed(
                CaptureProvider::Codex,
                SourceImportFileIndexUpdate {
                    source_root: &file.source_root,
                    source_path: &file.source_path,
                    file_size_bytes: file.file_size_bytes,
                    file_modified_at_ms: file.file_modified_at_ms,
                    indexed_at_ms: 3,
                },
            )
            .unwrap();
        assert_eq!(store.count_source_import_zero_yield_anomalies().unwrap(), 1);
    }

    #[test]
    fn existing_external_session_ids_handles_empty_dedupe_chunks_and_provider_isolation() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        assert!(store
            .existing_external_session_ids(CaptureProvider::Codex, &[])
            .unwrap()
            .is_empty());

        for (provider, external_id) in [
            ("codex", "codex-1"),
            ("codex", "codex-499"),
            ("codex", "codex-500"),
            ("claude", "codex-1"),
        ] {
            store.conn.execute(
                "INSERT INTO sessions (id, provider, external_session_id, agent_type, is_primary, status, fidelity, started_at_ms, created_at_ms, updated_at_ms, visibility, sync_state, sync_version, metadata_json) VALUES (?1, ?2, ?3, 'primary', 1, 'imported', 'imported', 0, 0, 0, 'local_only', 'local_only', 0, '{}')",
                params![new_id().to_string(), provider, external_id],
            ).unwrap();
        }

        let mut ids = (0..=1001)
            .map(|idx| format!("codex-{idx}"))
            .collect::<Vec<_>>();
        ids.push("codex-1".to_owned());
        ids.push("".to_owned());
        let existing = store
            .existing_external_session_ids(CaptureProvider::Codex, &ids)
            .unwrap();
        assert_eq!(existing.len(), 3);
        assert!(existing.contains("codex-1"));
        assert!(existing.contains("codex-499"));
        assert!(existing.contains("codex-500"));

        let claude = store
            .existing_external_session_ids(CaptureProvider::Claude, &["codex-1".to_owned()])
            .unwrap();
        assert_eq!(claude.len(), 1);
        assert!(claude.contains("codex-1"));
    }

    #[test]
    fn catalog_schema_includes_import_state_columns() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let schema = store.schema().unwrap();
        assert!(schema.contains("indexed_at_ms INTEGER"));
        assert!(schema.contains("indexed_file_size_bytes INTEGER"));
        assert!(schema.contains("indexed_file_modified_at_ms INTEGER"));
        assert!(schema.contains("indexed_status TEXT NOT NULL DEFAULT 'pending'"));
        assert!(schema.contains("indexed_error TEXT"));
        assert!(schema.contains("indexed_event_count INTEGER"));
        assert!(schema.contains("last_imported_at_ms INTEGER"));
        assert!(schema.contains("last_imported_file_size_bytes INTEGER"));
        assert!(schema.contains("last_imported_file_modified_at_ms INTEGER"));
        assert!(schema.contains("last_imported_file_sha256 TEXT"));
        assert!(schema.contains("last_imported_event_count INTEGER"));
    }

    #[test]
    fn raw_sql_query_reads_stable_views() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let schema = store.schema().unwrap();
        for view in [
            "CREATE VIEW ctx_sessions",
            "CREATE VIEW ctx_events",
            "CREATE VIEW ctx_files_touched",
            "CREATE VIEW ctx_sources",
        ] {
            assert!(schema.contains(view), "schema missing {view}");
        }

        let result = store
            .raw_sql_query(
                "SELECT COUNT(*) AS session_count FROM ctx_sessions",
                RawSqlOptions::default(),
            )
            .unwrap();
        assert_eq!(result.columns[0].name, "session_count");
        assert_eq!(result.returned_rows, 1);
        assert_eq!(result.rows[0][0], RawSqlValue::Integer(0));
    }

    #[test]
    fn ctx_files_touched_resolves_session_from_source_id() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let record_id = "018f45d0-0000-7000-8000-000000080001";
        let source_id = "018f45d0-0000-7000-8000-000000080002";
        let session_id = "018f45d0-0000-7000-8000-000000080003";
        let touch_id = "018f45d0-0000-7000-8000-000000080004";
        let detached_source_id = "018f45d0-0000-7000-8000-000000080005";
        let detached_touch_id = "018f45d0-0000-7000-8000-000000080006";

        store
            .conn
            .execute(
                r#"
                INSERT INTO history_records
                (id, title, last_activity_at_ms, created_at_ms, updated_at_ms, body, created_at, updated_at)
                VALUES (?1, 'Touched file view record', 1, 1, 1, '', '', '')
                "#,
                [record_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO capture_sources
                (id, kind, provider, machine_id, raw_source_path, external_session_id, started_at_ms, fidelity)
                VALUES (?1, 'provider_import', 'codex', 'test-machine', '/tmp/session.jsonl', 'codex-session-1', 1, 'imported')
                "#,
                [source_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO capture_sources
                (id, kind, provider, machine_id, raw_source_path, external_session_id, started_at_ms, fidelity)
                VALUES (?1, 'provider_import', 'opencode', 'test-machine', '/tmp/opencode.db', 'opencode-session-1', 1, 'imported')
                "#,
                [detached_source_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO sessions
                (
                    id, history_record_id, capture_source_id, provider, external_session_id,
                    agent_type, is_primary, status, fidelity, started_at_ms, created_at_ms, updated_at_ms
                )
                VALUES (?1, ?2, ?3, 'codex', 'codex-session-1', 'primary', 1, 'imported', 'imported', 1, 1, 1)
                "#,
                params![session_id, record_id, source_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO files_touched
                (id, source_id, path, change_kind, confidence, created_at_ms, updated_at_ms, fidelity)
                VALUES (?1, ?2, 'src/main.rs', 'modified', 'explicit', 1, 1, 'imported')
                "#,
                params![touch_id, source_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO files_touched
                (id, source_id, path, change_kind, confidence, created_at_ms, updated_at_ms, fidelity)
                VALUES (?1, ?2, 'detached.rs', 'modified', 'explicit', 1, 1, 'imported')
                "#,
                params![detached_touch_id, detached_source_id],
            )
            .unwrap();

        let result = store
            .raw_sql_query(
                "SELECT provider, provider_session_id, ctx_session_id, history_record_id FROM ctx_files_touched WHERE path = 'src/main.rs'",
                RawSqlOptions::default(),
            )
            .unwrap();
        assert_eq!(result.returned_rows, 1);
        assert_eq!(
            result.rows[0][0],
            RawSqlValue::Text {
                value: "codex".to_owned(),
                bytes: 5,
                truncated: false,
            }
        );
        assert_eq!(
            result.rows[0][1],
            RawSqlValue::Text {
                value: "codex-session-1".to_owned(),
                bytes: 15,
                truncated: false,
            }
        );
        assert_eq!(
            result.rows[0][2],
            RawSqlValue::Text {
                value: session_id.to_owned(),
                bytes: session_id.len(),
                truncated: false,
            }
        );
        assert_eq!(
            result.rows[0][3],
            RawSqlValue::Text {
                value: record_id.to_owned(),
                bytes: record_id.len(),
                truncated: false,
            }
        );

        let detached = store
            .raw_sql_query(
                "SELECT provider, provider_session_id, ctx_session_id, history_record_id FROM ctx_files_touched WHERE path = 'detached.rs'",
                RawSqlOptions::default(),
            )
            .unwrap();
        assert_eq!(detached.returned_rows, 1);
        assert_eq!(
            detached.rows[0][0],
            RawSqlValue::Text {
                value: "opencode".to_owned(),
                bytes: 8,
                truncated: false,
            }
        );
        assert_eq!(
            detached.rows[0][1],
            RawSqlValue::Text {
                value: "opencode-session-1".to_owned(),
                bytes: 18,
                truncated: false,
            }
        );
        assert_eq!(detached.rows[0][2], RawSqlValue::Null);
        assert_eq!(detached.rows[0][3], RawSqlValue::Null);
    }

    #[test]
    fn raw_sql_query_rejects_writes_parameters_and_multiple_statements() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();

        assert!(matches!(
            store
                .raw_sql_query("", RawSqlOptions::default())
                .unwrap_err(),
            StoreError::RawSqlEmpty
        ));
        assert!(matches!(
            store
                .raw_sql_query("SELECT ?1", RawSqlOptions::default())
                .unwrap_err(),
            StoreError::RawSqlHasParameters
        ));
        assert!(matches!(
            store
                .raw_sql_query("CREATE TABLE nope(x INTEGER)", RawSqlOptions::default())
                .unwrap_err(),
            StoreError::RawSqlNotReadOnly
        ));
        assert!(matches!(
            store
                .raw_sql_query("SELECT 1; SELECT 2", RawSqlOptions::default())
                .unwrap_err(),
            StoreError::Sql(rusqlite::Error::MultipleStatement)
        ));
    }

    #[test]
    fn raw_sql_query_caps_rows_and_values() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let result = store
            .raw_sql_query(
                "SELECT 'abcdef' AS text_value, X'01020304' AS blob_value UNION ALL SELECT 'ghijkl', X'05060708'",
                RawSqlOptions {
                    max_rows: 1,
                    max_value_bytes: 3,
                    ..RawSqlOptions::default()
                },
            )
            .unwrap();
        assert_eq!(result.returned_rows, 1);
        assert_eq!(result.columns[0].name, "text_value");
        assert_eq!(result.columns[1].name, "blob_value");
        assert_eq!(
            result.rows[0][0],
            RawSqlValue::Text {
                value: "abc".to_owned(),
                bytes: 6,
                truncated: true,
            }
        );
        assert_eq!(
            result.rows[0][1],
            RawSqlValue::Blob {
                bytes: 4,
                preview_hex: "010203".to_owned(),
                truncated: true,
            }
        );
        assert!(result.truncated.rows);
        assert!(result.truncated.values);
    }

    #[test]
    fn raw_sql_query_times_out_long_running_queries() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let err = store
            .raw_sql_query(
                r#"
                WITH RECURSIVE numbers(x) AS (
                    SELECT 1
                    UNION ALL
                    SELECT x + 1 FROM numbers WHERE x < 100000000
                )
                SELECT sum(x) FROM numbers
                "#,
                RawSqlOptions {
                    timeout: Duration::from_millis(1),
                    ..RawSqlOptions::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, StoreError::RawSqlTimedOut { .. }));
    }

    #[test]
    fn raw_sql_query_enforces_sqlite_value_length_limit() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let err = store
            .raw_sql_query(
                "SELECT length(randomblob(200000))",
                RawSqlOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::Sql(rusqlite::Error::SqliteFailure(error, _))
                if error.code == ErrorCode::TooBig
        ));
    }

    #[test]
    fn schema_v8_migrates_legacy_history_record_table_names() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&legacy_history_record_sql(CREATE_TABLES_SQL))
                .unwrap();
            conn.execute_batch(&legacy_history_record_sql(FTS_TABLES_SQL))
                .unwrap();
            let record_id = new_id();
            conn.execute(
                "INSERT INTO work_records (id, title, last_activity_at_ms, body, created_at, updated_at)
                 VALUES (?1, 'Legacy record', 0, '', '2026-06-23T12:00:00+00:00', '2026-06-23T12:00:00+00:00')",
                [record_id.to_string()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO sessions
                 (id, work_record_id, provider, agent_type, is_primary, status, fidelity, started_at_ms, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 'codex', 'primary', 1, 'imported', 'partial', 0, 0, 0)",
                params![new_id().to_string(), record_id.to_string()],
            )
            .unwrap();
            conn.execute_batch("PRAGMA user_version = 7;").unwrap();
        }

        let store = Store::open(&path).unwrap();
        assert!(table_exists(&store.conn, "history_records").unwrap());
        assert!(!table_exists(&store.conn, "work_records").unwrap());
        assert!(table_exists(&store.conn, "history_record_links").unwrap());
        assert!(!table_exists(&store.conn, "work_record_links").unwrap());
        for table in ["sessions", "runs", "events", "summaries", "files_touched"] {
            assert!(table_has_column(&store.conn, table, "history_record_id").unwrap());
            assert!(!table_has_column(&store.conn, table, "work_record_id").unwrap());
        }
        let version: i64 = store
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn schema_v12_invalidates_provider_import_indexes_for_reimport() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(CREATE_TABLES_SQL).unwrap();
            conn.execute(
                r#"
                INSERT INTO catalog_sessions
                (
                    source_path, provider, source_format, source_root, external_session_id,
                    agent_type, file_size_bytes, file_modified_at_ms, cataloged_at_ms,
                    indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms,
                    indexed_status, indexed_event_count
                )
                VALUES
                (
                    '/tmp/codex/session.jsonl', 'codex', 'codex_rollout_jsonl', '/tmp/codex',
                    'session-1', 'primary', 10, 20, 30, 40, 10, 20, 'indexed', 5
                )
                "#,
                [],
            )
            .unwrap();
            conn.execute(
                r#"
                INSERT INTO source_import_files
                (
                    provider, source_format, source_root, source_path,
                    file_size_bytes, file_modified_at_ms, observed_at_ms,
                    indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms,
                    indexed_status
                )
                VALUES
                (
                    'antigravity', 'antigravity_cli_transcript_jsonl', '/tmp/agy',
                    '/tmp/agy/transcript.jsonl', 10, 20, 30, 40, 10, 20, 'indexed'
                )
                "#,
                [],
            )
            .unwrap();
            conn.execute_batch("PRAGMA user_version = 11;").unwrap();
        }

        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        let catalog_status: (String, Option<i64>, Option<i64>, Option<i64>, Option<i64>) = store
            .conn
            .query_row(
                "SELECT indexed_status, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms, indexed_event_count FROM catalog_sessions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(
            catalog_status,
            ("pending".to_owned(), None, None, None, None)
        );

        let file_status: (String, Option<i64>, Option<i64>, Option<i64>) = store
            .conn
            .query_row(
                "SELECT indexed_status, indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms FROM source_import_files",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(file_status, ("pending".to_owned(), None, None, None));
    }

    #[test]
    fn schema_v14_backfills_catalog_import_checkpoints() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            let legacy_sql = CREATE_TABLES_SQL
                .replace("    last_imported_at_ms INTEGER,\n", "")
                .replace("    last_imported_file_size_bytes INTEGER,\n", "")
                .replace("    last_imported_file_modified_at_ms INTEGER,\n", "")
                .replace("    last_imported_file_sha256 TEXT,\n", "")
                .replace("    last_imported_event_count INTEGER,\n", "");
            conn.execute_batch(&legacy_sql).unwrap();
            conn.execute(
                r#"
                INSERT INTO catalog_sessions
                (
                    source_path, provider, source_format, source_root, external_session_id,
                    agent_type, file_size_bytes, file_modified_at_ms, cataloged_at_ms,
                    indexed_at_ms, indexed_file_size_bytes, indexed_file_modified_at_ms,
                    indexed_status, indexed_event_count
                )
                VALUES
                (
                    '/tmp/codex/session.jsonl', 'codex', 'codex_rollout_jsonl', '/tmp/codex',
                    'session-1', 'primary', 20, 30, 40, 50, 10, 15, 'pending', 7
                )
                "#,
                [],
            )
            .unwrap();
            conn.execute_batch("PRAGMA user_version = 13;").unwrap();
        }

        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        let checkpoint: (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = store
            .conn
            .query_row(
                "SELECT last_imported_at_ms, last_imported_file_size_bytes, last_imported_file_modified_at_ms, last_imported_event_count FROM catalog_sessions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(checkpoint, (Some(50), Some(10), Some(15), Some(7)));
    }

    fn legacy_history_record_sql(sql: &str) -> String {
        sql.replace("history_record_links", "work_record_links")
            .replace("history_record_tags", "work_record_tags")
            .replace("history_records", "work_records")
            .replace("history_record_id", "work_record_id")
    }

    #[test]
    fn provider_check_constraints_accept_search_only_providers() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        rebuild_capture_sources_provider_check(&store.conn).unwrap();
        rebuild_catalog_sessions_provider_check(&store.conn).unwrap();

        let schema = store.schema().unwrap();
        for (provider, source_format) in [
            ("copilot_cli", "copilot_cli_session_events_jsonl"),
            ("factory_ai_droid", "factory_ai_droid_sessions_jsonl"),
            ("custom", "ctx_history_jsonl_v1"),
        ] {
            assert!(
                schema.contains(provider),
                "schema provider checks should include {provider}"
            );
            store
                .conn
                .execute(
                    r#"
                    INSERT INTO capture_sources
                    (id, kind, provider, machine_id, started_at_ms, fidelity)
                    VALUES (?1, 'provider_import', ?2, 'test-machine', 0, 'partial')
                    "#,
                    params![new_id().to_string(), provider],
                )
                .unwrap();
            store
                .conn
                .execute(
                    r#"
                    INSERT INTO catalog_sessions
                    (source_path, provider, source_format, source_root, agent_type, file_size_bytes, file_modified_at_ms, cataloged_at_ms)
                    VALUES (?1, ?2, ?3, '/tmp/provider', 'primary', 1, 0, 0)
                    "#,
                    params![format!("/tmp/provider/{provider}.jsonl"), provider, source_format],
                )
                .unwrap();
        }

        let source_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM capture_sources WHERE provider IN ('copilot_cli', 'factory_ai_droid', 'custom')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let catalog_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM catalog_sessions WHERE provider IN ('copilot_cli', 'factory_ai_droid', 'custom')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_count, 3);
        assert_eq!(catalog_count, 3);
    }

    #[test]
    fn latest_indexed_source_at_uses_manifest_and_session_before_event_fallback() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO source_import_files
                (provider, source_format, source_root, source_path, file_size_bytes,
                 file_modified_at_ms, observed_at_ms, indexed_at_ms, indexed_status)
                VALUES ('pi', 'pi_sessions_jsonl', '/tmp/pi', '/tmp/pi/sessions.jsonl',
                        1, 2, 3, 9000, 'indexed')
                "#,
                [],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO sessions
                (id, provider, external_session_id, agent_type, is_primary, status,
                 fidelity, started_at_ms, created_at_ms, updated_at_ms, visibility,
                 sync_state, sync_version, metadata_json)
                VALUES (?1, 'pi', 's1', 'primary', 1, 'completed', 'imported',
                        1, 1, 8000, 'local_only', 'local_only', 0, '{}')
                "#,
                params![new_id().to_string()],
            )
            .unwrap();
        assert_eq!(store.latest_indexed_source_at_ms().unwrap(), Some(9000));

        store
            .conn
            .execute("DELETE FROM source_import_files", [])
            .unwrap();
        assert_eq!(store.latest_indexed_source_at_ms().unwrap(), Some(8000));
    }

    #[test]
    fn latest_indexed_source_at_falls_back_to_event_only_store() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO events
                (id, seq, event_type, occurred_at_ms, payload_json, visibility,
                 redaction_state, fidelity, sync_state, sync_version, metadata_json)
                VALUES (?1, 1, 'message', 7000, '{}', 'local_only', 'safe_preview',
                        'imported', 'local_only', 0, '{}')
                "#,
                params![new_id().to_string()],
            )
            .unwrap();
        assert_eq!(store.latest_indexed_source_at_ms().unwrap(), Some(7000));
    }

    fn pagination_event(
        session_id: Uuid,
        seq: u64,
        event_type: EventType,
        role: Option<EventRole>,
    ) -> Event {
        Event {
            id: Uuid::from_u128(20_000 + seq as u128),
            seq,
            history_record_id: None,
            session_id: Some(session_id),
            run_id: None,
            event_type,
            role,
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({"text": format!("event-{seq}")}),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::LocalPreview,
            sync: sync_metadata(),
        }
    }

    #[test]
    fn selected_event_modes_share_count_and_bounded_keyset_predicates() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let session = imported_session("selected-mode-session");
        store.upsert_session(&session).unwrap();
        let rows = [
            (EventType::Message, Some(EventRole::System)),
            (EventType::Message, Some(EventRole::User)),
            (EventType::Message, Some(EventRole::Assistant)),
            (EventType::ToolCall, Some(EventRole::Tool)),
            (EventType::Message, Some(EventRole::Assistant)),
            (EventType::Message, Some(EventRole::User)),
            (EventType::Message, Some(EventRole::Assistant)),
            (EventType::ToolOutput, Some(EventRole::Tool)),
        ];
        for (seq, (event_type, role)) in rows.into_iter().enumerate() {
            store
                .upsert_event(&pagination_event(session.id, seq as u64, event_type, role))
                .unwrap();
        }
        for (mode, expected) in [
            (SelectedEventMode::Log, vec![0, 1, 2, 3, 4, 5, 6, 7]),
            (SelectedEventMode::Full, vec![0, 1, 2, 4, 5, 6]),
            (SelectedEventMode::Lite, vec![1, 4, 5, 6]),
        ] {
            assert_eq!(
                store
                    .selected_event_count_for_session(session.id, mode)
                    .unwrap(),
                expected.len()
            );
            assert!(store
                .selected_events_for_session_after(session.id, mode, None, 0)
                .unwrap()
                .is_empty());
            let first = store
                .selected_events_for_session_after(session.id, mode, None, 2)
                .unwrap();
            let second = store
                .selected_events_for_session_after(
                    session.id,
                    mode,
                    first.last().map(|event| (event.seq, event.id)),
                    usize::MAX,
                )
                .unwrap();
            let actual = first
                .iter()
                .chain(&second)
                .map(|event| event.seq)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            for (position, seq) in expected.iter().enumerate() {
                let id = Uuid::from_u128(20_000 + *seq as u128);
                assert_eq!(
                    store
                        .selected_event_cursor_position(session.id, mode, (*seq, id))
                        .unwrap(),
                    Some(position)
                );
            }
        }
        assert_eq!(
            store
                .selected_event_cursor_position(
                    session.id,
                    SelectedEventMode::Lite,
                    (2, Uuid::from_u128(20_002)),
                )
                .unwrap(),
            None
        );

        let lite_plan = store
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN SELECT e.id FROM events AS e WHERE e.session_id = ?1 AND ({}) AND (e.seq, e.id) > (?2, ?3) ORDER BY e.seq, e.id LIMIT ?4",
                selected_event_predicate(SelectedEventMode::Lite)
            ))
            .unwrap()
            .query_map(params![session.id.to_string(), -1_i64, "", 10_i64], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            lite_plan.contains("idx_events_session_seq_id"),
            "{lite_plan}"
        );
    }

    #[test]
    fn bounded_event_reads_cap_huge_limits_and_keep_sessionless_center() {
        let temp = tempdir();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let session = imported_session("long-session");
        store.upsert_session(&session).unwrap();
        let transaction = store.conn.unchecked_transaction().unwrap();
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO events (id, seq, session_id, event_type, role, occurred_at_ms, payload_json) VALUES (?1, ?2, ?3, 'message', 'user', 0, '{}')",
                )
                .unwrap();
            for seq in 0..=MAX_BOUNDED_EVENT_READ {
                statement
                    .execute(params![
                        Uuid::from_u128(1_000_000 + seq as u128).to_string(),
                        i64::try_from(seq).unwrap(),
                        session.id.to_string()
                    ])
                    .unwrap();
            }
        }
        transaction.commit().unwrap();
        let bounded = store
            .selected_events_for_session_after(session.id, SelectedEventMode::Log, None, usize::MAX)
            .unwrap();
        assert_eq!(bounded.len(), MAX_BOUNDED_EVENT_READ);

        let mut sessionless = pagination_event(Uuid::nil(), 20_000, EventType::Notice, None);
        sessionless.id = Uuid::from_u128(8_888_888);
        sessionless.session_id = None;
        store.upsert_event(&sessionless).unwrap();
        assert_eq!(
            store
                .event_window_bounded(sessionless.id, usize::MAX, usize::MAX)
                .unwrap(),
            vec![sessionless]
        );
    }

    #[test]
    fn snapshot_fingerprint_is_stable_sha256_and_tracks_wal_changes() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        let first = store.snapshot_fingerprint().unwrap();
        let same = store.snapshot_fingerprint().unwrap();
        assert_eq!(first, same);
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));

        let session = imported_session("fingerprint-session");
        store.upsert_session(&session).unwrap();
        let changed = store.snapshot_fingerprint().unwrap();
        assert_ne!(first, changed);
        store.checkpoint_wal_passive().unwrap();
        let checkpointed = store.snapshot_fingerprint().unwrap();
        assert_eq!(checkpointed.len(), 64);
        assert_ne!(
            changed, checkpointed,
            "a physical checkpoint transition must conservatively stale tokens"
        );
    }

    #[test]
    fn schema_v15_rebuilds_provider_checks_with_referenced_sources_and_indexes() {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let source_id = new_id();
        let session_id;
        let event_id;
        {
            let store = Store::open(&path).unwrap();
            let source = CaptureSource {
                id: source_id,
                descriptor: CaptureSourceDescriptor {
                    kind: ctx_history_core::CaptureSourceKind::ProviderImport,
                    provider: CaptureProvider::Codex,
                    machine_id: "test-machine".to_owned(),
                    process_id: None,
                    cwd: Some("/repo".to_owned()),
                    raw_source_path: Some("/home/user/.codex/sessions/session.jsonl".to_owned()),
                    external_session_id: Some("codex-session-1".to_owned()),
                },
                started_at: fixed_time(),
                ended_at: None,
                sync: sync_metadata(),
            };
            store.upsert_capture_source(&source).unwrap();

            let mut session = imported_session("codex-session-1");
            session.capture_source_id = Some(source_id);
            session_id = session.id;
            store.upsert_session(&session).unwrap();

            let event = Event {
                id: new_id(),
                seq: 0,
                history_record_id: None,
                session_id: Some(session_id),
                run_id: None,
                event_type: EventType::Message,
                role: Some(EventRole::User),
                occurred_at: fixed_time(),
                capture_source_id: Some(source_id),
                payload: serde_json::json!({"text": "migration source reference"}),
                payload_blob_id: None,
                dedupe_key: None,
                redaction_state: RedactionState::LocalPreview,
                sync: sync_metadata(),
            };
            event_id = event.id;
            store.upsert_event(&event).unwrap();
            store
                .conn
                .execute_batch("PRAGMA user_version = 14;")
                .unwrap();
        }

        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let source_refs: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sessions s JOIN events e ON e.session_id = s.id \
                 WHERE s.id = ?1 AND e.id = ?2 AND s.capture_source_id = ?3 AND e.capture_source_id = ?3",
                params![session_id.to_string(), event_id.to_string(), source_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source_refs, 1);
        for index in [
            "idx_capture_sources_external_session_id",
            "idx_catalog_sessions_provider_source_root_import",
            "idx_source_import_files_provider_source_root_import",
        ] {
            let exists: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                    [index],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "missing rebuilt index {index}");
        }
    }
}
