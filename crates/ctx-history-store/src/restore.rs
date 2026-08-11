//! Restore a verified v1 bundle into a fresh data root.
//!
//! Targets are strictly absent (an existing empty directory is rejected): the
//! final exclusive atomic rename cannot safely replace an existing directory.

use crate::{
    archive::{
        read_capped_line, sync_tree, verify_archive_bundle_internal, AnchoredDir,
        ArchiveVerifyOptions, ManifestInfo, VerifiedArchive,
    },
    object_relative_path, rebuild_search_projection, Result, Store, StoreError,
};
use chrono::{TimeZone, Utc};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, OptionalExtension};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const FILES: [&str; 15] = [
    "01-capture_sources.jsonl",
    "02-vcs_workspaces.jsonl",
    "03-history_records.jsonl",
    "04-artifacts.jsonl",
    "05-sessions.jsonl",
    "06-session_edges.jsonl",
    "07-runs.jsonl",
    "08-events.jsonl",
    "09-vcs_changes.jsonl",
    "10-summaries.jsonl",
    "11-files_touched.jsonl",
    "12-tags.jsonl",
    "13-history_record_tags.jsonl",
    "14-history_record_links.jsonl",
    "15-record_edges.jsonl",
];
const TABLES: &[(&str, &[&str])] = &[
    (
        "capture_sources",
        &[
            "id",
            "kind",
            "provider",
            "machine_id",
            "process_id",
            "cwd",
            "raw_source_path",
            "external_session_id",
            "started_at_ms",
            "ended_at_ms",
            "fidelity",
            "visibility",
            "sync_state",
            "sync_version",
            "metadata_json",
        ],
    ),
    (
        "vcs_workspaces",
        &[
            "id",
            "kind",
            "root_path",
            "repo_fingerprint",
            "primary_remote_url_normalized",
            "host",
            "owner",
            "name",
            "monorepo_subpath",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "history_records",
        &[
            "id",
            "title",
            "summary",
            "status",
            "primary_vcs_workspace_id",
            "started_at_ms",
            "last_activity_at_ms",
            "completed_at_ms",
            "confidence",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
            "body",
            "tags_json",
            "kind",
            "workspace",
        ],
    ),
    (
        "artifacts",
        &[
            "id",
            "kind",
            "blob_hash",
            "byte_size",
            "media_type",
            "preview_text",
            "redaction_state",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "sessions",
        &[
            "id",
            "history_record_id",
            "parent_session_id",
            "root_session_id",
            "capture_source_id",
            "provider",
            "external_session_id",
            "external_agent_id",
            "agent_type",
            "role_hint",
            "is_primary",
            "status",
            "fidelity",
            "transcript_blob_id",
            "started_at_ms",
            "ended_at_ms",
            "created_at_ms",
            "updated_at_ms",
            "visibility",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "session_edges",
        &[
            "id",
            "from_session_id",
            "to_session_id",
            "edge_type",
            "confidence",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "runs",
        &[
            "id",
            "history_record_id",
            "session_id",
            "run_type",
            "status",
            "started_at_ms",
            "ended_at_ms",
            "exit_code",
            "cwd",
            "command_preview",
            "input_blob_id",
            "output_blob_id",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "events",
        &[
            "id",
            "seq",
            "history_record_id",
            "session_id",
            "run_id",
            "event_type",
            "role",
            "occurred_at_ms",
            "capture_source_id",
            "payload_json",
            "payload_blob_id",
            "dedupe_key",
            "visibility",
            "redaction_state",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "vcs_changes",
        &[
            "id",
            "vcs_workspace_id",
            "kind",
            "change_id",
            "parent_change_ids_json",
            "branch_or_bookmark",
            "tree_hash",
            "author_time_ms",
            "confidence",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "summaries",
        &[
            "id",
            "history_record_id",
            "session_id",
            "kind",
            "model_or_source",
            "text",
            "citations_json",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "files_touched",
        &[
            "id",
            "history_record_id",
            "run_id",
            "event_id",
            "vcs_workspace_id",
            "path",
            "change_kind",
            "old_path",
            "line_count_delta",
            "confidence",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "tags",
        &[
            "id",
            "name",
            "kind",
            "created_at_ms",
            "updated_at_ms",
            "metadata_json",
        ],
    ),
    (
        "history_record_tags",
        &[
            "history_record_id",
            "tag_id",
            "source_id",
            "confidence",
            "created_at_ms",
        ],
    ),
    (
        "history_record_links",
        &[
            "id",
            "history_record_id",
            "target_type",
            "target_id",
            "link_type",
            "confidence",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
    (
        "record_edges",
        &[
            "id",
            "from_record_id",
            "to_record_id",
            "edge_type",
            "confidence",
            "created_at_ms",
            "updated_at_ms",
            "source_id",
            "visibility",
            "fidelity",
            "sync_state",
            "sync_version",
            "deleted_at_ms",
            "metadata_json",
        ],
    ),
];

#[derive(Debug, Clone)]
pub struct ArchiveRestoreReport {
    pub path: PathBuf,
    pub format: String,
    pub archive_id: Uuid,
    pub source_schema_version: i64,
    pub entity_count: u64,
    pub object_count: u64,
    pub object_bytes: u64,
    pub inserted_count: u64,
    pub reused_count: u64,
    pub selected_root_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ArchiveRestoreSelection {
    /// Empty means every authenticated root in the selective bundle.
    pub session_ids: Vec<Uuid>,
}
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() && fs::remove_dir_all(&self.0).is_err() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

#[cfg(test)]
type RestorePhaseHook = Box<dyn FnOnce() -> Result<()>>;
#[cfg(test)]
thread_local! {
    static RESTORE_PHASE_HOOK: std::cell::RefCell<Option<RestorePhaseHook>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn set_restore_phase_hook(hook: impl FnOnce() -> Result<()> + 'static) {
    RESTORE_PHASE_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}
#[cfg(test)]
fn run_restore_phase_hook() -> Result<()> {
    RESTORE_PHASE_HOOK.with(|slot| match slot.borrow_mut().take() {
        Some(hook) => hook(),
        None => Ok(()),
    })
}

#[cfg(test)]
type RestoreSyncHook = Box<dyn FnOnce(&Path) -> Result<()>>;
#[cfg(test)]
thread_local! {
    static RESTORE_SYNC_HOOK: std::cell::RefCell<Option<RestoreSyncHook>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn set_restore_sync_hook(hook: impl FnOnce(&Path) -> Result<()> + 'static) {
    RESTORE_SYNC_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}
#[cfg(test)]
fn clear_restore_sync_hook() {
    RESTORE_SYNC_HOOK.with(|slot| slot.borrow_mut().take());
}
#[cfg(test)]
fn run_restore_sync_hook(stage: &Path) -> Result<()> {
    RESTORE_SYNC_HOOK.with(|slot| match slot.borrow_mut().take() {
        Some(hook) => hook(stage),
        None => Ok(()),
    })
}

#[cfg(test)]
thread_local! { static SELECTIVE_FAIL_PHASE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
pub(crate) fn set_selective_fail_phase(phase: Option<&'static str>) {
    SELECTIVE_FAIL_PHASE.with(|slot| slot.set(phase));
}
fn selective_failpoint(phase: &'static str) -> Result<()> {
    #[cfg(test)]
    if SELECTIVE_FAIL_PHASE.with(|slot| slot.get() == Some(phase)) {
        return Err(error(format!(
            "injected selective restore failure: {phase}"
        )));
    }
    #[cfg(all(test, unix))]
    if std::env::var_os("CTX_TEST_SELECTIVE_RESTORE_SIGKILL_PHASE")
        .as_deref()
        .and_then(|value| value.to_str())
        == Some(phase)
    {
        // The subprocess test uses the same hook as the in-process failpoint,
        // but terminates here rather than unwinding. This covers the actual
        // durability boundary between object publication and the DB commit.
        unsafe {
            libc::kill(libc::getpid(), libc::SIGKILL);
        }
        return Err(error(format!(
            "selective restore subprocess did not terminate: {phase}"
        )));
    }
    let _ = phase;
    Ok(())
}

pub fn restore_archive_bundle(
    bundle: &Path,
    target: &Path,
    options: ArchiveVerifyOptions,
) -> Result<ArchiveRestoreReport> {
    let verified = verify_archive_bundle_internal(bundle, options)?; // no destination mutation before full verification
    if verified.manifest.format == "ctx-selective-archive" {
        return restore_selective(&verified, target, &ArchiveRestoreSelection::default());
    }
    if verified.manifest.format != "ctx-archive" {
        return Err(error("unsupported archive family for restore"));
    }
    reject_nesting(bundle, target)?;
    absent(target)?;
    let parent = target
        .parent()
        .ok_or_else(|| error("restore target has no parent"))?;
    let parent_anchor = AnchoredDir::open_path(parent)?;
    let target_name = target
        .file_name()
        .ok_or_else(|| error("restore target has no final component"))?;
    let mut stage_name = target_name.to_os_string();
    stage_name.push(format!(".tmp-{}", Uuid::new_v4()));
    let stage = parent.join(stage_name);
    mkdir(&stage)?;
    let mut cleanup = Cleanup(stage.clone());
    #[cfg(test)]
    run_restore_phase_hook()?;
    install_objects(&verified, &stage)?;
    let db = stage.join("work.sqlite");
    let store = Store::open(&db)?;
    store
        .conn
        .execute_batch("PRAGMA foreign_keys=ON; BEGIN IMMEDIATE")?;
    let loaded = (|| -> Result<u64> {
        let mut n = 0;
        for (index, (table, cols)) in TABLES.iter().enumerate() {
            n += load(&store, &verified, index, table, cols)?;
        }
        session_links(&store, &verified)?;
        rebuild_search_projection(&store.conn)?;
        let bad: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| {
                    r.get(0)
                })?;
        if bad != 0 {
            return Err(error("restored archive failed foreign-key check"));
        }
        Ok(n)
    })();
    match loaded {
        Ok(n) if n == verified.manifest.entity_count => store.conn.execute_batch("COMMIT")?,
        Ok(_) => {
            let _ = store.conn.execute_batch("ROLLBACK");
            return Err(error("restored entity count mismatch"));
        }
        Err(e) => {
            let _ = store.conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    }
    postcheck(&store, &verified.manifest)?;
    drop(store);
    sidecars(&db)?;
    #[cfg(test)]
    run_restore_sync_hook(&stage)?;
    sync_tree(&stage)?;
    publish(&stage, target, &parent_anchor)?;
    cleanup.0 = PathBuf::new();
    Ok(ArchiveRestoreReport {
        path: target.into(),
        format: verified.manifest.format.clone(),
        archive_id: verified.manifest.archive_id,
        source_schema_version: verified.manifest.source_schema_version,
        entity_count: verified.report.entity_count,
        object_count: verified.report.object_count,
        object_bytes: verified.report.object_bytes,
        inserted_count: verified.report.entity_count,
        reused_count: 0,
        selected_root_count: 0,
    })
}

/// Restore an authenticated union of selective roots into an existing v1005
/// store. Verification always precedes destination access. The selected member
/// set is reconstructed from authenticated per-root evidence rather than from
/// caller supplied ids or the manifest's union.
pub fn restore_archive_bundle_selective(
    bundle: &Path,
    target: &Path,
    options: ArchiveVerifyOptions,
    selection: ArchiveRestoreSelection,
) -> Result<ArchiveRestoreReport> {
    let verified = verify_archive_bundle_internal(bundle, options)?;
    if verified.manifest.format != "ctx-selective-archive" {
        if selection.session_ids.is_empty() {
            return restore_archive_bundle(bundle, target, options);
        }
        return Err(error("session selectors require a selective archive"));
    }
    restore_selective(&verified, target, &selection)
}

fn restore_selective(
    verified: &VerifiedArchive,
    target: &Path,
    selection: &ArchiveRestoreSelection,
) -> Result<ArchiveRestoreReport> {
    let db = target.join("work.sqlite");
    if !target.is_dir() || !db.is_file() {
        return Err(error("selective restore requires an existing data root"));
    }
    let plan = verified
        .manifest
        .selective_plan
        .as_ref()
        .ok_or_else(|| error("selective plan missing"))?;
    let requested: BTreeSet<String> = if selection.session_ids.is_empty() {
        plan.selected_root_ids.iter().cloned().collect()
    } else {
        selection.session_ids.iter().map(Uuid::to_string).collect()
    };
    for root in &requested {
        if !plan.selected_root_ids.iter().any(|id| id == root) {
            return Err(error(format!(
                "requested session root is absent from archive: {root}"
            )));
        }
    }
    let members = selected_members(verified, &requested)?;
    if members.is_empty() && !requested.is_empty() {
        return Err(error("requested root closure is empty or incomplete"));
    }
    let mut store = Store::open(&db)?;
    let archive_id = verified.manifest.archive_id.to_string();
    let registered: bool = store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM compaction_archives WHERE archive_id=?1 AND manifest_sha256=?2)",
        params![archive_id, verified.manifest_sha256], |r| r.get(0))?;
    if !registered {
        return Err(error("selective archive is not registered in this store"));
    }

    // This pass is deliberately read-only. It catches canonical/deferred-link,
    // natural-key, object, membership, and suppression conflicts before the
    // restore creates a directory or object file.
    preflight_selective(&store.conn, verified, target, &requested, &members)?;
    selective_failpoint("after_preflight")?;

    // Publish verified content-addressed bytes before references. A failure
    // afterwards can leave only hash-verified orphans, which a retry reuses.
    let (object_count, object_bytes) = install_selected_objects(verified, target, &members)?;
    let tx = store
        .conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    // Close the verification/write race after acquiring the writer lock. This
    // remains read-only; only a successful second pass may proceed to inserts.
    preflight_database(&tx, verified, &requested, &members)?;
    selective_failpoint("after_locked_preflight")?;
    tx.execute_batch("CREATE TEMP TABLE IF NOT EXISTS restore_new_rows(kind TEXT NOT NULL,key TEXT NOT NULL,PRIMARY KEY(kind,key)) WITHOUT ROWID; DELETE FROM restore_new_rows;")?;
    let mut inserted = 0_u64;
    let mut reused = 0_u64;
    for (index, (table, cols)) in TABLES.iter().enumerate() {
        let (a, b) = merge_stream(&tx, verified, index, table, cols, &members)?;
        inserted += a;
        reused += b;
    }
    merge_session_links(&tx, verified, &members)?;
    project_new_rows(&tx)?;
    selective_failpoint("after_projection")?;
    let bad: i64 = tx.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| {
        r.get(0)
    })?;
    if bad != 0 {
        return Err(error("selective restore failed foreign-key check"));
    }
    let now = crate::utc_now().timestamp_millis();
    for (kind, key) in &members {
        tx.execute("UPDATE compaction_archive_members SET membership_state='restored',hot_state='present',updated_at_ms=?4 WHERE archive_id=?1 AND entity_kind=?2 AND entity_key=?3", params![archive_id,kind,key,now])?;
    }
    // Only associations whose archived session root was requested transition.
    // Suppression facts are one-per selected root in deterministic plan order.
    let facts = verified.selective_suppression_facts_for_sessions(Some(&requested))?;
    for fact in &facts {
        let marker = restore_marker(&archive_id, &requested, &members);
        tx.execute(
            "INSERT INTO compaction_restore_markers VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
            params![archive_id, fact.identity_key, fact.content_key, marker, now],
        )?;
        tx.execute("UPDATE compaction_archive_suppressions SET association_state='restored',updated_at_ms=?4,last_error_code=NULL WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3", params![archive_id,fact.identity_key,fact.content_key,now])?;
        crate::suppression::derive_effective_state(
            &tx,
            &fact.identity_key,
            &fact.content_key,
            now,
        )?;
    }
    let request = restore_marker(&archive_id, &requested, &members);
    tx.execute("INSERT INTO compaction_operations VALUES(?1,'restore',?2,?3,'selective','committed',1,NULL,?4,?4) ON CONFLICT DO UPDATE SET attempt_count=attempt_count+1,phase='committed',updated_at_ms=excluded.updated_at_ms", params![Uuid::now_v7().to_string(),archive_id,request,now])?;
    selective_failpoint("before_db_commit")?;
    tx.commit()?;
    selective_failpoint("after_db_commit")?;
    Ok(ArchiveRestoreReport {
        path: target.into(),
        format: verified.manifest.format.clone(),
        archive_id: verified.manifest.archive_id,
        source_schema_version: verified.manifest.source_schema_version,
        entity_count: inserted + reused,
        object_count,
        object_bytes,
        inserted_count: inserted,
        reused_count: reused,
        selected_root_count: requested.len() as u64,
    })
}

fn restore_marker(
    archive: &str,
    roots: &BTreeSet<String>,
    members: &BTreeSet<(String, String)>,
) -> String {
    let mut h = Sha256::new();
    h.update(b"ctx-selective-restore/v1");
    h.update(archive);
    for x in roots {
        h.update((x.len() as u64).to_be_bytes());
        h.update(x);
    }
    for (k, v) in members {
        h.update(k);
        h.update([0]);
        h.update(v);
    }
    format!("{:x}", h.finalize())
}

fn selected_members(
    verified: &VerifiedArchive,
    roots: &BTreeSet<String>,
) -> Result<BTreeSet<(String, String)>> {
    let evidence = verified.root.dir("evidence")?;
    let mut r = BufReader::new(evidence.file("root-members.jsonl")?);
    let mut line = Vec::new();
    let mut out = BTreeSet::new();
    loop {
        line.clear();
        if read_capped_line(&mut r, &mut line)? == 0 {
            break;
        }
        let v: Value = serde_json::from_slice(&line)?;
        if v.get("kind").and_then(Value::as_str) == Some("root_member")
            && v.get("root_session_id")
                .and_then(Value::as_str)
                .is_some_and(|id| roots.contains(id))
        {
            let m = v
                .get("member")
                .and_then(Value::as_object)
                .ok_or_else(|| error("malformed root member evidence"))?;
            out.insert((
                m.get("entity_kind")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error("member kind missing"))?
                    .into(),
                m.get("entity_key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error("member key missing"))?
                    .into(),
            ));
        }
    }
    Ok(out)
}

fn member_key(table: &str, o: &Map<String, Value>) -> Result<String> {
    if table == "history_record_tags" {
        return Ok(format!(
            "{}:{}",
            o.get("history_record_id")
                .and_then(Value::as_str)
                .ok_or_else(|| error("tag assignment record missing"))?,
            o.get("tag_id")
                .and_then(Value::as_str)
                .ok_or_else(|| error("tag assignment tag missing"))?
        ));
    }
    o.get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| error(format!("{table} id missing")))
}

fn preflight_selective(
    conn: &rusqlite::Connection,
    verified: &VerifiedArchive,
    target: &Path,
    requested: &BTreeSet<String>,
    members: &BTreeSet<(String, String)>,
) -> Result<()> {
    preflight_database(conn, verified, requested, members)?;
    preflight_objects(verified, target, members)
}

fn preflight_database(
    conn: &rusqlite::Connection,
    verified: &VerifiedArchive,
    requested: &BTreeSet<String>,
    members: &BTreeSet<(String, String)>,
) -> Result<()> {
    for (index, (table, cols)) in TABLES.iter().enumerate() {
        preflight_stream(conn, verified, index, table, cols, members)?;
    }
    preflight_session_links(conn, verified, members)?;
    let archive_id = verified.manifest.archive_id.to_string();
    let plan = verified
        .manifest
        .selective_plan
        .as_ref()
        .ok_or_else(|| error("selective plan missing"))?;
    for (kind, key) in members {
        let member = plan
            .members
            .iter()
            .find(|m| m.entity_kind == *kind && m.entity_key == *key)
            .ok_or_else(|| error("selected member is absent from authenticated union"))?;
        let state: Option<(String, String, String)> = conn
            .query_row(
                "SELECT content_key,membership_state,hot_state FROM compaction_archive_members WHERE archive_id=?1 AND entity_kind=?2 AND entity_key=?3",
                params![archive_id,kind,key],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
            )
            .optional()?;
        let (content, membership, hot) =
            state.ok_or_else(|| error("registered archive member is missing"))?;
        if content != member.content_key
            || !matches!(
                membership.as_str(),
                "verified" | "suppressed" | "compacted" | "restored"
            )
            || !matches!(hot.as_str(), "present" | "absent")
        {
            return Err(error(
                "registered archive member conflicts with authenticated evidence",
            ));
        }
    }
    let facts = verified.selective_suppression_facts_for_sessions(Some(requested))?;
    let marker = restore_marker(&archive_id, requested, members);
    for fact in facts {
        let state: Option<String> = conn
            .query_row(
                "SELECT association_state FROM compaction_archive_suppressions WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3",
                params![archive_id,fact.identity_key,fact.content_key],
                |r| r.get(0),
            )
            .optional()?;
        if !state
            .as_deref()
            .is_some_and(|s| matches!(s, "active" | "overridden" | "restored"))
        {
            return Err(error("requested root suppression transition is not legal"));
        }
        let old: Option<String> = conn
            .query_row(
                "SELECT canonical_marker FROM compaction_restore_markers WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3",
                params![archive_id,fact.identity_key,fact.content_key],
                |r| r.get(0),
            )
            .optional()?;
        if old.as_deref().is_some_and(|value| value != marker) {
            return Err(error(
                "canonical restore marker conflicts with prior handoff",
            ));
        }
    }
    Ok(())
}

fn preflight_stream(
    conn: &rusqlite::Connection,
    verified: &VerifiedArchive,
    index: usize,
    table: &str,
    archive_cols: &[&str],
    members: &BTreeSet<(String, String)>,
) -> Result<()> {
    let meta = &verified.manifest.streams[index];
    let streams = verified.root.dir("streams")?;
    let mut reader = BufReader::new(streams.file(FILES[index])?);
    let mut line = Vec::new();
    let mut digest = Sha256::new();
    let (mut count, mut bytes) = (0_u64, 0_u64);
    let mut cols = archive_cols.to_vec();
    if table == "history_records" {
        cols.extend(["created_at", "updated_at"]);
    }
    if table == "artifacts" {
        cols.push("blob_path");
    }
    let compare: Vec<_> = if table == "sessions" {
        cols.iter()
            .copied()
            .filter(|c| *c != "parent_session_id" && *c != "root_session_id")
            .collect()
    } else {
        cols.clone()
    };
    let key_where = if table == "history_record_tags" {
        "history_record_id=?1 AND tag_id=?2"
    } else {
        "id=?1"
    };
    let select = format!(
        "SELECT {} FROM {table} WHERE {key_where}",
        compare.join(",")
    );
    loop {
        line.clear();
        let read = read_capped_line(&mut reader, &mut line)?;
        if read == 0 {
            break;
        }
        digest.update(&line);
        count += 1;
        bytes += read as u64;
        if count > meta.count || bytes > meta.bytes {
            return Err(error("archive stream exceeds authenticated bounds"));
        }
        let value_json: Value = serde_json::from_slice(&line)?;
        let row = value_json
            .as_object()
            .ok_or_else(|| error("stream row is not an object"))?;
        let key = member_key(table, row)?;
        if !members.contains(&(table.to_owned(), key.clone())) {
            continue;
        }
        let expected = compare
            .iter()
            .map(|c| value(table, c, row))
            .collect::<Result<Vec<_>>>()?;
        let existing = if table == "history_record_tags" {
            conn.query_row(
                &select,
                params![row["history_record_id"].as_str(), row["tag_id"].as_str()],
                |r| {
                    (0..compare.len())
                        .map(|i| r.get(i))
                        .collect::<rusqlite::Result<Vec<SqlValue>>>()
                },
            )
            .optional()?
        } else {
            conn.query_row(&select, [&key], |r| {
                (0..compare.len())
                    .map(|i| r.get(i))
                    .collect::<rusqlite::Result<Vec<SqlValue>>>()
            })
            .optional()?
        };
        if existing.as_ref().is_some_and(|actual| actual != &expected) {
            return Err(error(format!(
                "canonical content conflict for {table}/{key}"
            )));
        }
        if existing.is_none() {
            preflight_natural_key(conn, table, row, &key)?;
        }
    }
    if count != meta.count
        || bytes != meta.bytes
        || format!("{:x}", digest.finalize()) != meta.sha256
    {
        return Err(error("archive stream changed during preflight"));
    }
    Ok(())
}

fn preflight_natural_key(
    conn: &rusqlite::Connection,
    table: &str,
    row: &Map<String, Value>,
    id: &str,
) -> Result<()> {
    if table == "events" {
        preflight_natural_columns(conn, table, row, id, &["seq"])?;
        if row.get("dedupe_key").is_some() {
            preflight_natural_columns(conn, table, row, id, &["dedupe_key"])?;
        }
        return Ok(());
    }
    let columns: &[&str] = match table {
        "vcs_workspaces" => &["kind", "repo_fingerprint"],
        "artifacts" => &["blob_hash", "kind"],
        "vcs_changes" => &["vcs_workspace_id", "kind", "change_id"],
        "history_record_links" => &["history_record_id", "target_type", "target_id", "link_type"],
        _ => return Ok(()),
    };
    preflight_natural_columns(conn, table, row, id, columns)
}

fn preflight_natural_columns(
    conn: &rusqlite::Connection,
    table: &str,
    row: &Map<String, Value>,
    id: &str,
    columns: &[&str],
) -> Result<()> {
    let predicates = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{c}=?{}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT id FROM {table} WHERE {predicates} LIMIT 1");
    let values = columns
        .iter()
        .map(|c| sql_value(row.get(*c)))
        .collect::<Result<Vec<_>>>()?;
    let conflict: Option<String> = conn
        .query_row(&sql, params_from_iter(values), |r| r.get(0))
        .optional()?;
    if conflict.as_deref().is_some_and(|other| other != id) {
        return Err(error(format!(
            "natural-key conflict while restoring {table}/{id}"
        )));
    }
    Ok(())
}

fn preflight_session_links(
    conn: &rusqlite::Connection,
    verified: &VerifiedArchive,
    members: &BTreeSet<(String, String)>,
) -> Result<()> {
    let streams = verified.root.dir("streams")?;
    let mut reader = BufReader::new(streams.file(FILES[4])?);
    let mut line = Vec::new();
    loop {
        line.clear();
        if read_capped_line(&mut reader, &mut line)? == 0 {
            break;
        }
        let v: Value = serde_json::from_slice(&line)?;
        let row = v
            .as_object()
            .ok_or_else(|| error("session row malformed"))?;
        let id = row
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| error("session id missing"))?;
        if !members.contains(&("sessions".into(), id.into())) {
            continue;
        }
        let actual: Option<(SqlValue, SqlValue)> = conn
            .query_row(
                "SELECT parent_session_id,root_session_id FROM sessions WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let expected = (
            sql_value(row.get("parent_session_id"))?,
            sql_value(row.get("root_session_id"))?,
        );
        if actual.is_some_and(|a| a != expected) {
            return Err(error(format!(
                "canonical content conflict for sessions/{id}"
            )));
        }
    }
    Ok(())
}

fn preflight_objects(
    verified: &VerifiedArchive,
    target: &Path,
    members: &BTreeSet<(String, String)>,
) -> Result<()> {
    let root = target.join("objects");
    for (_, hash) in members.iter().filter(|(kind, _)| kind == "object_blob") {
        let path = root
            .join(
                hash.get(..2)
                    .ok_or_else(|| error("object hash too short"))?,
            )
            .join(hash);
        if !path.exists() {
            continue;
        }
        let mut file = open_safe(&path, false)?;
        let mut digest = Sha256::new();
        let mut buf = [0; 65536];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            digest.update(&buf[..n]);
        }
        if format!("{:x}", digest.finalize()) != *hash {
            return Err(error("existing content-addressed object conflicts"));
        }
        let source = verified.root.dir("objects")?.dir(&hash[..2])?.file(hash)?;
        drop(source);
    }
    Ok(())
}

fn merge_stream(
    conn: &rusqlite::Connection,
    verified: &VerifiedArchive,
    index: usize,
    table: &str,
    archive_cols: &[&str],
    members: &BTreeSet<(String, String)>,
) -> Result<(u64, u64)> {
    let meta = &verified.manifest.streams[index];
    let streams = verified.root.dir("streams")?;
    let mut r = BufReader::new(streams.file(FILES[index])?);
    let mut line = Vec::new();
    let mut digest = Sha256::new();
    let (mut count, mut bytes) = (0_u64, 0_u64);
    let mut cols = archive_cols.to_vec();
    if table == "history_records" {
        cols.extend(["created_at", "updated_at"]);
    }
    if table == "artifacts" {
        cols.push("blob_path");
    }
    let bind: Vec<_> = if table == "sessions" {
        cols.iter()
            .copied()
            .filter(|c| *c != "parent_session_id" && *c != "root_session_id")
            .collect()
    } else {
        cols.clone()
    };
    let insert = format!(
        "INSERT INTO {table}({}) VALUES({})",
        bind.join(","),
        vec!["?"; bind.len()].join(",")
    );
    let key_where = if table == "history_record_tags" {
        "history_record_id=?1 AND tag_id=?2"
    } else {
        "id=?1"
    };
    let select = format!("SELECT {} FROM {table} WHERE {key_where}", bind.join(","));
    let mut inserted = 0;
    let mut reused = 0;
    loop {
        line.clear();
        let n = read_capped_line(&mut r, &mut line)?;
        if n == 0 {
            break;
        }
        digest.update(&line);
        bytes += n as u64;
        count += 1;
        if bytes > meta.bytes || count > meta.count {
            return Err(error("archive stream exceeds authenticated bounds"));
        }
        let v: Value = serde_json::from_slice(&line)?;
        let o = v
            .as_object()
            .ok_or_else(|| error("stream row is not an object"))?;
        let key = member_key(table, o)?;
        if !members.contains(&(table.to_owned(), key.clone())) {
            continue;
        }
        let values = bind
            .iter()
            .map(|c| value(table, c, o))
            .collect::<Result<Vec<_>>>()?;
        let mut stmt = conn.prepare(&select)?;
        let existing = if table == "history_record_tags" {
            stmt.query_row(
                params![o["history_record_id"].as_str(), o["tag_id"].as_str()],
                |row| {
                    (0..bind.len())
                        .map(|i| row.get(i))
                        .collect::<rusqlite::Result<Vec<SqlValue>>>()
                },
            )
            .optional()?
        } else {
            stmt.query_row([&key], |row| {
                (0..bind.len())
                    .map(|i| row.get(i))
                    .collect::<rusqlite::Result<Vec<SqlValue>>>()
            })
            .optional()?
        };
        if let Some(existing) = existing {
            if existing != values {
                return Err(error(format!(
                    "canonical content conflict for {table}/{key}"
                )));
            }
            reused += 1;
        } else {
            conn.execute(&insert, params_from_iter(values))
                .map_err(|e| {
                    error(format!(
                        "natural-key conflict while restoring {table}/{key}: {e}"
                    ))
                })?;
            conn.execute(
                "INSERT INTO restore_new_rows VALUES(?1,?2)",
                params![table, key],
            )?;
            inserted += 1;
        }
    }
    if count != meta.count
        || bytes != meta.bytes
        || format!("{:x}", digest.finalize()) != meta.sha256
    {
        return Err(error("archive stream changed during restore"));
    }
    Ok((inserted, reused))
}

fn merge_session_links(
    conn: &rusqlite::Connection,
    verified: &VerifiedArchive,
    members: &BTreeSet<(String, String)>,
) -> Result<()> {
    let meta = &verified.manifest.streams[4];
    let streams = verified.root.dir("streams")?;
    let mut r = BufReader::new(streams.file(FILES[4])?);
    let mut line = Vec::new();
    let mut d = Sha256::new();
    let (mut n, mut bytes) = (0_u64, 0_u64);
    loop {
        line.clear();
        let z = read_capped_line(&mut r, &mut line)?;
        if z == 0 {
            break;
        }
        d.update(&line);
        n += 1;
        bytes += z as u64;
        let v: Value = serde_json::from_slice(&line)?;
        let o = v
            .as_object()
            .ok_or_else(|| error("session row malformed"))?;
        let id = o
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| error("session id missing"))?;
        if members.contains(&("sessions".into(), id.into())) {
            let expected = (
                sql_value(o.get("parent_session_id"))?,
                sql_value(o.get("root_session_id"))?,
            );
            let is_new: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM restore_new_rows WHERE kind='sessions' AND key=?1)",
                [id],
                |r| r.get(0),
            )?;
            if is_new {
                conn.execute(
                    "UPDATE sessions SET parent_session_id=?2,root_session_id=?3 WHERE id=?1",
                    params![id, &expected.0, &expected.1],
                )?;
            } else {
                let actual: (SqlValue, SqlValue) = conn.query_row(
                    "SELECT parent_session_id,root_session_id FROM sessions WHERE id=?1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                if actual != expected {
                    return Err(error(format!(
                        "canonical content conflict for sessions/{id}"
                    )));
                }
            }
        }
    }
    if n != meta.count || bytes != meta.bytes || format!("{:x}", d.finalize()) != meta.sha256 {
        return Err(error("sessions changed during restore"));
    }
    Ok(())
}

fn install_selected_objects(
    verified: &VerifiedArchive,
    target: &Path,
    members: &BTreeSet<(String, String)>,
) -> Result<(u64, u64)> {
    let target_dir = AnchoredDir::open_path(target)?;
    let objects_created = mkdirat_private(&target_dir, "objects")?;
    if objects_created {
        target_dir.0.sync_all()?;
    }
    let objects_dir = target_dir.dir("objects")?;
    let mut count = 0;
    let mut total = 0;
    for (kind, hash) in members.iter().filter(|(k, _)| k == "object_blob") {
        let _ = kind;
        let shard = hash
            .get(..2)
            .ok_or_else(|| error("object hash too short"))?;
        let shard_created = mkdirat_private(&objects_dir, shard)?;
        if shard_created {
            objects_dir.0.sync_all()?;
        }
        let shard_dir = objects_dir.dir(shard)?;
        if let Ok(mut f) = shard_dir.file(hash) {
            let (mut d, mut size) = (Sha256::new(), 0_u64);
            let mut b = [0; 65536];
            loop {
                let n = f.read(&mut b)?;
                if n == 0 {
                    break;
                }
                d.update(&b[..n]);
                size += n as u64;
            }
            if format!("{:x}", d.finalize()) != *hash {
                return Err(error("existing content-addressed object conflicts"));
            }
            count += 1;
            total += size;
            continue;
        }
        let srcdir = verified.root.dir("objects")?.dir(shard)?;
        let mut src = srcdir.file(hash)?;
        let tmp = format!(".{hash}.restore-{}", Uuid::new_v4());
        let mut out = private_file_at(&shard_dir, &tmp)?;
        let (mut d, mut size) = (Sha256::new(), 0_u64);
        let mut b = [0; 65536];
        loop {
            let n = src.read(&mut b)?;
            if n == 0 {
                break;
            }
            d.update(&b[..n]);
            out.write_all(&b[..n])?;
            size += n as u64;
        }
        out.sync_all()?;
        selective_failpoint("after_object_sync")?;
        if format!("{:x}", d.finalize()) != *hash {
            return Err(error("archive object changed during restore"));
        }
        drop(out);
        publish_object_exclusive(&shard_dir, &tmp, hash)?;
        selective_failpoint("after_object_publish")?;
        shard_dir.0.sync_all()?;
        selective_failpoint("after_shard_sync")?;
        count += 1;
        total += size;
    }
    Ok((count, total))
}

fn mkdirat_private(parent: &AnchoredDir, name: &str) -> Result<bool> {
    use std::{ffi::CString, os::unix::io::AsRawFd};
    let name = CString::new(name).map_err(|_| error("object path contains NUL"))?;
    let rc = unsafe { libc::mkdirat(parent.0.as_raw_fd(), name.as_ptr(), 0o700) };
    if rc == 0 {
        return Ok(true);
    }
    let e = std::io::Error::last_os_error();
    if e.kind() == std::io::ErrorKind::AlreadyExists {
        parent.dir(name.to_str().unwrap())?;
        Ok(false)
    } else {
        Err(e.into())
    }
}

fn private_file_at(parent: &AnchoredDir, name: &str) -> Result<File> {
    use std::{
        ffi::CString,
        os::unix::io::{AsRawFd, FromRawFd},
    };
    let name = CString::new(name).map_err(|_| error("object path contains NUL"))?;
    let fd = unsafe {
        libc::openat(
            parent.0.as_raw_fd(),
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn publish_object_exclusive(parent: &AnchoredDir, tmp: &str, dst: &str) -> Result<()> {
    use std::{ffi::CString, os::unix::io::AsRawFd};
    let a = CString::new(tmp).unwrap();
    let b = CString::new(dst).unwrap();
    let rc = unsafe {
        libc::linkat(
            parent.0.as_raw_fd(),
            a.as_ptr(),
            parent.0.as_raw_fd(),
            b.as_ptr(),
            0,
        )
    };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            let _ = unsafe { libc::unlinkat(parent.0.as_raw_fd(), a.as_ptr(), 0) };
            let mut existing = parent.file(dst)?;
            let mut d = Sha256::new();
            let mut buf = [0; 65536];
            loop {
                let n = existing.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                d.update(&buf[..n]);
            }
            if format!("{:x}", d.finalize()) == dst {
                return Ok(());
            }
            return Err(error("racing content-addressed object conflicts"));
        }
        return Err(e.into());
    }
    if unsafe { libc::unlinkat(parent.0.as_raw_fd(), a.as_ptr(), 0) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn project_new_rows(conn: &rusqlite::Connection) -> Result<()> {
    let mut records = conn.prepare(
        "SELECT r.id,r.title,r.body,r.tags_json FROM history_records r JOIN restore_new_rows n ON n.kind='history_records' AND n.key=r.id ORDER BY r.id",
    )?;
    let rows = records.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (id, title, body, tags) = row?;
        let tag_text = serde_json::from_str::<Vec<String>>(&tags)
            .unwrap_or_default()
            .join(" ");
        conn.execute("INSERT INTO ctx_history_search(record_id,title,summary,primary_user_text,decision_text,context_text,tag_text) VALUES(?1,?2,?3,?3,'','',?4)",params![id,crate::local_preview(&title,512),crate::local_preview(&body,2048),crate::local_preview(&tag_text,1024)])?;
        let rowid = conn.last_insert_rowid();
        crate::RECORD_SEARCH_ROWID_MAP.store_entry(conn, &id, rowid)?;
    }
    drop(records);
    let mut events=conn.prepare("SELECT e.id,COALESCE(e.history_record_id,r.history_record_id,s.history_record_id,rs.history_record_id),e.session_id,e.role,e.event_type,e.payload_json,e.redaction_state FROM events e JOIN restore_new_rows n ON n.kind='events' AND n.key=e.id LEFT JOIN runs r ON r.id=e.run_id LEFT JOIN sessions s ON s.id=e.session_id LEFT JOIN sessions rs ON rs.id=r.session_id ORDER BY e.occurred_at_ms,e.seq,e.id")?;
    let rows = events.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, String>(6)?,
        ))
    })?;
    for row in rows {
        let (id, record, session, role, event_type, payload, redaction) = row?;
        let preview = crate::event_search_preview(&payload, &redaction)?;
        if preview.trim().is_empty() {
            continue;
        }
        conn.execute("INSERT INTO event_search(event_id,history_record_id,session_id,role,safe_preview_text,rank_bucket) VALUES(?1,?2,?3,?4,?5,?6)",params![id,record,session,role,preview,event_type])?;
        let rowid = conn.last_insert_rowid();
        crate::EVENT_SEARCH_ROWID_MAP.store_entry(conn, &id, rowid)?;
    }
    Ok(())
}

fn load(
    store: &Store,
    verified: &VerifiedArchive,
    index: usize,
    table: &str,
    archive_cols: &[&str],
) -> Result<u64> {
    let s = verified
        .manifest
        .streams
        .get(index)
        .ok_or_else(|| error("authenticated manifest stream missing"))?;
    if s.name != table {
        return Err(error("authenticated manifest stream order mismatch"));
    }
    let streams = verified.root.dir("streams")?;
    let mut r = BufReader::new(streams.file(FILES[index])?);
    let mut line = Vec::new();
    let mut digest = Sha256::new();
    let (mut count, mut bytes) = (0u64, 0u64);
    let mut cols = archive_cols.to_vec();
    if table == "history_records" {
        cols.extend(["created_at", "updated_at"])
    }
    if table == "artifacts" {
        cols.push("blob_path")
    }
    let bind_cols: Vec<_> = if table == "sessions" {
        cols.iter()
            .copied()
            .filter(|c| *c != "parent_session_id" && *c != "root_session_id")
            .collect()
    } else {
        cols.clone()
    };
    let sql = format!(
        "INSERT INTO {table}({}) VALUES ({})",
        bind_cols.join(","),
        vec!["?"; bind_cols.len()].join(",")
    );
    let mut stmt = store.conn.prepare(&sql)?;
    loop {
        line.clear();
        let n = read_capped_line(&mut r, &mut line)?;
        if n == 0 {
            break;
        }
        digest.update(&line);
        bytes += n as u64;
        if bytes > s.bytes || count >= s.count {
            return Err(error("archive stream exceeds authenticated bounds"));
        }
        let v: Value = serde_json::from_slice(&line)?;
        let o = v
            .as_object()
            .ok_or_else(|| error("stream row is not an object"))?;
        let values = bind_cols
            .iter()
            .map(|c| value(table, c, o))
            .collect::<Result<Vec<_>>>()?;
        stmt.execute(params_from_iter(values))?;
        count += 1;
    }
    if count != s.count || bytes != s.bytes || format!("{:x}", digest.finalize()) != s.sha256 {
        return Err(error("archive stream changed after verification"));
    }
    Ok(count)
}
fn value(table: &str, col: &str, o: &Map<String, Value>) -> Result<SqlValue> {
    if table == "history_records" && (col == "created_at" || col == "updated_at") {
        let ms = o
            .get(&format!("{col}_ms"))
            .and_then(Value::as_i64)
            .ok_or_else(|| error("record timestamp missing"))?;
        return Ok(SqlValue::Text(
            Utc.timestamp_millis_opt(ms)
                .single()
                .ok_or_else(|| error("record timestamp invalid"))?
                .to_rfc3339(),
        ));
    }
    if table == "artifacts" && col == "blob_path" {
        let h = o
            .get("blob_hash")
            .and_then(Value::as_str)
            .ok_or_else(|| error("artifact hash missing"))?;
        if h.get(..2).is_none() {
            return Err(error("artifact hash too short"));
        }
        return Ok(SqlValue::Text(object_relative_path(h)));
    }
    sql_value(o.get(col))
}
fn sql_value(v: Option<&Value>) -> Result<SqlValue> {
    Ok(match v {
        None | Some(Value::Null) => SqlValue::Null,
        Some(Value::String(s)) => SqlValue::Text(s.clone()),
        Some(Value::Bool(b)) => SqlValue::Integer(i64::from(*b)),
        Some(Value::Number(n)) => SqlValue::Integer(
            n.as_i64()
                .ok_or_else(|| error("non-integer stream value"))?,
        ),
        _ => return Err(error("unsupported stream value")),
    })
}
fn session_links(store: &Store, verified: &VerifiedArchive) -> Result<()> {
    let s = verified
        .manifest
        .streams
        .get(4)
        .ok_or_else(|| error("authenticated sessions stream missing"))?;
    let streams = verified.root.dir("streams")?;
    let mut r = BufReader::new(streams.file(FILES[4])?);
    let mut line = Vec::new();
    let mut d = Sha256::new();
    let (mut n, mut bytes) = (0, 0);
    loop {
        line.clear();
        let z = read_capped_line(&mut r, &mut line)?;
        if z == 0 {
            break;
        }
        d.update(&line);
        bytes += z as u64;
        if bytes > s.bytes || n >= s.count {
            return Err(error("sessions stream exceeds authenticated bounds"));
        }
        let v: Value = serde_json::from_slice(&line)?;
        let o = v
            .as_object()
            .ok_or_else(|| error("session stream row is not an object"))?;
        store.conn.execute(
            "UPDATE sessions SET parent_session_id=?2,root_session_id=?3 WHERE id=?1",
            params![
                sql_value(o.get("id"))?,
                sql_value(o.get("parent_session_id"))?,
                sql_value(o.get("root_session_id"))?
            ],
        )?;
        n += 1;
    }
    if n != s.count || bytes != s.bytes || format!("{:x}", d.finalize()) != s.sha256 {
        return Err(error("sessions changed during second pass"));
    }
    Ok(())
}

fn install_objects(verified: &VerifiedArchive, stage: &Path) -> Result<()> {
    let root = stage.join("objects");
    mkdir(&root)?;
    let s = verified
        .manifest
        .streams
        .get(3)
        .ok_or_else(|| error("authenticated artifacts stream missing"))?;
    let streams = verified.root.dir("streams")?;
    let mut r = BufReader::new(streams.file(FILES[3])?);
    let mut line = Vec::new();
    let mut objects = BTreeMap::new();
    let mut stream_digest = Sha256::new();
    let (mut stream_count, mut stream_bytes) = (0_u64, 0_u64);
    loop {
        line.clear();
        let read = read_capped_line(&mut r, &mut line)?;
        if read == 0 {
            break;
        }
        stream_digest.update(&line);
        stream_count += 1;
        stream_bytes += read as u64;
        if stream_count > s.count || stream_bytes > s.bytes {
            return Err(error("artifacts stream exceeds authenticated bounds"));
        }
        let v: Value = serde_json::from_slice(&line)?;
        let o = v
            .as_object()
            .ok_or_else(|| error("artifact stream row is not an object"))?;
        let hash = o
            .get("blob_hash")
            .and_then(Value::as_str)
            .ok_or_else(|| error("artifact blob_hash missing"))?;
        let size = o
            .get("byte_size")
            .and_then(Value::as_u64)
            .ok_or_else(|| error("artifact byte_size missing"))?;
        objects.insert(hash.to_owned(), size);
    }
    if stream_count != s.count
        || stream_bytes != s.bytes
        || format!("{:x}", stream_digest.finalize()) != s.sha256
    {
        return Err(error("artifacts changed before object installation"));
    }
    let (mut count, mut total) = (0, 0);
    for (h, size) in objects {
        let shard_name = h.get(..2).ok_or_else(|| error("artifact hash too short"))?;
        let shard = root.join(shard_name);
        if !shard.exists() {
            mkdir(&shard)?
        }
        let objects_root = verified.root.dir("objects")?;
        let shard_root = objects_root.dir(shard_name)?;
        let mut src = shard_root.file(&h)?;
        let mut dst = private_file(&shard.join(&h))?;
        let mut d = Sha256::new();
        let mut buf = [0; 65536];
        let mut bytes = 0;
        loop {
            let n = src.read(&mut buf)?;
            if n == 0 {
                break;
            }
            d.update(&buf[..n]);
            dst.write_all(&buf[..n])?;
            bytes += n as u64;
            if bytes > size {
                return Err(error("archive object exceeds authenticated size"));
            }
        }
        dst.sync_all()?;
        if bytes != size || format!("{:x}", d.finalize()) != h {
            return Err(error("archive object changed after verification"));
        }
        count += 1;
        total += bytes;
    }
    if count != verified.manifest.object_count || total != verified.manifest.object_bytes {
        return Err(error("object totals mismatch"));
    }
    Ok(())
}
fn postcheck(store: &Store, m: &ManifestInfo) -> Result<()> {
    let q: String = store
        .conn
        .query_row("PRAGMA quick_check", [], |r| r.get(0))?;
    if q != "ok" {
        return Err(error("quick_check failed"));
    }
    let(a,b,c,d):(i64,i64,i64,i64)=store.conn.query_row("SELECT (SELECT count(*) FROM ctx_history_search),(SELECT count(*) FROM record_search_rowids),(SELECT count(*) FROM event_search),(SELECT count(*) FROM event_search_rowids)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    if a != b || c != d {
        return Err(error("projection accounting mismatch"));
    }
    let _: i64 = store
        .conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    let mut n = 0;
    for (t, _) in TABLES {
        n += store
            .conn
            .query_row::<i64, _, _>(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))?
            as u64;
    }
    if n != m.entity_count {
        return Err(error("post-restore counts mismatch"));
    }
    Ok(())
}

fn error(s: impl Into<String>) -> StoreError {
    StoreError::Archive(s.into())
}
fn absent(p: &Path) -> Result<()> {
    match fs::symlink_metadata(p) {
        Ok(_) => Err(error(format!(
            "restore target already exists: {}",
            p.display()
        ))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
fn reject_nesting(bundle: &Path, target: &Path) -> Result<()> {
    fn lexical_absolute(path: &Path) -> Result<PathBuf> {
        use std::path::Component;
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let mut out = PathBuf::new();
        for component in absolute.components() {
            match component {
                Component::RootDir => out.push(Path::new("/")),
                Component::CurDir => {}
                Component::ParentDir => {
                    out.pop();
                }
                Component::Normal(name) => out.push(name),
                Component::Prefix(_) => return Err(error("unsupported restore path prefix")),
            }
        }
        Ok(out)
    }
    fn resolved_existing_prefix(path: &Path) -> Result<PathBuf> {
        let mut missing = Vec::new();
        let mut existing = path;
        while fs::symlink_metadata(existing).is_err() {
            missing.push(
                existing
                    .file_name()
                    .ok_or_else(|| error("restore path has no existing prefix"))?
                    .to_os_string(),
            );
            existing = existing
                .parent()
                .ok_or_else(|| error("restore path has no existing prefix"))?;
        }
        let mut resolved = fs::canonicalize(existing)?;
        for component in missing.iter().rev() {
            resolved.push(component);
        }
        Ok(resolved)
    }
    let bundle_lex = lexical_absolute(bundle)?;
    let target_lex = lexical_absolute(target)?;
    let bundle_resolved = resolved_existing_prefix(&bundle_lex)?;
    let target_resolved = resolved_existing_prefix(&target_lex)?;
    if target_lex.starts_with(&bundle_lex)
        || bundle_lex.starts_with(&target_lex)
        || target_resolved.starts_with(&bundle_resolved)
        || bundle_resolved.starts_with(&target_resolved)
    {
        return Err(error(
            "archive bundle and restore target must not contain one another",
        ));
    }
    Ok(())
}
#[cfg(unix)]
fn mkdir(p: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let mut b = fs::DirBuilder::new();
    b.mode(0o700).create(p)?;
    Ok(())
}
#[cfg(unix)]
fn private_file(p: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(p)?)
}
#[cfg(unix)]
fn open_safe(p: &Path, dir: bool) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | if dir { libc::O_DIRECTORY } else { 0 })
        .open(p)?;
    let m = f.metadata()?;
    if (dir && !m.is_dir()) || (!dir && (!m.is_file() || m.nlink() != 1)) {
        return Err(error("archive entry is not a safe regular file"));
    }
    Ok(f)
}
fn sidecars(db: &Path) -> Result<()> {
    for x in ["-wal", "-shm", "-journal"] {
        let p = PathBuf::from(format!("{}{x}", db.display()));
        if p.exists() {
            fs::remove_file(p)?
        }
    }
    Ok(())
}
#[cfg(unix)]
fn publish(stage: &Path, target: &Path, parent: &AnchoredDir) -> Result<()> {
    use std::{
        ffi::CString,
        os::unix::{ffi::OsStrExt, io::AsRawFd},
    };
    let a = CString::new(stage.file_name().unwrap().as_bytes()).unwrap();
    let b = CString::new(target.file_name().unwrap().as_bytes()).unwrap();
    #[cfg(target_os = "macos")]
    let rc = unsafe {
        libc::renameatx_np(
            parent.0.as_raw_fd(),
            a.as_ptr(),
            parent.0.as_raw_fd(),
            b.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    let rc = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.0.as_raw_fd(),
            a.as_ptr(),
            parent.0.as_raw_fd(),
            b.as_ptr(),
            1,
        )
    } as i32;
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    parent.0.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{verify_archive_bundle, ArchiveOptions};
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    fn bundle() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::Builder::new()
            .prefix("ctx-restore-")
            .tempdir()
            .unwrap();
        let mut store = Store::open(temp.path().join("source.sqlite")).unwrap();
        let bundle = temp.path().join("source.ctxar");
        store
            .create_archive(&bundle, ArchiveOptions::default())
            .unwrap();
        (temp, bundle)
    }

    #[test]
    fn injected_post_staging_failure_cleans_target_and_stage_without_touching_bundle() {
        let (temp, bundle) = bundle();
        let target = temp.path().join("restored");
        set_restore_phase_hook(|| Err(error("injected restore failure")));
        assert!(restore_archive_bundle(&bundle, &target, ArchiveVerifyOptions::default()).is_err());
        assert!(!target.exists());
        assert!(!temp.path().join("restored.tmp-residue").exists());
        assert!(!fs::read_dir(temp.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("restored.tmp-")));
        verify_archive_bundle(&bundle).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn staged_child_substitution_aborts_restore_without_publication() {
        for mode in ["symlink", "hardlink", "fifo"] {
            let (temp, bundle) = bundle();
            let target = temp.path().join("restored");
            set_restore_sync_hook(move |stage| {
                let child = stage.join("streams/01-capture_sources.jsonl");
                let replacement = stage.join("COMPLETE");
                fs::remove_file(&child)?;
                match mode {
                    "symlink" => std::os::unix::fs::symlink(&replacement, &child)?,
                    "hardlink" => fs::hard_link(&replacement, &child)?,
                    "fifo" => {
                        let path = CString::new(child.as_os_str().as_bytes()).unwrap();
                        if unsafe { libc::mkfifo(path.as_ptr(), 0o600) } != 0 {
                            return Err(std::io::Error::last_os_error().into());
                        }
                    }
                    _ => unreachable!(),
                }
                Ok(())
            });
            assert!(
                restore_archive_bundle(&bundle, &target, ArchiveVerifyOptions::default()).is_err(),
                "mode {mode}"
            );
            clear_restore_sync_hook();
            assert!(!target.exists(), "mode {mode}");
            assert!(!fs::read_dir(temp.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("restored.tmp-")
            }));
            verify_archive_bundle(&bundle).unwrap();
        }
    }

    #[test]
    fn stream_mutation_between_verify_and_load_is_reauthenticated() {
        let (temp, bundle) = bundle();
        let target = temp.path().join("restored");
        let stream = bundle.join("streams/01-capture_sources.jsonl");
        set_restore_phase_hook(move || {
            OpenOptions::new()
                .append(true)
                .open(stream)?
                .write_all(b"{}\n")?;
            Ok(())
        });
        assert!(restore_archive_bundle(&bundle, &target, ArchiveVerifyOptions::default()).is_err());
        assert!(!target.exists());
        assert!(!fs::read_dir(temp.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("restored.tmp-")));
    }

    #[test]
    fn racing_restores_publish_exactly_one_complete_root() {
        let (temp, bundle) = bundle();
        let target = temp.path().join("winner");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let bundle = bundle.clone();
            let target = target.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                restore_archive_bundle(&bundle, &target, ArchiveVerifyOptions::default())
            }));
        }
        barrier.wait();
        let successes = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(Result::is_ok)
            .count();
        assert_eq!(successes, 1);
        Store::open_read_only(target.join("work.sqlite")).unwrap();
        assert!(!fs::read_dir(temp.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("winner.tmp-")));
        verify_archive_bundle(&bundle).unwrap();
    }

    #[test]
    fn restores_into_tmp_end_to_end() {
        let (_temp, bundle) = bundle();
        let target = Path::new("/tmp").join(format!("ctx-restore-{}", Uuid::new_v4()));
        assert_eq!(target.parent(), Some(Path::new("/tmp")));

        restore_archive_bundle(&bundle, &target, ArchiveVerifyOptions::default()).unwrap();
        Store::open_read_only(target.join("work.sqlite")).unwrap();

        fs::remove_dir_all(target).unwrap();
    }
}
