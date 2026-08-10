//! Restore a verified v1 bundle into a fresh data root.
//!
//! Targets are strictly absent (an existing empty directory is rejected): the
//! final exclusive atomic rename cannot safely replace an existing directory.

use crate::{
    archive::{
        read_capped_line, verify_archive_bundle_internal, AnchoredDir, ArchiveVerifyOptions,
        ManifestInfo, VerifiedArchive,
    },
    object_relative_path, rebuild_search_projection, Result, Store, StoreError,
};
use chrono::{TimeZone, Utc};
use rusqlite::{params, params_from_iter, types::Value as SqlValue};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
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
    pub archive_id: Uuid,
    pub source_schema_version: i64,
    pub entity_count: u64,
    pub object_count: u64,
    pub object_bytes: u64,
}
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = fs::remove_dir_all(&self.0);
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

pub fn restore_archive_bundle(
    bundle: &Path,
    target: &Path,
    options: ArchiveVerifyOptions,
) -> Result<ArchiveRestoreReport> {
    let verified = verify_archive_bundle_internal(bundle, options)?; // no destination mutation before full verification
    if verified.manifest.format != "ctx-archive" {
        return Err(crate::StoreError::Archive(
            "selective archive restore is not implemented".into(),
        ));
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
    sync_tree(&stage)?;
    publish(&stage, target, &parent_anchor)?;
    cleanup.0 = PathBuf::new();
    Ok(ArchiveRestoreReport {
        path: target.into(),
        archive_id: verified.manifest.archive_id,
        source_schema_version: verified.manifest.source_schema_version,
        entity_count: verified.report.entity_count,
        object_count: verified.report.object_count,
        object_bytes: verified.report.object_bytes,
    })
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
fn sync_tree(p: &Path) -> Result<()> {
    for e in fs::read_dir(p)? {
        let p = e?.path();
        if p.is_dir() {
            sync_tree(&p)?
        } else {
            open_safe(&p, false)?.sync_all()?
        }
    }
    open_safe(p, true)?.sync_all()?;
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
