use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use ctx_history_core::database_path;
use ctx_history_store::Store;
use serde_json::{json, Value};

pub const LOW_SPACE_WARNING_BYTES: u64 = 512 * 1024 * 1024;
pub const LOW_SPACE_CRITICAL_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct StorageSnapshot {
    pub initialized: bool,
    pub data_root: PathBuf,
    pub db_path: PathBuf,
    pub config_path: PathBuf,
    pub counts: StatusCounts,
    pub files: StorageFiles,
    pub sqlite: Option<SqliteStorage>,
    pub available_space_bytes: Option<u64>,
    pub diagnostics: Vec<StorageDiagnostic>,
    pub measurement_complete: bool,
}

#[derive(Debug, Clone)]
pub struct StorageDiagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Optional,
    Warning,
}

#[derive(Debug, Clone, Default)]
pub struct StatusCounts {
    pub items: usize,
    pub sessions: usize,
    pub events: usize,
    pub sources: usize,
    pub catalog_total: usize,
    pub catalog_indexed: usize,
    pub catalog_pending: usize,
    pub catalog_failed: usize,
    pub catalog_stale: usize,
}

#[derive(Debug, Clone, Default)]
pub struct StorageFiles {
    pub main_db_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub objects_bytes: u64,
    pub spool_bytes: u64,
    pub total_data_root_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SqliteStorage {
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_count: u64,
    pub logical_bytes: u64,
    pub freelist_bytes: u64,
    pub live_bytes: u64,
    pub fts_derived_bytes: Option<u64>,
    pub primary_live_bytes: Option<u64>,
}

pub fn snapshot(data_root: &Path, config_file: &str) -> Result<StorageSnapshot> {
    snapshot_with_options(data_root, config_file, false)
}

pub fn snapshot_deep(data_root: &Path, config_file: &str) -> Result<StorageSnapshot> {
    snapshot_with_options(data_root, config_file, true)
}

fn snapshot_with_options(
    data_root: &Path,
    config_file: &str,
    deep: bool,
) -> Result<StorageSnapshot> {
    let db_path = database_path(data_root.to_path_buf());
    let config_path = data_root.join(config_file);
    let mut snap = StorageSnapshot {
        initialized: db_path.exists(),
        data_root: data_root.to_path_buf(),
        db_path: db_path.clone(),
        config_path,
        ..Default::default()
    };
    snap.files.main_db_bytes = file_len(&db_path);
    snap.files.wal_bytes = file_len(db_path.with_extension("sqlite-wal"));
    snap.files.shm_bytes = file_len(db_path.with_extension("sqlite-shm"));
    let objects = data_root.join("objects");
    let spool = data_root.join("spool");
    let mut size_diagnostics = Vec::new();
    let root_size = sized_tree(data_root, &mut size_diagnostics);
    snap.files.total_data_root_bytes = root_size.total_bytes;
    snap.files.objects_bytes = root_size.child_bytes(&objects);
    snap.files.spool_bytes = root_size.child_bytes(&spool);
    snap.measurement_complete = root_size.complete;
    snap.diagnostics.extend(size_diagnostics);
    snap.available_space_bytes = available_space_bytes(data_root)
        .or_else(|| data_root.parent().and_then(available_space_bytes));
    if snap.initialized {
        let store = Store::open_read_only(&db_path).map_err(|_| {
            anyhow!("read-only store is unavailable; run `ctx setup` or `ctx import` to migrate writable storage if needed")
        })?;
        let c = store
            .indexed_history_counts()
            .map_err(|_| anyhow!("could not read index counts"))?;
        snap.counts.items = c.items();
        snap.counts.sessions = c.sessions;
        snap.counts.events = c.events;
        snap.counts.sources = store
            .capture_source_count()
            .map_err(|_| anyhow!("could not read source count"))?;
        let c = store
            .catalog_session_counts()
            .map_err(|_| anyhow!("could not read catalog counts"))?;
        snap.counts.catalog_total = c.total;
        snap.counts.catalog_indexed = c.indexed;
        snap.counts.catalog_pending = c.pending;
        snap.counts.catalog_failed = c.failed;
        snap.counts.catalog_stale = c.stale;
        if deep {
            match sqlite_metrics_read_only(&db_path) {
                Ok(m) => snap.sqlite = Some(m),
                Err(_) => snap
                    .diagnostics
                    .push_warning("sqlite storage metrics unavailable".to_string()),
            }
        }
    }
    if deep {
        unix_permission_diagnostics(&mut snap);
    }
    Ok(snap)
}

pub fn status_json(s: &StorageSnapshot) -> Value {
    let bytes_per_event = if s.counts.events > 0 {
        Some(s.files.total_data_root_bytes / s.counts.events as u64)
    } else {
        None
    };
    json!({
        "schema_version": 1,
        "initialized": s.initialized,
        "data_root": s.data_root,
        "database_path": s.db_path,
        "config_path": s.config_path,
        "indexed_items": s.counts.items,
        "indexed_sessions": s.counts.sessions,
        "indexed_events": s.counts.events,
        "indexed_sources": s.counts.sources,
        "cataloged_sessions": s.counts.catalog_total,
        "indexed_catalog_sessions": s.counts.catalog_indexed,
        "pending_catalog_sessions": s.counts.catalog_pending,
        "failed_catalog_sessions": s.counts.catalog_failed,
        "stale_catalog_sessions": s.counts.catalog_stale,
        "storage": {
            "main_db_bytes": s.files.main_db_bytes,
            "wal_bytes": s.files.wal_bytes,
            "shm_bytes": s.files.shm_bytes,
            "objects_bytes": s.files.objects_bytes,
            "spool_bytes": s.files.spool_bytes,
            "total_data_root_bytes": s.files.total_data_root_bytes,
            "approx_bytes_per_event": bytes_per_event,
            "available_space_bytes": s.available_space_bytes,
            "low_space": low_space(s.available_space_bytes),
            "warnings": warnings(s),
            "measurement_complete": s.measurement_complete,
        },
        "local_only": true,
        "read_only": true,
        "private": true,
        "share_safe": false,
        "diagnostics": diagnostic_messages(&s.diagnostics),
    })
}

pub fn storage_json(s: &StorageSnapshot) -> Value {
    json!({
        "files": storage_files_json(s),
        "sqlite": sqlite_json(s),
        "external_provider_sources": {
            "bytes": null,
            "measured": false,
            "reason": "external_provider_sources_not_measured_read_only",
        },
        "thresholds": {
            "warning_available_bytes": LOW_SPACE_WARNING_BYTES,
            "critical_available_bytes": LOW_SPACE_CRITICAL_BYTES,
        },
        "temporary_space_note": "imports and SQLite maintenance may require additional temporary free space; ctx doctor --storage never checkpoints, vacuums, optimizes, or deletes data",
    })
}

pub fn human_total(s: &StorageSnapshot) -> String {
    format!("storage_total: {} bytes (db {}, wal {}, objects {}, spool {}); run `ctx doctor --storage` for details", s.files.total_data_root_bytes, s.files.main_db_bytes, s.files.wal_bytes, s.files.objects_bytes, s.files.spool_bytes)
}

pub fn warnings(s: &StorageSnapshot) -> Vec<String> {
    match s.available_space_bytes {
        Some(v) if v < LOW_SPACE_CRITICAL_BYTES => vec![format!(
            "critical low free space: {v} bytes available; imports can need temporary space and may fail"
        )],
        Some(v) if v < LOW_SPACE_WARNING_BYTES => vec![format!(
            "low free space: {v} bytes available; imports can need temporary space"
        )],
        _ => Vec::new(),
    }
}

pub fn findings(s: &StorageSnapshot) -> Vec<String> {
    let mut findings = warnings(s);
    findings.extend(
        s.diagnostics
            .iter()
            .filter(|d| d.severity == DiagnosticSeverity::Warning)
            .map(|d| d.message.clone()),
    );
    findings
}

pub fn human_storage_lines(s: &StorageSnapshot) -> Vec<String> {
    let mut lines = vec![human_total(s)];
    if let Some(sqlite) = &s.sqlite {
        lines.push(format!(
            "sqlite: live {} bytes, primary_live {}, fts_derived {}, reclaimable {} bytes",
            sqlite.live_bytes,
            sqlite
                .primary_live_bytes
                .map(|bytes| format!("{bytes} bytes"))
                .unwrap_or_else(|| "unavailable".to_string()),
            sqlite
                .fts_derived_bytes
                .map(|bytes| format!("{bytes} bytes"))
                .unwrap_or_else(|| "unavailable".to_string()),
            sqlite.freelist_bytes
        ));
    } else if s.initialized {
        lines.push("sqlite: unavailable".to_string());
    }
    lines.push(format!(
        "free_space: {} ({})",
        s.available_space_bytes
            .map(|bytes| format!("{bytes} bytes"))
            .unwrap_or_else(|| "unavailable".to_string()),
        low_space(s.available_space_bytes)
    ));
    lines
}

pub fn optional_diagnostic_messages(s: &StorageSnapshot) -> Vec<String> {
    s.diagnostics
        .iter()
        .filter(|d| d.severity == DiagnosticSeverity::Optional)
        .map(|d| d.message.clone())
        .collect()
}
fn low_space(v: Option<u64>) -> &'static str {
    match v {
        Some(x) if x < LOW_SPACE_CRITICAL_BYTES => "critical",
        Some(x) if x < LOW_SPACE_WARNING_BYTES => "warning",
        Some(_) => "ok",
        None => "unknown",
    }
}
fn file_len(path: impl AsRef<Path>) -> u64 {
    fs::symlink_metadata(path)
        .ok()
        .filter(|m| m.file_type().is_file())
        .map(|m| m.len())
        .unwrap_or(0)
}

fn storage_files_json(s: &StorageSnapshot) -> Value {
    let bytes_per_event = if s.counts.events > 0 {
        Some(s.files.total_data_root_bytes / s.counts.events as u64)
    } else {
        None
    };
    json!({
        "main_db_bytes": s.files.main_db_bytes,
        "wal_bytes": s.files.wal_bytes,
        "shm_bytes": s.files.shm_bytes,
        "objects_bytes": s.files.objects_bytes,
        "spool_bytes": s.files.spool_bytes,
        "total_data_root_bytes": s.files.total_data_root_bytes,
        "approx_bytes_per_event": bytes_per_event,
        "available_space_bytes": s.available_space_bytes,
        "low_space": low_space(s.available_space_bytes),
        "warnings": warnings(s),
        "measurement_complete": s.measurement_complete,
    })
}

fn sqlite_json(s: &StorageSnapshot) -> Option<Value> {
    s.sqlite.as_ref().map(|m| {
        json!({
            "logical_bytes": m.logical_bytes,
            "live_bytes": m.live_bytes,
            "primary_live_bytes": m.primary_live_bytes,
            "fts_derived_bytes": m.fts_derived_bytes,
            "fts_derived_bytes_available": m.fts_derived_bytes.is_some(),
            "freelist_reclaimable_bytes": m.freelist_bytes,
            "page_size": m.page_size,
            "page_count": m.page_count,
            "freelist_count": m.freelist_count,
        })
    })
}

pub fn diagnostic_messages_for_snapshot(s: &StorageSnapshot) -> Vec<String> {
    diagnostic_messages(&s.diagnostics)
}

fn diagnostic_messages(diagnostics: &[StorageDiagnostic]) -> Vec<String> {
    diagnostics
        .iter()
        .map(|diagnostic| diagnostic.message.clone())
        .collect()
}

trait DiagnosticSink {
    fn push_warning(&mut self, message: String);
}

impl DiagnosticSink for Vec<StorageDiagnostic> {
    fn push_warning(&mut self, message: String) {
        self.push(StorageDiagnostic {
            severity: DiagnosticSeverity::Warning,
            message,
        });
    }
}

#[derive(Default)]
struct TreeSize {
    total_bytes: u64,
    children: Vec<(PathBuf, u64)>,
    complete: bool,
    omitted: usize,
}

impl TreeSize {
    fn child_bytes(&self, path: &Path) -> u64 {
        self.children
            .iter()
            .find_map(|(child, bytes)| (child == path).then_some(*bytes))
            .unwrap_or(0)
    }
}

fn sized_tree(path: &Path, diags: &mut Vec<StorageDiagnostic>) -> TreeSize {
    let mut tree = TreeSize {
        complete: true,
        ..Default::default()
    };
    let Ok(entries) = fs::read_dir(path) else {
        if path.exists() {
            tree.complete = false;
            push_measurement_diag(
                diags,
                &mut tree.omitted,
                "could not read data-root directory",
            );
        }
        return tree;
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                tree.complete = false;
                push_measurement_diag(diags, &mut tree.omitted, "could not read a data-root entry");
                continue;
            }
        };
        let child_path = entry.path();
        let bytes = dir_size(&child_path, diags, &mut tree.complete, &mut tree.omitted);
        tree.total_bytes = tree.total_bytes.saturating_add(bytes);
        tree.children.push((child_path, bytes));
    }
    if tree.omitted > 0 {
        diags.push_warning(format!("measurement diagnostics omitted: {}", tree.omitted));
    }
    tree
}

fn dir_size(
    path: &Path,
    diags: &mut Vec<StorageDiagnostic>,
    complete: &mut bool,
    omitted: &mut usize,
) -> u64 {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(_) => {
            *complete = false;
            push_measurement_diag(diags, omitted, "could not measure data-root entry");
            return 0;
        }
    };
    if meta.file_type().is_symlink() {
        return 0;
    }
    if meta.is_file() {
        return meta.len();
    }
    let mut total = 0;
    let rd = match fs::read_dir(path) {
        Ok(r) => r,
        Err(_) => {
            *complete = false;
            push_measurement_diag(diags, omitted, "could not read data-root directory entry");
            return 0;
        }
    };
    for entry in rd {
        match entry {
            Ok(entry) => total += dir_size(&entry.path(), diags, complete, omitted),
            Err(_) => {
                *complete = false;
                push_measurement_diag(diags, omitted, "could not read data-root directory entry");
            }
        }
    }
    total
}

fn push_measurement_diag(diags: &mut Vec<StorageDiagnostic>, omitted: &mut usize, message: &str) {
    const CAP: usize = 20;
    if diags.len() < CAP {
        diags.push_warning(message.to_string());
    } else {
        *omitted += 1;
    }
}
fn sqlite_metrics_read_only(path: &Path) -> Result<SqliteStorage> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .context("open sqlite database read-only")?;
    let tx = conn.unchecked_transaction()?;
    let page_size: u64 = tx.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let page_count: u64 = tx.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let freelist_count: u64 = tx.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    if freelist_count > page_count {
        return Err(anyhow!("sqlite freelist exceeds page count"));
    }
    let fts = tx
        .query_row(
            "SELECT COALESCE(sum(pgsize), 0) FROM dbstat WHERE name IN (
                'ctx_history_search', 'ctx_history_search_data', 'ctx_history_search_idx',
                'ctx_history_search_content', 'ctx_history_search_docsize', 'ctx_history_search_config',
                'event_search', 'event_search_data', 'event_search_idx',
                'event_search_content', 'event_search_docsize', 'event_search_config',
                'artifact_search', 'artifact_search_data', 'artifact_search_idx',
                'artifact_search_content', 'artifact_search_docsize', 'artifact_search_config'
            )",
            [],
            |r| r.get(0),
        )
        .ok();
    let live_bytes = page_size * (page_count.saturating_sub(freelist_count));
    let primary_live_bytes = fts.map(|bytes| live_bytes.saturating_sub(bytes));
    let metrics = SqliteStorage {
        page_size,
        page_count,
        freelist_count,
        logical_bytes: page_size * page_count,
        freelist_bytes: page_size * freelist_count,
        live_bytes,
        fts_derived_bytes: fts,
        primary_live_bytes,
    };
    tx.commit()?;
    Ok(metrics)
}

#[cfg(unix)]
fn unix_permission_diagnostics(s: &mut StorageSnapshot) {
    use std::os::unix::fs::PermissionsExt;
    check_mode(
        &s.data_root,
        0o700,
        "insecure data-root directory permissions",
        &mut s.diagnostics,
    );
    for path in [
        &s.db_path,
        &s.db_path.with_extension("sqlite-wal"),
        &s.db_path.with_extension("sqlite-shm"),
        &s.config_path,
    ] {
        if path.exists() {
            check_mode(
                path,
                0o600,
                "insecure ctx storage file permissions",
                &mut s.diagnostics,
            );
        }
    }
    fn check_mode(
        path: &Path,
        expected: u32,
        message: &str,
        diagnostics: &mut Vec<StorageDiagnostic>,
    ) {
        if let Ok(meta) = fs::symlink_metadata(path) {
            if meta.permissions().mode() & 0o777 != expected {
                diagnostics.push_warning(format!("{message}: expected {expected:o}"));
            }
        }
    }
}

#[cfg(not(unix))]
fn unix_permission_diagnostics(_s: &mut StorageSnapshot) {}

#[cfg(unix)]
fn available_space_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    let bytes = (stat.f_bavail as u128).saturating_mul(stat.f_frsize as u128);
    Some(bytes.min(u64::MAX as u128) as u64)
}
#[cfg(not(unix))]
fn available_space_bytes(_path: &Path) -> Option<u64> {
    None
}
