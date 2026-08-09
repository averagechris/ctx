//! The v1 logical archive writer.
//!
//! This module intentionally owns the format's encoder rather than using the
//! in-memory `SessionHistoryArchive`.  The latter is an import intermediate
//! and does not contain all of the canonical tables or object bytes.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};

use rusqlite::{types::Value as SqlValue, Connection, OptionalExtension, Row, Transaction};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Result, Store, StoreError};

const STREAMS: [(&str, &str, &str); 15] = [
    ("capture_sources", "01-capture_sources.jsonl", "id"),
    ("vcs_workspaces", "02-vcs_workspaces.jsonl", "id"),
    ("history_records", "03-history_records.jsonl", "id"),
    ("artifacts", "04-artifacts.jsonl", "id"),
    ("sessions", "05-sessions.jsonl", "id"),
    ("session_edges", "06-session_edges.jsonl", "id"),
    ("runs", "07-runs.jsonl", "id"),
    ("events", "08-events.jsonl", "seq"),
    ("vcs_changes", "09-vcs_changes.jsonl", "id"),
    ("summaries", "10-summaries.jsonl", "id"),
    ("files_touched", "11-files_touched.jsonl", "id"),
    ("tags", "12-tags.jsonl", "id"),
    (
        "history_record_tags",
        "13-history_record_tags.jsonl",
        "history_record_id, tag_id",
    ),
    (
        "history_record_links",
        "14-history_record_links.jsonl",
        "id",
    ),
    ("record_edges", "15-record_edges.jsonl", "id"),
];
const PROVIDERS: [&str; 19] = [
    "codex",
    "claude",
    "pi",
    "opencode",
    "antigravity",
    "gemini",
    "cursor",
    "copilot_cli",
    "factory_ai_droid",
    "openclaw",
    "hermes",
    "nanoclaw",
    "astrbot",
    "shell",
    "git",
    "jj",
    "gh",
    "custom",
    "unknown",
];

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_COMPLETE_BYTES: usize = 4 * 1024;
const EXPORT_PAGE_ROWS: usize = 256;

#[cfg(unix)]
struct AnchoredDir(File);

#[cfg(unix)]
struct DirStream(*mut libc::DIR);

#[cfg(unix)]
impl Drop for DirStream {
    fn drop(&mut self) {
        unsafe { libc::closedir(self.0) };
    }
}

#[cfg(all(unix, target_os = "macos"))]
unsafe fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

#[cfg(all(unix, not(target_os = "macos")))]
unsafe fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
fn normalize_macos_trusted_root_alias(path: &Path) -> PathBuf {
    for (public, private) in [
        (Path::new("/tmp"), Path::new("/private/tmp")),
        (Path::new("/var"), Path::new("/private/var")),
    ] {
        if path == public {
            return private.to_path_buf();
        }
        if let Ok(suffix) = path.strip_prefix(public) {
            return private.join(suffix);
        }
    }
    path.to_path_buf()
}

#[cfg(unix)]
impl AnchoredDir {
    fn open_path(path: &Path) -> Result<Self> {
        use std::path::Component;
        #[cfg(target_os = "macos")]
        let normalized = normalize_macos_trusted_root_alias(path);
        #[cfg(target_os = "macos")]
        let path = normalized.as_path();
        let start = if path.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        };
        let mut dir = open_read_nofollow(start, true)?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let mut normal_component = 0;
        for component in path.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(name) => {
                    #[cfg(any(target_os = "linux", target_os = "macos"))]
                    let next = if is_trusted_tmp_component(path, normal_component) {
                        openat_trusted_tmp(&dir, name)?
                    } else {
                        openat(&dir, name, true)?
                    };
                    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
                    let next = openat(&dir, name, true)?;
                    dir = next;
                    #[cfg(any(target_os = "linux", target_os = "macos"))]
                    {
                        normal_component += 1;
                    }
                }
                Component::ParentDir | Component::Prefix(_) => {
                    return Err(archive_error(
                        "anchored path contains an unsupported component",
                    ));
                }
            }
        }
        Ok(Self(dir))
    }

    fn dir(&self, name: &str) -> Result<Self> {
        Ok(Self(openat(&self.0, std::ffi::OsStr::new(name), true)?))
    }

    fn file(&self, name: &str) -> Result<File> {
        openat(&self.0, std::ffi::OsStr::new(name), false)
    }

    fn for_each_name(&self, mut visit: impl FnMut(&str) -> Result<()>) -> Result<()> {
        use std::ffi::CStr;
        let duplicate = unsafe { libc::dup(self.0.as_raw_fd()) };
        if duplicate < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            unsafe { libc::close(duplicate) };
            return Err(std::io::Error::last_os_error().into());
        }
        let stream = DirStream(stream);
        loop {
            unsafe { *errno_location() = 0 };
            let entry = unsafe { libc::readdir(stream.0) };
            if entry.is_null() {
                let errno = unsafe { *errno_location() };
                if errno != 0 {
                    return Err(std::io::Error::from_raw_os_error(errno).into());
                }
                break;
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            let name = name
                .to_str()
                .map_err(|_| archive_error("bundle entry name is not UTF-8"))?;
            if name != "." && name != ".." {
                visit(name)?;
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn openat(parent: &File, name: &std::ffi::OsStr, directory: bool) -> Result<File> {
    openat_checked(parent, name, directory, false)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn is_trusted_tmp_component(path: &Path, normal_component: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        path.starts_with("/tmp") && normal_component == 0
    }
    #[cfg(target_os = "macos")]
    {
        path.starts_with("/private/tmp") && normal_component == 1
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn openat_trusted_tmp(parent: &File, name: &std::ffi::OsStr) -> Result<File> {
    openat_checked(parent, name, true, true)
}

#[cfg(unix)]
fn openat_checked(
    parent: &File,
    name: &std::ffi::OsStr,
    directory: bool,
    allow_sticky_tmp: bool,
) -> Result<File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let name =
        CString::new(name.as_bytes()).map_err(|_| archive_error("path component contains NUL"))?;
    let flags = libc::O_RDONLY
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | if directory { libc::O_DIRECTORY } else { 0 };
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    validate_open_file(&file, directory, allow_sticky_tmp)?;
    Ok(file)
}

#[cfg(unix)]
fn validate_open_file(file: &File, directory: bool, allow_sticky_tmp: bool) -> Result<()> {
    let metadata = file.metadata()?;
    if directory {
        if !metadata.is_dir() {
            return Err(archive_error("expected a directory descriptor"));
        }
    } else if !metadata.is_file() || metadata.nlink() > 1 {
        return Err(archive_error("expected a regular non-hard-linked file"));
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let _ = allow_sticky_tmp;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if allow_sticky_tmp {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o7777 != 0o1777 {
            return Err(archive_error(
                "trusted temporary root has an unexpected mode",
            ));
        }
        return Ok(());
    }
    check_mode(&metadata)
}

/// Parameters that are normally generated by the writer.  Keeping these
/// injectable makes deterministic export tests possible without making the
/// command-line format less safe (the CLI always uses fresh values).
#[derive(Debug, Clone)]
pub struct ArchiveOptions {
    pub archive_id: Option<Uuid>,
    pub created_at_ms: Option<i64>,
    pub generator_version: String,
}

impl Default for ArchiveOptions {
    fn default() -> Self {
        Self {
            archive_id: None,
            created_at_ms: None,
            generator_version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveStreamReport {
    pub name: String,
    pub path: String,
    pub count: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct ArchiveReport {
    pub archive_id: Uuid,
    pub created_at_ms: i64,
    pub path: PathBuf,
    pub streams: Vec<ArchiveStreamReport>,
    pub object_count: u64,
    pub object_bytes: u64,
    pub entity_count: u64,
}

impl Store {
    /// Stream a complete v1 archive to an absent target and publish it only
    /// after checking the generated files and integrity chain.
    pub fn create_archive(
        &mut self,
        target: impl AsRef<Path>,
        options: ArchiveOptions,
    ) -> Result<ArchiveReport> {
        let target = target.as_ref().to_path_buf();
        let parent = target
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        ensure_directory(&parent, "archive parent")?;
        reject_existing_target(&target)?;

        let archive_id = options.archive_id.unwrap_or_else(Uuid::now_v7);
        let created_at_ms = options
            .created_at_ms
            .unwrap_or_else(|| crate::utc_now().timestamp_millis());
        let stage = parent.join(format!(
            "{}.tmp-{}",
            target
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| archive_error("archive target must have a UTF-8 file name"))?,
            archive_id
        ));
        reject_existing_target(&stage)?;
        create_private_dir(&stage)?;

        let result = self.create_archive_staged(
            &target,
            &stage,
            archive_id,
            created_at_ms,
            &options.generator_version,
        );
        if result.is_err() {
            let _ = fs::remove_dir_all(&stage);
        }
        result
    }

    fn create_archive_staged(
        &mut self,
        target: &Path,
        stage: &Path,
        archive_id: Uuid,
        created_at_ms: i64,
        generator_version: &str,
    ) -> Result<ArchiveReport> {
        let streams_dir = stage.join("streams");
        let objects_dir = stage.join("objects");
        create_private_dir(&streams_dir)?;
        create_private_dir(&objects_dir)?;

        let source_objects = self.object_dir.clone();
        let tx = self.conn.transaction()?;
        let source_schema_version: i64 =
            tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        let origin_device_id: Option<String> = tx.query_row(
            "SELECT CASE WHEN COUNT(*) = 1 THEN MIN(stable_device_id) END FROM local_devices",
            [],
            |row| row.get(0),
        )?;

        let mut stream_reports = Vec::with_capacity(STREAMS.len());

        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[0],
            "SELECT id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json FROM capture_sources ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("kind", row.get(1)?);
                o.str_field("provider", row.get(2)?);
                o.str_field("machine_id", row.get(3)?);
                let process_id: Option<i64> = row.get(4)?;
                if process_id.is_some_and(|value| u32::try_from(value).is_err()) {
                    return Err(archive_error("capture_sources.process_id is outside u32 range"));
                }
                o.opt_i64_field("process_id", process_id);
                o.opt_str_field("cwd", row.get(5)?);
                o.opt_str_field("raw_source_path", row.get(6)?);
                o.opt_str_field("external_session_id", row.get(7)?);
                o.i64_field("started_at_ms", row.get(8)?);
                o.opt_i64_field("ended_at_ms", row.get(9)?);
                o.str_field("fidelity", row.get(10)?);
                o.str_field("visibility", row.get(11)?);
                o.str_field("sync_state", row.get(12)?);
                o.i64_field("sync_version", row.get(13)?);
                o.json_str_field("metadata_json", row.get(14)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[1],
            "SELECT id, kind, root_path, repo_fingerprint, primary_remote_url_normalized, host, owner, name, monorepo_subpath, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM vcs_workspaces ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("kind", row.get(1)?);
                o.str_field("root_path", row.get(2)?);
                o.str_field("repo_fingerprint", row.get(3)?);
                o.opt_str_field("primary_remote_url_normalized", row.get(4)?);
                o.str_field("host", row.get(5)?);
                o.opt_str_field("owner", row.get(6)?);
                o.opt_str_field("name", row.get(7)?);
                o.opt_str_field("monorepo_subpath", row.get(8)?);
                o.i64_field("created_at_ms", row.get(9)?);
                o.i64_field("updated_at_ms", row.get(10)?);
                o.opt_str_field("source_id", row.get(11)?);
                o.str_field("visibility", row.get(12)?);
                o.str_field("fidelity", row.get(13)?);
                o.str_field("sync_state", row.get(14)?);
                o.i64_field("sync_version", row.get(15)?);
                o.opt_i64_field("deleted_at_ms", row.get(16)?);
                o.json_str_field("metadata_json", row.get(17)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[2],
            "SELECT id, title, summary, status, primary_vcs_workspace_id, started_at_ms, last_activity_at_ms, completed_at_ms, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json, body, tags_json, kind, workspace FROM history_records ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("title", row.get(1)?);
                o.opt_str_field("summary", row.get(2)?);
                o.str_field("status", row.get(3)?);
                o.opt_str_field("primary_vcs_workspace_id", row.get(4)?);
                o.opt_i64_field("started_at_ms", row.get(5)?);
                o.i64_field("last_activity_at_ms", row.get(6)?);
                o.opt_i64_field("completed_at_ms", row.get(7)?);
                o.str_field("confidence", row.get(8)?);
                o.i64_field("created_at_ms", row.get(9)?);
                o.i64_field("updated_at_ms", row.get(10)?);
                o.opt_str_field("source_id", row.get(11)?);
                o.str_field("visibility", row.get(12)?);
                o.str_field("fidelity", row.get(13)?);
                o.str_field("sync_state", row.get(14)?);
                o.i64_field("sync_version", row.get(15)?);
                o.opt_i64_field("deleted_at_ms", row.get(16)?);
                o.json_str_field("metadata_json", row.get(17)?);
                o.str_field("body", row.get(18)?);
                o.json_str_field("tags_json", row.get(19)?);
                o.str_field("kind", row.get(20)?);
                o.opt_str_field("workspace", row.get(21)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[3],
            "SELECT id, kind, blob_hash, byte_size, media_type, preview_text, redaction_state, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM artifacts ORDER BY id",
            |row| {
                let hash: String = row.get(2)?;
                let size: i64 = row.get(3)?;
                if size < 0 || !is_sha256_hex(&hash) {
                    return Err(archive_error("artifact has an invalid blob hash or size"));
                }
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("kind", row.get(1)?);
                o.str_field("blob_hash", hash);
                o.i64_field("byte_size", size);
                o.opt_str_field("media_type", row.get(4)?);
                o.opt_str_field("preview_text", row.get(5)?);
                o.str_field("redaction_state", row.get(6)?);
                o.i64_field("created_at_ms", row.get(7)?);
                o.i64_field("updated_at_ms", row.get(8)?);
                o.opt_str_field("source_id", row.get(9)?);
                o.str_field("visibility", row.get(10)?);
                o.str_field("fidelity", row.get(11)?);
                o.str_field("sync_state", row.get(12)?);
                o.i64_field("sync_version", row.get(13)?);
                o.opt_i64_field("deleted_at_ms", row.get(14)?);
                o.json_str_field("metadata_json", row.get(15)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[4],
            "SELECT id, history_record_id, parent_session_id, root_session_id, capture_source_id, provider, external_session_id, external_agent_id, agent_type, role_hint, is_primary, status, fidelity, transcript_blob_id, started_at_ms, ended_at_ms, created_at_ms, updated_at_ms, visibility, sync_state, sync_version, deleted_at_ms, metadata_json FROM sessions ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.opt_str_field("history_record_id", row.get(1)?);
                o.opt_str_field("parent_session_id", row.get(2)?);
                o.opt_str_field("root_session_id", row.get(3)?);
                o.opt_str_field("capture_source_id", row.get(4)?);
                let provider: String = row.get(5)?;
                if !PROVIDERS.contains(&provider.as_str()) {
                    return Err(archive_error("session provider is outside the v1 vocabulary"));
                }
                o.str_field("provider", provider);
                o.opt_str_field("external_session_id", row.get(6)?);
                o.opt_str_field("external_agent_id", row.get(7)?);
                o.str_field("agent_type", row.get(8)?);
                o.opt_str_field("role_hint", row.get(9)?);
                let is_primary: i64 = row.get(10)?;
                if !matches!(is_primary, 0 | 1) {
                    return Err(archive_error("sessions.is_primary is outside 0/1"));
                }
                o.bool_field("is_primary", is_primary == 1);
                o.str_field("status", row.get(11)?);
                o.str_field("fidelity", row.get(12)?);
                o.opt_str_field("transcript_blob_id", row.get(13)?);
                o.i64_field("started_at_ms", row.get(14)?);
                o.opt_i64_field("ended_at_ms", row.get(15)?);
                o.i64_field("created_at_ms", row.get(16)?);
                o.i64_field("updated_at_ms", row.get(17)?);
                o.str_field("visibility", row.get(18)?);
                o.str_field("sync_state", row.get(19)?);
                o.i64_field("sync_version", row.get(20)?);
                o.opt_i64_field("deleted_at_ms", row.get(21)?);
                o.json_str_field("metadata_json", row.get(22)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[5],
            "SELECT id, from_session_id, to_session_id, edge_type, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM session_edges ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("from_session_id", row.get(1)?);
                o.str_field("to_session_id", row.get(2)?);
                o.str_field("edge_type", row.get(3)?);
                o.str_field("confidence", row.get(4)?);
                o.i64_field("created_at_ms", row.get(5)?);
                o.i64_field("updated_at_ms", row.get(6)?);
                o.opt_str_field("source_id", row.get(7)?);
                o.str_field("visibility", row.get(8)?);
                o.str_field("fidelity", row.get(9)?);
                o.str_field("sync_state", row.get(10)?);
                o.i64_field("sync_version", row.get(11)?);
                o.opt_i64_field("deleted_at_ms", row.get(12)?);
                o.json_str_field("metadata_json", row.get(13)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[6],
            "SELECT id, history_record_id, session_id, run_type, status, started_at_ms, ended_at_ms, exit_code, cwd, command_preview, input_blob_id, output_blob_id, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM runs ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.opt_str_field("history_record_id", row.get(1)?);
                o.opt_str_field("session_id", row.get(2)?);
                o.str_field("run_type", row.get(3)?);
                o.str_field("status", row.get(4)?);
                o.i64_field("started_at_ms", row.get(5)?);
                o.opt_i64_field("ended_at_ms", row.get(6)?);
                let exit_code: Option<i64> = row.get(7)?;
                if exit_code.is_some_and(|value| i32::try_from(value).is_err()) {
                    return Err(archive_error("runs.exit_code is outside i32 range"));
                }
                o.opt_i64_field("exit_code", exit_code);
                o.opt_str_field("cwd", row.get(8)?);
                o.opt_str_field("command_preview", row.get(9)?);
                o.opt_str_field("input_blob_id", row.get(10)?);
                o.opt_str_field("output_blob_id", row.get(11)?);
                o.i64_field("created_at_ms", row.get(12)?);
                o.i64_field("updated_at_ms", row.get(13)?);
                o.opt_str_field("source_id", row.get(14)?);
                o.str_field("visibility", row.get(15)?);
                o.str_field("fidelity", row.get(16)?);
                o.str_field("sync_state", row.get(17)?);
                o.i64_field("sync_version", row.get(18)?);
                o.opt_i64_field("deleted_at_ms", row.get(19)?);
                o.json_str_field("metadata_json", row.get(20)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[7],
            "SELECT id, seq, history_record_id, session_id, run_id, event_type, role, occurred_at_ms, capture_source_id, payload_json, payload_blob_id, dedupe_key, visibility, redaction_state, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM events ORDER BY seq",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.i64_field("seq", row.get(1)?);
                o.opt_str_field("history_record_id", row.get(2)?);
                o.opt_str_field("session_id", row.get(3)?);
                o.opt_str_field("run_id", row.get(4)?);
                o.str_field("event_type", row.get(5)?);
                o.opt_str_field("role", row.get(6)?);
                o.i64_field("occurred_at_ms", row.get(7)?);
                o.opt_str_field("capture_source_id", row.get(8)?);
                o.json_str_field("payload_json", row.get(9)?);
                o.opt_str_field("payload_blob_id", row.get(10)?);
                o.opt_str_field("dedupe_key", row.get(11)?);
                o.str_field("visibility", row.get(12)?);
                o.str_field("redaction_state", row.get(13)?);
                o.str_field("fidelity", row.get(14)?);
                o.str_field("sync_state", row.get(15)?);
                o.i64_field("sync_version", row.get(16)?);
                o.opt_i64_field("deleted_at_ms", row.get(17)?);
                o.json_str_field("metadata_json", row.get(18)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[8],
            "SELECT id, vcs_workspace_id, kind, change_id, parent_change_ids_json, branch_or_bookmark, tree_hash, author_time_ms, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM vcs_changes ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("vcs_workspace_id", row.get(1)?);
                o.str_field("kind", row.get(2)?);
                o.str_field("change_id", row.get(3)?);
                o.json_str_field("parent_change_ids_json", row.get(4)?);
                o.opt_str_field("branch_or_bookmark", row.get(5)?);
                o.opt_str_field("tree_hash", row.get(6)?);
                o.opt_i64_field("author_time_ms", row.get(7)?);
                o.str_field("confidence", row.get(8)?);
                o.i64_field("created_at_ms", row.get(9)?);
                o.i64_field("updated_at_ms", row.get(10)?);
                o.opt_str_field("source_id", row.get(11)?);
                o.str_field("visibility", row.get(12)?);
                o.str_field("fidelity", row.get(13)?);
                o.str_field("sync_state", row.get(14)?);
                o.i64_field("sync_version", row.get(15)?);
                o.opt_i64_field("deleted_at_ms", row.get(16)?);
                o.json_str_field("metadata_json", row.get(17)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[9],
            "SELECT id, history_record_id, session_id, kind, model_or_source, text, citations_json, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM summaries ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.opt_str_field("history_record_id", row.get(1)?);
                o.opt_str_field("session_id", row.get(2)?);
                o.str_field("kind", row.get(3)?);
                o.opt_str_field("model_or_source", row.get(4)?);
                o.str_field("text", row.get(5)?);
                o.json_str_field("citations_json", row.get(6)?);
                o.i64_field("created_at_ms", row.get(7)?);
                o.i64_field("updated_at_ms", row.get(8)?);
                o.opt_str_field("source_id", row.get(9)?);
                o.str_field("visibility", row.get(10)?);
                o.str_field("fidelity", row.get(11)?);
                o.str_field("sync_state", row.get(12)?);
                o.i64_field("sync_version", row.get(13)?);
                o.opt_i64_field("deleted_at_ms", row.get(14)?);
                o.json_str_field("metadata_json", row.get(15)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[10],
            "SELECT id, history_record_id, run_id, event_id, vcs_workspace_id, path, change_kind, old_path, line_count_delta, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM files_touched ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.opt_str_field("history_record_id", row.get(1)?);
                o.opt_str_field("run_id", row.get(2)?);
                o.opt_str_field("event_id", row.get(3)?);
                o.opt_str_field("vcs_workspace_id", row.get(4)?);
                o.str_field("path", row.get(5)?);
                o.opt_str_field("change_kind", row.get(6)?);
                o.opt_str_field("old_path", row.get(7)?);
                o.opt_i64_field("line_count_delta", row.get(8)?);
                o.str_field("confidence", row.get(9)?);
                o.i64_field("created_at_ms", row.get(10)?);
                o.i64_field("updated_at_ms", row.get(11)?);
                o.opt_str_field("source_id", row.get(12)?);
                o.str_field("visibility", row.get(13)?);
                o.str_field("fidelity", row.get(14)?);
                o.str_field("sync_state", row.get(15)?);
                o.i64_field("sync_version", row.get(16)?);
                o.opt_i64_field("deleted_at_ms", row.get(17)?);
                o.json_str_field("metadata_json", row.get(18)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[11],
            "SELECT id, name, kind, created_at_ms, updated_at_ms, metadata_json FROM tags ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("name", row.get(1)?);
                o.str_field("kind", row.get(2)?);
                o.i64_field("created_at_ms", row.get(3)?);
                o.i64_field("updated_at_ms", row.get(4)?);
                o.json_str_field("metadata_json", row.get(5)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[12],
            "SELECT history_record_id, tag_id, source_id, confidence, created_at_ms FROM history_record_tags ORDER BY history_record_id, tag_id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("history_record_id", row.get(0)?);
                o.str_field("tag_id", row.get(1)?);
                o.opt_str_field("source_id", row.get(2)?);
                o.str_field("confidence", row.get(3)?);
                o.i64_field("created_at_ms", row.get(4)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[13],
            "SELECT id, history_record_id, target_type, target_id, link_type, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM history_record_links ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("history_record_id", row.get(1)?);
                o.str_field("target_type", row.get(2)?);
                o.str_field("target_id", row.get(3)?);
                o.str_field("link_type", row.get(4)?);
                o.str_field("confidence", row.get(5)?);
                o.i64_field("created_at_ms", row.get(6)?);
                o.i64_field("updated_at_ms", row.get(7)?);
                o.opt_str_field("source_id", row.get(8)?);
                o.str_field("visibility", row.get(9)?);
                o.str_field("fidelity", row.get(10)?);
                o.str_field("sync_state", row.get(11)?);
                o.i64_field("sync_version", row.get(12)?);
                o.opt_i64_field("deleted_at_ms", row.get(13)?);
                o.json_str_field("metadata_json", row.get(14)?);
                Ok(o.finish())
            },
        )?);
        stream_reports.push(write_stream(
            &tx,
            &streams_dir,
            STREAMS[14],
            "SELECT id, from_record_id, to_record_id, edge_type, confidence, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json FROM record_edges ORDER BY id",
            |row| {
                let mut o = JsonWriter::new();
                o.str_field("id", row.get(0)?);
                o.str_field("from_record_id", row.get(1)?);
                o.str_field("to_record_id", row.get(2)?);
                o.str_field("edge_type", row.get(3)?);
                o.str_field("confidence", row.get(4)?);
                o.i64_field("created_at_ms", row.get(5)?);
                o.i64_field("updated_at_ms", row.get(6)?);
                o.opt_str_field("source_id", row.get(7)?);
                o.str_field("visibility", row.get(8)?);
                o.str_field("fidelity", row.get(9)?);
                o.str_field("sync_state", row.get(10)?);
                o.i64_field("sync_version", row.get(11)?);
                o.opt_i64_field("deleted_at_ms", row.get(12)?);
                o.json_str_field("metadata_json", row.get(13)?);
                Ok(o.finish())
            },
        )?);
        let object_report = copy_objects(&tx, &source_objects, &objects_dir)?;
        // Keep the SQLite snapshot open until both rows and the referenced
        // bytes have been captured. Blob files are outside SQLite's
        // transaction, so copy_objects re-hashes every chunk and aborts on a
        // concurrent replacement.
        tx.commit()?;
        let entity_count = stream_reports.iter().try_fold(0_u64, |sum, report| {
            sum.checked_add(report.count)
                .ok_or_else(|| archive_error("entity count overflow"))
        })?;
        let manifest = manifest_bytes(ManifestInput {
            archive_id,
            created_at_ms,
            generator_version,
            source_schema_version,
            origin_device_id: origin_device_id.as_deref(),
            streams: &stream_reports,
            object_count: object_report.0,
            object_bytes: object_report.1,
            entity_count,
        });
        if manifest.len() > MAX_MANIFEST_BYTES {
            return Err(archive_error("archive manifest exceeds the 16 MiB limit"));
        }
        write_private_file(&stage.join("manifest.json"), &manifest)?;
        let marker = completion_bytes(&manifest);
        if marker.len() > MAX_COMPLETE_BYTES {
            return Err(archive_error(
                "archive completion marker exceeds the 4 KiB limit",
            ));
        }
        write_private_file(&stage.join("COMPLETE"), &marker)?;
        self_verify_staged(stage, &manifest, &stream_reports)
            .map_err(|error| archive_error(format!("self-verification failed: {error}")))?;
        verify_completion(stage, &manifest, &marker)
            .map_err(|error| archive_error(format!("completion verification failed: {error}")))?;
        sync_tree(stage)
            .map_err(|error| archive_error(format!("staging fsync failed: {error}")))?;
        atomic_publish(stage, target)
            .map_err(|error| archive_error(format!("atomic publication failed: {error}")))?;

        Ok(ArchiveReport {
            archive_id,
            created_at_ms,
            path: target.to_path_buf(),
            streams: stream_reports,
            object_count: object_report.0,
            object_bytes: object_report.1,
            entity_count,
        })
    }
}

fn write_stream<F>(
    tx: &Transaction<'_>,
    streams_dir: &Path,
    descriptor: (&str, &str, &str),
    sql: &str,
    mut encode: F,
) -> Result<ArchiveStreamReport>
where
    F: FnMut(&Row<'_>) -> Result<String>,
{
    let path = streams_dir.join(descriptor.1);
    let mut file = private_open(&path)?;
    let mut hasher = Sha256::new();
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    let (select, order) = sql
        .split_once(" ORDER BY")
        .ok_or_else(|| archive_error("archive stream query lacks an ordering clause"))?;
    let mut last_text: Option<String> = None;
    let mut last_number: Option<i64> = None;
    let mut last_pair: Option<(String, String)> = None;
    loop {
        let (predicate, bindings) = match descriptor.2 {
            "id" => match &last_text {
                Some(last) => (" WHERE id > ?1", vec![SqlValue::Text(last.clone())]),
                None => ("", Vec::new()),
            },
            "seq" => match last_number {
                Some(last) => (" WHERE seq > ?1", vec![SqlValue::Integer(last)]),
                None => ("", Vec::new()),
            },
            "history_record_id, tag_id" => match &last_pair {
                Some((record, tag)) => (
                    " WHERE history_record_id > ?1 OR (history_record_id = ?1 AND tag_id > ?2)",
                    vec![SqlValue::Text(record.clone()), SqlValue::Text(tag.clone())],
                ),
                None => ("", Vec::new()),
            },
            _ => return Err(archive_error("unsupported archive sort key")),
        };
        let page_sql = format!("{select}{predicate} ORDER BY{order} LIMIT {EXPORT_PAGE_ROWS}");
        let mut statement = tx.prepare(&page_sql)?;
        let mut rows = statement.query(rusqlite::params_from_iter(bindings))?;
        let mut page_count = 0_u64;
        while let Some(row) = rows.next()? {
            let record = encode(row)?;
            let record_bytes = record.as_bytes();
            if record_bytes.len() > MAX_JSONL_LINE_BYTES {
                return Err(archive_error(
                    "archive JSONL record exceeds the 32 MiB limit",
                ));
            }
            file.write_all(record_bytes)?;
            file.write_all(b"\n")?;
            hasher.update(record_bytes);
            hasher.update(b"\n");
            count = count
                .checked_add(1)
                .ok_or_else(|| archive_error("stream count overflow"))?;
            page_count = page_count
                .checked_add(1)
                .ok_or_else(|| archive_error("stream page count overflow"))?;
            bytes = bytes
                .checked_add(record_bytes.len() as u64 + 1)
                .ok_or_else(|| archive_error("stream byte count overflow"))?;
            match descriptor.2 {
                "id" => last_text = Some(row.get(0)?),
                "seq" => last_number = Some(row.get(1)?),
                "history_record_id, tag_id" => {
                    last_pair = Some((row.get(0)?, row.get(1)?));
                }
                _ => unreachable!(),
            }
        }
        if page_count == 0 {
            break;
        }
    }
    file.sync_all()?;
    Ok(ArchiveStreamReport {
        name: descriptor.0.to_owned(),
        path: format!("streams/{}", descriptor.1),
        count,
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

struct JsonWriter {
    out: String,
    first: bool,
}

impl JsonWriter {
    fn new() -> Self {
        Self {
            out: String::from("{"),
            first: true,
        }
    }

    fn field(&mut self, name: &str, value: String) {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
        self.out
            .push_str(&serde_json::to_string(name).expect("field names are valid"));
        self.out.push(':');
        self.out.push_str(&value);
    }

    fn str_field(&mut self, name: &str, value: String) {
        self.field(
            name,
            serde_json::to_string(&value).expect("database text is valid UTF-8"),
        );
    }

    fn opt_str_field(&mut self, name: &str, value: Option<String>) {
        if let Some(value) = value {
            self.str_field(name, value);
        }
    }

    fn json_str_field(&mut self, name: &str, value: String) {
        self.str_field(name, value);
    }

    fn i64_field(&mut self, name: &str, value: i64) {
        self.field(name, value.to_string());
    }

    fn opt_i64_field(&mut self, name: &str, value: Option<i64>) {
        if let Some(value) = value {
            self.i64_field(name, value);
        }
    }

    fn bool_field(&mut self, name: &str, value: bool) {
        self.field(name, value.to_string());
    }

    fn finish(mut self) -> String {
        self.out.push('}');
        self.out
    }
}

struct ManifestInput<'a> {
    archive_id: Uuid,
    created_at_ms: i64,
    generator_version: &'a str,
    source_schema_version: i64,
    origin_device_id: Option<&'a str>,
    streams: &'a [ArchiveStreamReport],
    object_count: u64,
    object_bytes: u64,
    entity_count: u64,
}

fn manifest_bytes(input: ManifestInput<'_>) -> Vec<u8> {
    let ManifestInput {
        archive_id,
        created_at_ms,
        generator_version,
        source_schema_version,
        origin_device_id,
        streams,
        object_count,
        object_bytes,
        entity_count,
    } = input;
    let mut o = JsonWriter::new();
    o.str_field("format", "ctx-archive".to_owned());
    o.i64_field("format_version", 1);
    o.str_field("archive_id", archive_id.to_string());
    o.i64_field("created_at_ms", created_at_ms);
    o.field(
        "generator",
        format!(
            "{{\"name\":\"ctx\",\"version\":{}}}",
            serde_json::to_string(generator_version).expect("version is valid UTF-8")
        ),
    );
    o.i64_field("source_schema_version", source_schema_version);
    if let Some(origin) = origin_device_id {
        o.str_field("origin_device_id", origin.to_owned());
    }
    o.field("scope", "{\"kind\":\"full\"}".to_owned());
    let stream_json = streams
        .iter()
        .map(|stream| {
            format!(
                "{{\"name\":{},\"path\":{},\"count\":{},\"bytes\":{},\"sha256\":{}}}",
                serde_json::to_string(&stream.name).unwrap(),
                serde_json::to_string(&stream.path).unwrap(),
                stream.count,
                stream.bytes,
                serde_json::to_string(&stream.sha256).unwrap()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    o.field("streams", format!("[{}]", stream_json));
    o.field(
        "objects",
        format!(
            "{{\"count\":{},\"total_bytes\":{}}}",
            object_count, object_bytes
        ),
    );
    o.i64_field(
        "entity_count",
        i64::try_from(entity_count).unwrap_or(i64::MAX),
    );
    let mut bytes = o.finish().into_bytes();
    bytes.push(b'\n');
    bytes
}

fn completion_bytes(manifest: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(manifest);
    format!(
        "{{\"format\":\"ctx-archive-complete\",\"format_version\":1,\"manifest_sha256\":{}}}\n",
        serde_json::to_string(&hex_digest(digest)).unwrap()
    )
    .into_bytes()
}

fn copy_objects(
    tx: &Transaction<'_>,
    source_objects: &Path,
    destination_objects: &Path,
) -> Result<(u64, u64)> {
    #[cfg(unix)]
    let source_root = AnchoredDir::open_path(source_objects)?;
    let mut total_bytes = 0_u64;
    let mut object_count = 0_u64;
    let mut statement = tx.prepare(
        "SELECT blob_hash, MIN(byte_size), MAX(byte_size) FROM artifacts GROUP BY blob_hash ORDER BY blob_hash",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let hash: String = row.get(0)?;
        let minimum_size: i64 = row.get(1)?;
        let maximum_size: i64 = row.get(2)?;
        if minimum_size != maximum_size || !is_sha256_hex(&hash) || minimum_size < 0 {
            return Err(archive_error(
                "artifact blob references have conflicting or invalid sizes",
            ));
        }
        let shard = &hash[..2];
        #[cfg(unix)]
        let source_shard_fd = source_root.dir(shard).map_err(|_| {
            archive_error(format!(
                "referenced blob shard is not a regular directory: {shard}"
            ))
        })?;
        #[cfg(unix)]
        let mut input = source_shard_fd.file(&hash).map_err(|_| {
            archive_error(format!(
                "referenced blob is not a private regular file: {hash}"
            ))
        })?;
        #[cfg(not(unix))]
        let mut input = {
            let source_shard = source_objects.join(shard);
            let _source_shard_fd = open_read_nofollow(&source_shard, true).map_err(|_| {
                archive_error(format!(
                    "referenced blob shard is not a regular directory: {shard}"
                ))
            })?;
            open_read_nofollow(&source_shard.join(&hash), false).map_err(|_| {
                archive_error(format!(
                    "referenced blob is not a private regular file: {hash}"
                ))
            })?
        };
        let shard_dir = destination_objects.join(shard);
        if !shard_dir.exists() {
            create_private_dir(&shard_dir)?;
        }
        let destination = shard_dir.join(&hash);
        let mut input = BufReader::with_capacity(COPY_BUFFER_BYTES, &mut input);
        let mut output = private_open(&destination)?;
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        let mut buffer = [0_u8; COPY_BUFFER_BYTES];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            hasher.update(&buffer[..read]);
            size = size
                .checked_add(read as u64)
                .ok_or_else(|| archive_error("blob size overflow"))?;
        }
        output.sync_all()?;
        if size != u64::try_from(minimum_size).unwrap_or(u64::MAX)
            || hex_digest(hasher.finalize()) != *hash
        {
            return Err(archive_error(format!(
                "referenced blob changed while exporting: {hash}"
            )));
        }
        total_bytes = total_bytes
            .checked_add(size)
            .ok_or_else(|| archive_error("object byte count overflow"))?;
        object_count = object_count
            .checked_add(1)
            .ok_or_else(|| archive_error("object count overflow"))?;
    }
    Ok((object_count, total_bytes))
}

fn self_verify_staged(
    stage: &Path,
    manifest: &[u8],
    streams: &[ArchiveStreamReport],
) -> Result<()> {
    if streams.len() != STREAMS.len() {
        return Err(archive_error("writer did not create all canonical streams"));
    }
    if read_bounded(&stage.join("manifest.json"), MAX_MANIFEST_BYTES, "manifest")? != manifest {
        return Err(archive_error("manifest changed before completion"));
    }
    verify_v1_bundle(stage)
}

/// Verify a complete v1 bundle without modifying it. The public command and
/// stable rejection vocabulary are intentionally owned by the follow-up
/// verifier ticket; this reusable core is shared by the writer now.
pub fn verify_archive_bundle(path: impl AsRef<Path>) -> Result<()> {
    verify_v1_bundle(path.as_ref())
}

#[derive(Debug, Clone)]
enum JsonNode {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<JsonNode>),
    Object(Vec<(String, JsonNode)>),
}

fn canonical_json(value: &JsonNode) -> String {
    match value {
        JsonNode::Null => "null".to_owned(),
        JsonNode::Bool(value) => value.to_string(),
        JsonNode::Number(value) => value.to_string(),
        JsonNode::String(value) => serde_json::to_string(value).expect("JSON strings are valid"),
        JsonNode::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        JsonNode::Object(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|(name, value)| format!(
                    "{}:{}",
                    serde_json::to_string(name).expect("JSON field names are valid"),
                    canonical_json(value)
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

impl<'de> Deserialize<'de> for JsonNode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NodeVisitor;
        impl<'de> Visitor<'de> for NodeVisitor {
            type Value = JsonNode;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }
            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(JsonNode::Null)
            }
            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(JsonNode::Bool(value))
            }
            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(JsonNode::Number(value.into()))
            }
            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(JsonNode::Number(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(JsonNode::Number)
                    .ok_or_else(|| de::Error::custom("non-finite JSON number"))
            }
            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(JsonNode::String(value.to_owned()))
            }
            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
                Ok(JsonNode::String(value))
            }
            fn visit_seq<A>(self, mut access: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = access.next_element()? {
                    values.push(value);
                }
                Ok(JsonNode::Array(values))
            }
            fn visit_map<A>(self, mut access: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(key) = access.next_key::<String>()? {
                    if values.iter().any(|(previous, _)| previous == &key) {
                        return Err(de::Error::custom("duplicate JSON object key"));
                    }
                    values.push((key, access.next_value()?));
                }
                Ok(JsonNode::Object(values))
            }
        }
        deserializer.deserialize_any(NodeVisitor)
    }
}

type OrderedObject = Vec<(String, JsonNode)>;

fn parse_json_object(bytes: &[u8]) -> Result<OrderedObject> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = JsonNode::deserialize(&mut deserializer)?;
    deserializer.end()?;
    match value {
        JsonNode::Object(object) => Ok(object),
        _ => Err(archive_error("archive record is not a JSON object")),
    }
}

fn object_value<'a>(object: &'a OrderedObject, name: &str) -> Result<&'a JsonNode> {
    object
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
        .ok_or_else(|| archive_error(format!("missing archive field: {name}")))
}

fn required_string(object: &OrderedObject, name: &str) -> Result<String> {
    match object_value(object, name)? {
        JsonNode::String(value) => Ok(value.clone()),
        _ => Err(archive_error(format!(
            "archive field {name} is not a string"
        ))),
    }
}

fn optional_string(object: &OrderedObject, name: &str) -> Result<Option<String>> {
    match object.iter().find(|(key, _)| key == name) {
        None => Ok(None),
        Some((_, JsonNode::String(value))) => Ok(Some(value.clone())),
        Some(_) => Err(archive_error(format!(
            "archive field {name} is not a string"
        ))),
    }
}

fn required_i64(object: &OrderedObject, name: &str) -> Result<i64> {
    match object_value(object, name)? {
        JsonNode::Number(value) => value
            .as_i64()
            .ok_or_else(|| archive_error(format!("archive field {name} is not a signed integer"))),
        _ => Err(archive_error(format!(
            "archive field {name} is not an integer"
        ))),
    }
}

fn required_nonnegative_i64(object: &OrderedObject, name: &str) -> Result<i64> {
    let value = required_i64(object, name)?;
    if value < 0 {
        return Err(archive_error(format!("archive field {name} is negative")));
    }
    Ok(value)
}

fn required_bool(object: &OrderedObject, name: &str) -> Result<bool> {
    match object_value(object, name)? {
        JsonNode::Bool(value) => Ok(*value),
        _ => Err(archive_error(format!(
            "archive field {name} is not a boolean"
        ))),
    }
}

#[derive(Clone, Copy)]
struct FieldSpec {
    name: &'static str,
    optional: bool,
}

fn schema_for(index: usize) -> Vec<FieldSpec> {
    let names: &[(&str, bool)] = match index {
        0 => &[
            ("id", false),
            ("kind", false),
            ("provider", false),
            ("machine_id", false),
            ("process_id", true),
            ("cwd", true),
            ("raw_source_path", true),
            ("external_session_id", true),
            ("started_at_ms", false),
            ("ended_at_ms", true),
            ("fidelity", false),
            ("visibility", false),
            ("sync_state", false),
            ("sync_version", false),
            ("metadata_json", false),
        ],
        1 => &[
            ("id", false),
            ("kind", false),
            ("root_path", false),
            ("repo_fingerprint", false),
            ("primary_remote_url_normalized", true),
            ("host", false),
            ("owner", true),
            ("name", true),
            ("monorepo_subpath", true),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        2 => &[
            ("id", false),
            ("title", false),
            ("summary", true),
            ("status", false),
            ("primary_vcs_workspace_id", true),
            ("started_at_ms", true),
            ("last_activity_at_ms", false),
            ("completed_at_ms", true),
            ("confidence", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
            ("body", false),
            ("tags_json", false),
            ("kind", false),
            ("workspace", true),
        ],
        3 => &[
            ("id", false),
            ("kind", false),
            ("blob_hash", false),
            ("byte_size", false),
            ("media_type", true),
            ("preview_text", true),
            ("redaction_state", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        4 => &[
            ("id", false),
            ("history_record_id", true),
            ("parent_session_id", true),
            ("root_session_id", true),
            ("capture_source_id", true),
            ("provider", false),
            ("external_session_id", true),
            ("external_agent_id", true),
            ("agent_type", false),
            ("role_hint", true),
            ("is_primary", false),
            ("status", false),
            ("fidelity", false),
            ("transcript_blob_id", true),
            ("started_at_ms", false),
            ("ended_at_ms", true),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("visibility", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        5 => &[
            ("id", false),
            ("from_session_id", false),
            ("to_session_id", false),
            ("edge_type", false),
            ("confidence", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        6 => &[
            ("id", false),
            ("history_record_id", true),
            ("session_id", true),
            ("run_type", false),
            ("status", false),
            ("started_at_ms", false),
            ("ended_at_ms", true),
            ("exit_code", true),
            ("cwd", true),
            ("command_preview", true),
            ("input_blob_id", true),
            ("output_blob_id", true),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        7 => &[
            ("id", false),
            ("seq", false),
            ("history_record_id", true),
            ("session_id", true),
            ("run_id", true),
            ("event_type", false),
            ("role", true),
            ("occurred_at_ms", false),
            ("capture_source_id", true),
            ("payload_json", false),
            ("payload_blob_id", true),
            ("dedupe_key", true),
            ("visibility", false),
            ("redaction_state", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        8 => &[
            ("id", false),
            ("vcs_workspace_id", false),
            ("kind", false),
            ("change_id", false),
            ("parent_change_ids_json", false),
            ("branch_or_bookmark", true),
            ("tree_hash", true),
            ("author_time_ms", true),
            ("confidence", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        9 => &[
            ("id", false),
            ("history_record_id", true),
            ("session_id", true),
            ("kind", false),
            ("model_or_source", true),
            ("text", false),
            ("citations_json", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        10 => &[
            ("id", false),
            ("history_record_id", true),
            ("run_id", true),
            ("event_id", true),
            ("vcs_workspace_id", true),
            ("path", false),
            ("change_kind", true),
            ("old_path", true),
            ("line_count_delta", true),
            ("confidence", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        11 => &[
            ("id", false),
            ("name", false),
            ("kind", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("metadata_json", false),
        ],
        12 => &[
            ("history_record_id", false),
            ("tag_id", false),
            ("source_id", true),
            ("confidence", false),
            ("created_at_ms", false),
        ],
        13 => &[
            ("id", false),
            ("history_record_id", false),
            ("target_type", false),
            ("target_id", false),
            ("link_type", false),
            ("confidence", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        14 => &[
            ("id", false),
            ("from_record_id", false),
            ("to_record_id", false),
            ("edge_type", false),
            ("confidence", false),
            ("created_at_ms", false),
            ("updated_at_ms", false),
            ("source_id", true),
            ("visibility", false),
            ("fidelity", false),
            ("sync_state", false),
            ("sync_version", false),
            ("deleted_at_ms", true),
            ("metadata_json", false),
        ],
        _ => unreachable!(),
    };
    names
        .iter()
        .map(|(name, optional)| FieldSpec {
            name,
            optional: *optional,
        })
        .collect()
}

fn validate_field_order(object: &OrderedObject, schema: &[FieldSpec]) -> Result<()> {
    let mut position = 0;
    for (key, value) in object {
        while position < schema.len() && schema[position].name != key {
            if !schema[position].optional {
                return Err(archive_error(format!(
                    "archive field order or required field mismatch: {key}"
                )));
            }
            position += 1;
        }
        if position == schema.len() {
            return Err(archive_error(format!("unknown archive field: {key}")));
        }
        if matches!(value, JsonNode::Null) {
            return Err(archive_error(format!("archive field is null: {key}")));
        }
        position += 1;
    }
    while position < schema.len() {
        if !schema[position].optional {
            return Err(archive_error(format!(
                "missing archive field: {}",
                schema[position].name
            )));
        }
        position += 1;
    }
    Ok(())
}

fn uuid_field(name: &str) -> bool {
    name == "id"
        || (name.ends_with("_id")
            && !matches!(
                name,
                "machine_id"
                    | "process_id"
                    | "change_id"
                    | "external_session_id"
                    | "external_agent_id"
            ))
}

fn validate_uuid(value: &str) -> Result<()> {
    let uuid = Uuid::parse_str(value)
        .map_err(|_| archive_error(format!("archive UUID is malformed: {value}")))?;
    if uuid.to_string() != value {
        return Err(archive_error(
            "archive UUID is not canonical lowercase hyphenated form",
        ));
    }
    Ok(())
}

fn enum_values(stream: usize, field: &str) -> Option<&'static [&'static str]> {
    const KIND: &[&str] = &["provider_import", "provider_hook", "direct_cli", "manual"];
    const PROVIDER: &[&str] = &PROVIDERS;
    const VCS_KIND: &[&str] = &["git", "jj"];
    const HOST: &[&str] = &["github", "gitlab", "bitbucket", "local", "unknown"];
    const STATUS_RECORD: &[&str] = &["open", "active", "completed", "abandoned", "archived"];
    const VISIBILITY: &[&str] = &[
        "local_only",
        "reportable",
        "sync_metadata",
        "sync_full",
        "withheld",
    ];
    const FIDELITY: &[&str] = &["full", "partial", "imported", "inferred", "summary_only"];
    const SYNC: &[&str] = &["local_only", "pending", "synced", "failed", "withheld"];
    const CONFIDENCE: &[&str] = &["explicit", "high", "medium", "low", "unknown"];
    const REDACTION: &[&str] = &["raw", "redacted", "safe_preview", "withheld"];
    match field {
        "provider" => Some(PROVIDER),
        "visibility" => Some(VISIBILITY),
        "fidelity" => Some(FIDELITY),
        "sync_state" => Some(SYNC),
        "confidence" => Some(CONFIDENCE),
        "redaction_state" => Some(REDACTION),
        "kind" if stream == 0 => Some(KIND),
        "kind" if stream == 1 => Some(VCS_KIND),
        "host" => Some(HOST),
        "status" if stream == 2 => Some(STATUS_RECORD),
        "kind" if stream == 3 => Some(&[
            "transcript",
            "stdout",
            "stderr",
            "screenshot",
            "report",
            "diff",
            "file_snapshot",
            "json",
            "markdown",
            "binary",
        ]),
        "agent_type" => Some(&[
            "primary",
            "subagent",
            "agent_team_member",
            "reviewer",
            "implementer",
            "unknown",
        ]),
        "status" if stream == 4 => Some(&[
            "started",
            "active",
            "idle",
            "completed",
            "failed",
            "interrupted",
            "imported",
        ]),
        "edge_type" if stream == 5 => Some(&[
            "parent_child",
            "delegated",
            "reviewed",
            "spawned",
            "resumed_from",
            "imported_related",
        ]),
        "run_type" => Some(&[
            "agent_turn",
            "command",
            "tool_call",
            "review",
            "import",
            "summary",
        ]),
        "status" if stream == 6 => Some(&[
            "queued",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "partial",
        ]),
        "event_type" => Some(&[
            "message",
            "tool_call",
            "tool_output",
            "command_started",
            "command_output",
            "command_finished",
            "file_touched",
            "vcs_change",
            "artifact",
            "summary",
            "notice",
        ]),
        "role" => Some(&["user", "assistant", "system", "tool", "unknown"]),
        "kind" if stream == 8 => Some(&[
            "git_commit",
            "git_branch",
            "git_worktree",
            "jj_change",
            "jj_bookmark",
            "patch",
            "working_copy",
        ]),
        "kind" if stream == 9 => Some(&[
            "imported_provider_summary",
            "ctx_generated",
            "agent_supplied",
            "human_note",
        ]),
        "change_kind" => Some(&[
            "read", "created", "modified", "deleted", "renamed", "unknown",
        ]),
        "kind" if stream == 11 => Some(&["user", "system", "inferred"]),
        "target_type" => Some(&[
            "session",
            "run",
            "event",
            "vcs_workspace",
            "vcs_change",
            "artifact",
        ]),
        "link_type" => Some(&["produced", "touched", "references", "likely_related"]),
        "edge_type" if stream == 14 => Some(&[
            "continues",
            "duplicates",
            "blocks",
            "related",
            "supersedes",
            "split_from",
        ]),
        _ => None,
    }
}

fn validate_record(stream: usize, object: &OrderedObject) -> Result<()> {
    validate_field_order(object, &schema_for(stream))?;
    for (key, value) in object {
        if uuid_field(key) {
            validate_uuid(match value {
                JsonNode::String(value) => value,
                _ => return Err(archive_error(format!("UUID field is not a string: {key}"))),
            })?;
        }
        if let Some(values) = enum_values(stream, key) {
            let value = match value {
                JsonNode::String(value) => value,
                _ => return Err(archive_error(format!("enum field is not a string: {key}"))),
            };
            if !values.contains(&value.as_str()) {
                return Err(archive_error(format!(
                    "unknown archive vocabulary value for {key}"
                )));
            }
        }
        match key.as_str() {
            "is_primary" => {
                required_bool(object, key)?;
            }
            "seq" | "byte_size" | "sync_version" => {
                required_nonnegative_i64(object, key)?;
            }
            "process_id" => {
                u32::try_from(required_i64(object, key)?)
                    .map_err(|_| archive_error("archive process_id is outside u32 range"))?;
            }
            "exit_code" => {
                i32::try_from(required_i64(object, key)?)
                    .map_err(|_| archive_error("archive exit_code is outside i32 range"))?;
            }
            "line_count_delta"
            | "created_at_ms"
            | "updated_at_ms"
            | "started_at_ms"
            | "ended_at_ms"
            | "last_activity_at_ms"
            | "completed_at_ms"
            | "occurred_at_ms"
            | "author_time_ms"
            | "deleted_at_ms" => {
                required_i64(object, key)?;
            }
            _ => {
                if !matches!(value, JsonNode::String(_)) {
                    return Err(archive_error(format!(
                        "archive field has wrong type: {key}"
                    )));
                }
            }
        }
    }
    if let Some(hash) = optional_string(object, "blob_hash")? {
        if !is_sha256_hex(&hash) {
            return Err(archive_error("archive blob hash is malformed"));
        }
    }
    if let Some(provider) = optional_string(object, "provider")? {
        if !PROVIDERS.contains(&provider.as_str()) {
            return Err(archive_error(
                "archive provider is outside the v1 vocabulary",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct StreamMeta {
    name: String,
    path: String,
    count: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Debug)]
struct ManifestInfo {
    entity_count: u64,
    object_count: u64,
    object_bytes: u64,
    streams: Vec<StreamMeta>,
}

fn named_schema(name: &str) -> Vec<FieldSpec> {
    let names: &[(&str, bool)] = match name {
        "manifest" => &[
            ("format", false),
            ("format_version", false),
            ("archive_id", false),
            ("created_at_ms", false),
            ("generator", false),
            ("source_schema_version", false),
            ("origin_device_id", true),
            ("scope", false),
            ("streams", false),
            ("objects", false),
            ("entity_count", false),
        ],
        "stream" => &[
            ("name", false),
            ("path", false),
            ("count", false),
            ("bytes", false),
            ("sha256", false),
        ],
        "objects" => &[("count", false), ("total_bytes", false)],
        "generator" => &[("name", false), ("version", false)],
        "scope" => &[("kind", false)],
        "complete" => &[
            ("format", false),
            ("format_version", false),
            ("manifest_sha256", false),
        ],
        _ => unreachable!(),
    };
    names
        .iter()
        .map(|(field, optional)| FieldSpec {
            name: field,
            optional: *optional,
        })
        .collect()
}

fn as_object<'a>(value: &'a JsonNode, label: &str) -> Result<&'a OrderedObject> {
    match value {
        JsonNode::Object(object) => Ok(object),
        _ => Err(archive_error(format!("{label} is not an object"))),
    }
}

fn as_array<'a>(value: &'a JsonNode, label: &str) -> Result<&'a [JsonNode]> {
    match value {
        JsonNode::Array(values) => Ok(values),
        _ => Err(archive_error(format!("{label} is not an array"))),
    }
}

fn required_u64(object: &OrderedObject, name: &str) -> Result<u64> {
    let value = required_nonnegative_i64(object, name)?;
    u64::try_from(value).map_err(|_| archive_error(format!("archive field {name} is out of range")))
}

fn parse_manifest(bytes: &[u8]) -> Result<ManifestInfo> {
    let line = single_line(bytes, MAX_MANIFEST_BYTES, "manifest")?;
    let object = parse_json_object(line)?;
    validate_field_order(&object, &named_schema("manifest"))?;
    if required_string(&object, "format")? != "ctx-archive"
        || required_i64(&object, "format_version")? != 1
    {
        return Err(archive_error("unsupported archive format or version"));
    }
    let archive_id = Uuid::parse_str(&required_string(&object, "archive_id")?)
        .map_err(|_| archive_error("manifest archive_id is malformed"))?;
    if archive_id.to_string() != required_string(&object, "archive_id")? {
        return Err(archive_error("manifest archive_id is not canonical"));
    }
    required_i64(&object, "created_at_ms")?;
    required_i64(&object, "source_schema_version")?;
    optional_string(&object, "origin_device_id")?;
    let generator = as_object(object_value(&object, "generator")?, "manifest generator")?;
    validate_field_order(generator, &named_schema("generator"))?;
    if required_string(generator, "name")? != "ctx" {
        return Err(archive_error("manifest generator name is invalid"));
    }
    required_string(generator, "version")?;
    let scope = as_object(object_value(&object, "scope")?, "manifest scope")?;
    validate_field_order(scope, &named_schema("scope"))?;
    if required_string(scope, "kind")? != "full" {
        return Err(archive_error("manifest scope is not full"));
    }
    let streams = as_array(object_value(&object, "streams")?, "manifest streams")?;
    if streams.len() != STREAMS.len() {
        return Err(archive_error(
            "manifest does not list exactly fifteen streams",
        ));
    }
    let mut stream_meta = Vec::with_capacity(STREAMS.len());
    for (index, value) in streams.iter().enumerate() {
        let stream = as_object(value, "manifest stream")?;
        validate_field_order(stream, &named_schema("stream"))?;
        let meta = StreamMeta {
            name: required_string(stream, "name")?,
            path: required_string(stream, "path")?,
            count: required_u64(stream, "count")?,
            bytes: required_u64(stream, "bytes")?,
            sha256: required_string(stream, "sha256")?,
        };
        if meta.name != STREAMS[index].0
            || meta.path != format!("streams/{}", STREAMS[index].1)
            || !is_sha256_hex(&meta.sha256)
        {
            return Err(archive_error("manifest stream index or path is invalid"));
        }
        stream_meta.push(meta);
    }
    let objects = as_object(object_value(&object, "objects")?, "manifest objects")?;
    validate_field_order(objects, &named_schema("objects"))?;
    if canonical_json(&JsonNode::Object(object.clone())).as_bytes() != line {
        return Err(archive_error("manifest is not canonical v1 JSON"));
    }
    Ok(ManifestInfo {
        entity_count: required_u64(&object, "entity_count")?,
        object_count: required_u64(objects, "count")?,
        object_bytes: required_u64(objects, "total_bytes")?,
        streams: stream_meta,
    })
}

fn single_line<'a>(bytes: &'a [u8], maximum: usize, label: &str) -> Result<&'a [u8]> {
    if bytes.len() > maximum {
        return Err(archive_error(format!("{label} exceeds its size limit")));
    }
    if !bytes.ends_with(b"\n")
        || bytes[..bytes.len() - 1].contains(&b'\n')
        || bytes.contains(&b'\r')
    {
        return Err(archive_error(format!(
            "{label} is not one newline-terminated line"
        )));
    }
    Ok(&bytes[..bytes.len() - 1])
}

fn read_bounded(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let file = open_read_nofollow(path, false)?;
    read_bounded_file(file, maximum, label)
}

fn read_bounded_file(mut file: File, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let limit = u64::try_from(maximum)
        .map_err(|_| archive_error(format!("{label} size limit is out of range")))?;
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1024).saturating_add(1));
    Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(archive_error(format!("{label} exceeds its size limit")));
    }
    Ok(bytes)
}

fn read_capped_line<R: BufRead>(reader: &mut R, line: &mut Vec<u8>) -> Result<usize> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(line.len());
        }
        let remaining = MAX_JSONL_LINE_BYTES
            .saturating_add(1)
            .saturating_sub(line.len());
        if remaining == 0 {
            return Err(archive_error("stream line exceeds the 32 MiB limit"));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if take > remaining {
            return Err(archive_error("stream line exceeds the 32 MiB limit"));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(line.len());
        }
    }
}

fn open_read_nofollow(path: &Path, directory: bool) -> Result<File> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| archive_error(format!("open {}: {error}", path.display())))?
    };
    #[cfg(not(unix))]
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|error| archive_error(format!("open {}: {error}", path.display())))?;
    let metadata = file.metadata()?;
    if directory {
        if !metadata.is_dir() {
            return Err(archive_error("expected a directory descriptor"));
        }
    } else if !metadata.is_file() || {
        #[cfg(unix)]
        {
            metadata.nlink() > 1
        }
        #[cfg(not(unix))]
        {
            false
        }
    } {
        return Err(archive_error("expected a regular non-hard-linked file"));
    }
    Ok(file)
}

fn verify_v1_bundle(stage: &Path) -> Result<()> {
    #[cfg(unix)]
    let root = AnchoredDir::open_path(stage)?;
    verify_root_layout(
        stage,
        #[cfg(unix)]
        &root,
    )
    .map_err(|error| archive_error(format!("layout: {error}")))?;
    #[cfg(unix)]
    let manifest_bytes =
        read_bounded_file(root.file("manifest.json")?, MAX_MANIFEST_BYTES, "manifest")
            .map_err(|error| archive_error(format!("manifest read: {error}")))?;
    #[cfg(not(unix))]
    let manifest_bytes = read_bounded(&stage.join("manifest.json"), MAX_MANIFEST_BYTES, "manifest")
        .map_err(|error| archive_error(format!("manifest read: {error}")))?;
    let manifest = parse_manifest(&manifest_bytes)?;
    #[cfg(unix)]
    let marker = read_bounded_file(
        root.file("COMPLETE")?,
        MAX_COMPLETE_BYTES,
        "completion marker",
    )
    .map_err(|error| archive_error(format!("marker read: {error}")))?;
    #[cfg(not(unix))]
    let marker = read_bounded(
        &stage.join("COMPLETE"),
        MAX_COMPLETE_BYTES,
        "completion marker",
    )
    .map_err(|error| archive_error(format!("marker read: {error}")))?;
    let marker_line = single_line(&marker, MAX_COMPLETE_BYTES, "completion marker")?;
    let marker_object = parse_json_object(marker_line)?;
    validate_field_order(&marker_object, &named_schema("complete"))?;
    if required_string(&marker_object, "format")? != "ctx-archive-complete"
        || required_i64(&marker_object, "format_version")? != 1
        || required_string(&marker_object, "manifest_sha256")?
            != hex_digest(Sha256::digest(&manifest_bytes))
    {
        return Err(archive_error(
            "completion marker does not authenticate manifest",
        ));
    }
    if canonical_json(&JsonNode::Object(marker_object)).as_bytes() != marker_line {
        return Err(archive_error("completion marker is not canonical v1 JSON"));
    }
    let mut state = VerifierState::new(stage.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|error| archive_error(format!("state: {error}")))?;
    let mut entities = 0_u64;
    for (index, descriptor) in STREAMS.iter().enumerate() {
        let (count, _) = verify_stream_file(
            stage,
            #[cfg(unix)]
            &root,
            index,
            descriptor,
            &manifest.streams[index],
            &mut state,
        )
        .map_err(|error| archive_error(format!("stream {}: {error}", index + 1)))?;
        entities = entities
            .checked_add(count)
            .ok_or_else(|| archive_error("entity count overflow"))?;
    }
    if entities != manifest.entity_count {
        return Err(archive_error("manifest entity count mismatch"));
    }
    verify_references(&state).map_err(|error| archive_error(format!("references: {error}")))?;
    let (object_count, object_bytes) = verify_objects(
        stage,
        #[cfg(unix)]
        &root,
        &manifest,
        &mut state,
    )
    .map_err(|error| archive_error(format!("objects: {error}")))?;
    if object_count != manifest.object_count || object_bytes != manifest.object_bytes {
        return Err(archive_error("manifest object totals mismatch"));
    }
    Ok(())
}

fn verify_root_layout(_stage: &Path, #[cfg(unix)] root: &AnchoredDir) -> Result<()> {
    #[cfg(unix)]
    {
        for name in ["manifest.json", "COMPLETE"] {
            root.file(name)?;
        }
        let streams = root.dir("streams")?;
        root.dir("objects")?;
        root.for_each_name(|name| {
            if matches!(name, "manifest.json" | "COMPLETE" | "streams" | "objects") {
                Ok(())
            } else {
                Err(archive_error("bundle contains an unexpected root entry"))
            }
        })?;
        for (_, file, _) in STREAMS {
            streams.file(file)?;
        }
        streams.for_each_name(|name| {
            if STREAMS.iter().any(|(_, file, _)| name == *file) {
                Ok(())
            } else {
                Err(archive_error("bundle contains an unexpected stream file"))
            }
        })?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        check_directory(_stage)?;
        for name in ["manifest.json", "COMPLETE"] {
            check_regular(&_stage.join(name))?;
        }
        check_directory(&_stage.join("streams"))?;
        check_directory(&_stage.join("objects"))?;
        for entry in fs::read_dir(_stage)? {
            let entry = entry?;
            let name = entry.file_name();
            let valid = name == "manifest.json"
                || name == "COMPLETE"
                || name == "streams"
                || name == "objects";
            if !valid {
                return Err(archive_error("bundle contains an unexpected root entry"));
            }
        }
        for (_, file, _) in STREAMS {
            check_regular(&_stage.join("streams").join(file))?;
        }
        for entry in fs::read_dir(_stage.join("streams"))? {
            let name = entry?.file_name();
            let valid = name
                .to_str()
                .map(|name| STREAMS.iter().any(|(_, file, _)| name == *file))
                .unwrap_or(false);
            if !valid {
                return Err(archive_error("bundle contains an unexpected stream file"));
            }
        }
        Ok(())
    }
}

#[cfg(not(unix))]
fn check_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(archive_error(format!(
            "bundle directory is not regular: {}",
            path.display()
        )));
    }
    check_mode(&metadata)
}

#[cfg(not(unix))]
fn check_regular(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(archive_error(format!(
            "bundle entry is not regular: {}",
            path.display()
        )));
    }
    check_mode(&metadata)?;
    #[cfg(unix)]
    if metadata.nlink() > 1 {
        return Err(archive_error("bundle entry is hard-linked"));
    }
    Ok(())
}

fn check_mode(metadata: &fs::Metadata) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(archive_error("bundle entry is group/world writable"));
        }
    }
    Ok(())
}

fn verify_stream_file(
    _stage: &Path,
    #[cfg(unix)] root: &AnchoredDir,
    index: usize,
    descriptor: &(&str, &str, &str),
    expected: &StreamMeta,
    state: &mut VerifierState,
) -> Result<(u64, u64)> {
    #[cfg(not(unix))]
    let path = _stage.join("streams").join(descriptor.1);
    if expected.name != descriptor.0 || expected.path != format!("streams/{}", descriptor.1) {
        return Err(archive_error("stream path differs from contract"));
    }
    #[cfg(unix)]
    let mut file = root.dir("streams")?.file(descriptor.1)?;
    #[cfg(not(unix))]
    let mut file = open_read_nofollow(&path, false)?;
    let mut reader = BufReader::new(&mut file);
    let mut line = Vec::with_capacity(64 * 1024);
    let mut hasher = Sha256::new();
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    let mut previous = None;
    loop {
        let read = read_capped_line(&mut reader, &mut line)?;
        if read == 0 {
            break;
        }
        if !line.ends_with(b"\n") || line[..line.len() - 1].contains(&b'\r') {
            return Err(archive_error(
                "stream line is too long, truncated, or contains CR",
            ));
        }
        hasher.update(&line);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| archive_error("stream byte count overflow"))?;
        let record = &line[..line.len() - 1];
        if record.is_empty() {
            return Err(archive_error("stream contains a blank line"));
        }
        let object = parse_json_object(record)?;
        validate_record(index, &object)?;
        if canonical_json(&JsonNode::Object(object.clone())).as_bytes() != record {
            return Err(archive_error("stream record is not canonical v1 JSON"));
        }
        let sort_key = state.record(index, &object)?;
        if let Some(previous) = &previous {
            if compare_sort_keys(previous, &sort_key) != std::cmp::Ordering::Less {
                return Err(archive_error("stream is not in canonical order"));
            }
        }
        previous = Some(sort_key);
        count = count
            .checked_add(1)
            .ok_or_else(|| archive_error("stream count overflow"))?;
    }
    if count != expected.count
        || bytes != expected.bytes
        || hex_digest(hasher.finalize()) != expected.sha256
    {
        return Err(archive_error("stream count, size, or digest mismatch"));
    }
    Ok((count, bytes))
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum SortKey {
    Text(String),
    Number(i64),
    Pair(String, String),
}

fn compare_sort_keys(left: &SortKey, right: &SortKey) -> std::cmp::Ordering {
    match (left, right) {
        (SortKey::Text(a), SortKey::Text(b)) => a.cmp(b),
        (SortKey::Number(a), SortKey::Number(b)) => a.cmp(b),
        (SortKey::Pair(a, b), SortKey::Pair(c, d)) => a.cmp(c).then_with(|| b.cmp(d)),
        _ => std::cmp::Ordering::Equal,
    }
}

fn composite_key(components: &[&str]) -> Vec<u8> {
    let mut key = Vec::new();
    for component in components {
        let bytes = component.as_bytes();
        key.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        key.extend_from_slice(bytes);
    }
    key
}

struct VerifierState {
    temp_dir: PathBuf,
    conn: Connection,
}

impl VerifierState {
    fn new(parent: &Path) -> Result<Self> {
        let temp_dir = parent.join(format!(".ctxar-verify-{}", Uuid::new_v4()));
        create_private_dir(&temp_dir)?;
        let result = (|| {
            let database = temp_dir.join("state.sqlite");
            let conn = Connection::open(&database)?;
            restrict_private_file(&database)?;
            conn.execute_batch("PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL; CREATE TABLE ids(id TEXT PRIMARY KEY, kind INTEGER NOT NULL); CREATE TABLE refs(kind INTEGER NOT NULL, id TEXT NOT NULL); CREATE TABLE unique_keys(kind TEXT NOT NULL, key BLOB NOT NULL, PRIMARY KEY(kind, key)); CREATE TABLE blobs(hash TEXT PRIMARY KEY, byte_size INTEGER NOT NULL); CREATE TABLE order_events(seq INTEGER PRIMARY KEY);")?;
            Ok(Self {
                temp_dir: temp_dir.clone(),
                conn,
            })
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&temp_dir);
        }
        result
    }

    fn insert_unique(&self, kind: &str, key: &[u8]) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO unique_keys(kind, key) VALUES (?1, ?2)",
                rusqlite::params![kind, key],
            )
            .map_err(|_| archive_error(format!("duplicate or conflicting natural key: {kind}")))?;
        Ok(())
    }

    fn reference(&self, kind: i64, id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO refs(kind, id) VALUES (?1, ?2)",
            rusqlite::params![kind, id],
        )?;
        Ok(())
    }

    fn record(&self, stream: usize, object: &OrderedObject) -> Result<SortKey> {
        let key = match stream {
            7 => SortKey::Number(required_nonnegative_i64(object, "seq")?),
            12 => SortKey::Pair(
                required_string(object, "history_record_id")?,
                required_string(object, "tag_id")?,
            ),
            _ => SortKey::Text(required_string(object, "id")?),
        };
        if stream != 12 {
            let id = required_string(object, "id")?;
            self.conn
                .execute(
                    "INSERT INTO ids(id, kind) VALUES (?1, ?2)",
                    rusqlite::params![id, stream as i64],
                )
                .map_err(|_| archive_error("duplicate or cross-stream entity ID"))?;
        }
        match stream {
            0 => {
                if let Some((_, value)) = object.iter().find(|(name, _)| name == "process_id") {
                    let value = match value {
                        JsonNode::Number(_) => required_i64(object, "process_id")?,
                        _ => {
                            return Err(archive_error("archive field process_id is not an integer"))
                        }
                    };
                    u32::try_from(value)
                        .map_err(|_| archive_error("archive process_id is outside u32 range"))?;
                }
            }
            1 => {
                self.insert_unique(
                    "vcs_workspace",
                    &composite_key(&[
                        &required_string(object, "kind")?,
                        &required_string(object, "repo_fingerprint")?,
                    ]),
                )?;
                if let Some(id) = optional_string(object, "source_id")? {
                    self.reference(0, &id)?;
                }
            }
            2 => {
                if let Some(id) = optional_string(object, "primary_vcs_workspace_id")? {
                    self.reference(1, &id)?;
                }
            }
            3 => {
                self.insert_unique(
                    "artifact",
                    &composite_key(&[
                        &required_string(object, "blob_hash")?,
                        &required_string(object, "kind")?,
                    ]),
                )?;
                let hash = required_string(object, "blob_hash")?;
                let size = required_nonnegative_i64(object, "byte_size")?;
                self.conn.execute("INSERT INTO blobs(hash, byte_size) VALUES (?1, ?2) ON CONFLICT(hash) DO UPDATE SET byte_size = CASE WHEN blobs.byte_size = excluded.byte_size THEN blobs.byte_size ELSE -1 END", rusqlite::params![hash, size])?;
            }
            4 => {
                for (field, kind) in [
                    ("history_record_id", 2),
                    ("parent_session_id", 4),
                    ("root_session_id", 4),
                    ("capture_source_id", 0),
                    ("transcript_blob_id", 3),
                ] {
                    if let Some(id) = optional_string(object, field)? {
                        self.reference(kind, &id)?;
                    }
                }
            }
            5 => {
                for (field, kind) in [
                    ("from_session_id", 4),
                    ("to_session_id", 4),
                    ("source_id", 0),
                ] {
                    if let Some(id) = optional_string(object, field)? {
                        self.reference(kind, &id)?;
                    }
                }
            }
            6 => {
                if object.iter().any(|(name, _)| name == "exit_code") {
                    let value = required_i64(object, "exit_code")?;
                    i32::try_from(value)
                        .map_err(|_| archive_error("archive exit_code is outside i32 range"))?;
                }
                for (field, kind) in [
                    ("history_record_id", 2),
                    ("session_id", 4),
                    ("input_blob_id", 3),
                    ("output_blob_id", 3),
                    ("source_id", 0),
                ] {
                    if let Some(id) = optional_string(object, field)? {
                        self.reference(kind, &id)?;
                    }
                }
            }
            7 => {
                self.insert_unique(
                    "event_seq",
                    &required_nonnegative_i64(object, "seq")?.to_be_bytes(),
                )?;
                if let Some(key) = optional_string(object, "dedupe_key")? {
                    self.insert_unique("event_dedupe", key.as_bytes())?;
                }
                for (field, kind) in [
                    ("history_record_id", 2),
                    ("session_id", 4),
                    ("run_id", 6),
                    ("capture_source_id", 0),
                    ("payload_blob_id", 3),
                ] {
                    if let Some(id) = optional_string(object, field)? {
                        self.reference(kind, &id)?;
                    }
                }
            }
            8 => {
                self.reference(1, &required_string(object, "vcs_workspace_id")?)?;
                if let Some(id) = optional_string(object, "source_id")? {
                    self.reference(0, &id)?;
                }
                self.insert_unique(
                    "vcs_change",
                    &composite_key(&[
                        &required_string(object, "vcs_workspace_id")?,
                        &required_string(object, "kind")?,
                        &required_string(object, "change_id")?,
                    ]),
                )?;
            }
            9 => {
                for (field, kind) in [
                    ("history_record_id", 2),
                    ("session_id", 4),
                    ("source_id", 0),
                ] {
                    if let Some(id) = optional_string(object, field)? {
                        self.reference(kind, &id)?;
                    }
                }
            }
            10 => {
                for (field, kind) in [
                    ("history_record_id", 2),
                    ("run_id", 6),
                    ("event_id", 7),
                    ("vcs_workspace_id", 1),
                    ("source_id", 0),
                ] {
                    if let Some(id) = optional_string(object, field)? {
                        self.reference(kind, &id)?;
                    }
                }
            }
            11 => self.insert_unique("tag_name", required_string(object, "name")?.as_bytes())?,
            12 => {
                self.reference(2, &required_string(object, "history_record_id")?)?;
                self.reference(11, &required_string(object, "tag_id")?)?;
                if let Some(id) = optional_string(object, "source_id")? {
                    self.reference(0, &id)?;
                }
                self.insert_unique(
                    "record_tag",
                    &composite_key(&[
                        &required_string(object, "history_record_id")?,
                        &required_string(object, "tag_id")?,
                    ]),
                )?;
            }
            13 => {
                self.reference(2, &required_string(object, "history_record_id")?)?;
                let target_kind = match required_string(object, "target_type")?.as_str() {
                    "session" => 4,
                    "run" => 6,
                    "event" => 7,
                    "vcs_workspace" => 1,
                    "vcs_change" => 8,
                    "artifact" => 3,
                    _ => unreachable!(),
                };
                self.reference(target_kind, &required_string(object, "target_id")?)?;
                if let Some(id) = optional_string(object, "source_id")? {
                    self.reference(0, &id)?;
                }
                self.insert_unique(
                    "record_link",
                    &composite_key(&[
                        &required_string(object, "history_record_id")?,
                        &required_string(object, "target_type")?,
                        &required_string(object, "target_id")?,
                        &required_string(object, "link_type")?,
                    ]),
                )?;
            }
            14 => {
                self.reference(2, &required_string(object, "from_record_id")?)?;
                self.reference(2, &required_string(object, "to_record_id")?)?;
                if let Some(id) = optional_string(object, "source_id")? {
                    self.reference(0, &id)?;
                }
            }
            _ => unreachable!(),
        }
        Ok(key)
    }
}

impl Drop for VerifierState {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.temp_dir);
    }
}

fn verify_references(state: &VerifierState) -> Result<()> {
    let dangling: i64 = state.conn.query_row("SELECT COUNT(*) FROM refs WHERE NOT EXISTS (SELECT 1 FROM ids WHERE ids.id = refs.id AND ids.kind = refs.kind)", [], |row| row.get(0))?;
    if dangling != 0 {
        return Err(archive_error("archive contains dangling reference"));
    }
    let conflicting_sizes: i64 = state.conn.query_row(
        "SELECT COUNT(*) FROM blobs WHERE byte_size < 0",
        [],
        |row| row.get(0),
    )?;
    if conflicting_sizes != 0 {
        return Err(archive_error(
            "archive artifacts have conflicting blob sizes",
        ));
    }
    Ok(())
}

fn verify_objects(
    _stage: &Path,
    #[cfg(unix)] root: &AnchoredDir,
    manifest: &ManifestInfo,
    state: &mut VerifierState,
) -> Result<(u64, u64)> {
    #[cfg(unix)]
    return verify_objects_anchored(root, manifest, state);
    #[cfg(not(unix))]
    {
        let mut count = 0_u64;
        let mut bytes = 0_u64;
        for entry in fs::read_dir(_stage.join("objects"))? {
            let entry = entry?;
            let shard = entry.file_name().to_string_lossy().into_owned();
            check_directory(&entry.path())?;
            if shard.len() != 2
                || !shard
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(archive_error("object shard is malformed"));
            }
            for child in fs::read_dir(entry.path())? {
                let child = child?;
                let hash = child.file_name().to_string_lossy().into_owned();
                check_regular(&child.path())?;
                if !is_sha256_hex(&hash) || hash[..2] != *shard {
                    return Err(archive_error("object path is malformed"));
                }
                let mut file = open_read_nofollow(&child.path(), false)?;
                let mut reader = BufReader::new(&mut file);
                let mut hasher = Sha256::new();
                let mut size = 0_u64;
                let mut buffer = [0_u8; COPY_BUFFER_BYTES];
                loop {
                    let read = reader.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                    size += read as u64;
                }
                if hex_digest(hasher.finalize()) != hash {
                    return Err(archive_error("object checksum mismatch"));
                }
                let expected: Option<i64> = state
                    .conn
                    .query_row(
                        "SELECT byte_size FROM blobs WHERE hash = ?1",
                        [&hash],
                        |row| row.get(0),
                    )
                    .optional()?;
                let expected = expected
                    .ok_or_else(|| archive_error("archive contains an unreferenced object"))?;
                if size != u64::try_from(expected).unwrap_or(u64::MAX) {
                    return Err(archive_error("object size mismatch"));
                }
                state
                    .conn
                    .execute("DELETE FROM blobs WHERE hash = ?1", [&hash])?;
                count += 1;
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| archive_error("object byte count overflow"))?;
            }
        }
        let remaining: i64 = state
            .conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))?;
        if remaining != 0 {
            return Err(archive_error("archive is missing a referenced object"));
        }
        if count != manifest.object_count || bytes != manifest.object_bytes {
            return Err(archive_error("manifest object totals mismatch"));
        }
        Ok((count, bytes))
    }
}

#[cfg(unix)]
fn verify_objects_anchored(
    root: &AnchoredDir,
    manifest: &ManifestInfo,
    state: &mut VerifierState,
) -> Result<(u64, u64)> {
    let objects = root.dir("objects")?;
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    objects.for_each_name(|shard| {
        if shard.len() != 2
            || !shard
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(archive_error("object shard is malformed"));
        }
        let shard_dir = objects.dir(shard)?;
        shard_dir.for_each_name(|hash| {
            if !is_sha256_hex(hash) || hash[..2] != *shard {
                return Err(archive_error("object path is malformed"));
            }
            let mut reader = BufReader::new(shard_dir.file(hash)?);
            let expected: Option<i64> = state
                .conn
                .query_row(
                    "SELECT byte_size FROM blobs WHERE hash = ?1",
                    [&hash],
                    |row| row.get(0),
                )
                .optional()?;
            let expected =
                expected.ok_or_else(|| archive_error("archive contains an unreferenced object"))?;
            let expected =
                u64::try_from(expected).map_err(|_| archive_error("object size is invalid"))?;
            let mut hasher = Sha256::new();
            let mut size = 0_u64;
            let mut buffer = [0_u8; COPY_BUFFER_BYTES];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                size = size
                    .checked_add(read as u64)
                    .ok_or_else(|| archive_error("object size overflow"))?;
                if size > expected {
                    return Err(archive_error("object size mismatch"));
                }
                hasher.update(&buffer[..read]);
            }
            if size != expected || hex_digest(hasher.finalize()) != hash {
                return Err(archive_error("object size or checksum mismatch"));
            }
            state
                .conn
                .execute("DELETE FROM blobs WHERE hash = ?1", [&hash])?;
            count = count
                .checked_add(1)
                .ok_or_else(|| archive_error("object count overflow"))?;
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| archive_error("object byte count overflow"))?;
            if count > manifest.object_count || bytes > manifest.object_bytes {
                return Err(archive_error("manifest object totals exceeded"));
            }
            Ok(())
        })?;
        Ok(())
    })?;
    let remaining: i64 = state
        .conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))?;
    if remaining != 0 {
        return Err(archive_error("archive is missing a referenced object"));
    }
    if count != manifest.object_count || bytes != manifest.object_bytes {
        return Err(archive_error("manifest object totals mismatch"));
    }
    Ok((count, bytes))
}

fn verify_completion(stage: &Path, manifest: &[u8], marker: &[u8]) -> Result<()> {
    let expected = completion_bytes(manifest);
    if expected != marker
        || read_bounded(
            &stage.join("COMPLETE"),
            MAX_COMPLETE_BYTES,
            "completion marker",
        )? != marker
    {
        return Err(archive_error("completion marker failed self-verification"));
    }
    Ok(())
}

fn sync_tree(root: &Path) -> Result<()> {
    // The staging tree is created 0700 and is never exposed before the
    // exclusive rename. Path enumeration is therefore protected from other
    // users; every file and directory is nevertheless reopened with
    // O_NOFOLLOW before it is synced.
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            sync_tree(&path)?;
        } else if metadata.is_file() {
            open_read_nofollow(&path, false)?.sync_all()?;
        }
    }
    sync_directory(root)
}

fn sync_directory(path: &Path) -> Result<()> {
    open_read_nofollow(path, true)?.sync_all()?;
    Ok(())
}

fn private_open(path: &Path) -> Result<File> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .mode(0o600)
            .open(path)?
    };
    #[cfg(not(unix))]
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(path)?;
    restrict_private_file(path)?;
    Ok(file)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = private_open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn create_private_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700).create(path)?;
    }
    #[cfg(not(unix))]
    fs::create_dir(path)?;
    restrict_private_dir(path)?;
    Ok(())
}

fn ensure_directory(path: &Path, label: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let normalized = normalize_macos_trusted_root_alias(path);
    #[cfg(target_os = "macos")]
    let path = normalized.as_path();
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| archive_error(format!("{label} does not exist: {}", path.display())))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(archive_error(format!("{label} is not a directory")));
    }
    Ok(())
}

fn reject_existing_target(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(archive_error(format!(
            "archive target already exists: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn restrict_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_private_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_private_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_private_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn atomic_publish(stage: &Path, target: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let parent = target
        .parent()
        .ok_or_else(|| archive_error("archive target has no parent"))?;
    if stage.parent() != Some(parent) {
        return Err(archive_error("staging and target directories differ"));
    }
    let parent = AnchoredDir::open_path(parent)?;
    let source = CString::new(
        stage
            .file_name()
            .ok_or_else(|| archive_error("staging path has no file name"))?
            .as_bytes(),
    )
    .map_err(|_| archive_error("staging path contains NUL"))?;
    let destination = CString::new(
        target
            .file_name()
            .ok_or_else(|| archive_error("archive path has no file name"))?
            .as_bytes(),
    )
    .map_err(|_| archive_error("archive path contains NUL"))?;
    let result = unsafe {
        #[cfg(target_os = "linux")]
        {
            libc::syscall(
                libc::SYS_renameat2,
                parent.0.as_raw_fd(),
                source.as_ptr(),
                parent.0.as_raw_fd(),
                destination.as_ptr(),
                1_u32, // RENAME_NOREPLACE
            )
        }
        #[cfg(target_os = "macos")]
        {
            libc::renameatx_np(
                parent.0.as_raw_fd(),
                source.as_ptr(),
                parent.0.as_raw_fd(),
                destination.as_ptr(),
                libc::RENAME_EXCL,
            ) as libc::c_long
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if target.exists() {
                -1
            } else {
                std::fs::rename(stage, target).map(|_| 0).unwrap_or(-1)
            }
        }
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    parent.0.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn atomic_publish(stage: &Path, target: &Path) -> Result<()> {
    reject_existing_target(target)?;
    fs::rename(stage, target)?;
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hex_digest<D: AsRef<[u8]>>(digest: D) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn archive_error(message: impl Into<String>) -> StoreError {
    StoreError::Archive(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    fn root() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn capped_read_rejects_oversized_unterminated_line_before_extra_growth() {
        let input = vec![b'x'; MAX_JSONL_LINE_BYTES + 2];
        let mut reader = BufReader::with_capacity(4096, input.as_slice());
        let mut line = Vec::new();
        assert!(read_capped_line(&mut reader, &mut line).is_err());
        assert!(line.capacity() <= MAX_JSONL_LINE_BYTES + 1);
        assert!(line.len() <= MAX_JSONL_LINE_BYTES + 1);
    }

    #[test]
    fn bounded_file_read_does_not_trust_initial_size() {
        let temp = root();
        let path = temp.path().join("growing");
        fs::write(&path, vec![0_u8; 33]).unwrap();
        let file = File::open(path).unwrap();
        assert!(read_bounded_file(file, 32, "test file").is_err());
    }

    #[test]
    fn canonical_json_rejects_whitespace_and_alternate_escaping() {
        for bytes in [
            b" {\"a\":\"x\"}".as_slice(),
            b"{\"a\":\"\\u0078\"}".as_slice(),
        ] {
            let object = parse_json_object(bytes).unwrap();
            assert_ne!(canonical_json(&JsonNode::Object(object)).as_bytes(), bytes);
        }
    }

    #[test]
    fn composite_natural_keys_are_unambiguous() {
        assert_ne!(composite_key(&["a\0b", "c"]), composite_key(&["a", "b\0c"]));
    }

    #[test]
    fn empty_store_writes_all_streams_and_is_repeatable() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let id = Uuid::from_u128(0x12345678123456781234567812345678);
        let options = ArchiveOptions {
            archive_id: Some(id),
            created_at_ms: Some(1_700_000_000_000),
            generator_version: "test".into(),
        };
        let first = temp.path().join("one.ctxar");
        let second = temp.path().join("two.ctxar");
        store.create_archive(&first, options.clone()).unwrap();
        store.create_archive(&second, options).unwrap();
        for (_, file, _) in STREAMS {
            assert!(first.join("streams").join(file).is_file());
            assert_eq!(
                fs::read(first.join("streams").join(file)).unwrap(),
                fs::read(second.join("streams").join(file)).unwrap()
            );
        }
        for file in ["manifest.json", "COMPLETE"] {
            assert_eq!(
                fs::read(first.join(file)).unwrap(),
                fs::read(second.join(file)).unwrap()
            );
        }
    }

    #[test]
    fn keyset_pagination_exports_more_than_one_page_exactly_once() {
        let temp = root();
        let mut store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let tx = store.conn.transaction().unwrap();
        for index in 0..(EXPORT_PAGE_ROWS + 17) {
            let id = Uuid::from_u128(0x7000_0000_0000_7000_8000_0000_0000_0000 + index as u128);
            tx.execute(
                "INSERT INTO capture_sources (id,kind,provider,machine_id,started_at_ms,fidelity,visibility,sync_state,sync_version,metadata_json) VALUES (?1,'provider_import','codex','machine',1,'full','local_only','local_only',0,'{}')",
                [id.to_string()],
            ).unwrap();
        }
        tx.commit().unwrap();
        let target = temp.path().join("paged.ctxar");
        let report = store
            .create_archive(&target, ArchiveOptions::default())
            .unwrap();
        assert_eq!(report.streams[0].count, (EXPORT_PAGE_ROWS + 17) as u64);
        let lines = fs::read_to_string(target.join("streams/01-capture_sources.jsonl")).unwrap();
        let ids = lines
            .lines()
            .map(|line| {
                required_string(&parse_json_object(line.as_bytes()).unwrap(), "id").unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), EXPORT_PAGE_ROWS + 17);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn writer_fails_closed_on_non_boolean_sqlite_is_primary() {
        let temp = root();
        let mut store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store.conn.execute("INSERT INTO sessions (id,provider,agent_type,is_primary,status,fidelity,started_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json) VALUES (?1,'codex','primary',2,'imported','full',1,1,1,'local_only','local_only',0,'{}')", ["70000000-0000-7000-8000-000000000001"]).unwrap();
        let target = temp.path().join("invalid.ctxar");
        assert!(store
            .create_archive(&target, ArchiveOptions::default())
            .is_err());
        assert!(!target.exists());
    }

    #[test]
    fn writer_rejects_out_of_range_process_id_and_exit_code() {
        let temp = root();
        let mut store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store.conn.execute("INSERT INTO capture_sources (id,kind,provider,machine_id,process_id,started_at_ms,fidelity,visibility,sync_state,sync_version,metadata_json) VALUES (?1,'provider_import','codex','machine',?2,1,'full','local_only','local_only',0,'{}')", rusqlite::params!["70000000-0000-7000-8000-000000000001", i64::from(u32::MAX) + 1]).unwrap();
        assert!(store
            .create_archive(
                temp.path().join("bad-process.ctxar"),
                ArchiveOptions::default()
            )
            .unwrap_err()
            .to_string()
            .contains("process_id"));

        store
            .conn
            .execute("DELETE FROM capture_sources", [])
            .unwrap();
        store.conn.execute("INSERT INTO runs (id,run_type,status,started_at_ms,exit_code,created_at_ms,updated_at_ms,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES (?1,'command','failed',1,?2,1,1,'local_only','full','local_only',0,'{}')", rusqlite::params!["70000000-0000-7000-8000-000000000002", i64::from(i32::MAX) + 1]).unwrap();
        assert!(store
            .create_archive(
                temp.path().join("bad-exit.ctxar"),
                ArchiveOptions::default()
            )
            .unwrap_err()
            .to_string()
            .contains("exit_code"));
    }

    #[test]
    fn verifier_rejects_out_of_range_process_id_and_exit_code_records() {
        let process = parse_json_object(br#"{"id":"70000000-0000-7000-8000-000000000001","kind":"provider_import","provider":"codex","machine_id":"machine","process_id":4294967296,"started_at_ms":1,"fidelity":"full","visibility":"local_only","sync_state":"local_only","sync_version":0,"metadata_json":"{}"}"#).unwrap();
        assert!(validate_record(0, &process)
            .unwrap_err()
            .to_string()
            .contains("process_id"));

        let run = parse_json_object(br#"{"id":"70000000-0000-7000-8000-000000000002","run_type":"command","status":"failed","started_at_ms":1,"exit_code":2147483648,"created_at_ms":1,"updated_at_ms":1,"visibility":"local_only","fidelity":"full","sync_state":"local_only","sync_version":0,"metadata_json":"{}"}"#).unwrap();
        assert!(validate_record(6, &run)
            .unwrap_err()
            .to_string()
            .contains("exit_code"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn exclusive_publication_allows_exactly_one_racing_writer() {
        use std::sync::{Arc, Barrier};
        let temp = root();
        let database = temp.path().join("work.sqlite");
        Store::open(&database).unwrap();
        let target = temp.path().join("race.ctxar");
        let barrier = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|_| {
                let database = database.clone();
                let target = target.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut store = Store::open_read_only(&database).unwrap();
                    barrier.wait();
                    store
                        .create_archive(&target, ArchiveOptions::default())
                        .is_ok()
                })
            })
            .collect::<Vec<_>>();
        let successes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|success| *success)
            .count();
        assert_eq!(successes, 1);
        verify_archive_bundle(target).unwrap();
    }

    #[test]
    fn copies_referenced_blob_once_and_keeps_streaming_output_private() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let bytes = b"synthetic secret artifact";
        let hash = hex_digest(Sha256::digest(bytes));
        let shard = temp.path().join("objects").join(&hash[..2]);
        fs::create_dir_all(&shard).unwrap();
        fs::write(shard.join(&hash), bytes).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO artifacts (id, kind, blob_hash, blob_path, byte_size, media_type, preview_text, redaction_state, created_at_ms, updated_at_ms, metadata_json) VALUES (?1, 'binary', ?2, ?3, ?4, NULL, NULL, 'raw', 1, 1, '{}')",
                rusqlite::params![
                    "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
                    hash,
                    format!("objects/{}/{}", &hash[..2], hash),
                    bytes.len() as i64
                ],
            )
            .unwrap();
        let target = temp.path().join("blob.ctxar");
        #[cfg(unix)]
        let previous_umask = unsafe { libc::umask(0o077) };
        store
            .create_archive(
                &target,
                ArchiveOptions {
                    archive_id: Some(Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)),
                    created_at_ms: Some(1),
                    generator_version: "test".into(),
                },
            )
            .unwrap();
        #[cfg(unix)]
        unsafe {
            libc::umask(previous_umask);
        }
        assert_eq!(
            fs::read(target.join("objects").join(&hash[..2]).join(&hash)).unwrap(),
            bytes
        );
        assert!(fs::metadata(target.join("COMPLETE")).unwrap().len() < 4096);
        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(&target).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(target.join("COMPLETE"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            fn assert_private_tree(path: &Path) {
                for entry in fs::read_dir(path).unwrap() {
                    let entry = entry.unwrap();
                    let metadata = entry.metadata().unwrap();
                    let expected = if metadata.is_dir() { 0o700 } else { 0o600 };
                    assert_eq!(
                        metadata.permissions().mode() & 0o777,
                        expected,
                        "{}",
                        entry.path().display()
                    );
                    if metadata.is_dir() {
                        assert_private_tree(&entry.path());
                    }
                }
            }
            assert_private_tree(&target);
        }
    }

    #[cfg(unix)]
    #[test]
    fn verifier_rejects_symlinked_parent_component() {
        let temp = root();
        let mut store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let target = temp.path().join("real.ctxar");
        store
            .create_archive(&target, ArchiveOptions::default())
            .unwrap();
        let link = temp.path().join("parent-link");
        std::os::unix::fs::symlink(temp.path(), &link).unwrap();
        assert!(verify_archive_bundle(link.join("real.ctxar")).is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn archive_round_trip_works_under_trusted_tmp_root() {
        let temp = tempfile::Builder::new()
            .prefix("ctx-archive-")
            .tempdir_in("/tmp")
            .unwrap();
        let mut store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let target = temp.path().join("round-trip.ctxar");

        store
            .create_archive(&target, ArchiveOptions::default())
            .unwrap();
        verify_archive_bundle(&target).unwrap();

        fs::set_permissions(&target, fs::Permissions::from_mode(0o702)).unwrap();
        assert!(verify_archive_bundle(&target).is_err());

        let untrusted = temp.path().join("world-writable");
        fs::create_dir(&untrusted).unwrap();
        fs::set_permissions(&untrusted, fs::Permissions::from_mode(0o777)).unwrap();
        let untrusted_target = untrusted.join("archive.ctxar");
        assert!(store
            .create_archive(&untrusted_target, ArchiveOptions::default())
            .is_err());
        assert!(!untrusted_target.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_trusted_root_aliases_are_narrow_and_keep_var_compatibility() {
        assert_eq!(
            normalize_macos_trusted_root_alias(Path::new("/tmp/archive.ctxar")),
            PathBuf::from("/private/tmp/archive.ctxar")
        );
        assert_eq!(
            normalize_macos_trusted_root_alias(Path::new("/var/folders/work")),
            PathBuf::from("/private/var/folders/work")
        );
        assert_eq!(
            normalize_macos_trusted_root_alias(Path::new("/various/archive.ctxar")),
            PathBuf::from("/various/archive.ctxar")
        );
    }

    #[test]
    fn populated_fixture_covers_all_streams_and_optional_fields_deterministically() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let ids = [
            "00000000-0000-7000-8000-000000000001",
            "00000000-0000-7000-8000-000000000002",
            "00000000-0000-7000-8000-000000000003",
            "00000000-0000-7000-8000-000000000004",
            "00000000-0000-7000-8000-000000000005",
            "00000000-0000-7000-8000-000000000006",
            "00000000-0000-7000-8000-000000000007",
            "00000000-0000-7000-8000-000000000008",
            "00000000-0000-7000-8000-000000000009",
            "00000000-0000-7000-8000-000000000010",
            "00000000-0000-7000-8000-000000000011",
            "00000000-0000-7000-8000-000000000012",
            "00000000-0000-7000-8000-000000000013",
            "00000000-0000-7000-8000-000000000014",
        ];
        store.conn.execute_batch(&format!(
            "INSERT INTO capture_sources (id, kind, provider, machine_id, process_id, cwd, raw_source_path, external_session_id, started_at_ms, ended_at_ms, fidelity, visibility, sync_state, sync_version, metadata_json) VALUES ('{}','provider_import','codex','machine',7,'/work','/tmp/source','external',1,2,'full','reportable','synced',3,'{{\"source\":true}}');
             INSERT INTO vcs_workspaces (id, kind, root_path, repo_fingerprint, primary_remote_url_normalized, host, owner, name, monorepo_subpath, created_at_ms, updated_at_ms, source_id, visibility, fidelity, sync_state, sync_version, deleted_at_ms, metadata_json) VALUES ('{}','git','/repo','fingerprint','https://example.invalid/repo','github','owner','repo','sub',1,2,'{}','reportable','full','synced',3,4,'{{\"workspace\":true}}');
             INSERT INTO history_records (id,title,summary,status,primary_vcs_workspace_id,started_at_ms,last_activity_at_ms,completed_at_ms,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json,body,tags_json,kind,workspace) VALUES ('{}','title','summary','completed','{}',1,2,3,'high',1,2,'{}','reportable','full','synced',3,4,'{{\"record\":true}}','body','[\"tag\"]','note','/repo');",
            ids[0], ids[1], ids[0], ids[2], ids[1], ids[0]
        )).unwrap();
        let bytes = b"shared populated archive blob";
        let hash = hex_digest(Sha256::digest(bytes));
        let shard = temp.path().join("objects").join(&hash[..2]);
        fs::create_dir_all(&shard).unwrap();
        fs::write(shard.join(&hash), bytes).unwrap();
        store.conn.execute(
            "INSERT INTO artifacts (id,kind,blob_hash,blob_path,byte_size,media_type,preview_text,redaction_state,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES (?1,'binary',?2,?3,?4,'application/octet-stream','preview','raw',1,2,?5,'reportable','full','synced',3,4,'{\"artifact\":true}')",
            rusqlite::params![ids[3], hash, format!("objects/{}/{}", &hash[..2], hash), bytes.len() as i64, ids[0]],
        ).unwrap();
        store.conn.execute_batch(&format!(
            "INSERT INTO sessions (id,history_record_id,parent_session_id,root_session_id,capture_source_id,provider,external_session_id,external_agent_id,agent_type,role_hint,is_primary,status,fidelity,transcript_blob_id,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','{}','{}','{}','codex','session','agent','primary','role',1,'completed','full','{}',1,2,1,2,'reportable','synced',3,4,'{{\"session\":true}}');
             INSERT INTO session_edges (id,from_session_id,to_session_id,edge_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','{}','delegated','high',1,2,'{}','reportable','full','synced',3,4,'{{}}');
             INSERT INTO runs (id,history_record_id,session_id,run_type,status,started_at_ms,ended_at_ms,exit_code,cwd,command_preview,input_blob_id,output_blob_id,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','{}','command','succeeded',1,2,0,'/work','echo ok','{}','{}',1,2,'{}','reportable','full','synced',3,4,'{{}}');
             INSERT INTO events (id,seq,history_record_id,session_id,run_id,event_type,role,occurred_at_ms,capture_source_id,payload_json,payload_blob_id,dedupe_key,visibility,redaction_state,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}',7,'{}','{}','{}','artifact','assistant',2,'{}','{{\"text\":\"hello\"}}','{}','dedupe','reportable','raw','full','synced',3,4,'{{}}');
             INSERT INTO vcs_changes (id,vcs_workspace_id,kind,change_id,parent_change_ids_json,branch_or_bookmark,tree_hash,author_time_ms,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','git_commit','abc','[\"parent\"]','main','tree',1,'high',1,2,'{}','reportable','full','synced',3,4,'{{}}');
             INSERT INTO summaries (id,history_record_id,session_id,kind,model_or_source,text,citations_json,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','{}','human_note','human','summary','[{{\"event_id\":\"{}\"}}]',1,2,'{}','reportable','full','synced',3,4,'{{}}');
             INSERT INTO files_touched (id,history_record_id,run_id,event_id,vcs_workspace_id,path,change_kind,old_path,line_count_delta,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','{}','{}','{}','src/main.rs','renamed','src/old.rs',2,'high',1,2,'{}','reportable','full','synced',3,4,'{{}}');
             INSERT INTO tags (id,name,kind,created_at_ms,updated_at_ms,metadata_json) VALUES ('{}','important','user',1,2,'{{}}');
             INSERT INTO history_record_tags (history_record_id,tag_id,source_id,confidence,created_at_ms) VALUES ('{}','{}','{}','high',1);
             INSERT INTO history_record_links (id,history_record_id,target_type,target_id,link_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','artifact','{}','references','high',1,2,'{}','reportable','full','synced',3,4,'{{}}');
             INSERT INTO record_edges (id,from_record_id,to_record_id,edge_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json) VALUES ('{}','{}','{}','related','high',1,2,'{}','reportable','full','synced',3,4,'{{}}');",
             ids[4],ids[2],ids[4],ids[4],ids[0],ids[3], ids[5],ids[4],ids[4],ids[0], ids[6],ids[2],ids[4],ids[3],ids[3],ids[0], ids[7],ids[2],ids[4],ids[6],ids[0],ids[3], ids[8],ids[1],ids[0], ids[9],ids[2],ids[4],ids[7],ids[0], ids[10],ids[2],ids[6],ids[7],ids[1],ids[0], ids[11],ids[2],ids[11],ids[0],ids[12],ids[2],ids[3],ids[0],ids[13],ids[2],ids[2],ids[0]
        )).unwrap();
        let first = temp.path().join("populated-one.ctxar");
        let second = temp.path().join("populated-two.ctxar");
        let options = ArchiveOptions {
            archive_id: Some(Uuid::from_u128(0x11111111111171118111111111111111)),
            created_at_ms: Some(9),
            generator_version: "test".into(),
        };
        let report = store.create_archive(&first, options.clone()).unwrap();
        store.create_archive(&second, options).unwrap();
        assert_eq!(
            report
                .streams
                .iter()
                .filter(|stream| stream.count > 0)
                .count(),
            15
        );
        for (_, file, _) in STREAMS {
            assert_eq!(
                fs::read(first.join("streams").join(file)).unwrap(),
                fs::read(second.join("streams").join(file)).unwrap()
            );
        }
        for file in ["manifest.json", "COMPLETE"] {
            assert_eq!(
                fs::read(first.join(file)).unwrap(),
                fs::read(second.join(file)).unwrap()
            );
        }
        assert_eq!(
            fs::read(first.join("objects").join(&hash[..2]).join(&hash)).unwrap(),
            bytes
        );
        assert_eq!(report.object_count, 1);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_hardlinked_and_symlinked_source_blobs_without_publishing() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let bytes = b"source blob";
        let hash = hex_digest(Sha256::digest(bytes));
        let shard = temp.path().join("objects").join(&hash[..2]);
        fs::create_dir_all(&shard).unwrap();
        let source = shard.join(&hash);
        fs::write(&source, bytes).unwrap();
        store.conn.execute(
            "INSERT INTO artifacts (id,kind,blob_hash,blob_path,byte_size,redaction_state,created_at_ms,updated_at_ms,metadata_json) VALUES (?1,'binary',?2,?3,?4,'raw',1,1,'{}')",
            rusqlite::params!["cccccccc-cccc-7ccc-8ccc-cccccccccccc", hash, format!("objects/{}/{}", &hash[..2], hash), bytes.len() as i64],
        ).unwrap();
        fs::hard_link(&source, shard.join("hardlink")).unwrap();
        let hardlink_target = temp.path().join("hardlink.ctxar");
        assert!(store
            .create_archive(&hardlink_target, ArchiveOptions::default())
            .is_err());
        assert!(!hardlink_target.exists());
        fs::remove_file(shard.join("hardlink")).unwrap();
        fs::remove_file(&source).unwrap();
        let other = temp.path().join("other-bytes");
        fs::write(&other, bytes).unwrap();
        std::os::unix::fs::symlink(&other, &source).unwrap();
        let symlink_target = temp.path().join("symlink.ctxar");
        assert!(store
            .create_archive(&symlink_target, ArchiveOptions::default())
            .is_err());
        assert!(!symlink_target.exists());
    }

    #[test]
    fn reusable_verifier_rejects_mutated_published_stream() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let target = temp.path().join("verify.ctxar");
        store
            .create_archive(&target, ArchiveOptions::default())
            .unwrap();
        let mut stream = OpenOptions::new()
            .append(true)
            .open(target.join("streams/01-capture_sources.jsonl"))
            .unwrap();
        stream.write_all(b"{}\n").unwrap();
        assert!(verify_archive_bundle(&target).is_err());
    }

    #[test]
    fn missing_blob_removes_staging_and_never_publishes_target() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        store
            .conn
            .execute(
                "INSERT INTO artifacts (id, kind, blob_hash, blob_path, byte_size, redaction_state, created_at_ms, updated_at_ms, metadata_json) VALUES (?1, 'binary', ?2, ?3, 3, 'raw', 1, 1, '{}')",
                rusqlite::params![
                    "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
                    hash,
                    format!("objects/{}/{}", &hash[..2], hash)
                ],
            )
            .unwrap();
        let target = temp.path().join("failed.ctxar");
        assert!(store
            .create_archive(&target, ArchiveOptions::default())
            .is_err());
        assert!(!target.exists());
        let leftovers = fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".ctxar.tmp-"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "staging leftovers: {leftovers:?}");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_existing_and_leaves_failed_stage_clean() {
        let temp = root();
        let db = temp.path().join("work.sqlite");
        let mut store = Store::open(&db).unwrap();
        let target = temp.path().join("existing.ctxar");
        fs::create_dir(&target).unwrap();
        assert!(store
            .create_archive(&target, ArchiveOptions::default())
            .is_err());
        let link = temp.path().join("link.ctxar");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(store
            .create_archive(&link, ArchiveOptions::default())
            .is_err());
    }
}
