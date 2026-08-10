use anyhow::{anyhow, bail, Context, Result};
use ctx_history_core::database_path;
use ctx_history_store::ReclaimLock;
use rusqlite::{types::ValueRef, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{
        ffi::OsStrExt,
        fs::{OpenOptionsExt, PermissionsExt},
    },
    path::Path,
};

const SCHEMA_VERSION: i64 = 1005;
const MARGIN_BYTES: u64 = 16 * 1024 * 1024;
const OUTPUT: &str = ".work.sqlite.reclaim-output";
const BACKUP: &str = ".work.sqlite.reclaim-backup";
const JOURNAL: &str = ".work.sqlite.reclaim-journal";
const JOURNAL_NEW: &str = ".work.sqlite.reclaim-journal-new";

#[derive(Debug, Serialize)]
struct Report {
    status: &'static str,
    reason: Option<&'static str>,
    before: Snapshot,
    after: Option<Snapshot>,
    estimated_temporary_bytes: u64,
}
#[derive(Debug, Clone, Serialize)]
struct Snapshot {
    main_db_bytes: u64,
    wal_bytes: u64,
    shm_bytes: u64,
    objects_bytes: u64,
    spool_bytes: u64,
    freelist_bytes: u64,
    available_space_bytes: u64,
    temporary_bytes: u64,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Fingerprint {
    schema: Vec<(String, String, String)>,
    digest: String,
}
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Prepared,
    BackupDurable,
    CandidateInstalled,
    ValidatedCommit,
}
#[derive(Serialize, Deserialize)]
struct Journal {
    version: u8,
    phase: Phase,
    expected: Fingerprint,
    sidecars: String,
}

pub fn run(root: &Path, json: bool) -> Result<()> {
    match reclaim(root) {
        Ok(r) => {
            if json {
                println!("{}", serde_json::to_string(&r)?)
            } else {
                println!("storage reclaim {}: db {} bytes, WAL {}, SHM {}, objects {}, spool {}, freelist {}, free {}, temporary requirement {} bytes{}",r.status,r.before.main_db_bytes,r.before.wal_bytes,r.before.shm_bytes,r.before.objects_bytes,r.before.spool_bytes,r.before.freelist_bytes,r.before.available_space_bytes,r.estimated_temporary_bytes,r.reason.map(|v|format!(" ({v})")).unwrap_or_default())
            }
            Ok(())
        }
        Err(e) if json => {
            eprintln!(
                "{}",
                serde_json::json!({"status":"failed","error":{"code":"storage_reclaim_failed","message":"physical reclaim was refused or failed"}})
            );
            let _ = e;
            Err(anyhow!(crate::SilentExit { code: 1 }))
        }
        Err(e) => Err(e),
    }
}

fn reclaim(root: &Path) -> Result<Report> {
    let db = database_path(root.to_path_buf());
    let lock = ReclaimLock::exclusive_nonblocking(&db)?;
    test_hold_after_reclaim_lock();
    validate_root_permissions(root)?;
    recover(root, &db)?;
    validate_canonical_permissions(&db)?;
    let before = measure(root, &db, 0)?;
    let estimate = before
        .main_db_bytes
        .checked_add(before.wal_bytes)
        .and_then(|v| v.checked_add(before.shm_bytes))
        .and_then(|v| v.checked_add(before.main_db_bytes.saturating_sub(before.freelist_bytes)))
        .and_then(|v| v.checked_add(MARGIN_BYTES))
        .ok_or_else(|| anyhow!("temporary estimate overflow"))?;
    if before.available_space_bytes < estimate {
        bail!("insufficient temporary space")
    }
    let mut before = before;
    before.temporary_bytes = estimate;
    if before.freelist_bytes == 0 && before.wal_bytes == 0 {
        return Ok(Report {
            status: "skipped",
            reason: Some("nothing_reclaimable"),
            before,
            after: None,
            estimated_temporary_bytes: estimate,
        });
    }
    let output = root.join(OUTPUT);
    let backup = root.join(BACKUP);
    let journal_path = root.join(JOURNAL);
    remove_synced(root, &output)?;
    let conn = Connection::open(&db)?;
    conn.busy_timeout(std::time::Duration::ZERO)?;
    if conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))? != SCHEMA_VERSION {
        bail!("unsupported schema version")
    }
    conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE; COMMIT;")
        .context("external SQLite handle may be active")?;
    let (busy, _, _): (i64, i64, i64) =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    if busy != 0 {
        bail!("WAL checkpoint busy")
    };
    crash("checkpoint");
    let expected = fingerprint(&conn)?;
    let sidecars = hex(sidecar_digest(root)?);
    conn.execute_batch(&format!(
        "VACUUM INTO '{}'",
        output.to_string_lossy().replace('\'', "''")
    ))?;
    drop(conn);
    fs::set_permissions(&output, fs::Permissions::from_mode(0o600))?;
    File::open(&output)?.sync_all()?;
    sync_dir(root)?;
    crash("output");
    let candidate = Connection::open_with_flags(&output, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    validate_candidate(&candidate, &expected)?;
    drop(candidate);
    crash("validation");
    let mut journal = Journal {
        version: 1,
        phase: Phase::Prepared,
        expected,
        sidecars,
    };
    write_journal(root, &journal_path, &journal)?;
    crash("journal");
    fs::rename(&db, &backup)?;
    crash("db-rename");
    sync_dir(root)?;
    crash("db-dir-fsync");
    journal.phase = Phase::BackupDurable;
    write_journal(root, &journal_path, &journal)?;
    crash("rename");
    fs::rename(&output, &db)?;
    crash("candidate-rename");
    sync_dir(root)?;
    crash("candidate-dir-fsync");
    journal.phase = Phase::CandidateInstalled;
    write_journal(root, &journal_path, &journal)?;
    crash("fsync");
    let published = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    validate_candidate(&published, &journal.expected)?;
    drop(published);
    if hex(sidecar_digest(root)?) != journal.sidecars {
        bail!("object or spool content changed")
    }
    let post_commit = measure(root, &db, 0)?;
    journal.phase = Phase::ValidatedCommit;
    write_journal(root, &journal_path, &journal)?;
    crash("commit");
    remove_synced(root, &backup)?;
    remove_synced(root, &output)?;
    remove_synced(root, &journal_path)?;
    crash("cleanup");
    let after = measure(root, &db, 0)?;
    if snapshot_logical_fields(&after) != snapshot_logical_fields(&post_commit)
        || hex(sidecar_digest(root)?) != journal.sidecars
    {
        bail!("post-reclaim storage changed")
    }
    drop(lock);
    Ok(Report {
        status: "completed",
        reason: None,
        before,
        after: Some(after),
        estimated_temporary_bytes: estimate,
    })
}

fn recover(root: &Path, db: &Path) -> Result<()> {
    let b = root.join(BACKUP);
    let o = root.join(OUTPUT);
    let j = root.join(JOURNAL);
    let n = root.join(JOURNAL_NEW);
    if !j.exists() {
        match (db.exists(), b.exists()) {
            (false, true) => {
                fs::rename(&b, db)?;
                sync_dir(root)?
            }
            (true, true) => bail!("ambiguous reclaim backup without journal"),
            (false, false) => bail!("canonical database absent"),
            _ => {}
        }
        remove_synced_with_hooks(root, &o, Some("initial-stale-output"))?;
        remove_synced_with_hooks(root, &n, Some("initial-stale-journal-new"))?;
        return Ok(());
    }
    let parsed = fs::read(&j)
        .ok()
        .and_then(|v| serde_json::from_slice::<Journal>(&v).ok());
    if let Some(state) = parsed
        .as_ref()
        .filter(|s| s.version == 1 && s.phase == Phase::ValidatedCommit)
    {
        let valid = db.exists()
            && Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .ok()
                .and_then(|c| validate_candidate(&c, &state.expected).ok())
                .is_some()
            && hex(sidecar_digest(root)?) == state.sidecars;
        if valid {
            remove_synced(root, &b)?;
            remove_synced(root, &o)?;
            remove_synced(root, &j)?;
            remove_synced(root, &n)?;
            return Ok(());
        }
    }
    if b.exists() {
        if db.exists() {
            remove_synced(root, db)?
        }
        fs::rename(&b, db)?;
        sync_dir(root)?;
        remove_synced(root, &o)?;
        remove_synced(root, &j)?;
        remove_synced(root, &n)?;
        return Ok(());
    }
    if db.exists() {
        remove_synced(root, &o)?;
        remove_synced(root, &j)?;
        remove_synced(root, &n)?;
        return Ok(());
    }
    bail!("reclaim recovery has no usable canonical database or backup")
}

fn fingerprint(c: &Connection) -> Result<Fingerprint> {
    let schema=c.prepare("SELECT type,name,COALESCE(sql,'') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let tables=c.prepare("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_data' AND name NOT LIKE '%_idx' AND name NOT LIKE '%_content' AND name NOT LIKE '%_docsize' AND name NOT LIKE '%_config' ORDER BY name")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut h = Sha256::new();
    for t in tables {
        h.update(t.as_bytes());
        let q = t.replace('"', "\"\"");
        let p = c.prepare(&format!("SELECT * FROM \"{q}\""))?;
        let order = (1..=p.column_count())
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");
        drop(p);
        let mut s = c.prepare(&format!("SELECT * FROM \"{q}\" ORDER BY {order}"))?;
        let n = s.column_count();
        let mut rows = s.query([])?;
        while let Some(r) = rows.next()? {
            for i in 0..n {
                digest_value(&mut h, r.get_ref(i)?)
            }
            h.update([255])
        }
    }
    Ok(Fingerprint {
        schema,
        digest: hex(h.finalize().into()),
    })
}
fn digest_value(h: &mut Sha256, v: ValueRef<'_>) {
    match v {
        ValueRef::Null => h.update([0]),
        ValueRef::Integer(v) => {
            h.update([1]);
            h.update(v.to_le_bytes())
        }
        ValueRef::Real(v) => {
            h.update([2]);
            h.update(v.to_bits().to_le_bytes())
        }
        ValueRef::Text(v) => {
            h.update([3]);
            h.update((v.len() as u64).to_le_bytes());
            h.update(v)
        }
        ValueRef::Blob(v) => {
            h.update([4]);
            h.update((v.len() as u64).to_le_bytes());
            h.update(v)
        }
    }
}
fn validate_candidate(c: &Connection, e: &Fingerprint) -> Result<()> {
    if c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))? != SCHEMA_VERSION {
        bail!("schema mismatch")
    };
    if c.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))? != "ok" {
        bail!("integrity failure")
    };
    if c.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| {
        r.get::<_, i64>(0)
    })? != 0
    {
        bail!("foreign key failure")
    };
    validate_map(c, "ctx_history_search", "record_id", "record_search_rowids")?;
    validate_map(c, "event_search", "event_id", "event_search_rowids")?;
    if &fingerprint(c)? != e {
        bail!("logical fingerprint changed")
    }
    Ok(())
}
fn validate_map(c: &Connection, f: &str, id: &str, m: &str) -> Result<()> {
    let q=format!("SELECT (SELECT COUNT(*) FROM {f})!=(SELECT COUNT(*) FROM {m}) OR EXISTS(SELECT 1 FROM {f} f LEFT JOIN {m} m ON m.search_rowid=f.rowid AND m.{id}=f.{id} WHERE m.search_rowid IS NULL) OR EXISTS(SELECT 1 FROM {m} m LEFT JOIN {f} f ON f.rowid=m.search_rowid AND f.{id}=m.{id} WHERE f.rowid IS NULL)");
    if c.query_row(&q, [], |r| r.get::<_, i64>(0))? != 0 {
        bail!("FTS map mismatch")
    }
    Ok(())
}

fn measure(root: &Path, db: &Path, temp: u64) -> Result<Snapshot> {
    let c = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let page: u64 = c.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let free: u64 = c.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    Ok(Snapshot {
        main_db_bytes: file_len(db),
        wal_bytes: file_len(Path::new(&format!("{}-wal", db.display()))),
        shm_bytes: file_len(Path::new(&format!("{}-shm", db.display()))),
        objects_bytes: tree_bytes(&root.join("objects"))?,
        spool_bytes: tree_bytes(&root.join("spool"))?,
        freelist_bytes: page.saturating_mul(free),
        available_space_bytes: available(root)?,
        temporary_bytes: temp,
    })
}
fn snapshot_logical_fields(s: &Snapshot) -> (u64, u64, u64, u64, u64) {
    (
        s.main_db_bytes,
        s.wal_bytes,
        s.shm_bytes,
        s.objects_bytes,
        s.spool_bytes,
    )
}
fn file_len(p: &Path) -> u64 {
    fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}
fn tree_bytes(p: &Path) -> Result<u64> {
    if !p.exists() {
        return Ok(0);
    }
    let mut n = 0;
    for e in fs::read_dir(p)? {
        let e = e?;
        let m = e.file_type()?;
        if m.is_symlink() {
            bail!("symlink in private storage")
        };
        n += if m.is_dir() {
            tree_bytes(&e.path())?
        } else {
            e.metadata()?.len()
        }
    }
    Ok(n)
}
fn available(p: &Path) -> Result<u64> {
    if let Ok(value) = std::env::var("CTX_TEST_RECLAIM_AVAILABLE_BYTES") {
        return value
            .parse()
            .context("invalid test available-space override");
    }
    let c = std::ffi::CString::new(p.as_os_str().as_bytes())?;
    let mut s = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c.as_ptr(), s.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let s = unsafe { s.assume_init() };
    Ok((s.f_bavail as u128 * s.f_frsize as u128).min(u64::MAX as u128) as u64)
}
fn sidecar_digest(root: &Path) -> Result<[u8; 32]> {
    fn walk(p: &Path, b: &Path, h: &mut Sha256) -> Result<()> {
        if !p.exists() {
            return Ok(());
        }
        let mut es = fs::read_dir(p)?.collect::<std::io::Result<Vec<_>>>()?;
        es.sort_by_key(|e| e.file_name());
        for e in es {
            let p = e.path();
            let m = fs::symlink_metadata(&p)?;
            if m.file_type().is_symlink() {
                bail!("symlink in sidecars")
            };
            h.update(p.strip_prefix(b)?.as_os_str().as_bytes());
            if m.is_dir() {
                walk(&p, b, h)?
            } else if m.is_file() {
                h.update(fs::read(p)?)
            } else {
                bail!("special sidecar file")
            }
        }
        Ok(())
    }
    let mut h = Sha256::new();
    walk(&root.join("objects"), root, &mut h)?;
    walk(&root.join("spool"), root, &mut h)?;
    Ok(h.finalize().into())
}
fn hex(v: [u8; 32]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
fn validate_root_permissions(r: &Path) -> Result<()> {
    if fs::symlink_metadata(r)?.permissions().mode() & 0o777 != 0o700 {
        bail!("data root must be 0700")
    }
    for n in [JOURNAL, BACKUP, OUTPUT, JOURNAL_NEW] {
        let p = r.join(n);
        if p.exists() {
            let m = fs::symlink_metadata(p)?;
            if !m.is_file() || m.permissions().mode() & 0o777 != 0o600 {
                bail!("reclaim state must be regular 0600 files")
            }
        }
    }
    Ok(())
}
fn validate_canonical_permissions(p: &Path) -> Result<()> {
    let m = fs::symlink_metadata(p)?;
    if !m.is_file() || m.permissions().mode() & 0o777 != 0o600 {
        bail!("database must be regular 0600 file")
    }
    Ok(())
}
fn write_journal(root: &Path, p: &Path, j: &Journal) -> Result<()> {
    let t = root.join(JOURNAL_NEW);
    remove_synced(root, &t)?;
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&t)?;
    f.write_all(&serde_json::to_vec(j)?)?;
    f.sync_all()?;
    crash(&journal_hook(j.phase, "file-fsync"));
    fs::rename(&t, p)?;
    crash(&journal_hook(j.phase, "rename"));
    sync_dir(root)?;
    crash(&journal_hook(j.phase, "dir-fsync"));
    Ok(())
}
fn remove_synced(root: &Path, p: &Path) -> Result<()> {
    remove_synced_with_hooks(root, p, cleanup_hook_prefix(p))
}

fn remove_synced_with_hooks(
    root: &Path,
    p: &Path,
    hook_prefix: Option<&'static str>,
) -> Result<()> {
    match fs::remove_file(p) {
        Ok(()) => {
            if let Some(prefix) = hook_prefix {
                crash(&format!("{prefix}-remove"));
            }
            sync_dir(root)?;
            if let Some(prefix) = hook_prefix {
                crash(&format!("{prefix}-dir-fsync"));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn journal_hook(phase: Phase, point: &str) -> String {
    let phase = match phase {
        Phase::Prepared => "prepared",
        Phase::BackupDurable => "backup-durable",
        Phase::CandidateInstalled => "candidate-installed",
        Phase::ValidatedCommit => "validated-commit",
    };
    format!("journal-{phase}-{point}")
}

fn cleanup_hook_prefix(path: &Path) -> Option<&'static str> {
    let name = path.file_name()?.to_str()?;
    let item = match name {
        BACKUP => "backup",
        OUTPUT => "output",
        JOURNAL => "journal",
        JOURNAL_NEW => "journal-new",
        _ => return None,
    };
    Some(match item {
        "backup" => "cleanup-backup",
        "output" => "cleanup-output",
        "journal" => "cleanup-journal",
        "journal-new" => "cleanup-journal-new",
        _ => unreachable!(),
    })
}
fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}
fn crash(stage: &str) {
    if std::env::var("CTX_TEST_RECLAIM_CRASH").ok().as_deref() == Some(stage) {
        unsafe { libc::_exit(86) }
    }
}

fn test_hold_after_reclaim_lock() {
    let Some(marker) = std::env::var_os("CTX_TEST_RECLAIM_HOLD_MARKER") else {
        return;
    };
    let Some(release) = std::env::var_os("CTX_TEST_RECLAIM_HOLD_RELEASE") else {
        return;
    };
    let _ = fs::write(&marker, b"exclusive reclaim lock held before SQLite open");
    while Path::new(&release).exists() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let _ = fs::remove_file(marker);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ctx_history_store::Store;
    #[test]
    fn repeated_reclaim_shrinks_then_skips() {
        let t = tempfile::tempdir().unwrap();
        drop(Store::open(t.path().join("work.sqlite")).unwrap());
        let c = Connection::open(t.path().join("work.sqlite")).unwrap();
        c.execute_batch("CREATE TABLE reclaim_fixture(value BLOB);INSERT INTO reclaim_fixture VALUES(zeroblob(1048576));DROP TABLE reclaim_fixture;").unwrap();
        drop(c);
        let before = file_len(&t.path().join("work.sqlite"));
        assert_eq!(reclaim(t.path()).unwrap().status, "completed");
        assert!(file_len(&t.path().join("work.sqlite")) < before);
        assert_eq!(reclaim(t.path()).unwrap().status, "skipped")
    }
    #[test]
    fn active_store_refuses() {
        let t = tempfile::tempdir().unwrap();
        let _s = Store::open(t.path().join("work.sqlite")).unwrap();
        assert!(reclaim(t.path())
            .unwrap_err()
            .to_string()
            .contains("active"))
    }
}
