//! Explicit, archive-authenticated removal of selective hot rows.

use std::path::Path;

use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    archive::{verify_archive_bundle_internal, ArchiveVerifyOptions},
    compaction::plan,
    utc_now, Result, Store, StoreError, EVENT_SEARCH_ROWID_MAP, RECORD_SEARCH_ROWID_MAP,
};

#[cfg(test)]
thread_local! {
    static FAIL_PHASE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn injected(phase: &'static str) -> Result<()> {
    if FAIL_PHASE.with(|slot| slot.get() == Some(phase)) {
        return Err(reject("injected deletion phase failure"));
    }
    Ok(())
}
#[cfg(not(test))]
fn injected(_: &'static str) -> Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ArchiveDeletionOptions {
    /// Authenticate and re-plan, but roll the transaction back before writes.
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveDeletionReport {
    pub archive_id: String,
    pub request_digest: String,
    pub deletion_member_count: u64,
    pub deleted_member_count: u64,
    pub retained_member_count: u64,
    pub retained_object_blob_count: u64,
    pub dry_run: bool,
    pub duplicate: bool,
}

/// Every row-backed archive kind, in child-before-parent deletion order. This
/// is deliberately exhaustive rather than accepting a table name from data.
const ROW_KINDS: &[(&str, &str, &str)] = &[
    ("files_touched", "files_touched", "id"),
    ("summaries", "summaries", "id"),
    ("events", "events", "id"),
    ("runs", "runs", "id"),
    ("session_edges", "session_edges", "id"),
    (
        "history_record_tags",
        "history_record_tags",
        "history_record_id || ':' || tag_id",
    ),
    ("history_record_links", "history_record_links", "id"),
    ("record_edges", "record_edges", "id"),
    ("sessions", "sessions", "id"),
    ("vcs_changes", "vcs_changes", "id"),
    ("history_records", "history_records", "id"),
    ("artifacts", "artifacts", "id"),
    ("vcs_workspaces", "vcs_workspaces", "id"),
    ("tags", "tags", "id"),
    ("capture_sources", "capture_sources", "id"),
];

fn reject(message: &str) -> StoreError {
    StoreError::Archive(format!("hot deletion refused: {message}"))
}

fn request_digest(archive_id: &str, manifest_sha: &str, deletion_set: &str) -> String {
    let mut hash = Sha256::new();
    for part in [
        b"ctx-hot-delete/v1".as_slice(),
        archive_id.as_bytes(),
        manifest_sha.as_bytes(),
        deletion_set.as_bytes(),
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    format!("{:x}", hash.finalize())
}

impl Store {
    /// Re-verify a published selective archive and atomically remove exactly
    /// its still-authenticated, exclusive deletion members. Object files are
    /// deliberately outside this operation.
    pub fn commit_archive_deletion(
        &mut self,
        bundle: &Path,
        options: ArchiveDeletionOptions,
    ) -> Result<ArchiveDeletionReport> {
        // Verification happens on every invocation, including duplicates: a
        // lost response must not turn an absent/corrupt bundle into success.
        let verified = verify_archive_bundle_internal(bundle, ArchiveVerifyOptions::default())?;
        if verified.manifest.format != "ctx-selective-archive" {
            return Err(reject("bundle is not a selective archive"));
        }
        let archived = verified
            .manifest
            .selective_plan
            .as_ref()
            .ok_or_else(|| reject("selective evidence is missing"))?;
        let expected_suppressions = verified.selective_suppression_facts()?;
        let suppressible_roots = archived
            .members
            .iter()
            .filter(|member| {
                member.entity_kind == "sessions"
                    && matches!(member.disposition.as_str(), "selected_root" | "owned_child")
            })
            .count();
        if expected_suppressions.len() != suppressible_roots {
            return Err(reject(
                "a selected session has no suppressible identity/content",
            ));
        }
        let retained_object_blob_count = archived
            .members
            .iter()
            .filter(|member| member.entity_kind == "object_blob" && !member.deletion_authorized)
            .count() as u64;
        let archive_id = verified.manifest.archive_id.to_string();
        let request = request_digest(
            &archive_id,
            &verified.manifest_sha256,
            &archived.deletion_set_digest,
        );
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        let registered: Option<(String, String, String, String, String, String, u64)> = tx
            .query_row(
                "SELECT manifest_sha256,plan_digest,closure_digest,membership_digest,root_set_digest,deletion_set_digest,deletion_member_count FROM compaction_archives WHERE archive_id=?1",
                [&archive_id],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)),
            )
            .optional()?;
        let registered = registered.ok_or_else(|| reject("archive is not registered"))?;
        if registered
            != (
                verified.manifest_sha256.clone(),
                archived.plan_digest.clone(),
                archived.closure_digest.clone(),
                archived.membership_digest.clone(),
                archived.root_set_digest.clone(),
                archived.deletion_set_digest.clone(),
                archived.deletion_authorized_count,
            )
        {
            return Err(reject(
                "registered archive digests do not match the published bundle",
            ));
        }

        let committed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM compaction_operations WHERE operation_kind='delete' AND archive_id=?1 AND request_digest=?2 AND phase='committed')",
            params![archive_id,request], |r| r.get(0))?;
        if committed {
            let absent: u64 = tx.query_row(
                "SELECT count(*) FROM compaction_archive_members WHERE archive_id=?1 AND membership_state='compacted' AND hot_state='absent'",
                [&archive_id], |r| r.get(0))?;
            tx.commit()?;
            return Ok(ArchiveDeletionReport {
                archive_id,
                request_digest: request,
                deletion_member_count: archived.deletion_authorized_count,
                deleted_member_count: absent,
                retained_member_count: archived.members.len() as u64 - absent,
                retained_object_blob_count,
                dry_run: false,
                duplicate: true,
            });
        }

        let registered_suppressions = tx
            .prepare("SELECT s.identity_key,s.content_key,s.association_state IS 'active',f.effective_state IS 'active' FROM compaction_archive_suppressions s LEFT JOIN compaction_suppression_facts f USING(identity_key,content_key) WHERE s.archive_id=?1 ORDER BY s.identity_key,s.content_key")?
            .query_map([&archive_id], |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,bool>(2)?,row.get::<_,bool>(3)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut expected = expected_suppressions
            .iter()
            .map(|identity| (identity.identity_key.clone(), identity.content_key.clone()))
            .collect::<Vec<_>>();
        expected.sort();
        let observed = registered_suppressions
            .iter()
            .map(|(identity, content, _, _)| (identity.clone(), content.clone()))
            .collect::<Vec<_>>();
        // `IS` is intentionally NULL-safe: absent facts from corruption and
        // NULL states can never pass as active.
        let all_active = registered_suppressions
            .iter()
            .all(|(_, _, association_active, fact_active)| *association_active && *fact_active);
        if observed != expected || !all_active {
            return Err(reject(
                "complete active registered suppression coverage is missing or changed",
            ));
        }

        // This fresh plan reloads every canonical row, validates FKs and
        // recomputes closure, ownership, incoming references and the deletion
        // fixed point under the same IMMEDIATE transaction as deletion.
        let current = plan(&tx, archived.cutoff_ms)?;
        if current.plan_digest != archived.plan_digest
            || current.root_set_digest != archived.root_set_digest
            || current.closure_digest != archived.closure_digest
            || current.membership_digest != archived.membership_digest
            || current.deletion_set_digest != archived.deletion_set_digest
        {
            return Err(reject("hot canonical content or reference closure changed"));
        }
        let authenticated: u64 = tx.query_row(
            "SELECT count(*) FROM compaction_deletion_members d JOIN compaction_archive_members m USING(archive_id,entity_kind,entity_key,content_key) WHERE d.archive_id=?1 AND m.ownership='exclusive' AND m.membership_state='verified' AND m.hot_state='present'",
            [&archive_id], |r| r.get(0))?;
        if authenticated != archived.deletion_authorized_count {
            return Err(reject("deletion membership state changed"));
        }
        if options.dry_run {
            tx.rollback()?;
            return Ok(ArchiveDeletionReport {
                archive_id,
                request_digest: request,
                deletion_member_count: authenticated,
                deleted_member_count: 0,
                retained_member_count: archived.members.len() as u64 - authenticated,
                retained_object_blob_count,
                dry_run: true,
                duplicate: false,
            });
        }

        let now = utc_now().timestamp_millis();
        let operation_id = Uuid::now_v7().to_string();
        tx.execute("INSERT INTO compaction_operations VALUES(?1,'delete',?2,?3,'selective','started',1,NULL,?4,?4)", params![operation_id,archive_id,request,now])?;

        // Derived rows first. Helpers verify mapped SQLite-assigned rowids and
        // use the full-scan healing path for missing/stale/mismatched maps.
        let mut stmt = tx.prepare("SELECT d.entity_kind,d.entity_key,d.content_key FROM compaction_deletion_members d JOIN compaction_archive_members m USING(archive_id,entity_kind,entity_key,content_key) WHERE d.archive_id=?1 AND m.ownership='exclusive' AND m.membership_state='verified' AND m.hot_state='present'")?;
        let members = stmt
            .query_map([&archive_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        if members.len() as u64 != authenticated {
            return Err(reject(
                "authorized deletion members are missing or duplicated",
            ));
        }
        for (kind, key, _) in &members {
            match kind.as_str() {
                "history_records" => RECORD_SEARCH_ROWID_MAP.delete_projection_rows(&tx, key)?,
                "events" => EVENT_SEARCH_ROWID_MAP.delete_projection_rows(&tx, key)?,
                "object_blob" => return Err(reject("object blobs are never hot-deletion members")),
                other if ROW_KINDS.iter().any(|(known, _, _)| *known == other) => {}
                _ => return Err(reject("unknown authorized entity kind")),
            }
        }
        injected("projection")?;

        // FK-safe leaf-to-root order. Do not sort these members: ROW_KINDS is
        // the declared dependency order, and the WHERE EXISTS clause
        // authenticates every row again and prevents this operation from
        // broadening.
        let mut deleted = 0_u64;
        for (declared_kind, table, key_expression) in ROW_KINDS {
            for (kind, key, content) in members.iter().filter(|(kind, _, _)| kind == declared_kind)
            {
                let sql = format!("DELETE FROM {table} WHERE {key_expression}=?1 AND EXISTS(SELECT 1 FROM compaction_deletion_members d JOIN compaction_archive_members m USING(archive_id,entity_kind,entity_key,content_key) WHERE d.archive_id=?2 AND d.entity_kind=?3 AND d.entity_key=?1 AND d.content_key=?4 AND m.ownership='exclusive' AND m.membership_state='verified' AND m.hot_state='present')");
                let affected = tx.execute(&sql, params![key, archive_id, kind, content])?;
                if affected != 1 {
                    return Err(reject(
                        "authorized member did not delete exactly one canonical row",
                    ));
                }
                deleted += 1;
            }
        }
        if deleted != authenticated {
            return Err(reject("canonical deletion accounting mismatch"));
        }
        injected("base")?;
        tx.execute("UPDATE compaction_archive_members SET membership_state='compacted',hot_state='absent',updated_at_ms=?2 WHERE archive_id=?1 AND (entity_kind,entity_key) IN (SELECT entity_kind,entity_key FROM compaction_deletion_members WHERE archive_id=?1)", params![archive_id,now])?;
        tx.execute("UPDATE compaction_archive_members SET membership_state='suppressed',updated_at_ms=?2 WHERE archive_id=?1 AND membership_state='verified' AND hot_state='present'", params![archive_id,now])?;
        tx.execute(
            "UPDATE compaction_archives SET updated_at_ms=?2 WHERE archive_id=?1",
            params![archive_id, now],
        )?;
        injected("ledger")?;
        tx.execute("UPDATE compaction_operations SET phase='committed',updated_at_ms=?2 WHERE operation_id=?1", params![operation_id,now])?;
        tx.commit()?;
        Ok(ArchiveDeletionReport {
            archive_id,
            request_digest: request,
            deletion_member_count: authenticated,
            deleted_member_count: authenticated,
            retained_member_count: archived.members.len() as u64 - authenticated,
            retained_object_blob_count,
            dry_run: false,
            duplicate: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::process::ExitStatusExt, process::Command};
    use tempfile::tempdir;

    const SELECTED_SESSION: &str = "70000000-0000-7000-8000-000000000099";
    const SELECTED_EVENT: &str = "70000000-0000-7000-8000-000000000100";
    const SELECTED_ARTIFACT: &str = "70000000-0000-7000-8000-000000000104";

    fn atomic_snapshot(store: &Store) -> Vec<String> {
        [
            "SELECT id FROM sessions ORDER BY id",
            "SELECT id FROM events ORDER BY id",
            "SELECT rowid || ':' || event_id FROM event_search ORDER BY rowid",
            "SELECT event_id || ':' || search_rowid FROM event_search_rowids ORDER BY event_id",
            "SELECT rowid || ':' || record_id FROM ctx_history_search ORDER BY rowid",
            "SELECT record_id || ':' || search_rowid FROM record_search_rowids ORDER BY record_id",
            "SELECT archive_id || ':' || entity_kind || ':' || entity_key || ':' || membership_state || ':' || hot_state FROM compaction_archive_members ORDER BY archive_id,entity_kind,entity_key",
            "SELECT identity_key || ':' || content_key || ':' || effective_state FROM compaction_suppression_facts ORDER BY identity_key,content_key",
            "SELECT archive_id || ':' || identity_key || ':' || content_key || ':' || association_state FROM compaction_archive_suppressions ORDER BY archive_id,identity_key,content_key",
            "SELECT operation_id || ':' || operation_kind || ':' || phase FROM compaction_operations ORDER BY operation_id",
        ]
        .into_iter()
        .map(|sql| {
            let mut statement = store.conn.prepare(sql).unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .join("|")
        })
        .collect()
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        fixture_with_object(false)
    }

    fn object_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        fixture_with_object(true)
    }

    fn fixture_with_object(
        with_object: bool,
    ) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let db = temp.path().join("work.sqlite");
        let bundle = temp.path().join("selected.ctxar");
        let store = Store::open(&db).unwrap();
        store.conn.execute_batch(
            "INSERT INTO capture_sources(id,kind,provider,machine_id,started_at_ms,fidelity,visibility,sync_state,sync_version,metadata_json) VALUES('70000000-0000-7000-8000-000000000098','direct_cli','codex','test',1,'full','local_only','local_only',0,'{}');
             INSERT INTO sessions(id,capture_source_id,provider,external_session_id,agent_type,is_primary,status,fidelity,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json)
             VALUES('70000000-0000-7000-8000-000000000099','70000000-0000-7000-8000-000000000098','codex','stable','primary',1,'completed','full',1,2,1,2,'local_only','local_only',0,'{\"source_format\":\"codex-jsonl-v1\"}');
             INSERT INTO events(id,seq,session_id,event_type,occurred_at_ms,payload_json,dedupe_key,visibility,redaction_state,fidelity,sync_state,sync_version,metadata_json)
             VALUES('70000000-0000-7000-8000-000000000100',1,'70000000-0000-7000-8000-000000000099','message',1,'{}','d','local_only','raw','full','local_only',0,'{}');
             INSERT INTO capture_sources(id,kind,provider,machine_id,started_at_ms,fidelity,visibility,sync_state,sync_version,metadata_json) VALUES('70000000-0000-7000-8000-000000000101','direct_cli','codex','test',1,'full','local_only','local_only',0,'{}');
             INSERT INTO sessions(id,capture_source_id,provider,external_session_id,agent_type,is_primary,status,fidelity,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json)
             VALUES('70000000-0000-7000-8000-000000000102','70000000-0000-7000-8000-000000000101','codex','stable-two','primary',1,'completed','full',1,2,1,2,'local_only','local_only',0,'{\"source_format\":\"codex-jsonl-v1\"}');
             INSERT INTO events(id,seq,session_id,event_type,occurred_at_ms,payload_json,dedupe_key,visibility,redaction_state,fidelity,sync_state,sync_version,metadata_json)
             VALUES('70000000-0000-7000-8000-000000000103',2,'70000000-0000-7000-8000-000000000102','message',1,'{}','d2','local_only','raw','full','local_only',0,'{}');"
         ).unwrap();
        if with_object {
            use std::os::unix::fs::PermissionsExt;

            let bytes = b"selective restore object fixture";
            let hash = format!("{:x}", Sha256::digest(bytes));
            let shard = temp.path().join("objects").join(&hash[..2]);
            fs::create_dir_all(&shard).unwrap();
            fs::write(shard.join(&hash), bytes).unwrap();
            fs::set_permissions(
                temp.path().join("objects"),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            fs::set_permissions(&shard, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(shard.join(&hash), fs::Permissions::from_mode(0o600)).unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO artifacts(id,kind,blob_hash,blob_path,byte_size,media_type,preview_text,redaction_state,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,'binary',?2,?3,?4,'application/octet-stream','fixture','raw',1,2,'70000000-0000-7000-8000-000000000098','local_only','full','local_only',0,'{}')",
                    params![
                        SELECTED_ARTIFACT,
                        hash,
                        format!("objects/{}/{}", &hash[..2], hash),
                        bytes.len() as i64
                    ],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "UPDATE events SET payload_blob_id=?1 WHERE id=?2",
                    params![SELECTED_ARTIFACT, SELECTED_EVENT],
                )
                .unwrap();
        }
        drop(store);
        let mut ro = Store::open_read_only(&db).unwrap();
        ro.create_selective_archive(&bundle, 2, crate::ArchiveOptions::default())
            .unwrap();
        drop(ro);
        let mut store = Store::open(&db).unwrap();
        let registration = store.register_selective_archive(&bundle).unwrap();
        assert_eq!(registration.suppression_count, 2);
        (temp, db, bundle)
    }

    #[test]
    fn selective_restore_rehydrates_requested_root_and_is_idempotent() {
        let (_temp, db, bundle) = fixture();
        let root = db.parent().unwrap();
        let mut store = Store::open(&db).unwrap();
        store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .unwrap();
        store.conn.execute("INSERT INTO event_search(event_id,safe_preview_text,rank_bucket) VALUES('70000000-0000-7000-8000-000000000999','unrelated','notice')",[]).unwrap();
        let unrelated_rowid = store.conn.last_insert_rowid();
        EVENT_SEARCH_ROWID_MAP
            .store_entry(
                &store.conn,
                "70000000-0000-7000-8000-000000000999",
                unrelated_rowid,
            )
            .unwrap();
        drop(store);
        let selected = crate::ArchiveRestoreSelection {
            session_ids: vec![Uuid::parse_str("70000000-0000-7000-8000-000000000099").unwrap()],
        };
        let first = crate::restore_archive_bundle_selective(
            &bundle,
            root,
            ArchiveVerifyOptions::default(),
            selected.clone(),
        )
        .unwrap();
        assert!(first.inserted_count >= 2);
        let store = Store::open_read_only(&db).unwrap();
        assert_eq!(
            store
                .conn
                .query_row::<i64, _, _>(
                    "SELECT count(*) FROM sessions WHERE id='70000000-0000-7000-8000-000000000099'",
                    [],
                    |r| r.get(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .conn
                .query_row::<i64, _, _>(
                    "SELECT count(*) FROM sessions WHERE id='70000000-0000-7000-8000-000000000102'",
                    [],
                    |r| r.get(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(store.conn.query_row::<i64,_,_>("SELECT search_rowid FROM event_search_rowids WHERE event_id='70000000-0000-7000-8000-000000000999'",[],|r|r.get(0)).unwrap(), unrelated_rowid);
        assert_eq!(store.conn.query_row::<i64,_,_>("SELECT count(*) FROM compaction_archive_suppressions WHERE association_state='restored'",[],|r|r.get(0)).unwrap(), 1);
        assert_eq!(store.conn.query_row::<i64,_,_>("SELECT count(*) FROM compaction_archive_suppressions WHERE association_state='active'",[],|r|r.get(0)).unwrap(), 1);
        drop(store);
        let second = crate::restore_archive_bundle_selective(
            &bundle,
            root,
            ArchiveVerifyOptions::default(),
            selected,
        )
        .unwrap();
        assert_eq!(second.inserted_count, 0);
        assert!(second.reused_count >= 2);
    }

    #[test]
    fn selective_restore_conflict_is_zero_mutation() {
        let (_temp, db, bundle) = fixture();
        let root = db.parent().unwrap();
        let mut store = Store::open(&db).unwrap();
        store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .unwrap();
        store.conn.execute("INSERT INTO sessions(id,provider,agent_type,is_primary,status,fidelity,started_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json) VALUES('70000000-0000-7000-8000-000000000099','codex','primary',1,'completed','full',1,1,2,'local_only','local_only',0,'{\"conflict\":true}')",[]).unwrap();
        let before: (i64,i64,i64)=store.conn.query_row("SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM compaction_operations),(SELECT count(*) FROM compaction_restore_markers)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        drop(store);
        let result = crate::restore_archive_bundle_selective(
            &bundle,
            root,
            ArchiveVerifyOptions::default(),
            crate::ArchiveRestoreSelection {
                session_ids: vec![Uuid::parse_str("70000000-0000-7000-8000-000000000099").unwrap()],
            },
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("canonical content conflict"));
        let store = Store::open_read_only(&db).unwrap();
        let after: (i64,i64,i64)=store.conn.query_row("SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM compaction_operations),(SELECT count(*) FROM compaction_restore_markers)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(after, before);
    }

    #[test]
    fn selective_restore_retries_database_crash_boundaries() {
        for phase in [
            "after_preflight",
            "after_locked_preflight",
            "after_projection",
            "before_db_commit",
            "after_db_commit",
        ] {
            let (_temp, db, bundle) = fixture();
            let root = db.parent().unwrap();
            let mut store = Store::open(&db).unwrap();
            store
                .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
                .unwrap();
            drop(store);
            let selected = crate::ArchiveRestoreSelection {
                session_ids: vec![Uuid::parse_str("70000000-0000-7000-8000-000000000099").unwrap()],
            };
            crate::restore::set_selective_fail_phase(Some(phase));
            assert!(
                crate::restore_archive_bundle_selective(
                    &bundle,
                    root,
                    ArchiveVerifyOptions::default(),
                    selected.clone()
                )
                .is_err(),
                "{phase}"
            );
            crate::restore::set_selective_fail_phase(None);
            crate::restore_archive_bundle_selective(
                &bundle,
                root,
                ArchiveVerifyOptions::default(),
                selected,
            )
            .unwrap();
            let store = Store::open_read_only(&db).unwrap();
            assert_eq!(store.conn.query_row::<i64,_,_>("SELECT count(*) FROM sessions WHERE id='70000000-0000-7000-8000-000000000099'",[],|r|r.get(0)).unwrap(),1,"{phase}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn selective_restore_retries_object_crash_boundaries_in_a_killed_subprocess() {
        if let (Some(phase), Some(db), Some(bundle)) = (
            std::env::var_os("CTX_TEST_SELECTIVE_RESTORE_CHILD_PHASE"),
            std::env::var_os("CTX_TEST_SELECTIVE_RESTORE_CHILD_DB"),
            std::env::var_os("CTX_TEST_SELECTIVE_RESTORE_CHILD_BUNDLE"),
        ) {
            let phase = phase.to_string_lossy().into_owned();
            let db = std::path::PathBuf::from(db);
            let bundle = std::path::PathBuf::from(bundle);
            std::env::set_var("CTX_TEST_SELECTIVE_RESTORE_SIGKILL_PHASE", &phase);
            let _ = crate::restore_archive_bundle_selective(
                &bundle,
                db.parent().unwrap(),
                ArchiveVerifyOptions::default(),
                crate::ArchiveRestoreSelection {
                    session_ids: vec![Uuid::parse_str(SELECTED_SESSION).unwrap()],
                },
            );
            panic!("selective restore child returned without SIGKILL at {phase}");
        }

        for phase in [
            "after_object_sync",
            "after_object_publish",
            "after_shard_sync",
        ] {
            let (_temp, db, bundle) = object_fixture();
            let root = db.parent().unwrap();
            let mut store = Store::open(&db).unwrap();
            store
                .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
                .unwrap();

            // Shared artifact rows are intentionally retained by hot deletion;
            // remove this selected canonical row to model the missing hot
            // reference that restore must recreate. The archive and ledger
            // remain the authenticated source of truth.
            let hash: String = store
                .conn
                .query_row(
                    "SELECT blob_hash FROM artifacts WHERE id=?1",
                    [SELECTED_ARTIFACT],
                    |row| row.get(0),
                )
                .unwrap();
            store
                .conn
                .execute("DELETE FROM artifacts WHERE id=?1", [SELECTED_ARTIFACT])
                .unwrap();
            drop(store);
            fs::remove_file(root.join("objects").join(&hash[..2]).join(&hash)).unwrap();

            let status = Command::new(std::env::current_exe().unwrap())
                .arg("selective_restore_retries_object_crash_boundaries_in_a_killed_subprocess")
                .arg("--nocapture")
                .env("CTX_TEST_SELECTIVE_RESTORE_CHILD_PHASE", phase)
                .env("CTX_TEST_SELECTIVE_RESTORE_CHILD_DB", db.as_os_str())
                .env(
                    "CTX_TEST_SELECTIVE_RESTORE_CHILD_BUNDLE",
                    bundle.as_os_str(),
                )
                .status()
                .unwrap();
            assert_eq!(status.signal(), Some(libc::SIGKILL), "{phase}: {status}");

            let store = Store::open_read_only(&db).unwrap();
            assert_eq!(
                store
                    .conn
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM artifacts WHERE id=?1",
                        [SELECTED_ARTIFACT],
                        |row| row.get(0),
                    )
                    .unwrap(),
                0,
                "{phase}: object publication must precede, but not commit, the artifact ref"
            );
            assert_eq!(
                store
                    .conn
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM compaction_operations WHERE operation_kind='restore' AND phase='committed'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                0,
                "{phase}: restore operation committed before the crash"
            );
            assert_eq!(
                store
                    .conn
                    .query_row::<String, _, _>(
                        "SELECT membership_state || ':' || hot_state FROM compaction_archive_members WHERE entity_kind='artifacts' AND entity_key=?1",
                        [SELECTED_ARTIFACT],
                        |row| row.get(0),
                    )
                    .unwrap(),
                "suppressed:present",
                "{phase}: ledger changed before DB commit"
            );
            assert_eq!(
                store
                    .conn
                    .query_row::<String, _, _>(
                        "SELECT association_state FROM compaction_archive_suppressions LIMIT 1",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                "active",
                "{phase}: suppression changed before DB commit"
            );
            drop(store);

            let selected = crate::ArchiveRestoreSelection {
                session_ids: vec![Uuid::parse_str(SELECTED_SESSION).unwrap()],
            };
            crate::restore_archive_bundle_selective(
                &bundle,
                root,
                ArchiveVerifyOptions::default(),
                selected.clone(),
            )
            .unwrap();

            let store = Store::open_read_only(&db).unwrap();
            let (artifact_hash, artifact_size, artifact_path): (String, i64, String) = store
                .conn
                .query_row(
                    "SELECT blob_hash,byte_size,blob_path FROM artifacts WHERE id=?1",
                    [SELECTED_ARTIFACT],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let object = root.join(&artifact_path);
            let bytes = fs::read(&object).unwrap();
            assert_eq!(bytes.len() as i64, artifact_size, "{phase}");
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                artifact_hash,
                "{phase}"
            );
            assert_eq!(artifact_hash, hash, "{phase}");
            let mut artifacts = store
                .conn
                .prepare("SELECT id,blob_hash,byte_size,blob_path FROM artifacts ORDER BY id")
                .unwrap();
            let rows = artifacts
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .unwrap();
            let mut artifact_count = 0;
            for row in rows {
                let (id, row_hash, row_size, row_path) = row.unwrap();
                let bytes = fs::read(root.join(row_path)).unwrap();
                assert_eq!(bytes.len() as i64, row_size, "{phase}: {id}");
                assert_eq!(
                    format!("{:x}", Sha256::digest(&bytes)),
                    row_hash,
                    "{phase}: {id}"
                );
                artifact_count += 1;
            }
            assert_eq!(
                artifact_count, 1,
                "{phase}: every restored artifact must be checked"
            );
            drop(artifacts);
            assert_eq!(
                store
                    .conn
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM events WHERE payload_blob_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM artifacts a WHERE a.id=events.payload_blob_id)",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                0,
                "{phase}: dangling canonical artifact reference"
            );
            for (kind, key) in [
                ("sessions", SELECTED_SESSION),
                ("events", SELECTED_EVENT),
                ("artifacts", SELECTED_ARTIFACT),
                ("object_blob", hash.as_str()),
            ] {
                assert_eq!(
                    store
                        .conn
                        .query_row::<String, _, _>(
                            "SELECT membership_state || ':' || hot_state FROM compaction_archive_members WHERE entity_kind=?1 AND entity_key=?2",
                            params![kind, key],
                            |row| row.get(0),
                        )
                        .unwrap(),
                    "restored:present",
                    "{phase}: selected ledger member {kind}/{key} did not converge"
                );
            }
            assert_eq!(
                store
                    .conn
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM compaction_archive_suppressions WHERE association_state='restored'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                1,
                "{phase}: suppression ledger did not converge"
            );
            assert_eq!(
                store
                    .conn
                    .query_row::<String, _, _>(
                        "SELECT f.effective_state FROM compaction_suppression_facts f JOIN compaction_archive_suppressions s USING(identity_key,content_key) WHERE s.association_state='restored'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                "restored",
                "{phase}: suppression fact did not converge"
            );
            assert_eq!(
                store
                    .conn
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM compaction_restore_markers",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                1,
                "{phase}: canonical restore marker missing"
            );
            assert_eq!(
                store
                    .conn
                    .query_row::<i64, _, _>(
                        "SELECT count(*) FROM compaction_operations WHERE operation_kind='restore' AND phase='committed'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap(),
                1,
                "{phase}: canonical restore operation missing"
            );
            drop(store);

            assert_private_tree(root);
        }
    }

    #[cfg(unix)]
    fn assert_private_tree(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(path).unwrap();
        let expected = if metadata.is_dir() { 0o700 } else { 0o600 };
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            expected,
            "{}",
            path.display()
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                assert_private_tree(&entry.unwrap().path());
            }
        }
    }

    fn full_fk_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let db = temp.path().join("work.sqlite");
        let bundle = temp.path().join("full-chain.ctxar");
        let store = Store::open(&db).unwrap();
        let source = "70000000-0000-7000-8000-000000000201";
        let workspace = "70000000-0000-7000-8000-000000000202";
        let record = "70000000-0000-7000-8000-000000000203";
        let artifact = "70000000-0000-7000-8000-000000000204";
        let session = "70000000-0000-7000-8000-000000000205";
        let session_edge = "70000000-0000-7000-8000-000000000206";
        let run = "70000000-0000-7000-8000-000000000207";
        let event = "70000000-0000-7000-8000-000000000208";
        let change = "70000000-0000-7000-8000-000000000209";
        let summary = "70000000-0000-7000-8000-000000000210";
        let file = "70000000-0000-7000-8000-000000000211";
        let tag = "70000000-0000-7000-8000-000000000212";
        let link = "70000000-0000-7000-8000-000000000213";
        let record_edge = "70000000-0000-7000-8000-000000000214";
        let object = b"full FK-chain object";
        let blob_hash = format!("{:x}", Sha256::digest(object));
        let object_shard = temp.path().join("objects").join(&blob_hash[..2]);
        std::fs::create_dir_all(&object_shard).unwrap();
        std::fs::write(object_shard.join(&blob_hash), object).unwrap();

        store
            .conn
            .execute(
                "INSERT INTO capture_sources(id,kind,provider,machine_id,started_at_ms,fidelity,visibility,sync_state,sync_version,metadata_json) VALUES(?1,'direct_cli','codex','test',1,'full','local_only','local_only',0,'{}')",
                [source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO vcs_workspaces(id,kind,root_path,repo_fingerprint,host,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,'git','/repo','full-chain','local',1,2,?2,'local_only','full','local_only',0,'{}')",
                params![workspace, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO history_records(id,title,summary,status,primary_vcs_workspace_id,started_at_ms,last_activity_at_ms,completed_at_ms,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json,body,tags_json,kind,workspace,created_at,updated_at) VALUES(?1,'record','record','completed',?2,1,2,2,'high',1,2,?3,'local_only','full','local_only',0,'{}','body','[]','note','/repo','1970-01-01T00:00:00Z','1970-01-01T00:00:00Z')",
                params![record, workspace, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO artifacts(id,kind,blob_hash,blob_path,byte_size,media_type,preview_text,redaction_state,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,'transcript',?2,?3,?4,'text/plain','preview','raw',1,2,?5,'local_only','full','local_only',0,'{}')",
                params![
                    artifact,
                    blob_hash,
                    format!("objects/{}/{}", &blob_hash[..2], blob_hash),
                    object.len() as i64,
                    source
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO sessions(id,history_record_id,root_session_id,capture_source_id,provider,external_session_id,agent_type,is_primary,status,fidelity,transcript_blob_id,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json) VALUES(?1,?2,?1,?3,'codex','full-chain','primary',1,'completed','full',?4,1,2,1,2,'local_only','local_only',0,'{\"source_format\":\"codex-jsonl-v1\"}')",
                params![session, record, source, artifact],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO session_edges(id,from_session_id,to_session_id,edge_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,?2,'delegated','high',1,2,?3,'local_only','full','local_only',0,'{}')",
                params![session_edge, session, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO runs(id,history_record_id,session_id,run_type,status,started_at_ms,ended_at_ms,exit_code,input_blob_id,output_blob_id,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,?3,'command','succeeded',1,2,0,?4,?4,1,2,?5,'local_only','full','local_only',0,'{}')",
                params![run, record, session, artifact, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO events(id,seq,history_record_id,session_id,run_id,event_type,role,occurred_at_ms,capture_source_id,payload_json,payload_blob_id,dedupe_key,visibility,redaction_state,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,1,?2,?3,?4,'message','assistant',2,?5,'{}',?6,'full-chain','local_only','raw','full','local_only',0,'{}')",
                params![event, record, session, run, source, artifact],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO vcs_changes(id,vcs_workspace_id,kind,change_id,parent_change_ids_json,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,'git_commit','full-chain','[]','high',1,2,?3,'local_only','full','local_only',0,'{}')",
                params![change, workspace, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO summaries(id,history_record_id,session_id,kind,model_or_source,text,citations_json,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,?3,'human_note','test','summary',?4,1,2,?5,'local_only','full','local_only',0,'{}')",
                params![
                    summary,
                    record,
                    session,
                    format!(
                        "[{{\"event_id\":\"{}\"}},{{\"vcs_change_id\":\"{}\"}}]",
                        event, change
                    ),
                    source
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO files_touched(id,history_record_id,run_id,event_id,vcs_workspace_id,path,change_kind,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,?3,?4,?5,'src/main.rs','modified',1,2,?6,'local_only','full','local_only',0,'{}')",
                params![file, record, run, event, workspace, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO tags(id,name,kind,created_at_ms,updated_at_ms,metadata_json) VALUES(?1,'full-chain','user',1,2,'{}')",
                [tag],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO history_record_tags(history_record_id,tag_id,source_id,confidence,created_at_ms) VALUES(?1,?2,?3,'high',1)",
                params![record, tag, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO history_record_links(id,history_record_id,target_type,target_id,link_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,'artifact',?3,'references','high',1,2,?4,'local_only','full','local_only',0,'{}')",
                params![link, record, artifact, source],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO record_edges(id,from_record_id,to_record_id,edge_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,metadata_json) VALUES(?1,?2,?2,'related','high',1,2,?3,'local_only','full','local_only',0,'{}')",
                params![record_edge, record, source],
            )
            .unwrap();
        drop(store);

        let mut read_only = Store::open_read_only(&db).unwrap();
        read_only
            .create_selective_archive(&bundle, 2, crate::ArchiveOptions::default())
            .unwrap();
        drop(read_only);
        let mut store = Store::open(&db).unwrap();
        store.register_selective_archive(&bundle).unwrap();
        (temp, db, bundle)
    }

    #[test]
    fn commit_heals_stale_event_map_and_is_idempotent() {
        let (_temp, db, bundle) = fixture();
        let mut store = Store::open(&db).unwrap();
        store.conn.execute("INSERT INTO event_search(event_id,history_record_id,session_id,role,safe_preview_text,rank_bucket) VALUES(?1,'','',NULL,'secret',0)", ["70000000-0000-7000-8000-000000000100"]).unwrap();
        store
            .conn
            .execute(
                "UPDATE event_search_rowids SET search_rowid=999999 WHERE event_id=?1",
                ["70000000-0000-7000-8000-000000000100"],
            )
            .unwrap();
        let report = store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .unwrap();
        assert_eq!(report.deleted_member_count, 4);
        let counts: (i64,i64,i64) = store.conn.query_row("SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM events),(SELECT count(*) FROM event_search)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(counts, (0, 0, 0));
        assert!(
            store
                .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
                .unwrap()
                .duplicate
        );
    }

    #[test]
    fn deletion_uses_declared_child_before_parent_order_for_full_fk_chain() {
        let (_temp, db, bundle) = full_fk_fixture();
        let mut store = Store::open(&db).unwrap();
        let report = store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .unwrap();
        assert_eq!(report.deletion_member_count, 4);
        assert_eq!(report.deleted_member_count, 4);
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        for (table, expected) in [
            ("sessions", 1),
            ("runs", 0),
            ("events", 0),
            ("summaries", 0),
            ("files_touched", 0),
            ("session_edges", 1),
            ("history_records", 1),
            ("artifacts", 1),
            ("vcs_workspaces", 1),
            ("capture_sources", 1),
            ("tags", 1),
            ("history_record_tags", 1),
            ("history_record_links", 1),
            ("record_edges", 1),
            ("vcs_changes", 1),
        ] {
            let sql = format!("SELECT count(*) FROM {table}");
            assert_eq!(
                store
                    .conn
                    .query_row(&sql, [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected,
                "unexpected remaining rows in {table}"
            );
        }
    }

    #[test]
    fn every_deletion_phase_failure_rolls_back_base_projection_and_ledger() {
        for phase in ["projection", "base", "ledger"] {
            let (_temp, db, bundle) = fixture();
            let mut store = Store::open(&db).unwrap();
            store.conn.execute("INSERT INTO event_search(event_id,history_record_id,session_id,role,safe_preview_text,rank_bucket) VALUES(?1,'','',NULL,'secret',0)", ["70000000-0000-7000-8000-000000000100"]).unwrap();
            let before = atomic_snapshot(&store);
            FAIL_PHASE.with(|slot| slot.set(Some(phase)));
            assert!(store
                .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
                .is_err());
            FAIL_PHASE.with(|slot| slot.set(None));
            assert_eq!(atomic_snapshot(&store), before, "phase {phase}");
        }
    }

    #[test]
    fn two_expected_suppressions_refuse_one_missing_association_null_safely() {
        let (_temp, db, bundle) = fixture();
        let mut store = Store::open(&db).unwrap();
        let association: (String, String) = store.conn.query_row(
            "SELECT identity_key,content_key FROM compaction_archive_suppressions ORDER BY identity_key,content_key LIMIT 1",
            [], |row| Ok((row.get(0)?,row.get(1)?))).unwrap();
        store.conn.execute(
            "DELETE FROM compaction_archive_suppressions WHERE identity_key=?1 AND content_key=?2",
            params![association.0,association.1]).unwrap();
        let before: (i64, i64) = store
            .conn
            .query_row(
                "SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM events)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let error = store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("complete active registered suppression coverage"));
        let after: (i64, i64) = store
            .conn
            .query_row(
                "SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM events)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn conflicted_or_overridden_suppression_refuses() {
        for (association_state, fact_state) in
            [("conflict", "conflict"), ("overridden", "overridden")]
        {
            let (_temp, db, bundle) = fixture();
            let mut store = Store::open(&db).unwrap();
            let pair: (String, String) = store
                .conn
                .query_row(
                    "SELECT identity_key,content_key FROM compaction_archive_suppressions LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            store.conn.execute("UPDATE compaction_archive_suppressions SET association_state=?3 WHERE identity_key=?1 AND content_key=?2", params![pair.0,pair.1,association_state]).unwrap();
            store.conn.execute("UPDATE compaction_suppression_facts SET effective_state=?3 WHERE identity_key=?1 AND content_key=?2", params![pair.0,pair.1,fact_state]).unwrap();
            assert!(store
                .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
                .is_err());
            assert_eq!(
                store
                    .conn
                    .query_row("SELECT count(*) FROM sessions", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                2
            );
        }
    }

    #[test]
    fn unsuppressible_selected_root_refuses() {
        let temp = tempdir().unwrap();
        let db = temp.path().join("work.sqlite");
        let bundle = temp.path().join("selected.ctxar");
        let store = Store::open(&db).unwrap();
        store.conn.execute("INSERT INTO sessions(id,provider,agent_type,is_primary,status,fidelity,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json) VALUES('70000000-0000-7000-8000-000000000200','codex','primary',1,'completed','full',1,2,1,2,'local_only','local_only',0,'{}')", []).unwrap();
        drop(store);
        let mut read_only = Store::open_read_only(&db).unwrap();
        read_only
            .create_selective_archive(&bundle, 2, crate::ArchiveOptions::default())
            .unwrap();
        drop(read_only);
        let mut store = Store::open(&db).unwrap();
        assert_eq!(
            store
                .register_selective_archive(&bundle)
                .unwrap()
                .unsuppressible_count,
            1
        );
        assert!(store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .is_err());
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM sessions", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn valid_stale_and_hijacked_rowids_heal_for_record_and_event_projections() {
        let temp = tempdir().unwrap();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        for id in [
            "record-valid",
            "record-stale",
            "record-hijacked",
            "record-survivor",
        ] {
            store.conn.execute("INSERT INTO ctx_history_search(record_id,title,summary,primary_user_text,decision_text,context_text,tag_text) VALUES(?1,'x','','','','','')", [id]).unwrap();
            let rowid = store.conn.last_insert_rowid();
            let mapped = match id {
                "record-stale" => 999_991,
                "record-hijacked" => rowid + 1,
                _ => rowid,
            };
            if id != "record-survivor" {
                store
                    .conn
                    .execute(
                        "INSERT INTO record_search_rowids VALUES(?1,?2)",
                        params![id, mapped],
                    )
                    .unwrap();
            }
        }
        for id in [
            "event-valid",
            "event-stale",
            "event-hijacked",
            "event-survivor",
        ] {
            store.conn.execute("INSERT INTO event_search(event_id,history_record_id,session_id,role,safe_preview_text,rank_bucket) VALUES(?1,'','',NULL,'x',0)", [id]).unwrap();
            let rowid = store.conn.last_insert_rowid();
            let mapped = match id {
                "event-stale" => 999_992,
                "event-hijacked" => rowid + 1,
                _ => rowid,
            };
            if id != "event-survivor" {
                store
                    .conn
                    .execute(
                        "INSERT INTO event_search_rowids VALUES(?1,?2)",
                        params![id, mapped],
                    )
                    .unwrap();
            }
        }
        for id in ["record-valid", "record-stale", "record-hijacked"] {
            RECORD_SEARCH_ROWID_MAP
                .delete_projection_rows(&store.conn, id)
                .unwrap();
        }
        for id in ["event-valid", "event-stale", "event-hijacked"] {
            EVENT_SEARCH_ROWID_MAP
                .delete_projection_rows(&store.conn, id)
                .unwrap();
        }
        let records: String = store
            .conn
            .query_row(
                "SELECT group_concat(record_id) FROM ctx_history_search",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let events: String = store
            .conn
            .query_row(
                "SELECT group_concat(event_id) FROM event_search",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(records, "record-survivor");
        assert_eq!(events, "event-survivor");
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM record_search_rowids", [], |row| row
                    .get::<_, i64>(
                    0
                ))
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM event_search_rowids", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn dry_run_is_atomic_and_content_or_new_incoming_reference_refuses() {
        let (_temp, db, bundle) = fixture();
        let mut store = Store::open(&db).unwrap();
        let before = atomic_snapshot(&store);
        let report = store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions { dry_run: true })
            .unwrap();
        assert!(report.dry_run);
        assert_eq!(atomic_snapshot(&store), before);
        store.conn.execute("UPDATE events SET payload_json='{\"changed\":true}' WHERE id='70000000-0000-7000-8000-000000000100'", []).unwrap();
        assert!(store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .is_err());

        let (_temp, db, bundle) = fixture();
        let mut store = Store::open(&db).unwrap();
        store.conn.execute("INSERT INTO sessions(id,parent_session_id,provider,agent_type,is_primary,status,fidelity,started_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json) VALUES('70000000-0000-7000-8000-000000000104','70000000-0000-7000-8000-000000000099','codex','primary',1,'active','full',3,3,3,'local_only','local_only',0,'{}')", []).unwrap();
        assert!(store
            .commit_archive_deletion(&bundle, ArchiveDeletionOptions::default())
            .is_err());
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM sessions", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            3
        );
    }
}
