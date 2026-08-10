//! Durable, path-free reimport suppression ledger.

use std::path::Path;

use rusqlite::{params, OptionalExtension, Transaction};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    archive::verify_archive_bundle_internal, utc_now, ArchiveVerifyOptions, Result, Store,
    StoreError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuppressionIdentity {
    pub identity_key: String,
    pub content_key: String,
}

pub struct ProviderSuppressionSession<'a> {
    pub provider: &'a str,
    pub source_format: &'a str,
    pub external_id: &'a str,
    pub external_agent_id: Option<&'a str>,
    pub agent_type: &'a str,
    pub role_hint: Option<&'a str>,
    pub is_primary: bool,
    pub status: &'a str,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
}

/// One path-free member of a normalized provider session's prospective write
/// set. `kind` and `key` provide deterministic ordering; `value` is encoded
/// with explicit JSON type and null framing rather than textual JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct SuppressionWrite {
    pub kind: &'static str,
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressionDecision {
    Allow,
    Suppress,
    Conflict,
    AllowAudited,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveRegistrationReport {
    pub archive_id: String,
    pub member_count: u64,
    pub suppression_count: u64,
    pub unsuppressible_count: u64,
    pub duplicate: bool,
    #[serde(skip)]
    pub suppressions: Vec<SuppressionIdentity>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SuppressionOverrideReport {
    pub operation_id: String,
    pub affected_associations: u64,
    pub effective_state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SuppressionStatus {
    pub active: u64,
    pub conflict: u64,
    pub restored: u64,
    pub overridden: u64,
}

fn digest(fields: &[&[u8]]) -> String {
    let mut h = Sha256::new();
    h.update(b"ctx-compaction/provider-identity/v1");
    h.update((fields.len() as u32).to_be_bytes());
    for field in fields {
        h.update((field.len() as u64).to_be_bytes());
        h.update(field);
    }
    hex_lower(&h.finalize())
}

fn frame_value(h: &mut Sha256, value: &Value) {
    match value {
        Value::Null => h.update(b"n"),
        Value::Bool(value) => h.update(if *value { b"b1" } else { b"b0" }),
        Value::Number(value) => {
            h.update(b"d");
            let text = value.to_string();
            h.update((text.len() as u64).to_be_bytes());
            h.update(text.as_bytes());
        }
        Value::String(value) => {
            h.update(b"s");
            h.update((value.len() as u64).to_be_bytes());
            h.update(value.as_bytes());
        }
        Value::Array(values) => {
            h.update(b"a");
            h.update((values.len() as u64).to_be_bytes());
            for value in values {
                frame_value(h, value);
            }
        }
        Value::Object(values) => {
            h.update(b"o");
            h.update((values.len() as u64).to_be_bytes());
            let mut fields = values.iter().collect::<Vec<_>>();
            fields.sort_by_key(|(name, _)| *name);
            for (name, value) in fields {
                h.update((name.len() as u64).to_be_bytes());
                h.update(name.as_bytes());
                frame_value(h, value);
            }
        }
    }
}

/// Remove import-observation and machine/path fields from normalized metadata.
/// This is intentionally applied only to metadata, never transcript payloads
/// or file-touch paths.
pub fn suppression_metadata(mut value: Value) -> Value {
    fn scrub(value: &mut Value) {
        match value {
            Value::Object(fields) => {
                for key in [
                    "cursor",
                    "cwd",
                    "fixture_line",
                    "imported_at",
                    "machine_id",
                    "observed_at",
                    "old_path",
                    "path",
                    "raw_source_path",
                    "raw_uri",
                    "root_path",
                    "source_path",
                ] {
                    fields.remove(key);
                }
                for value in fields.values_mut() {
                    scrub(value);
                }
            }
            Value::Array(values) => values.iter_mut().for_each(scrub),
            _ => {}
        }
    }
    scrub(&mut value);
    value
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 15) as usize] as char);
    }
    out
}

pub(crate) fn derive_effective_state(
    tx: &Transaction<'_>,
    identity_key: &str,
    content_key: &str,
    now: i64,
) -> Result<&'static str> {
    let association_state: (bool, bool) = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM compaction_archive_suppressions WHERE identity_key=?1 AND content_key=?2 AND association_state='active'),
                EXISTS(SELECT 1 FROM compaction_archive_suppressions WHERE identity_key=?1 AND content_key=?2 AND association_state='restored')",
        params![identity_key, content_key],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let conflict: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM compaction_suppression_conflicts WHERE identity_key=?1 AND content_key=?2 AND conflict_state='active')",
        params![identity_key, content_key],
        |row| row.get(0),
    )?;
    let state = if association_state.0 {
        "active"
    } else if conflict {
        "conflict"
    } else if association_state.1 {
        "restored"
    } else {
        "overridden"
    };
    tx.execute(
        "UPDATE compaction_suppression_facts SET effective_state=?3,updated_at_ms=?4,last_error_code=CASE WHEN ?3='conflict' THEN 'content_changed' ELSE NULL END WHERE identity_key=?1 AND content_key=?2",
        params![identity_key, content_key, state, now],
    )?;
    Ok(state)
}

impl SuppressionIdentity {
    /// Canonical provider identity. Callers must pass provider-format stable
    /// values only; paths, observation times and machine-local catalog data do
    /// not belong here.
    pub fn provider(
        provider: &str,
        source_format: &str,
        external_id: &str,
        external_agent_id: Option<&str>,
        origin_device_id: Option<&str>,
        content_key: String,
    ) -> Self {
        Self {
            identity_key: digest(&[
                provider.as_bytes(),
                source_format.as_bytes(),
                external_id.as_bytes(),
                external_agent_id.unwrap_or("").as_bytes(),
                origin_device_id.unwrap_or("").as_bytes(),
            ]),
            content_key,
        }
    }

    pub fn normalized_session(session: ProviderSuppressionSession<'_>) -> Self {
        Self::normalized_write_set(session, Vec::new())
    }

    /// Canonical v2 suppression digest over the complete normalized write set.
    /// Callers exclude only import-local source paths, machine identity and
    /// observation timestamps when constructing records.
    pub fn normalized_write_set(
        session: ProviderSuppressionSession<'_>,
        mut writes: Vec<SuppressionWrite>,
    ) -> Self {
        let ProviderSuppressionSession {
            provider,
            source_format,
            external_id,
            external_agent_id,
            agent_type,
            role_hint,
            is_primary,
            status,
            started_at_ms,
            ended_at_ms,
        } = session;
        let identity_key = digest(&[
            b"provider-session",
            provider.as_bytes(),
            source_format.as_bytes(),
            external_id.as_bytes(),
            external_agent_id.unwrap_or("").as_bytes(),
        ]);
        writes.push(SuppressionWrite {
            kind: "session",
            key: external_id.to_owned(),
            value: serde_json::json!({
                "agent_type": agent_type,
                "ended_at_ms": ended_at_ms,
                "external_agent_id": external_agent_id,
                "is_primary": is_primary,
                "role_hint": role_hint,
                "started_at_ms": started_at_ms,
                "status": status,
            }),
        });
        writes.sort_by(|left, right| {
            (left.kind, left.key.as_str(), left.value.to_string()).cmp(&(
                right.kind,
                right.key.as_str(),
                right.value.to_string(),
            ))
        });
        let mut content = Sha256::new();
        content.update(b"ctx-compaction/provider-write-set/v2");
        content.update((writes.len() as u64).to_be_bytes());
        for write in writes {
            content.update((write.kind.len() as u64).to_be_bytes());
            content.update(write.kind.as_bytes());
            content.update((write.key.len() as u64).to_be_bytes());
            content.update(write.key.as_bytes());
            frame_value(&mut content, &write.value);
        }
        let content_key = hex_lower(&content.finalize());
        Self {
            identity_key,
            content_key,
        }
    }
}

impl Store {
    pub fn suppression_guard_required(&self) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM compaction_suppression_facts WHERE effective_state IN ('active','conflict'))",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    /// Register and activate only the verifier's reconstructed selective plan.
    /// The archive path is never persisted. All rows commit together.
    pub fn register_selective_archive(
        &mut self,
        bundle: &Path,
    ) -> Result<ArchiveRegistrationReport> {
        let verified = verify_archive_bundle_internal(bundle, ArchiveVerifyOptions::default())?;
        if verified.manifest.format != "ctx-selective-archive" {
            return Err(StoreError::Archive(
                "only a complete verified selective archive can be registered".into(),
            ));
        }
        let plan =
            verified.manifest.selective_plan.as_ref().ok_or_else(|| {
                StoreError::Archive("verified selective evidence is missing".into())
            })?;
        let suppression_facts = verified.selective_suppression_facts()?;
        let suppressible_candidates = plan
            .members
            .iter()
            .filter(|member| {
                member.entity_kind == "sessions"
                    && matches!(member.disposition.as_str(), "selected_root" | "owned_child")
            })
            .count() as u64;
        let unsuppressible_count =
            suppressible_candidates.saturating_sub(suppression_facts.len() as u64);
        let archive_id = verified.manifest.archive_id.to_string();
        let manifest_sha = verified.manifest_sha256.clone();
        let now = utc_now().timestamp_millis();
        let tx = self.conn.transaction()?;
        let duplicate: bool = tx
            .query_row(
                "SELECT 1 FROM compaction_archives WHERE archive_id=?1",
                [&archive_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if duplicate {
            let existing: String = tx.query_row(
                "SELECT manifest_sha256 FROM compaction_archives WHERE archive_id=?1",
                [&archive_id],
                |r| r.get(0),
            )?;
            if existing != manifest_sha {
                return Err(StoreError::Archive(
                    "archive identity conflicts with registered manifest".into(),
                ));
            }
            tx.commit()?;
            let suppression_count = self.conn.query_row(
                "SELECT count(*) FROM compaction_archive_suppressions WHERE archive_id=?1",
                [&archive_id],
                |r| r.get::<_, u64>(0),
            )?;
            return Ok(ArchiveRegistrationReport {
                archive_id: archive_id.clone(),
                member_count: plan.members.len() as u64,
                suppression_count,
                unsuppressible_count,
                duplicate: true,
                suppressions: self.conn.prepare("SELECT identity_key,content_key FROM compaction_archive_suppressions WHERE archive_id=?1 ORDER BY identity_key,content_key")?.query_map([&archive_id], |row| Ok(SuppressionIdentity { identity_key: row.get(0)?, content_key: row.get(1)? }))?.collect::<rusqlite::Result<Vec<_>>>()?,
            });
        }
        tx.execute("INSERT INTO compaction_archives VALUES(?1,'ctx-selective-archive',1,'selective',?2,?3,?4,?5,?6,?7,?8,?9,NULL,?10,?10,?10,?10)", params![archive_id,manifest_sha,plan.plan_digest,plan.closure_digest,plan.membership_digest,plan.root_set_digest,plan.deletion_set_digest,plan.deletion_authorized_count,verified.manifest.source_schema_version,now])?;
        for root in &plan.roots {
            tx.execute(
                "INSERT INTO compaction_archive_roots VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    archive_id,
                    root.session_id,
                    root.disposition,
                    plan.cutoff_ms,
                    root.observed_status,
                    root.observed_ended_at_ms,
                    root.closure_digest,
                    root.member_count,
                    root.deletion_member_count,
                    now
                ],
            )?;
        }
        let mut suppression_count = 0_u64;
        for member in &plan.members {
            tx.execute("INSERT INTO compaction_archive_members VALUES(?1,?2,?3,?4,?5,?6,'verified','present',?7,?7)", params![archive_id,member.entity_kind,member.entity_key,member.content_key,member.disposition,member.ownership,now])?;
            if member.deletion_authorized {
                tx.execute(
                    "INSERT INTO compaction_deletion_members VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        archive_id,
                        member.entity_kind,
                        member.entity_key,
                        member.content_key,
                        member.disposition,
                        now
                    ],
                )?;
            }
        }
        for fact in &suppression_facts {
            tx.execute("INSERT INTO compaction_suppression_facts VALUES(?1,?2,'overridden',NULL,?3,?3,NULL) ON CONFLICT(identity_key,content_key) DO NOTHING", params![fact.identity_key,fact.content_key,now])?;
            tx.execute("INSERT INTO compaction_archive_suppressions VALUES(?1,?2,?3,'active',NULL,?4,?4,NULL)", params![archive_id,fact.identity_key,fact.content_key,now])?;
            derive_effective_state(&tx, &fact.identity_key, &fact.content_key, now)?;
            suppression_count += 1;
        }
        let request = digest(&[b"register", manifest_sha.as_bytes()]);
        tx.execute("INSERT INTO compaction_operations VALUES(?1,'archive_register',?2,?3,'none','committed',1,NULL,?4,?4)",params![Uuid::now_v7().to_string(),archive_id,request,now])?;
        tx.commit()?;
        Ok(ArchiveRegistrationReport {
            archive_id,
            member_count: plan.members.len() as u64,
            suppression_count,
            unsuppressible_count,
            duplicate: false,
            suppressions: suppression_facts,
        })
    }

    pub fn suppression_decision(&self, key: &SuppressionIdentity) -> Result<SuppressionDecision> {
        let state: Option<String>=self.conn.query_row("SELECT effective_state FROM compaction_suppression_facts WHERE identity_key=?1 AND content_key=?2",params![key.identity_key,key.content_key],|r|r.get(0)).optional()?;
        if let Some(state) = state.as_deref() {
            return Ok(match state {
                "active" => SuppressionDecision::Suppress,
                "conflict" => SuppressionDecision::Conflict,
                _ => SuppressionDecision::AllowAudited,
            });
        }
        if state.is_none() {
            let active_other: bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM compaction_suppression_facts WHERE identity_key=?1 AND effective_state='active')",[&key.identity_key],|r|r.get(0))?;
            return Ok(if active_other {
                SuppressionDecision::Conflict
            } else {
                SuppressionDecision::Allow
            });
        }
        Ok(SuppressionDecision::Allow)
    }

    /// Shared pre-canonical-write guard. A changed-content observation is
    /// durably recorded as bounded metadata before returning `Conflict`.
    pub fn guard_import(&mut self, key: &SuppressionIdentity) -> Result<SuppressionDecision> {
        let decision = self.suppression_decision(key)?;
        if decision != SuppressionDecision::Conflict {
            return Ok(decision);
        }
        let now = utc_now().timestamp_millis();
        let own_transaction = self.conn.is_autocommit();
        if own_transaction {
            self.conn.execute_batch("BEGIN IMMEDIATE")?;
        }
        let result = (|| -> Result<()> {
            self.conn.execute(
            "INSERT INTO compaction_suppression_facts VALUES(?1,?2,'conflict',NULL,?3,?3,'content_changed')
             ON CONFLICT(identity_key,content_key) DO UPDATE SET effective_state='conflict',updated_at_ms=excluded.updated_at_ms,last_error_code='content_changed'",
            params![key.identity_key, key.content_key, now],
        )?;
            self.conn.execute(
            "INSERT INTO compaction_suppression_conflicts VALUES(?1,?2,'active',NULL,?3,?3)
             ON CONFLICT(identity_key,content_key) DO UPDATE SET conflict_state='active',resolution=NULL,updated_at_ms=excluded.updated_at_ms",
            params![key.identity_key, key.content_key, now],
        )?;
            Ok(())
        })();
        match result {
            Ok(()) if own_transaction => self.conn.execute_batch("COMMIT")?,
            Err(error) if own_transaction => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
            Err(error) => return Err(error),
            Ok(()) => {}
        }
        Ok(SuppressionDecision::Conflict)
    }

    pub fn suppression_status(&self) -> Result<SuppressionStatus> {
        let count = |state: &str| {
            self.conn
                .query_row(
                    "SELECT count(*) FROM compaction_suppression_facts WHERE effective_state=?1",
                    [state],
                    |r| r.get::<_, u64>(0),
                )
                .map_err(StoreError::from)
        };
        Ok(SuppressionStatus {
            active: count("active")?,
            conflict: count("conflict")?,
            restored: count("restored")?,
            overridden: count("overridden")?,
        })
    }

    pub fn override_suppression(
        &mut self,
        archive_id: Option<&str>,
        identity_key: &str,
        content_key: &str,
        reason: &str,
    ) -> Result<SuppressionOverrideReport> {
        if reason.trim().is_empty() || reason.len() > 128 {
            return Err(StoreError::Archive(
                "override reason must contain 1..128 bytes".into(),
            ));
        }
        let now = utc_now().timestamp_millis();
        let tx = self.conn.transaction()?;
        let changed = if let Some(archive_id) = archive_id {
            tx.execute("UPDATE compaction_archive_suppressions SET association_state='overridden',updated_at_ms=?4,last_error_code='explicit_override' WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3 AND association_state='active'",params![archive_id,identity_key,content_key,now])? as u64
        } else {
            0
        };
        let conflict_changed = if changed == 0 {
            tx.execute("UPDATE compaction_suppression_conflicts SET conflict_state='resolved',resolution='override',updated_at_ms=?3 WHERE identity_key=?1 AND content_key=?2 AND conflict_state='active'",params![identity_key,content_key,now])? as u64
        } else {
            0
        };
        if changed == 0 && conflict_changed == 0 {
            return Err(StoreError::Archive(
                "active archive association or conflict fact not found".into(),
            ));
        }
        let state = derive_effective_state(&tx, identity_key, content_key, now)?;
        let operation_id = Uuid::now_v7().to_string();
        let request = digest(&[
            archive_id.unwrap_or("").as_bytes(),
            identity_key.as_bytes(),
            content_key.as_bytes(),
            reason.as_bytes(),
        ]);
        tx.execute("INSERT INTO compaction_operations VALUES(?1,'override',?2,?3,'none','committed',1,NULL,?4,?4)",params![operation_id,archive_id,request,now])?;
        tx.commit()?;
        Ok(SuppressionOverrideReport {
            operation_id,
            affected_associations: changed,
            effective_state: state.into(),
        })
    }

    /// #286 must call this inside the same transaction that restores and
    /// verifies the canonical rows. The marker contains no restored content;
    /// it is a digest of #286's canonical restore result and makes a later
    /// handoff recoverable after process exit.
    pub fn record_restore_suppression_marker(
        &self,
        archive_id: &str,
        identity_key: &str,
        content_key: &str,
        canonical_marker: &str,
    ) -> Result<()> {
        if canonical_marker.len() != 64
            || !canonical_marker
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(StoreError::Archive(
                "canonical restore marker must be 64 lowercase hex bytes".into(),
            ));
        }
        let now = utc_now().timestamp_millis();
        self.conn.execute(
            "INSERT INTO compaction_restore_markers VALUES(?1,?2,?3,?4,?5) ON CONFLICT(archive_id,identity_key,content_key) DO NOTHING",
            params![archive_id, identity_key, content_key, canonical_marker, now],
        )?;
        let stored: String = self.conn.query_row(
            "SELECT canonical_marker FROM compaction_restore_markers WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3",
            params![archive_id, identity_key, content_key],
            |row| row.get(0),
        )?;
        if stored != canonical_marker {
            return Err(StoreError::Archive(
                "canonical restore marker conflicts with prior handoff".into(),
            ));
        }
        Ok(())
    }

    /// Reconcile a durable #286 canonical marker into suppression state. The
    /// operation and association transition commit atomically; an identical
    /// repeat returns the original successful operation.
    pub fn handoff_restored_suppression(
        &mut self,
        archive_id: &str,
        identity_key: &str,
        content_key: &str,
        canonical_marker: &str,
        reason: &str,
    ) -> Result<SuppressionOverrideReport> {
        if reason.trim().is_empty() || reason.len() > 128 {
            return Err(StoreError::Archive(
                "restore reason must contain 1..128 bytes".into(),
            ));
        }
        let now = utc_now().timestamp_millis();
        let request = digest(&[
            archive_id.as_bytes(),
            identity_key.as_bytes(),
            content_key.as_bytes(),
            canonical_marker.as_bytes(),
            reason.as_bytes(),
        ]);
        let tx = self.conn.transaction()?;
        let marker_exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM compaction_restore_markers WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3 AND canonical_marker=?4)",
            params![archive_id, identity_key, content_key, canonical_marker],
            |row| row.get(0),
        )?;
        if !marker_exists {
            return Err(StoreError::Archive(
                "canonical restore marker is not durably committed".into(),
            ));
        }
        let existing: Option<(String, String)> = tx.query_row(
            "SELECT operation_id,phase FROM compaction_operations WHERE operation_kind='restore' AND archive_id=?1 AND request_digest=?2",
            params![archive_id, request],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if let Some((operation_id, phase)) = &existing {
            if phase == "committed" {
                let state: String = tx.query_row(
                    "SELECT effective_state FROM compaction_suppression_facts WHERE identity_key=?1 AND content_key=?2",
                    params![identity_key, content_key],
                    |row| row.get(0),
                )?;
                tx.commit()?;
                return Ok(SuppressionOverrideReport {
                    operation_id: operation_id.clone(),
                    affected_associations: 0,
                    effective_state: state,
                });
            }
        }
        let operation_id = existing
            .map(|(id, _)| id)
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        tx.execute(
            "INSERT INTO compaction_operations VALUES(?1,'restore',?2,?3,'none','started',1,NULL,?4,?4) ON CONFLICT DO UPDATE SET attempt_count=attempt_count+1,updated_at_ms=excluded.updated_at_ms",
            params![operation_id, archive_id, request, now],
        )?;
        restore_handoff_fault("before_ledger")?;
        let changed = tx.execute(
            "UPDATE compaction_archive_suppressions SET association_state='restored',updated_at_ms=?4,last_error_code=NULL WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3 AND association_state IN ('active','overridden')",
            params![archive_id, identity_key, content_key, now],
        )? as u64;
        let already_restored: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM compaction_archive_suppressions WHERE archive_id=?1 AND identity_key=?2 AND content_key=?3 AND association_state='restored')",
            params![archive_id, identity_key, content_key],
            |row| row.get(0),
        )?;
        if changed == 0 && !already_restored {
            return Err(StoreError::Archive(
                "archive suppression association not found".into(),
            ));
        }
        let state = derive_effective_state(&tx, identity_key, content_key, now)?;
        restore_handoff_fault("after_ledger")?;
        tx.execute(
            "UPDATE compaction_operations SET phase='committed',last_error_code=NULL,updated_at_ms=?2 WHERE operation_id=?1",
            params![operation_id, now],
        )?;
        tx.commit()?;
        restore_handoff_fault("after_commit")?;
        Ok(SuppressionOverrideReport {
            operation_id,
            affected_associations: changed,
            effective_state: state.into(),
        })
    }
}

#[cfg(test)]
thread_local! {
    static RESTORE_HANDOFF_FAULT: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
}

fn restore_handoff_fault(phase: &str) -> Result<()> {
    #[cfg(test)]
    if RESTORE_HANDOFF_FAULT
        .with(|fault| fault.borrow().as_ref().is_some_and(|value| *value == phase))
    {
        return Err(StoreError::Archive(format!(
            "injected restore handoff fault: {phase}"
        )));
    }
    let _ = phase;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn path_and_local_observation_do_not_participate_in_keys() {
        let input = || ProviderSuppressionSession {
            provider: "codex",
            source_format: "codex-jsonl-v1",
            external_id: "stable",
            external_agent_id: Some("agent"),
            agent_type: "primary",
            role_hint: None,
            is_primary: true,
            status: "completed",
            started_at_ms: 1,
            ended_at_ms: Some(2),
        };
        let left = SuppressionIdentity::normalized_session(input());
        let right = SuppressionIdentity::normalized_session(input());
        assert_eq!(left, right);
    }

    #[test]
    fn verified_selective_registration_is_idempotent_and_does_not_touch_search() {
        let temp = tempdir().unwrap();
        let db = temp.path().join("work.sqlite");
        let bundle = temp.path().join("selected.ctxar");
        let store = Store::open(&db).unwrap();
        store.conn.execute(
            "INSERT INTO sessions(id,provider,external_session_id,agent_type,is_primary,status,fidelity,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,metadata_json)
             VALUES('70000000-0000-7000-8000-000000000099','codex','stable','primary',1,'completed','full',1,2,1,2,'local_only','local_only',0,'{\"source_format\":\"codex-jsonl-v1\"}')",
            [],
        ).unwrap();
        drop(store);
        let mut readonly = Store::open_read_only(&db).unwrap();
        assert_eq!(
            readonly.plan_compaction(2).unwrap().selected_root_ids.len(),
            1
        );
        readonly
            .create_selective_archive(&bundle, 2, super::super::ArchiveOptions::default())
            .unwrap();
        drop(readonly);
        let mut store = Store::open(&db).unwrap();
        let before: (i64,i64,i64,i64)=store.conn.query_row("SELECT (SELECT count(*) FROM ctx_history_search),(SELECT count(*) FROM event_search),(SELECT count(*) FROM record_search_rowids),(SELECT count(*) FROM event_search_rowids)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        let first = store.register_selective_archive(&bundle).unwrap();
        assert!(!first.duplicate);
        assert_eq!(first.suppression_count, 0, "{first:?}");
        let second = store.register_selective_archive(&bundle).unwrap();
        assert!(second.duplicate);
        let after: (i64,i64,i64,i64)=store.conn.query_row("SELECT (SELECT count(*) FROM ctx_history_search),(SELECT count(*) FROM event_search),(SELECT count(*) FROM record_search_rowids),(SELECT count(*) FROM event_search_rowids)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn restore_handoff_marker_and_operation_recover_every_crash_boundary() {
        let temp = tempdir().unwrap();
        let mut store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let archive_id = "70000000-0000-7000-8000-000000000001";
        let identity = "1".repeat(64);
        let content = "2".repeat(64);
        let marker = "3".repeat(64);
        let hash = "a".repeat(64);
        store.conn.execute("INSERT INTO compaction_archives VALUES(?1,'ctx-selective-archive',1,'selective',?2,?2,?2,?2,?2,?2,0,1005,NULL,1,1,1,1)", params![archive_id,hash]).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO compaction_suppression_facts VALUES(?1,?2,'active',NULL,1,1,NULL)",
                params![identity, content],
            )
            .unwrap();
        store.conn.execute("INSERT INTO compaction_archive_suppressions VALUES(?1,?2,?3,'active',NULL,1,1,NULL)", params![archive_id,identity,content]).unwrap();

        assert!(store
            .handoff_restored_suppression(archive_id, &identity, &content, &marker, "verified")
            .is_err());
        store.begin_immediate_batch().unwrap();
        store
            .record_restore_suppression_marker(archive_id, &identity, &content, &marker)
            .unwrap();
        store.rollback_batch().unwrap();
        assert!(store
            .handoff_restored_suppression(archive_id, &identity, &content, &marker, "verified")
            .is_err());

        store
            .record_restore_suppression_marker(archive_id, &identity, &content, &marker)
            .unwrap();
        for phase in ["before_ledger", "after_ledger"] {
            RESTORE_HANDOFF_FAULT.with(|fault| *fault.borrow_mut() = Some(phase));
            assert!(store
                .handoff_restored_suppression(archive_id, &identity, &content, &marker, "verified",)
                .is_err());
            RESTORE_HANDOFF_FAULT.with(|fault| *fault.borrow_mut() = None);
            assert_eq!(
                store
                    .suppression_decision(&SuppressionIdentity {
                        identity_key: identity.clone(),
                        content_key: content.clone(),
                    })
                    .unwrap(),
                SuppressionDecision::Suppress
            );
        }

        RESTORE_HANDOFF_FAULT.with(|fault| *fault.borrow_mut() = Some("after_commit"));
        assert!(store
            .handoff_restored_suppression(archive_id, &identity, &content, &marker, "verified")
            .is_err());
        RESTORE_HANDOFF_FAULT.with(|fault| *fault.borrow_mut() = None);
        let repeated = store
            .handoff_restored_suppression(archive_id, &identity, &content, &marker, "verified")
            .unwrap();
        assert_eq!(repeated.affected_associations, 0);
        assert_eq!(repeated.effective_state, "restored");
        let operations: i64 = store.conn.query_row(
            "SELECT count(*) FROM compaction_operations WHERE operation_kind='restore' AND phase='committed'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(operations, 1);
    }
}
