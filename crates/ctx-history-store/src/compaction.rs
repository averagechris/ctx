//! Deterministic, read-only selective-compaction planning.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rusqlite::{types::ValueRef, Transaction};
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Result, Store, StoreError, SCHEMA_VERSION};

const ALGORITHM: &str = "ctx-compaction-directional-closure/v1";
const STREAMS: &[(&str, &str, &str)] = &[
    ("capture_sources", "id", "id,kind,provider,machine_id,process_id,cwd,raw_source_path,external_session_id,started_at_ms,ended_at_ms,fidelity,visibility,sync_state,sync_version,metadata_json"),
    ("vcs_workspaces", "id", "id,kind,root_path,repo_fingerprint,primary_remote_url_normalized,host,owner,name,monorepo_subpath,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("history_records", "id", "id,title,summary,status,primary_vcs_workspace_id,started_at_ms,last_activity_at_ms,completed_at_ms,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json,body,tags_json,kind,workspace"),
    ("artifacts", "id", "id,kind,blob_hash,byte_size,media_type,preview_text,redaction_state,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("sessions", "id", "id,history_record_id,parent_session_id,root_session_id,capture_source_id,provider,external_session_id,external_agent_id,agent_type,role_hint,is_primary,status,fidelity,transcript_blob_id,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms,visibility,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("session_edges", "id", "id,from_session_id,to_session_id,edge_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("runs", "id", "id,history_record_id,session_id,run_type,status,started_at_ms,ended_at_ms,exit_code,cwd,command_preview,input_blob_id,output_blob_id,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("events", "id", "id,seq,history_record_id,session_id,run_id,event_type,role,occurred_at_ms,capture_source_id,payload_json,payload_blob_id,dedupe_key,visibility,redaction_state,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("vcs_changes", "id", "id,vcs_workspace_id,kind,change_id,parent_change_ids_json,branch_or_bookmark,tree_hash,author_time_ms,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("summaries", "id", "id,history_record_id,session_id,kind,model_or_source,text,citations_json,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("files_touched", "id", "id,history_record_id,run_id,event_id,vcs_workspace_id,path,change_kind,old_path,line_count_delta,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("tags", "id", "id,name,kind,created_at_ms,updated_at_ms,metadata_json"),
    ("history_record_tags", "history_record_id || ':' || tag_id", "history_record_id,tag_id,source_id,confidence,created_at_ms"),
    ("history_record_links", "id", "id,history_record_id,target_type,target_id,link_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
    ("record_edges", "id", "id,from_record_id,to_record_id,edge_type,confidence,created_at_ms,updated_at_ms,source_id,visibility,fidelity,sync_state,sync_version,deleted_at_ms,metadata_json"),
];

#[derive(Debug, Clone, Serialize)]
pub struct CompactionRootDecision {
    pub session_id: String,
    pub disposition: String,
    pub rationale: String,
    pub observed_status: String,
    pub observed_ended_at_ms: Option<i64>,
    pub closure_digest: Option<String>,
    pub member_count: u64,
    pub deletion_member_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactionPlanMember {
    pub entity_kind: String,
    pub entity_key: String,
    pub content_key: String,
    pub disposition: String,
    pub ownership: String,
    pub deletion_authorized: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactionEstimate {
    pub bytes: Option<u64>,
    pub reason_unknown: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactionPlan {
    pub format: &'static str,
    pub format_version: u32,
    pub private: bool,
    pub source_schema_version: i64,
    pub cutoff_ms: i64,
    pub graph_algorithm: &'static str,
    pub roots: Vec<CompactionRootDecision>,
    pub members: Vec<CompactionPlanMember>,
    pub selected_root_ids: Vec<String>,
    pub ambiguous_root_count: u64,
    pub retained_root_count: u64,
    pub closure_counts_by_kind: BTreeMap<String, u64>,
    pub deletion_authorized_count: u64,
    pub shared_retained_count: u64,
    pub logical_bytes: u64,
    pub object_bytes: u64,
    pub expected_selective_archive_bytes: u64,
    pub current_sqlite_freelist_bytes: CompactionEstimate,
    pub plan_attributable_reclaimable_bytes: CompactionEstimate,
    pub available_space_bytes: CompactionEstimate,
    pub temporary_space_required_bytes: CompactionEstimate,
    pub root_set_digest: String,
    pub closure_digest: String,
    pub membership_digest: String,
    pub deletion_set_digest: String,
    pub plan_digest: String,
}

#[derive(Clone)]
struct CanonicalRow {
    fields: Map<String, Value>,
    canonical: Vec<CanonicalValue>,
    bytes: Vec<u8>,
}
#[derive(Clone)]
enum CanonicalValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(String),
}
type Rows = BTreeMap<String, BTreeMap<String, CanonicalRow>>;

impl Store {
    /// Plan compaction from one read-only SQLite snapshot. The caller must have
    /// opened this store with [`Store::open_read_only`].
    pub fn plan_compaction(&mut self, cutoff_ms: i64) -> Result<CompactionPlan> {
        let tx = self.conn.transaction()?;
        let plan = plan(&tx, cutoff_ms)?;
        tx.commit()?;
        Ok(plan)
    }
}

fn plan(tx: &Transaction<'_>, cutoff_ms: i64) -> Result<CompactionPlan> {
    let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version != SCHEMA_VERSION {
        return Err(StoreError::UnsupportedSchemaVersion(version));
    }
    let fk_errors: i64 =
        tx.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })?;
    if fk_errors != 0 {
        return invalid("dangling canonical foreign-key reference");
    }
    let rows = load_rows(tx)?;
    validate_rows(&rows)?;
    let sessions = table(&rows, "sessions")?;
    let mut roots = Vec::new();
    let mut selected = Vec::new();
    for (id, row) in sessions {
        let status = text(row, "status")?;
        let ended = integer_opt(row, "ended_at_ms")?;
        let started = integer(row, "started_at_ms")?;
        if ended.is_some_and(|end| started > end) {
            return invalid("session starts after it ends");
        }
        let (disposition, rationale) = eligibility(status, ended, cutoff_ms);
        if disposition == "selected" {
            selected.push(id.clone());
        }
        roots.push(CompactionRootDecision {
            session_id: id.clone(),
            disposition: disposition.into(),
            rationale: rationale.into(),
            observed_status: status.into(),
            observed_ended_at_ms: ended,
            closure_digest: None,
            member_count: 0,
            deletion_member_count: 0,
        });
    }

    let mut union: BTreeMap<(String, String), (String, bool)> = BTreeMap::new();
    let mut closures = BTreeMap::new();
    for root in &selected {
        let closure = closure_for_root(&rows, root, cutoff_ms)?;
        for (key, value) in &closure {
            union
                .entry(key.clone())
                .and_modify(|old| {
                    old.1 |= value.1;
                    if disposition_rank(&value.0) < disposition_rank(&old.0) {
                        old.0 = value.0.clone();
                    }
                })
                .or_insert_with(|| value.clone());
        }
        closures.insert(root.clone(), closure);
    }
    authorize_deletion_fixed_point(&rows, &mut union)?;
    let mut per_root = BTreeMap::new();
    for root in &selected {
        let closure = closures
            .get(root)
            .expect("closure was collected for every selected root");
        let finalized = closure
            .iter()
            .map(|(key, (disposition, _))| {
                (
                    key.clone(),
                    (
                        disposition.clone(),
                        union.get(key).is_some_and(|(_, delete)| *delete),
                    ),
                )
            })
            .collect();
        let digest = digest_members("closure", &finalized, &rows)?;
        let deletion_count = finalized.values().filter(|(_, d)| *d).count() as u64;
        per_root.insert(
            root.clone(),
            (digest, finalized.len() as u64, deletion_count),
        );
    }
    for root in &mut roots {
        if let Some((digest, count, deletion)) = per_root.get(&root.session_id) {
            root.closure_digest = Some(digest.clone());
            root.member_count = *count;
            root.deletion_member_count = *deletion;
        }
    }
    let members = materialize_members(&rows, &union)?;
    let membership_digest = digest_plan_members("membership", &members, false);
    let deletion: Vec<_> = members.iter().filter(|m| m.deletion_authorized).collect();
    let deletion_set_digest = digest_plan_members_refs("deletion-set", &deletion, true);
    let deletion_authorized_count = deletion.len() as u64;
    let closure_digest = digest_members("closure", &union, &rows)?;
    let root_set_digest = digest_roots(cutoff_ms, &roots);
    let mut counts = BTreeMap::new();
    for m in &members {
        *counts.entry(m.entity_kind.clone()).or_insert(0) += 1;
    }
    let logical_bytes: u64 = union
        .keys()
        .map(|(k, id)| {
            rows.get(k)
                .and_then(|t| t.get(id))
                .map_or(0, |r| r.bytes.len() as u64)
        })
        .sum();
    let object_bytes: u64 = members
        .iter()
        .filter(|m| m.entity_kind == "object_blob")
        .filter_map(|m| object_size(&rows, &m.entity_key))
        .sum();
    // Every canonical row is one JSONL record. Add the newline framing, a
    // conservative selective manifest/evidence allowance, object bytes, and
    // one fixed object-entry allowance per distinct object.
    let expected = logical_bytes
        .saturating_add(
            members
                .iter()
                .filter(|m| m.entity_kind != "object_blob")
                .count() as u64,
        )
        .saturating_add(object_bytes)
        .saturating_add(
            (members
                .iter()
                .filter(|m| m.entity_kind == "object_blob")
                .count() as u64)
                .saturating_mul(256),
        )
        .saturating_add(64 * 1024);
    let page_size: u64 = tx.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let freelist: u64 = tx.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    let current_freelist = page_size.checked_mul(freelist);
    let plan_digest = digest_fields(
        "plan",
        &[
            ALGORITHM.as_bytes(),
            &cutoff_ms.to_be_bytes(),
            root_set_digest.as_bytes(),
            closure_digest.as_bytes(),
            membership_digest.as_bytes(),
            deletion_set_digest.as_bytes(),
        ],
    );
    Ok(CompactionPlan {
        format: "ctx-compaction-plan",
        format_version: 1,
        private: true,
        source_schema_version: version,
        cutoff_ms,
        graph_algorithm: ALGORITHM,
        selected_root_ids: selected,
        ambiguous_root_count: roots
            .iter()
            .filter(|r| r.disposition == "excluded_ambiguous")
            .count() as u64,
        retained_root_count: roots.iter().filter(|r| r.disposition != "selected").count() as u64,
        roots,
        members,
        closure_counts_by_kind: counts,
        deletion_authorized_count,
        shared_retained_count: union
            .values()
            .filter(|(d, del)| !del && d == "shared_resource")
            .count() as u64,
        logical_bytes,
        object_bytes,
        expected_selective_archive_bytes: expected,
        current_sqlite_freelist_bytes: CompactionEstimate {
            bytes: current_freelist,
            reason_unknown: current_freelist
                .is_none()
                .then(|| "sqlite_metric_overflow".into()),
        },
        plan_attributable_reclaimable_bytes: CompactionEstimate {
            bytes: None,
            reason_unknown: Some("requires_archive_backed_deletion_and_page_observation".into()),
        },
        available_space_bytes: CompactionEstimate {
            bytes: None,
            reason_unknown: Some("filesystem_free_space_not_measured_by_store".into()),
        },
        temporary_space_required_bytes: CompactionEstimate {
            bytes: expected.checked_add(4096),
            reason_unknown: None,
        },
        root_set_digest,
        closure_digest,
        membership_digest,
        deletion_set_digest,
        plan_digest,
    })
}

fn closure_for_root(
    rows: &Rows,
    root: &str,
    cutoff: i64,
) -> Result<BTreeMap<(String, String), (String, bool)>> {
    let mut out = BTreeMap::new();
    let mut owned_sessions = VecDeque::from([root.to_owned()]);
    let mut expanded = BTreeSet::new();
    include(&mut out, "sessions", root, "selected_root", true);
    while let Some(session) = owned_sessions.pop_front() {
        if !expanded.insert(session.clone()) {
            continue;
        }
        add_session_refs(rows, &mut out, &session)?;
        if let Some(record) = text_opt(require(rows, "sessions", &session)?, "history_record_id")? {
            expand_owned_record_relations(rows, &mut out, record)?;
        }
        for (id, edge) in table(rows, "session_edges")? {
            if text(edge, "from_session_id")? != session {
                continue;
            }
            let target = text(edge, "to_session_id")?;
            require(rows, "sessions", target)?;
            if text(edge, "edge_type")? == "parent_child" {
                let child = require(rows, "sessions", target)?;
                let (decision, _) = eligibility(
                    text(child, "status")?,
                    integer_opt(child, "ended_at_ms")?,
                    cutoff,
                );
                let disposition = match decision {
                    "selected" => "owned_child",
                    "excluded_active" => "retained_active",
                    "excluded_post_cutoff" => "retained_post_cutoff",
                    _ => "retained_ambiguous",
                };
                let deletable = decision == "selected";
                include(
                    &mut out,
                    "session_edges",
                    id,
                    if deletable {
                        "owned_child"
                    } else {
                        "boundary_edge"
                    },
                    deletable,
                );
                include(&mut out, "sessions", target, disposition, deletable);
                if deletable {
                    owned_sessions.push_back(target.into());
                } else {
                    add_session_refs(rows, &mut out, target)?;
                }
            } else {
                include(&mut out, "session_edges", id, "boundary_edge", false);
                include(&mut out, "sessions", target, "referenced_dependency", false);
                add_session_refs(rows, &mut out, target)?;
            }
            add_optional_ref(
                rows,
                &mut out,
                edge,
                "source_id",
                "capture_sources",
                "shared_resource",
            )?;
        }
        for (id, run) in table(rows, "runs")? {
            if text_opt(run, "session_id")? == Some(session.as_str()) {
                include(&mut out, "runs", id, "owned_child", true);
                add_direct_refs(rows, &mut out, "runs", run)?;
            }
        }
        for (id, event) in table(rows, "events")? {
            if text_opt(event, "session_id")? == Some(session.as_str())
                || text_opt(event, "run_id")?.is_some_and(|run| {
                    out.get(&("runs".into(), run.into()))
                        .is_some_and(|(_, d)| *d)
                })
            {
                include(&mut out, "events", id, "owned_child", true);
                add_direct_refs(rows, &mut out, "events", event)?;
            }
        }
        for (id, summary) in table(rows, "summaries")? {
            if text_opt(summary, "session_id")? == Some(session.as_str()) {
                include(&mut out, "summaries", id, "owned_child", true);
                add_direct_refs(rows, &mut out, "summaries", summary)?;
            }
        }
        for (id, file) in table(rows, "files_touched")? {
            let owned = text_opt(file, "run_id")?
                .is_some_and(|x| out.get(&("runs".into(), x.into())).is_some_and(|(_, d)| *d))
                || text_opt(file, "event_id")?.is_some_and(|x| {
                    out.get(&("events".into(), x.into()))
                        .is_some_and(|(_, d)| *d)
                });
            if owned {
                include(&mut out, "files_touched", id, "owned_child", true);
                add_direct_refs(rows, &mut out, "files_touched", file)?;
            }
        }
    }
    complete_dependency_references(rows, &mut out)?;
    Ok(out)
}

fn complete_dependency_references(
    rows: &Rows,
    out: &mut BTreeMap<(String, String), (String, bool)>,
) -> Result<()> {
    loop {
        let before = out.len();
        let members: Vec<_> = out.keys().cloned().collect();
        for (kind, id) in members {
            let row = if kind == "object_blob" {
                continue;
            } else {
                require(rows, &kind, &id)?
            };
            match kind.as_str() {
                "sessions" => add_session_refs(rows, out, &id)?,
                "history_records" => add_history_record_support(rows, out, row)?,
                "runs" | "events" | "summaries" | "files_touched" => {
                    add_direct_refs(rows, out, &kind, row)?
                }
                "vcs_workspaces" => add_optional_ref(
                    rows,
                    out,
                    row,
                    "source_id",
                    "capture_sources",
                    "shared_resource",
                )?,
                "artifacts" => {
                    add_optional_ref(
                        rows,
                        out,
                        row,
                        "source_id",
                        "capture_sources",
                        "shared_resource",
                    )?;
                    include(
                        out,
                        "object_blob",
                        text(row, "blob_hash")?,
                        "shared_resource",
                        false,
                    );
                }
                "vcs_changes" => {
                    add_optional_ref(
                        rows,
                        out,
                        row,
                        "vcs_workspace_id",
                        "vcs_workspaces",
                        "shared_resource",
                    )?;
                    add_optional_ref(
                        rows,
                        out,
                        row,
                        "source_id",
                        "capture_sources",
                        "shared_resource",
                    )?;
                }
                _ => {}
            }
        }
        if out.len() == before {
            return Ok(());
        }
    }
}

fn add_session_refs(
    rows: &Rows,
    out: &mut BTreeMap<(String, String), (String, bool)>,
    id: &str,
) -> Result<()> {
    let r = require(rows, "sessions", id)?;
    add_optional_ref(
        rows,
        out,
        r,
        "history_record_id",
        "history_records",
        "referenced_dependency",
    )?;
    for c in ["parent_session_id", "root_session_id"] {
        add_optional_ref(rows, out, r, c, "sessions", "referenced_dependency")?;
    }
    add_optional_ref(
        rows,
        out,
        r,
        "capture_source_id",
        "capture_sources",
        "shared_resource",
    )?;
    add_optional_ref(
        rows,
        out,
        r,
        "transcript_blob_id",
        "artifacts",
        "shared_resource",
    )?;
    Ok(())
}

fn add_history_record_support(
    rows: &Rows,
    out: &mut BTreeMap<(String, String), (String, bool)>,
    record: &CanonicalRow,
) -> Result<()> {
    add_optional_ref(
        rows,
        out,
        record,
        "primary_vcs_workspace_id",
        "vcs_workspaces",
        "shared_resource",
    )?;
    add_optional_ref(
        rows,
        out,
        record,
        "source_id",
        "capture_sources",
        "shared_resource",
    )
}

fn expand_owned_record_relations(
    rows: &Rows,
    out: &mut BTreeMap<(String, String), (String, bool)>,
    record: &str,
) -> Result<()> {
    let r = require(rows, "history_records", record)?;
    add_optional_ref(
        rows,
        out,
        r,
        "primary_vcs_workspace_id",
        "vcs_workspaces",
        "shared_resource",
    )?;
    add_optional_ref(
        rows,
        out,
        r,
        "source_id",
        "capture_sources",
        "shared_resource",
    )?;
    for (id, x) in table(rows, "history_record_tags")? {
        if text(x, "history_record_id")? == record {
            include(
                out,
                "history_record_tags",
                id,
                "referenced_dependency",
                false,
            );
            add_optional_ref(rows, out, x, "tag_id", "tags", "shared_resource")?;
            add_optional_ref(
                rows,
                out,
                x,
                "source_id",
                "capture_sources",
                "shared_resource",
            )?;
        }
    }
    for (id, x) in table(rows, "history_record_links")? {
        if text(x, "history_record_id")? == record {
            include(
                out,
                "history_record_links",
                id,
                "referenced_dependency",
                false,
            );
            let (kind, target) = link_target(x)?;
            require(rows, kind, target)?;
            include(
                out,
                kind,
                target,
                if matches!(kind, "artifacts" | "vcs_workspaces") {
                    "shared_resource"
                } else {
                    "referenced_dependency"
                },
                false,
            );
            add_optional_ref(
                rows,
                out,
                x,
                "source_id",
                "capture_sources",
                "shared_resource",
            )?;
        }
    }
    for (id, x) in table(rows, "record_edges")? {
        if text(x, "from_record_id")? == record {
            include(out, "record_edges", id, "boundary_edge", false);
            let target = text(x, "to_record_id")?;
            require(rows, "history_records", target)?;
            include(
                out,
                "history_records",
                target,
                "referenced_dependency",
                false,
            );
            add_optional_ref(
                rows,
                out,
                x,
                "source_id",
                "capture_sources",
                "shared_resource",
            )?;
        }
    }
    Ok(())
}

fn add_direct_refs(
    rows: &Rows,
    out: &mut BTreeMap<(String, String), (String, bool)>,
    kind: &str,
    row: &CanonicalRow,
) -> Result<()> {
    let specs: &[(&str, &str, &str)] = match kind {
        "runs" => &[
            (
                "history_record_id",
                "history_records",
                "referenced_dependency",
            ),
            ("input_blob_id", "artifacts", "shared_resource"),
            ("output_blob_id", "artifacts", "shared_resource"),
            ("source_id", "capture_sources", "shared_resource"),
            ("session_id", "sessions", "referenced_dependency"),
        ],
        "events" => &[
            (
                "history_record_id",
                "history_records",
                "referenced_dependency",
            ),
            ("capture_source_id", "capture_sources", "shared_resource"),
            ("payload_blob_id", "artifacts", "shared_resource"),
            ("session_id", "sessions", "referenced_dependency"),
            ("run_id", "runs", "referenced_dependency"),
        ],
        "summaries" => &[
            (
                "history_record_id",
                "history_records",
                "referenced_dependency",
            ),
            ("source_id", "capture_sources", "shared_resource"),
            ("session_id", "sessions", "referenced_dependency"),
        ],
        "files_touched" => &[
            (
                "history_record_id",
                "history_records",
                "referenced_dependency",
            ),
            ("vcs_workspace_id", "vcs_workspaces", "shared_resource"),
            ("source_id", "capture_sources", "shared_resource"),
            ("run_id", "runs", "referenced_dependency"),
            ("event_id", "events", "referenced_dependency"),
        ],
        _ => &[],
    };
    for (c, k, d) in specs {
        add_optional_ref(rows, out, row, c, k, d)?;
    }
    if kind == "summaries" {
        for (target_kind, target_id) in citation_targets(row)? {
            require(rows, target_kind, &target_id)?;
            include(out, target_kind, &target_id, "referenced_dependency", false);
        }
    }
    Ok(())
}

fn citation_targets(row: &CanonicalRow) -> Result<Vec<(&'static str, String)>> {
    let value: Value = serde_json::from_str(text(row, "citations_json")?)
        .map_err(|_| StoreError::Archive("compaction plan: malformed summary citations".into()))?;
    let citations = value.as_array().ok_or_else(|| {
        StoreError::Archive("compaction plan: summary citations must be an array".into())
    })?;
    citations
        .iter()
        .map(|citation| {
            let object = citation.as_object().ok_or_else(|| {
                StoreError::Archive("compaction plan: malformed summary citation".into())
            })?;
            let mut target = None;
            for (key, kind) in [
                ("history_record_id", "history_records"),
                ("session_id", "sessions"),
                ("run_id", "runs"),
                ("event_id", "events"),
                ("vcs_change_id", "vcs_changes"),
                ("artifact_id", "artifacts"),
                ("summary_id", "summaries"),
                ("file_id", "files_touched"),
            ] {
                if let Some(value) = object.get(key) {
                    if target.is_some() {
                        return invalid("summary citation declares multiple targets");
                    }
                    let id = value
                        .as_str()
                        .ok_or_else(|| StoreError::Archive("invalid summary citation id".into()))?;
                    target = Some((kind, id));
                }
            }
            let (kind, id) = target
                .ok_or_else(|| StoreError::Archive("summary citation target missing".into()))?;
            if Uuid::parse_str(id)
                .map(|u| u.to_string() != id)
                .unwrap_or(true)
            {
                return invalid("invalid summary citation id");
            }
            Ok((kind, id.to_owned()))
        })
        .collect()
}

fn add_optional_ref(
    rows: &Rows,
    out: &mut BTreeMap<(String, String), (String, bool)>,
    row: &CanonicalRow,
    column: &str,
    kind: &str,
    disp: &str,
) -> Result<()> {
    if let Some(id) = text_opt(row, column)? {
        require(rows, kind, id)?;
        include(out, kind, id, disp, false);
        if kind == "artifacts" {
            let a = require(rows, kind, id)?;
            let hash = text(a, "blob_hash")?;
            include(out, "object_blob", hash, "shared_resource", false);
        }
    }
    Ok(())
}
fn include(
    out: &mut BTreeMap<(String, String), (String, bool)>,
    kind: &str,
    id: &str,
    disp: &str,
    del: bool,
) {
    out.entry((kind.into(), id.into()))
        .and_modify(|v| v.1 |= del)
        .or_insert((disp.into(), del));
}

fn load_rows(tx: &Transaction<'_>) -> Result<Rows> {
    let mut all = Rows::new();
    for (table, key, columns) in STREAMS {
        let sql = format!("SELECT {key} AS entity_key,{columns} FROM {table} ORDER BY entity_key");
        let mut stmt = tx.prepare(&sql)?;
        let names: Vec<String> = stmt.column_names()[1..]
            .iter()
            .map(|s| (*s).into())
            .collect();
        let mut cursor = stmt.query([])?;
        let mut map = BTreeMap::new();
        while let Some(row) = cursor.next()? {
            let id: String = row.get(0)?;
            let mut fields = Map::new();
            let mut canonical = Vec::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                let (value, encoded) = match row.get_ref(i + 1)? {
                    ValueRef::Null => (Value::Null, CanonicalValue::Null),
                    ValueRef::Integer(v) => (Value::from(v), CanonicalValue::Integer(v)),
                    ValueRef::Real(v) => (Value::from(v), CanonicalValue::Real(v.to_bits())),
                    ValueRef::Text(v) => {
                        let text = String::from_utf8(v.to_vec()).map_err(|_| {
                            StoreError::Archive("compaction plan: non-UTF-8 canonical text".into())
                        })?;
                        (Value::String(text.clone()), CanonicalValue::Text(text))
                    }
                    ValueRef::Blob(_) => return invalid("blob value in canonical table"),
                };
                fields.insert(name.clone(), value);
                canonical.push(encoded);
            }
            let bytes = serde_json::to_vec(&fields)?;
            if map
                .insert(
                    id,
                    CanonicalRow {
                        fields,
                        canonical,
                        bytes,
                    },
                )
                .is_some()
            {
                return invalid("duplicate canonical entity key");
            }
        }
        all.insert((*table).into(), map);
    }
    Ok(all)
}

fn validate_rows(rows: &Rows) -> Result<()> {
    for (kind, table) in rows {
        for (id, row) in table {
            if kind != "history_record_tags"
                && Uuid::parse_str(id)
                    .map(|u| u.to_string() != *id)
                    .unwrap_or(true)
            {
                return invalid("non-canonical entity id");
            }
            if kind == "artifacts" {
                let h = text(row, "blob_hash")?;
                if h.len() != 64
                    || !h
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    || integer(row, "byte_size")? < 0
                {
                    return invalid("malformed artifact reference");
                }
            }
            if kind == "history_record_links" {
                let (k, target) = link_target(row)?;
                require(rows, k, target)?;
            }
            if kind == "summaries" {
                for (target_kind, target_id) in citation_targets(row)? {
                    require(rows, target_kind, &target_id)?;
                }
            }
        }
    }
    Ok(())
}
fn link_target(row: &CanonicalRow) -> Result<(&str, &str)> {
    let kind = match text(row, "target_type")? {
        "session" => "sessions",
        "run" => "runs",
        "event" => "events",
        "vcs_workspace" => "vcs_workspaces",
        "vcs_change" => "vcs_changes",
        "artifact" => "artifacts",
        _ => return invalid("invalid polymorphic target type"),
    };
    Ok((kind, text(row, "target_id")?))
}
fn eligibility(status: &str, ended: Option<i64>, cutoff: i64) -> (&'static str, &'static str) {
    if status == "completed" {
        match ended {
            None => ("excluded_ambiguous", "completed_without_end_time"),
            Some(end) if end <= cutoff => ("selected", "completed_at_or_before_inclusive_cutoff"),
            Some(_) => ("excluded_post_cutoff", "completed_after_cutoff"),
        }
    } else {
        ("excluded_active", "status_not_completed")
    }
}
fn table<'a>(rows: &'a Rows, kind: &str) -> Result<&'a BTreeMap<String, CanonicalRow>> {
    rows.get(kind).ok_or_else(|| {
        StoreError::Archive(format!("compaction plan: missing canonical stream {kind}"))
    })
}
fn require<'a>(rows: &'a Rows, kind: &str, id: &str) -> Result<&'a CanonicalRow> {
    table(rows, kind)?
        .get(id)
        .ok_or_else(|| StoreError::Archive(format!("compaction plan: dangling {kind} reference")))
}
fn text<'a>(r: &'a CanonicalRow, k: &str) -> Result<&'a str> {
    r.fields
        .get(k)
        .and_then(Value::as_str)
        .ok_or_else(|| StoreError::Archive(format!("compaction plan: invalid {k}")))
}
fn text_opt<'a>(r: &'a CanonicalRow, k: &str) -> Result<Option<&'a str>> {
    match r.fields.get(k) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(v)) => Ok(Some(v)),
        _ => invalid("invalid optional text"),
    }
}
fn integer(r: &CanonicalRow, k: &str) -> Result<i64> {
    r.fields
        .get(k)
        .and_then(Value::as_i64)
        .ok_or_else(|| StoreError::Archive(format!("compaction plan: invalid {k}")))
}
fn integer_opt(r: &CanonicalRow, k: &str) -> Result<Option<i64>> {
    match r.fields.get(k) {
        Some(Value::Null) | None => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| StoreError::Archive(format!("compaction plan: invalid {k}"))),
    }
}
fn invalid<T>(message: &str) -> Result<T> {
    Err(StoreError::Archive(format!("compaction plan: {message}")))
}
fn disposition_rank(d: &str) -> u8 {
    match d {
        "selected_root" => 0,
        "owned_child" => 1,
        "retained_active" | "retained_post_cutoff" | "retained_ambiguous" => 2,
        "boundary_edge" => 3,
        "referenced_dependency" => 4,
        _ => 5,
    }
}
fn content_key(kind: &str, id: &str, row: &CanonicalRow) -> String {
    let canonical = encode_canonical_values(&row.canonical);
    digest_fields("content-key", &[kind.as_bytes(), id.as_bytes(), &canonical])
}

fn authorize_deletion_fixed_point(
    rows: &Rows,
    union: &mut BTreeMap<(String, String), (String, bool)>,
) -> Result<()> {
    loop {
        let candidates: BTreeSet<_> = union
            .iter()
            .filter(|(_, (_, delete))| *delete)
            .map(|(key, _)| key.clone())
            .collect();
        let mut revoke = BTreeSet::new();
        for (owner_kind, owner_rows) in rows {
            for (owner_id, owner) in owner_rows {
                if candidates.contains(&(owner_kind.clone(), owner_id.clone())) {
                    continue;
                }
                for target in outgoing_references(owner_kind, owner)? {
                    if candidates.contains(&target) {
                        revoke.insert(target);
                    }
                }
            }
        }
        if revoke.is_empty() {
            return Ok(());
        }
        for key in revoke {
            if let Some(member) = union.get_mut(&key) {
                member.1 = false;
            }
        }
    }
}

fn outgoing_references(kind: &str, row: &CanonicalRow) -> Result<Vec<(String, String)>> {
    let columns: &[(&str, &str)] = match kind {
        "vcs_workspaces" => &[("source_id", "capture_sources")],
        "history_records" => &[
            ("primary_vcs_workspace_id", "vcs_workspaces"),
            ("source_id", "capture_sources"),
        ],
        "artifacts" => &[("source_id", "capture_sources")],
        "sessions" => &[
            ("history_record_id", "history_records"),
            ("parent_session_id", "sessions"),
            ("root_session_id", "sessions"),
            ("capture_source_id", "capture_sources"),
            ("transcript_blob_id", "artifacts"),
        ],
        "session_edges" => &[
            ("from_session_id", "sessions"),
            ("to_session_id", "sessions"),
            ("source_id", "capture_sources"),
        ],
        "runs" => &[
            ("history_record_id", "history_records"),
            ("session_id", "sessions"),
            ("input_blob_id", "artifacts"),
            ("output_blob_id", "artifacts"),
            ("source_id", "capture_sources"),
        ],
        "events" => &[
            ("history_record_id", "history_records"),
            ("session_id", "sessions"),
            ("run_id", "runs"),
            ("capture_source_id", "capture_sources"),
            ("payload_blob_id", "artifacts"),
        ],
        "vcs_changes" => &[
            ("vcs_workspace_id", "vcs_workspaces"),
            ("source_id", "capture_sources"),
        ],
        "summaries" => &[
            ("history_record_id", "history_records"),
            ("session_id", "sessions"),
            ("source_id", "capture_sources"),
        ],
        "files_touched" => &[
            ("history_record_id", "history_records"),
            ("run_id", "runs"),
            ("event_id", "events"),
            ("vcs_workspace_id", "vcs_workspaces"),
            ("source_id", "capture_sources"),
        ],
        "history_record_tags" => &[
            ("history_record_id", "history_records"),
            ("tag_id", "tags"),
            ("source_id", "capture_sources"),
        ],
        "history_record_links" => &[
            ("history_record_id", "history_records"),
            ("source_id", "capture_sources"),
        ],
        "record_edges" => &[
            ("from_record_id", "history_records"),
            ("to_record_id", "history_records"),
            ("source_id", "capture_sources"),
        ],
        _ => &[],
    };
    let mut refs = Vec::new();
    for (column, target_kind) in columns {
        if let Some(id) = text_opt(row, column)? {
            refs.push(((*target_kind).into(), id.into()));
        }
    }
    if kind == "history_record_links" {
        let (target_kind, id) = link_target(row)?;
        refs.push((target_kind.into(), id.into()));
    }
    if kind == "summaries" {
        refs.extend(
            citation_targets(row)?
                .into_iter()
                .map(|(k, id)| (k.into(), id)),
        );
    }
    Ok(refs)
}
fn materialize_members(
    rows: &Rows,
    union: &BTreeMap<(String, String), (String, bool)>,
) -> Result<Vec<CompactionPlanMember>> {
    union
        .iter()
        .map(|((kind, id), (disp, del))| {
            let key = if kind == "object_blob" {
                id.clone()
            } else {
                content_key(kind, id, require(rows, kind, id)?)
            };
            let shared = is_shared(rows, kind, id, union)? || disp == "shared_resource";
            Ok(CompactionPlanMember {
                entity_kind: kind.clone(),
                entity_key: id.clone(),
                content_key: key,
                disposition: disp.clone(),
                ownership: if shared {
                    "shared_retained"
                } else {
                    "exclusive"
                }
                .into(),
                deletion_authorized: *del
                    && !shared
                    && matches!(disp.as_str(), "selected_root" | "owned_child"),
            })
        })
        .collect()
}
fn is_shared(
    rows: &Rows,
    kind: &str,
    id: &str,
    union: &BTreeMap<(String, String), (String, bool)>,
) -> Result<bool> {
    if kind == "object_blob" {
        return Ok(table(rows, "artifacts")?.iter().any(|(aid, a)| {
            text(a, "blob_hash").ok() == Some(id)
                && !union.contains_key(&("artifacts".into(), aid.clone()))
        }));
    }
    if kind == "artifacts" {
        for t in rows.values() {
            for (rid, r) in t {
                for c in [
                    "transcript_blob_id",
                    "input_blob_id",
                    "output_blob_id",
                    "payload_blob_id",
                    "target_id",
                ] {
                    if text_opt(r, c).ok().flatten() == Some(id)
                        && !union.contains_key(&(infer_kind(t, rows), rid.clone()))
                    {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(matches!(
        kind,
        "capture_sources" | "vcs_workspaces" | "tags" | "artifacts" | "object_blob"
    ))
}
fn infer_kind(table: &BTreeMap<String, CanonicalRow>, rows: &Rows) -> String {
    rows.iter()
        .find(|(_, v)| std::ptr::eq(*v, table))
        .map(|(k, _)| k.clone())
        .unwrap_or_default()
}
fn object_size(rows: &Rows, hash: &str) -> Option<u64> {
    table(rows, "artifacts")
        .ok()?
        .values()
        .find(|a| text(a, "blob_hash").ok() == Some(hash))
        .and_then(|a| integer(a, "byte_size").ok())
        .and_then(|n| u64::try_from(n).ok())
}
fn digest_members(
    domain: &str,
    m: &BTreeMap<(String, String), (String, bool)>,
    rows: &Rows,
) -> Result<String> {
    let mut tuples = Vec::new();
    for ((kind, id), (disposition, delete)) in m {
        let content = if kind == "object_blob" {
            id.clone()
        } else {
            content_key(kind, id, require(rows, kind, id)?)
        };
        tuples.push(encode_fields(&[
            kind.as_bytes(),
            id.as_bytes(),
            content.as_bytes(),
            disposition.as_bytes(),
            if *delete { b"1" } else { b"0" },
        ]));
    }
    Ok(digest_fields(
        domain,
        &tuples.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    ))
}
fn digest_plan_members(
    domain: &str,
    members: &[CompactionPlanMember],
    deletion_only: bool,
) -> String {
    let refs: Vec<_> = members
        .iter()
        .filter(|m| !deletion_only || m.deletion_authorized)
        .collect();
    digest_plan_members_refs(domain, &refs, deletion_only)
}
fn digest_plan_members_refs(
    domain: &str,
    members: &[&CompactionPlanMember],
    authorization: bool,
) -> String {
    let tuples: Vec<_> = if authorization {
        let mut ordered: Vec<_> = members
            .iter()
            .map(|m| {
                (
                    m.entity_kind.as_str(),
                    m.entity_key.as_str(),
                    m.content_key.as_str(),
                    m.disposition.as_str(),
                )
            })
            .collect();
        ordered.sort_unstable();
        ordered
            .into_iter()
            .map(|(kind, key, content, reason)| {
                encode_fields(&[
                    kind.as_bytes(),
                    key.as_bytes(),
                    content.as_bytes(),
                    reason.as_bytes(),
                ])
            })
            .collect()
    } else {
        let mut ordered: Vec<_> = members
            .iter()
            .map(|m| {
                (
                    m.entity_kind.as_str(),
                    m.entity_key.as_str(),
                    m.content_key.as_str(),
                    m.disposition.as_str(),
                    m.ownership.as_str(),
                )
            })
            .collect();
        ordered.sort_unstable();
        ordered
            .into_iter()
            .map(|(kind, key, content, reason, ownership)| {
                encode_fields(&[
                    kind.as_bytes(),
                    key.as_bytes(),
                    content.as_bytes(),
                    reason.as_bytes(),
                    ownership.as_bytes(),
                ])
            })
            .collect()
    };
    digest_fields(
        domain,
        &tuples.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    )
}
fn digest_roots(cutoff: i64, roots: &[CompactionRootDecision]) -> String {
    let tuples: Vec<_> = roots
        .iter()
        .map(|r| {
            let ended = r
                .observed_ended_at_ms
                .map(|v| {
                    let mut b = vec![1];
                    b.extend_from_slice(&v.to_be_bytes());
                    b
                })
                .unwrap_or_else(|| vec![0]);
            encode_fields(&[
                r.session_id.as_bytes(),
                r.disposition.as_bytes(),
                r.observed_status.as_bytes(),
                &ended,
                &cutoff.to_be_bytes(),
                r.closure_digest.as_deref().unwrap_or("").as_bytes(),
                &r.member_count.to_be_bytes(),
                &r.deletion_member_count.to_be_bytes(),
            ])
        })
        .collect();
    digest_fields(
        "root-set",
        &tuples.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    )
}
fn encode_canonical_values(values: &[CanonicalValue]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for value in values {
        match value {
            CanonicalValue::Null => out.push(0),
            CanonicalValue::Integer(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_be_bytes())
            }
            CanonicalValue::Real(bits) => {
                out.push(2);
                out.extend_from_slice(&bits.to_be_bytes())
            }
            CanonicalValue::Text(v) => {
                out.push(3);
                out.extend_from_slice(&(v.len() as u64).to_be_bytes());
                out.extend_from_slice(v.as_bytes())
            }
        }
    }
    out
}
fn encode_fields(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(parts.len() as u32).to_be_bytes());
    for p in parts {
        out.extend_from_slice(&(p.len() as u64).to_be_bytes());
        out.extend_from_slice(p);
    }
    out
}
fn digest_fields(domain: &str, parts: &[&[u8]]) -> String {
    let mut h = Sha256::new();
    let tag = format!("ctx-compaction/{domain}/v1");
    h.update(tag);
    h.update(encode_fields(parts));
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn insert_session(store: &Store, id: &str, status: &str, ended: Option<i64>) {
        store.conn.execute(
            "INSERT INTO sessions(id,provider,agent_type,status,fidelity,started_at_ms,ended_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'claude','primary',?2,'full',1,?3,1,1)",
            rusqlite::params![id, status, ended],
        ).unwrap();
    }

    fn citation_row(citations: &str) -> CanonicalRow {
        let mut fields = Map::new();
        fields.insert("citations_json".into(), Value::String(citations.into()));
        CanonicalRow {
            fields,
            canonical: vec![],
            bytes: vec![],
        }
    }

    #[test]
    fn canonical_summary_citations_resolve_all_declared_target_keys() {
        let ids = [
            "00000000-0000-0000-0000-000000000021",
            "00000000-0000-0000-0000-000000000022",
            "00000000-0000-0000-0000-000000000023",
            "00000000-0000-0000-0000-000000000024",
            "00000000-0000-0000-0000-000000000025",
            "00000000-0000-0000-0000-000000000026",
            "00000000-0000-0000-0000-000000000027",
            "00000000-0000-0000-0000-000000000028",
        ];
        let citations = format!(
            "[{{\"history_record_id\":\"{}\"}},{{\"session_id\":\"{}\"}},{{\"run_id\":\"{}\"}},{{\"event_id\":\"{}\"}},{{\"vcs_change_id\":\"{}\"}},{{\"artifact_id\":\"{}\"}},{{\"summary_id\":\"{}\"}},{{\"file_id\":\"{}\"}}]",
            ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], ids[6], ids[7]
        );
        let summary_id = "00000000-0000-0000-0000-000000000020";
        let summary = citation_row(&citations);
        let mut rows = Rows::new();
        rows.insert(
            "summaries".into(),
            BTreeMap::from([(summary_id.into(), summary.clone())]),
        );
        for (kind, id) in [
            ("history_records", ids[0]),
            ("sessions", ids[1]),
            ("runs", ids[2]),
            ("events", ids[3]),
            ("vcs_changes", ids[4]),
            ("artifacts", ids[5]),
            ("summaries", ids[6]),
            ("files_touched", ids[7]),
        ] {
            rows.entry(kind.into())
                .or_default()
                .insert(id.into(), citation_row("[]"));
        }

        let mut out = BTreeMap::new();
        add_direct_refs(&rows, &mut out, "summaries", &summary).unwrap();
        assert_eq!(
            out.keys().cloned().collect::<Vec<_>>(),
            [
                ("artifacts".into(), ids[5].into()),
                ("events".into(), ids[3].into()),
                ("files_touched".into(), ids[7].into()),
                ("history_records".into(), ids[0].into()),
                ("runs".into(), ids[2].into()),
                ("sessions".into(), ids[1].into()),
                ("summaries".into(), ids[6].into()),
                ("vcs_changes".into(), ids[4].into()),
            ]
        );
    }

    #[test]
    fn malformed_summary_citations_are_rejected() {
        for citations in [
            "[{}]",
            "[{\"event_id\":1}]",
            "[{\"event_id\":\"00000000-0000-0000-0000-000000000021\",\"run_id\":\"00000000-0000-0000-0000-000000000022\"}]",
            "[{\"type\":\"event\",\"id\":\"00000000-0000-0000-0000-000000000021\"}]",
        ] {
            let error = citation_targets(&citation_row(citations)).unwrap_err();
            assert!(error.to_string().contains("summary citation"));
        }
    }

    #[test]
    fn dangling_summary_citation_is_rejected() {
        let summary = citation_row("[{\"event_id\":\"00000000-0000-0000-0000-000000000021\"}]");
        let mut rows = Rows::new();
        rows.insert(
            "summaries".into(),
            BTreeMap::from([(
                "00000000-0000-0000-0000-000000000020".into(),
                summary.clone(),
            )]),
        );
        rows.insert("events".into(), BTreeMap::new());
        let mut out = BTreeMap::new();
        let error = add_direct_refs(&rows, &mut out, "summaries", &summary).unwrap_err();
        assert!(error.to_string().contains("dangling events reference"));
    }

    #[test]
    fn inclusive_boundary_ambiguous_empty_and_repeat_are_stable() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("work.sqlite");
        let writable = Store::open(&db).unwrap();
        insert_session(
            &writable,
            "00000000-0000-0000-0000-000000000001",
            "completed",
            Some(1000),
        );
        insert_session(
            &writable,
            "00000000-0000-0000-0000-000000000002",
            "completed",
            Some(1001),
        );
        insert_session(
            &writable,
            "00000000-0000-0000-0000-000000000003",
            "completed",
            None,
        );
        drop(writable);
        let before = std::fs::read(&db).unwrap();
        let mut readonly = Store::open_read_only(&db).unwrap();
        let first = readonly.plan_compaction(1000).unwrap();
        let second = readonly.plan_compaction(1000).unwrap();
        assert_eq!(
            first.selected_root_ids,
            ["00000000-0000-0000-0000-000000000001"]
        );
        assert_eq!(first.ambiguous_root_count, 1);
        assert_eq!(first.plan_digest, second.plan_digest);
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        let empty = readonly.plan_compaction(0).unwrap();
        assert!(empty.selected_root_ids.is_empty());
        drop(readonly);
        assert_eq!(before, std::fs::read(&db).unwrap());
    }

    #[test]
    fn unsupported_schema_fails_before_planning() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("work.sqlite");
        let writable = Store::open(&db).unwrap();
        writable
            .conn
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        drop(writable);
        assert!(matches!(
            Store::open_read_only(&db),
            Err(StoreError::UnsupportedSchemaVersion(_))
        ));
    }

    #[test]
    fn retained_children_block_incoming_target_deletion_fixed_point() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("work.sqlite");
        let writable = Store::open(&db).unwrap();
        let parent = "00000000-0000-0000-0000-000000000011";
        let active = "00000000-0000-0000-0000-000000000012";
        insert_session(&writable, parent, "completed", Some(10));
        insert_session(&writable, active, "active", None);
        writable
            .conn
            .execute(
                "UPDATE sessions SET parent_session_id=?1,root_session_id=?1 WHERE id=?2",
                rusqlite::params![parent, active],
            )
            .unwrap();
        writable.conn.execute("INSERT INTO session_edges(id,from_session_id,to_session_id,edge_type,created_at_ms,updated_at_ms) VALUES('00000000-0000-0000-0000-000000000013',?1,?2,'parent_child',1,1)",rusqlite::params![parent,active]).unwrap();
        drop(writable);
        let mut readonly = Store::open_read_only(&db).unwrap();
        let plan = readonly.plan_compaction(10).unwrap();
        let member = |kind: &str, id: &str| {
            plan.members
                .iter()
                .find(|m| m.entity_kind == kind && m.entity_key == id)
                .unwrap()
        };
        assert_eq!(member("sessions", active).disposition, "retained_active");
        assert!(!member("sessions", active).deletion_authorized);
        assert!(!member("sessions", parent).deletion_authorized);
        assert_eq!(
            plan.roots
                .iter()
                .find(|root| root.session_id == parent)
                .unwrap()
                .deletion_member_count,
            0
        );
    }

    #[test]
    fn canonical_digest_framing_has_fixed_vector() {
        assert_eq!(
            digest_fields("test", &[b"a", b"bc"]),
            "db198696a9d1089f161c790e275b3f75c0dfd7d2c8333efd4327ef7b32877233"
        );
        assert_ne!(
            digest_fields("test", &[b"ab", b"c"]),
            digest_fields("test", &[b"a", b"bc"])
        );
    }

    #[test]
    fn deletion_set_digest_has_fixed_authorization_vector_without_ownership() {
        let mut root = CompactionPlanMember {
            entity_kind: "sessions".into(),
            entity_key: "session-a".into(),
            content_key: "content-1".into(),
            disposition: "selected_root".into(),
            ownership: "shared_retained".into(),
            deletion_authorized: true,
        };
        let child = CompactionPlanMember {
            entity_kind: "events".into(),
            entity_key: "event-b".into(),
            content_key: "content-2".into(),
            disposition: "owned_child".into(),
            ownership: "exclusive".into(),
            deletion_authorized: true,
        };
        let members = vec![&root, &child];
        assert_eq!(
            digest_plan_members_refs("deletion-set", &members, true),
            "387dbc64d539c5cd8c5d02d6e714152a7eb1315cd7595e29f141ea47aaf0edaa"
        );
        root.ownership = "exclusive".into();
        assert_eq!(
            digest_plan_members_refs("deletion-set", &[&root, &child], true),
            "387dbc64d539c5cd8c5d02d6e714152a7eb1315cd7595e29f141ea47aaf0edaa"
        );
    }
}
