CREATE TABLE compaction_archives (
 archive_id TEXT PRIMARY KEY NOT NULL,
 format_family TEXT NOT NULL CHECK(format_family='ctx-selective-archive'),
 format_version INTEGER NOT NULL CHECK(format_version=1),
 scope_kind TEXT NOT NULL CHECK(scope_kind='selective'),
 manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64 AND manifest_sha256 NOT GLOB '*[^0-9a-f]*'),
 plan_digest TEXT NOT NULL CHECK(length(plan_digest)=64 AND plan_digest NOT GLOB '*[^0-9a-f]*'),
 closure_digest TEXT NOT NULL CHECK(length(closure_digest)=64 AND closure_digest NOT GLOB '*[^0-9a-f]*'),
 membership_digest TEXT NOT NULL CHECK(length(membership_digest)=64 AND membership_digest NOT GLOB '*[^0-9a-f]*'),
 root_set_digest TEXT NOT NULL CHECK(length(root_set_digest)=64 AND root_set_digest NOT GLOB '*[^0-9a-f]*'),
 deletion_set_digest TEXT NOT NULL CHECK(length(deletion_set_digest)=64 AND deletion_set_digest NOT GLOB '*[^0-9a-f]*'),
 deletion_member_count INTEGER NOT NULL CHECK(deletion_member_count>=0),
 source_schema_version INTEGER NOT NULL,
 origin_device_id TEXT CHECK(origin_device_id IS NULL OR length(origin_device_id) BETWEEN 1 AND 256),
 created_at_ms INTEGER NOT NULL, published_at_ms INTEGER NOT NULL, verified_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 UNIQUE(format_family,format_version,manifest_sha256)
);
CREATE INDEX compaction_archives_updated ON compaction_archives(updated_at_ms);
CREATE TABLE compaction_archive_roots (
 archive_id TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT,
 root_session_id TEXT NOT NULL,
 root_disposition TEXT NOT NULL CHECK(root_disposition IN ('selected','excluded_active','excluded_post_cutoff','excluded_ambiguous')),
 cutoff_ms INTEGER NOT NULL,
 observed_status TEXT NOT NULL CHECK(observed_status IN ('started','active','idle','completed','failed','interrupted','imported')),
 observed_ended_at_ms INTEGER,
 root_closure_digest TEXT CHECK(root_closure_digest IS NULL OR (length(root_closure_digest)=64 AND root_closure_digest NOT GLOB '*[^0-9a-f]*')),
 root_member_count INTEGER NOT NULL CHECK(root_member_count>=0), root_deletion_member_count INTEGER NOT NULL CHECK(root_deletion_member_count>=0), created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(archive_id,root_session_id)
);
CREATE INDEX compaction_roots_disposition ON compaction_archive_roots(root_disposition,root_session_id);
CREATE INDEX compaction_roots_digest ON compaction_archive_roots(archive_id,root_closure_digest);
CREATE TABLE compaction_archive_members (
 archive_id TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT,
 entity_kind TEXT NOT NULL CHECK(entity_kind IN ('capture_sources','vcs_workspaces','history_records','artifacts','sessions','session_edges','runs','events','vcs_changes','summaries','files_touched','tags','history_record_tags','history_record_links','record_edges','object_blob')),
 entity_key TEXT NOT NULL CHECK(length(entity_key) BETWEEN 1 AND 512),
 content_key TEXT NOT NULL CHECK(length(content_key)=64 AND content_key NOT GLOB '*[^0-9a-f]*'),
 disposition TEXT NOT NULL CHECK(disposition IN ('selected_root','owned_child','referenced_dependency','shared_resource','boundary_edge','retained_active','retained_post_cutoff','retained_ambiguous')),
 ownership TEXT NOT NULL CHECK(ownership IN ('exclusive','shared_retained')),
 membership_state TEXT NOT NULL CHECK(membership_state IN ('verified','suppressed','compacted','restored','conflict')),
 hot_state TEXT NOT NULL CHECK(hot_state IN ('present','absent','conflict')),
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(archive_id,entity_kind,entity_key), UNIQUE(archive_id,entity_kind,entity_key,content_key)
);
CREATE INDEX compaction_members_entity ON compaction_archive_members(entity_kind,entity_key);
CREATE INDEX compaction_members_content ON compaction_archive_members(content_key);
CREATE INDEX compaction_members_disposition ON compaction_archive_members(archive_id,disposition);
CREATE INDEX compaction_members_state ON compaction_archive_members(archive_id,membership_state,hot_state);
CREATE TABLE compaction_deletion_members (
 archive_id TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT,
 entity_kind TEXT NOT NULL, entity_key TEXT NOT NULL, content_key TEXT NOT NULL,
 authorization_reason TEXT NOT NULL CHECK(authorization_reason IN ('selected_root','owned_child')), created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(archive_id,entity_kind,entity_key),
 FOREIGN KEY(archive_id,entity_kind,entity_key,content_key) REFERENCES compaction_archive_members(archive_id,entity_kind,entity_key,content_key) ON DELETE RESTRICT
);
CREATE INDEX compaction_deletion_members_entity ON compaction_deletion_members(entity_kind,entity_key);
CREATE INDEX compaction_deletion_members_archive ON compaction_deletion_members(archive_id);
-- v1005-atomic-boundary
CREATE TABLE compaction_suppression_facts (
 identity_key TEXT NOT NULL CHECK(length(identity_key)=64 AND identity_key NOT GLOB '*[^0-9a-f]*'),
 content_key TEXT NOT NULL CHECK(length(content_key)=64 AND content_key NOT GLOB '*[^0-9a-f]*'),
 effective_state TEXT NOT NULL CHECK(effective_state IN ('active','restored','overridden','conflict')),
 origin_device_id TEXT CHECK(origin_device_id IS NULL OR length(origin_device_id) BETWEEN 1 AND 256),
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 last_error_code TEXT CHECK(last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128),
 PRIMARY KEY(identity_key,content_key)
);
CREATE INDEX compaction_suppression_facts_identity ON compaction_suppression_facts(identity_key,effective_state,content_key);
CREATE INDEX compaction_suppression_facts_state ON compaction_suppression_facts(effective_state,updated_at_ms);
CREATE TABLE compaction_archive_suppressions (
 archive_id TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT,
 identity_key TEXT NOT NULL, content_key TEXT NOT NULL,
 association_state TEXT NOT NULL CHECK(association_state IN ('active','restored','overridden','conflict')),
 origin_device_id TEXT CHECK(origin_device_id IS NULL OR length(origin_device_id) BETWEEN 1 AND 256),
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 last_error_code TEXT CHECK(last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128),
 PRIMARY KEY(archive_id,identity_key,content_key),
 FOREIGN KEY(identity_key,content_key) REFERENCES compaction_suppression_facts(identity_key,content_key) ON DELETE RESTRICT
);
CREATE INDEX compaction_archive_suppressions_archive ON compaction_archive_suppressions(archive_id,association_state);
CREATE INDEX compaction_archive_suppressions_identity ON compaction_archive_suppressions(identity_key,association_state);
CREATE TABLE compaction_suppression_conflicts (
 identity_key TEXT NOT NULL, content_key TEXT NOT NULL,
 conflict_state TEXT NOT NULL CHECK(conflict_state IN ('active','resolved')),
 resolution TEXT CHECK(resolution IS NULL OR resolution IN ('override','restored')),
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(identity_key,content_key),
 FOREIGN KEY(identity_key,content_key) REFERENCES compaction_suppression_facts(identity_key,content_key) ON DELETE RESTRICT
);
CREATE INDEX compaction_suppression_conflicts_identity ON compaction_suppression_conflicts(identity_key,conflict_state,content_key);
CREATE TABLE compaction_restore_markers (
 archive_id TEXT NOT NULL, identity_key TEXT NOT NULL, content_key TEXT NOT NULL,
 canonical_marker TEXT NOT NULL CHECK(length(canonical_marker)=64 AND canonical_marker NOT GLOB '*[^0-9a-f]*'),
 created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(archive_id,identity_key,content_key),
 FOREIGN KEY(archive_id,identity_key,content_key) REFERENCES compaction_archive_suppressions(archive_id,identity_key,content_key) ON DELETE RESTRICT
);
CREATE TABLE compaction_operations (
 operation_id TEXT PRIMARY KEY NOT NULL,
 operation_kind TEXT NOT NULL CHECK(operation_kind IN ('archive_register','suppression_activate','delete','restore','override','conflict_resolve','reclaim')),
 archive_id TEXT REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT,
 request_digest TEXT NOT NULL CHECK(length(request_digest)=64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
 scope_kind TEXT NOT NULL CHECK(scope_kind IN ('none','whole','selective')),
 phase TEXT NOT NULL CHECK(phase IN ('started','staged','published','committed','failed')),
 attempt_count INTEGER NOT NULL CHECK(attempt_count>=1),
 last_error_code TEXT CHECK(last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128), created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX compaction_operations_idempotency ON compaction_operations(operation_kind,COALESCE(archive_id,''),request_digest);
CREATE INDEX compaction_operations_recovery ON compaction_operations(phase,updated_at_ms);
CREATE TABLE compaction_operation_objects (
 operation_id TEXT NOT NULL REFERENCES compaction_operations(operation_id) ON DELETE RESTRICT,
 blob_hash TEXT NOT NULL CHECK(length(blob_hash)=64 AND blob_hash NOT GLOB '*[^0-9a-f]*'),
 content_key TEXT NOT NULL CHECK(length(content_key)=64 AND content_key NOT GLOB '*[^0-9a-f]*' AND content_key=blob_hash),
 byte_size INTEGER NOT NULL CHECK(byte_size>=0), object_state TEXT NOT NULL CHECK(object_state IN ('staged','published','verified')), updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(operation_id,blob_hash)
);
CREATE INDEX compaction_operation_objects_state ON compaction_operation_objects(operation_id,object_state);
CREATE INDEX compaction_operation_objects_hash ON compaction_operation_objects(blob_hash,object_state);
PRAGMA user_version=1005;
