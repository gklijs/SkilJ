-- A bounded context's schema exactly as skilj v0.0.1's
-- provision_bounded_context_schema created it (extracted from the v0.0.1
-- tag), with {schema} standing for its quoted schema name. Used by
-- skilj/tests/startup_bounded_contexts.rs to check that startup brings a
-- bounded context this old up to the current shape (docs/architecture.md §159).
CREATE SCHEMA {schema};
CREATE TABLE {schema}.event_types (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            tag_mappings JSONB NOT NULL DEFAULT '[]',
            sensitive_fields JSONB NOT NULL DEFAULT '[]',
            external_creation_allowed BOOLEAN NOT NULL,
            direct_creation_allowed BOOLEAN NOT NULL,
            system_triggered_allowed BOOLEAN NOT NULL,
            system_triggered_schedule TEXT,
            missed_occurrence_policy TEXT CHECK (missed_occurrence_policy IN ('skip', 'fire_once', 'replay_backlog')),
            schedule_position TIMESTAMPTZ,
            last_fired_at TIMESTAMPTZ,
            event_read_allowed BOOLEAN NOT NULL
        );
CREATE TABLE {schema}.command_types (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            tag_mappings JSONB NOT NULL DEFAULT '[]',
            sensitive_fields JSONB NOT NULL DEFAULT '[]',
            rest_trigger_allowed BOOLEAN NOT NULL
        );
CREATE TABLE {schema}.encryption_keys (
            id BIGSERIAL PRIMARY KEY,
            subject_key TEXT NOT NULL,
            subject_value TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'destroyed')),
            created_at TIMESTAMPTZ NOT NULL,
            destroyed_at TIMESTAMPTZ,
            wrapped_key BYTEA,
            wrap_nonce BYTEA
        );
CREATE UNIQUE INDEX encryption_keys_unique_active ON {schema}.encryption_keys (subject_key, subject_value) WHERE status = 'active';
CREATE TABLE {schema}.commands (
            id BIGSERIAL PRIMARY KEY,
            external_id TEXT NOT NULL UNIQUE,
            command_type_name TEXT NOT NULL REFERENCES {schema}.command_types (name),
            payload TEXT NOT NULL,
            metadata_type TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            consistency_tags JSONB NOT NULL DEFAULT '[]',
            consistency_boundary BIGINT
        );
CREATE INDEX commands_by_created_at ON {schema}.commands (metadata_created_at);
CREATE TABLE {schema}.command_encryption_keys (
            command_id BIGINT NOT NULL REFERENCES {schema}.commands (id),
            encryption_key_id BIGINT NOT NULL REFERENCES {schema}.encryption_keys (id),
            PRIMARY KEY (command_id, encryption_key_id)
        );
CREATE TABLE {schema}.projections (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT
        );
CREATE TABLE {schema}.projection_consumed_event_types (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            PRIMARY KEY (projection_name, event_type_name)
        );
CREATE TABLE {schema}.projection_rebuilds (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT,
            status TEXT NOT NULL CHECK (status IN ('pending', 'building')),
            PRIMARY KEY (projection_name, status)
        );
CREATE TABLE {schema}.projection_rebuild_consumed_event_types (
            projection_name TEXT NOT NULL,
            status TEXT NOT NULL,
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            PRIMARY KEY (projection_name, status, event_type_name),
            FOREIGN KEY (projection_name, status)
                REFERENCES {schema}.projection_rebuilds (projection_name, status)
        );
CREATE TABLE {schema}.projection_rebuild_state (
            projection_name TEXT NOT NULL,
            status TEXT NOT NULL,
            key TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (projection_name, status, key),
            FOREIGN KEY (projection_name, status)
                REFERENCES {schema}.projection_rebuilds (projection_name, status)
        );
CREATE TABLE {schema}.projection_state (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            key TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (projection_name, key)
        );
CREATE TABLE {schema}.sequence (next_value BIGINT NOT NULL);
INSERT INTO {schema}.sequence (next_value) VALUES (-1);
CREATE TABLE {schema}.events (
            sequence BIGINT PRIMARY KEY,
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            payload TEXT NOT NULL,
            metadata_type TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            tags JSONB NOT NULL DEFAULT '[]',
            origin_kind TEXT NOT NULL CHECK (
                origin_kind IN ('external_triggered', 'directly_created', 'command_triggered', 'system_triggered')
            ),
            origin_source_content TEXT,
            origin_source_context TEXT,
            origin_command_id BIGINT REFERENCES {schema}.commands (id)
        );
CREATE INDEX events_by_type ON {schema}.events (event_type_name, sequence);
CREATE INDEX events_by_tags ON {schema}.events USING GIN (tags);
CREATE TABLE {schema}.snapshots (
            snapshot_name TEXT NOT NULL,
            tag_key TEXT NOT NULL,
            tag_value TEXT NOT NULL,
            snapshot_version BIGINT NOT NULL,
            as_of_sequence BIGINT NOT NULL,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (snapshot_name, tag_key, tag_value)
        );
CREATE TABLE {schema}.snapshot_progress (
            snapshot_name TEXT PRIMARY KEY,
            caught_up_to BIGINT NOT NULL
        );
CREATE TABLE {schema}.event_encryption_keys (
            event_sequence BIGINT NOT NULL REFERENCES {schema}.events (sequence),
            encryption_key_id BIGINT NOT NULL REFERENCES {schema}.encryption_keys (id),
            PRIMARY KEY (event_sequence, encryption_key_id)
        );
CREATE TABLE {schema}.access_tokens (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('external_event', 'direct_creation', 'event_read', 'command')),
            secret TEXT NOT NULL, -- hash_secret's output, never the plaintext (see AccessToken.secret)
            status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
            created_at TIMESTAMPTZ NOT NULL,
            revoked_at TIMESTAMPTZ,
            event_type_name TEXT REFERENCES {schema}.event_types (name),
            command_type_name TEXT REFERENCES {schema}.command_types (name),
            CHECK (
                (kind = 'command' AND command_type_name IS NOT NULL AND event_type_name IS NULL)
                OR (kind != 'command' AND event_type_name IS NOT NULL AND command_type_name IS NULL)
            )
        );
CREATE TABLE {schema}.read_cursors (
            token_id TEXT PRIMARY KEY REFERENCES {schema}.access_tokens (id),
            ack_mode TEXT NOT NULL CHECK (ack_mode IN ('auto_advance', 'manual_ack')),
            sequence BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
