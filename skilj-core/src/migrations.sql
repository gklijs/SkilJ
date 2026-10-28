-- SkilJ clean-room schema. Idempotent; run at every startup under an
-- advisory lock so instances starting together don't race.

create table if not exists skilj_meta (
    key text primary key,
    value text not null
);

create table if not exists bounded_contexts (
    name text primary key,
    status text not null check (status in ('active', 'archived')),
    created_at timestamptz not null,
    created_by_role text,
    template text references bounded_contexts(name) on delete set null,
    -- Which compiled declaration serves this context's deciders and folds.
    -- Not in the spec: `template` is cleared when the template is deleted,
    -- and a tenant must keep processing commands after that.
    dispatch_template text
);

create table if not exists sequence_counters (
    bounded_context text primary key references bounded_contexts(name) on delete cascade,
    next_sequence bigint not null default 0
);

create table if not exists event_types (
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    name text not null,
    def jsonb not null,
    primary key (bounded_context, name)
);

create table if not exists command_types (
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    name text not null,
    def jsonb not null,
    primary key (bounded_context, name)
);

create table if not exists projections (
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    name text not null,
    def jsonb not null,
    caught_up_to bigint,
    primary key (bounded_context, name)
);

create table if not exists projection_rebuilds (
    bounded_context text not null,
    projection text not null,
    status text not null check (status in ('pending', 'building')),
    def jsonb not null,
    caught_up_to bigint,
    primary key (bounded_context, projection, status),
    foreign key (bounded_context, projection) references projections(bounded_context, name) on delete cascade
);

create table if not exists projection_state (
    bounded_context text not null,
    projection text not null,
    generation text not null,          -- 'live' or 'rebuild'
    key text not null,
    state jsonb not null,
    owner text,
    primary key (bounded_context, projection, generation, key),
    foreign key (bounded_context, projection) references projections(bounded_context, name) on delete cascade
);

create table if not exists events (
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    sequence bigint not null,
    event_type text not null,
    payload jsonb not null,
    metadata jsonb not null,
    tags jsonb not null,
    encryption_key_ids text[] not null default '{}',
    origin jsonb not null,
    command_id text,
    created_at timestamptz not null,
    primary key (bounded_context, sequence)
);
create index if not exists events_by_type on events (bounded_context, event_type, sequence);
create index if not exists events_by_command on events (command_id) where command_id is not null;

create table if not exists event_tags (
    bounded_context text not null,
    sequence bigint not null,
    key text not null,
    value text,
    foreign key (bounded_context, sequence) references events(bounded_context, sequence) on delete cascade
);
create index if not exists event_tags_lookup on event_tags (bounded_context, key, value, sequence);

create table if not exists commands (
    id text primary key,
    recorded bigserial unique,
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    command_type text not null,
    payload jsonb not null,
    metadata jsonb not null,
    encryption_key_ids text[] not null default '{}',
    consistency_tags jsonb not null,
    consistency_query jsonb not null,
    consistency_boundary bigint,
    created_at timestamptz not null
);
create index if not exists commands_by_context on commands (bounded_context, recorded);

create table if not exists encryption_keys (
    id text primary key,
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    subject_key text not null,
    subject_value text not null,
    wrapped bytea,
    master_key_id text not null,
    status text not null check (status in ('active', 'destroyed')),
    created_at timestamptz not null,
    destroyed_at timestamptz
);
create unique index if not exists encryption_keys_one_active
    on encryption_keys (bounded_context, subject_key, subject_value) where status = 'active';
create index if not exists encryption_keys_by_value on encryption_keys (bounded_context, subject_value);

create table if not exists roles (
    id text primary key,
    external_subject text not null,
    name text not null,
    superadmin boolean not null,
    active boolean not null,
    created_at timestamptz not null,
    revoked_at timestamptz
);
create unique index if not exists roles_one_active_subject on roles (external_subject) where active;

create table if not exists access_mappings (
    id text primary key,
    role_id text not null references roles(id),
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    level text not null,
    can_read_sensitive boolean not null,
    scope text,
    capabilities text[] not null default '{}',
    delegable text[] not null default '{}',
    active boolean not null,
    created_at timestamptz not null,
    revoked_at timestamptz
);
create unique index if not exists access_mappings_one_active on access_mappings (role_id, bounded_context) where active;

create table if not exists tokens (
    id text primary key,
    secret_hash text not null,
    purpose text not null,
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    type_name text not null,
    scope text,
    active boolean not null,
    created_at timestamptz not null,
    revoked_at timestamptz,
    start_from text not null default 'beginning',
    start_at_sequence bigint,
    start_at_time timestamptz
);

create table if not exists read_cursors (
    token_id text primary key references tokens(id) on delete cascade,
    ack_mode text not null,
    sequence bigint not null,
    updated_at timestamptz not null,
    checked_out_at timestamptz
);

create table if not exists idempotency_keys (
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    command_type text not null,
    client_id text not null,
    key text not null,
    outcome jsonb not null,
    recorded_at timestamptz not null,
    primary key (bounded_context, command_type, client_id, key)
);

create table if not exists external_watermarks (
    token_id text not null references tokens(id) on delete cascade,
    partition_key text not null,
    watermark bigint not null,
    primary key (token_id, partition_key)
);

create table if not exists external_keys (
    token_id text not null references tokens(id) on delete cascade,
    key text not null,
    sequence bigint not null,
    recorded_at timestamptz not null,
    primary key (token_id, key)
);

create table if not exists private_field_grants (
    id text primary key,
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    grantor text not null references roles(id),
    grantee text not null references roles(id),
    event_sequence bigint,
    command_id text,
    active boolean not null,
    created_at timestamptz not null,
    revoked_at timestamptz,
    check (not (event_sequence is not null and command_id is not null))
);
create index if not exists private_field_grants_by_grantee on private_field_grants (bounded_context, grantee) where active;

create table if not exists blind_tags (
    bounded_context text not null references bounded_contexts(name) on delete cascade,
    tag_key text not null,
    normalizer_version integer not null,
    key_generation integer not null,
    key_fingerprint text,
    erasure_subject_key text,
    erasure_subject_tag text,
    primary key (bounded_context, tag_key)
);
