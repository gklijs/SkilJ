//! Cross-cutting value types, used across every domain module - and the
//! home for primitives (once implemented) reused by both
//! `access_control`'s `AccessToken` and `bootstrap`'s `BootstrapSecret`
//! (`generate_token_secret`, `secret_matches`, `generate_token_id`). See
//! docs/architecture.md §3.2 and `specs/skilj.allium`'s Value Types
//! section, which this module mirrors closely.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Names a payload field to filter, tag, or mark sensitive: a bare
/// top-level name, or a two-segment dotted path reaching one leaf inside
/// the spec's one permitted level of nesting (e.g. `"address.country"`).
/// See the payload schema shape note above `entity CommandType` in
/// `specs/skilj.allium`.
pub type FieldPath = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
/// One condition on a payload field, as `queryEvents` and projection queries
/// take it: `field` compared with `value` by `operator`.
pub struct Filter {
    /// The payload field compared.
    pub field: FieldPath,
    /// How it is compared.
    pub operator: FilterOperator,
    /// What it is compared with; the operator says how the string is read.
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
/// How a [`Filter`] compares a payload field with its value.
pub enum FilterOperator {
    /// The field equals the value.
    Equals,
    /// The string field contains the value.
    Contains,
    /// The string field matches the value as a SQL `LIKE` pattern (`%` and `_`
    /// wildcards).
    IsLike,
    /// The field is greater than the value.
    GreaterThan,
    /// The field is less than the value.
    LessThan,
    /// Only valid against a string leaf with `format: "geo-point"`
    /// (`"lat,lng"`). `Filter.value` is `"lat,lng,radius_meters"` - the
    /// target point plus how close counts as "near". See
    /// `event_store::geo_distance_within`.
    Near,
    /// Only valid against a string leaf with `format: "color"`
    /// (`"#RRGGBB"`). `Filter.value` is `"#RRGGBB,max_distance"` - the
    /// target color plus a similarity threshold (plain Euclidean RGB
    /// distance, not a perceptual metric). See
    /// `event_store::color_similarity_within`.
    SimilarColor,
    /// Only valid against a string leaf with `format: "ip"` (IPv4 or
    /// IPv6, via `std::net::IpAddr`). `Filter.value` is a CIDR, e.g.
    /// `"192.168.1.0/24"`. See `event_store::ip_in_subnet`.
    InSubnet,
    /// Valid against any scalar leaf (string/integer/number/boolean) -
    /// not tied to any particular `format`. `Filter.value` is a
    /// comma-separated list of candidates, e.g. `"a,b,c"` - no escaping,
    /// same as `IsLike`'s `%`/`_` wildcards already being unescaped.
    In,
}

/// See `value Metadata` in the spec. `version` and every other
/// sequence-adjacent integer in this codebase is `i64` - see
/// docs/architecture.md §2.2.1.
///
/// `correlation_id`/`causation_id` (issue #18) are `Option`
/// here regardless of the "every stored `Command`/`Event` ends up with a
/// `correlation_id`" guarantee the spec's `CorrelationIdIsAlwaysRecorded`
/// invariant makes - that guarantee is a property of the write path
/// (`event_store::process_command`/`create_external_event`/
/// `create_direct_event`/`create_system_event` all generate one when the
/// caller didn't supply one), not of this type. A row written before
/// this field existed genuinely has neither, and decoding it as
/// `Option::None` rather than inventing a value is the honest read.
/// Deliberately not the OTel trace id (`skilj-rest`/`skilj-graphql`'s own
/// `current_trace_id()`, docs/architecture.md §10b) - see the spec's own
/// note above `value Metadata` for why the two are unrelated: a trace id
/// is ephemeral and exporter-dependent, these are durable and queryable
/// from the store itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Metadata {
    /// The name of the event or command type the message conforms to.
    pub r#type: String,
    /// The type's `schema_version` when the message was created.
    pub version: i64,
    /// The client or caller that produced the message.
    pub client_id: String,
    /// When the message was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// The business transaction the message belongs to: every command and
    /// event in one causal chain carries the same value. Supplied by the
    /// caller or generated.
    pub correlation_id: Option<String>,
    /// The id of the command or event that directly produced this message;
    /// `None` when a caller originated it.
    pub causation_id: Option<String>,
}

/// `value` is `None` (absent) rather than the tag being omitted entirely
/// when the mapped payload field was itself absent - see `Tag.value` in
/// the spec for why that's deliberate, not an oversight.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct Tag {
    /// The tag key, from the [`TagMapping`] that derived it.
    pub key: String,
    /// The mapped field's value; `None` when the payload left the field out.
    pub value: Option<String>,
}

/// Derives a tag from a payload field. A `blind` mapping's tag value is a
/// keyed hash of the field rather than the field itself
/// (docs/architecture.md §205): it may name a sensitive field, and two
/// payloads carry the same tag exactly when their normalized values are
/// equal - a uniqueness check without storing the value. Its key must be
/// declared by a [`crate::plugin::BlindTag`] in the same bounded context;
/// build with [`TagMapping::blind`]. Stored with the type, so every
/// instance knows the tag must be hashed, including one that doesn't
/// declare it and so refuses rather than storing the value in the clear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TagMapping {
    /// The tag key.
    pub key: String,
    /// The payload field the tag value comes from.
    pub field: FieldPath,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    /// Whether the tag value is a keyed hash of the field rather than the
    /// field itself.
    pub blind: bool,
}

impl TagMapping {
    /// A plain mapping: the tag value is the field's value.
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            blind: false,
        }
    }

    /// A blind mapping under `B`'s constraint: the tag key is
    /// `B::TAG_KEY`, its value the keyed hash of `field`'s normalized
    /// value.
    pub fn blind<B: crate::plugin::BlindTag>(field: impl Into<String>) -> Self {
        Self {
            key: B::TAG_KEY.to_string(),
            field: field.into(),
            blind: true,
        }
    }
}

/// One item of a command type's consistency query (docs/architecture.md
/// §198, `value QueryItemMapping` in specs/skilj.allium), declared by
/// `CommandType::consistency_query`. Derived per command into a
/// [`QueryItem`], as a [`TagMapping`] is into a [`Tag`]. An event matches
/// the item when its type is one of `event_types` (any type when empty)
/// and it carries every tag `tag_mappings` derives. `latest`, when set,
/// asks for only that many matching events, those with the highest
/// sequences; `None` for every match.
///
/// ```
/// # use skilj_core::shared::{QueryItemMapping, TagMapping};
/// let company = TagMapping::new("company", "company_id");
/// // The company's latest lifecycle event, whatever kind it is.
/// let status = QueryItemMapping::types(&["CompanySignedUp", "CompanyExpired"])
///     .tagged(vec![company])
///     .latest();
/// assert_eq!(status.latest, Some(1));
/// // The requester's last three tickets.
/// let recent = QueryItemMapping::types(&["TicketCreated"]).last(3);
/// assert_eq!(recent.latest, Some(3));
/// // A command whose decide() needs no history: alone in its query.
/// let none = QueryItemMapping::nothing();
/// assert!(none.event_types.is_empty() && none.tag_mappings.is_empty());
/// ```
///
/// An item with `snapshot` set hands `decide_with()` that snapshot's
/// state for the tag value its tag mapping derives, brought up to date,
/// instead of events - see [`QueryItemMapping::snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QueryItemMapping {
    /// The event types the item matches; empty for any type.
    pub event_types: Vec<String>,
    /// Tags derived from the command's payload, all of which a matching event
    /// must carry.
    pub tag_mappings: Vec<TagMapping>,
    /// Only this many matching events, those with the highest sequences;
    /// `None` for every match.
    pub latest: Option<u32>,
    /// A `Snapshot::NAME`: this item is that snapshot's state for the tag
    /// value its one tag mapping derives (docs/architecture.md §202).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

impl QueryItemMapping {
    /// An item matching no event (docs/architecture.md §201). As a
    /// command type's whole query, `vec![QueryItemMapping::nothing()]`,
    /// it says `decide()` needs no history: the command reads no events
    /// and no event conflicts with it, while its `tag_mappings()` still
    /// tag it for owner scoping. `build()` refuses it next to other items
    /// or with `latest`.
    pub fn nothing() -> Self {
        Self {
            event_types: Vec::new(),
            tag_mappings: Vec::new(),
            latest: None,
            snapshot: None,
        }
    }

    /// Snapshot `S`'s state for the tag value the payload's `field`
    /// derives under `S::TAG_KEY` (docs/architecture.md §202). The
    /// command reads the stored row and the events carrying that value
    /// since its `as_of_sequence`, and the framework folds those on top
    /// with `S`'s own fold, so `decide_with()` gets the state as of the
    /// read through `Decision::snapshot` - not the events. Any later event
    /// carrying the value conflicts, and is folded in before deciding
    /// again. A list-valued `field` gives one state per value. The tag
    /// mapping must be one of the command type's `tag_mappings()`, and `S`
    /// registered in the same bounded context; `build()` checks both.
    pub fn snapshot<S: crate::plugin::Snapshot>(field: &str) -> Self {
        Self {
            event_types: Vec::new(),
            tag_mappings: vec![TagMapping::new(S::TAG_KEY, field)],
            latest: None,
            snapshot: Some(S::NAME.to_string()),
        }
    }

    /// An item matching these event types, every match, no tags yet.
    pub fn types(event_types: &[&str]) -> Self {
        Self {
            event_types: event_types.iter().map(|t| t.to_string()).collect(),
            tag_mappings: Vec::new(),
            latest: None,
            snapshot: None,
        }
    }

    /// An item matching any event type carrying these tags.
    pub fn tags(tag_mappings: Vec<TagMapping>) -> Self {
        Self {
            event_types: Vec::new(),
            tag_mappings,
            latest: None,
            snapshot: None,
        }
    }

    /// Also requires these tags.
    pub fn tagged(mut self, tag_mappings: Vec<TagMapping>) -> Self {
        self.tag_mappings.extend(tag_mappings);
        self
    }

    /// Only the matching event with the highest sequence: `last(1)`.
    pub fn latest(self) -> Self {
        self.last(1)
    }

    /// Only the `count` matching events with the highest sequences.
    /// `build()` refuses 0.
    pub fn last(mut self, count: u32) -> Self {
        self.latest = Some(count);
        self
    }
}

/// A [`QueryItemMapping`] derived from one command's payload - `value
/// QueryItem` in specs/skilj.allium, recorded on `Command.consistency_query`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QueryItem {
    /// The event types the item matches; empty for any type.
    pub event_types: Vec<String>,
    /// The tags a matching event must all carry.
    pub tags: Vec<Tag>,
    /// See [`QueryItemMapping::latest`].
    pub latest: Option<u32>,
    /// See [`QueryItemMapping::snapshot`]: the item matches as a tag item
    /// does, and its events are folded into that snapshot's state for its
    /// one tag rather than handed to `decide_with()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

/// `subject_field` is never encrypted - it's the plaintext identifier the
/// `EncryptionKey` is looked up or derived by. Only `field` is what gets
/// swapped for ciphertext (at the leaf, for a dotted path - see
/// `protect_sensitive_fields` in `event_store`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SensitiveField {
    /// The payload field that is encrypted.
    pub field: FieldPath,
    /// The namespace of the subject whose key encrypts it, e.g. `customer`.
    pub subject_key: String,
    /// The payload field holding the subject's identifier, never encrypted.
    pub subject_field: FieldPath,
}

impl SensitiveField {
    /// `field`, encrypted under the key of the subject named by
    /// `subject_field`'s value in the `subject_key` namespace - e.g.
    /// `SensitiveField::new("email", "customer", "customer_id")`.
    pub fn new(
        field: impl Into<String>,
        subject_key: impl Into<String>,
        subject_field: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            subject_key: subject_key.into(),
            subject_field: subject_field.into(),
        }
    }
}

/// See `value PrivateField` in the spec - the private-field mechanism's
/// own three-way visibility rule, standing beside `SensitiveField` rather
/// than inside it: a plain read-time redaction, no `EncryptionKey`
/// anywhere behind it. `team`/`addressee_field` are each set exactly when
/// `kind` calls for them, checked by `event_store::valid_private_fields`
/// at registration - `None` for whichever the other two kinds leave
/// unused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PrivateField {
    /// The payload field that is redacted.
    pub field: FieldPath,
    /// Who may read it.
    pub kind: PrivateFieldKind,
    /// The capability a grant must hold to read it, for
    /// [`PrivateFieldKind::Capability`]; `None` otherwise.
    pub capability: Option<String>,
    /// The payload field naming the reader, for
    /// [`PrivateFieldKind::Addressed`]; `None` otherwise.
    pub addressee_field: Option<FieldPath>,
}

/// See `enum PrivateFieldKind` in the spec for what each variant means in
/// full - `Own` (default reader: the record's own creator),
/// `Capability` (default reader: any grant holding `PrivateField.capability`),
/// `Addressed` (default reader: the party `PrivateField.addressee_field`
/// names in the payload). A fourth kind, `Draft` - visible only until an
/// external status changes - is a real, deliberately deferred future
/// kind: it needs a live `Projection` lookup at read time, a materially
/// different evaluability shape from these three, which are decidable
/// from the record and the reading caller's own identity alone. Nothing
/// here is shaped around it and nothing here forecloses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PrivateFieldKind {
    /// Readable by the record's creator.
    Own,
    /// Readable by any grant holding [`PrivateField::capability`].
    Capability,
    /// Readable by the party [`PrivateField::addressee_field`] names in the
    /// payload.
    Addressed,
}

/// The whole contract a bounded context's `decide()` returns - see
/// `value CommandDecision` in the spec, and docs/architecture.md
/// §4.1/§5.4/§7.3 for why a rejection is typed data, never a
/// GraphQL/HTTP-level error.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CommandDecision {
    /// The command is accepted, and these events are appended with it,
    /// atomically.
    Accepted {
        /// The events to append, in order; empty for none.
        events: Vec<EventSpec>,
    },
    /// The command is refused, and nothing is appended.
    Rejected {
        /// A human-readable explanation.
        reason: String,
        /// A stable, machine-readable code, e.g. `insufficient_funds`.
        kind: String,
    },
}

/// One event a `decide()` call wants appended, on acceptance.
#[derive(Debug, Clone)]
pub struct EventSpec {
    /// A registered `EventType::NAME` in the command's bounded context.
    pub event_type: String,
    /// The event's payload, which must match the event type's payload
    /// shape.
    pub payload: serde_json::Value,
}

// `secret_matches` below was added propagating `bootstrap::create_superadmin`
// - unlike `generate_token_secret`/`generate_token_id` below, it needed no
// caller-supplied-output treatment (it takes strings, not producing an
// opaque one), so it was real from the start rather than deferred.

/// Generates the opaque `id` half of an `AccessToken`/`Role` - 16 random
/// bytes from the OS CSPRNG, hex-encoded (32 lowercase hex characters),
/// matching every other identifier this codebase already treats as an
/// opaque `String` rather than a typed newtype. Not a secret itself -
/// safe to log, appear in a URL path, etc. - only
/// `generate_token_secret` below carries the confidentiality requirement
/// `secret_matches` is built to compare without leaking.
pub fn generate_token_id() -> String {
    random_hex::<16>()
}

/// Generates the `secret` half of an `AccessToken`/`BootstrapSecret` -
/// 32 random bytes from the OS CSPRNG (256 bits), hex-encoded (64
/// lowercase hex characters). `ring`'s `SystemRandom` reads from the OS
/// CSPRNG, so this is cryptographically unpredictable, not merely unique
/// like `generate_token_id` only needs to be.
pub fn generate_token_secret() -> String {
    random_hex::<32>()
}

/// `N` random bytes from the OS CSPRNG, lower-case hex - the shared body
/// of [`generate_token_id`]/[`generate_token_secret`] above. Uses `ring`
/// (already this crate's cryptographically-secure randomness source, via
/// `encryption`) rather than the `uuid` crate these two used to go
/// through: neither value grew any structure from being spelled as a
/// UUID, only the 32/64 hex characters did, so a plain byte-string is the
/// honest shape.
fn random_hex<const N: usize>() -> String {
    use ring::rand::{SecureRandom, SystemRandom};
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0u8; N];
    SystemRandom::new().fill(&mut bytes).expect(
        "SystemRandom::fill only fails on catastrophic OS RNG failure - see ring's own docs",
    );
    let mut out = String::with_capacity(N * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Hashes the `secret` half of an `AccessToken` for storage - see the
/// REST token presentation note above the surfaces in the spec: "the
/// secret half is then verified against that row's stored hash... Both
/// the hashing scheme and the comparison are black boxes here, the same
/// register as generate_token_secret and generate_token_id: the spec
/// owns that the secret is never stored or compared in plaintext, not
/// which primitive does it." `generate_token_secret` already gives 256
/// bits of CSPRNG randomness (see its own doc comment) - high-entropy,
/// unlike a user-chosen password - so a plain cryptographic hash
/// (SHA-256, via `ring`, already a dependency for `encryption`) is the
/// appropriate primitive: brute-forcing a 256-bit secret back out of its
/// hash is infeasible regardless of how fast the hash is, and a slow
/// password KDF (bcrypt/argon2/scrypt) would only add latency to every
/// request's auth check for no security benefit. Base64-encoded
/// (`crate::base64`), the same encoding `encryption::encrypt` already
/// uses for its own opaque byte output.
/// Deterministic (no per-secret salt) - safe here specifically because
/// the input is never a low-entropy human-chosen value a rainbow table
/// could target, unlike a password hash.
pub fn hash_secret(secret: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, secret.as_bytes());
    crate::base64::encode(digest.as_ref())
}

/// See the `secret_matches` note above `rule CreateSuperadmin`: compares
/// two secrets in time that depends only on their lengths, never on where
/// they first differ, so a failed presentation leaks nothing about how
/// much of the real secret was guessed correctly. Real rather than
/// deferred - simple enough that there's no trivial/reachable-case split
/// the way `protect_sensitive_fields`/`derive_tags` etc. have. A length
/// mismatch still short-circuits (the length itself isn't the secret -
/// only its content is), but every byte of an equal-length comparison is
/// folded in regardless of an early difference.
pub fn secret_matches(presented: &str, actual: &str) -> bool {
    let (a, b) = (presented.as_bytes(), actual.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
