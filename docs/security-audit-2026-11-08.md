# Security audit — 2026-11-08

Reviewed revision: `09150aa5c2c5b9c9a3d6535c37edc886ea751124`.

This is a source-based security review across the workspace, supported by bounded local reproductions. It is not a certification or an exhaustive line-by-line review. No production systems were contacted. This audit initially changed no application code; subsequent fixes are described in the remediation update.

## Findings

| ID | Severity | Finding | Prerequisite |
| --- | --- | --- | --- |
| SEC-01 | High | Small GraphQL documents cause exponential CPU work before rejection | Access to `/graphql`; no credential required |
| SEC-02 | Medium | Cached JWT signing keys have no maximum lifetime | An IdP signing key is compromised and subsequently removed |
| SEC-03 | Medium | A scoped admin can remove the owner restriction protecting other owners' records | Admin grant in a shared bounded context; documented design limitation |
| SEC-04 | Medium | External event provenance can expose a plaintext copy of protected payload fields | An integration supplies confidential data in `sourceContent`; a caller has event inspection access |

Severity reflects these prerequisites. SEC-03 is a verified consequence of an explicitly documented trust model, rather than a discrepancy from the documentation. SEC-04 is conditional on integration input; the bundled broker bridges currently use short provenance labels rather than copying their messages into this field.

### SEC-01 — Exponential GraphQL fragment expansion

Locations:

- `skilj-graphql/src/limits.rs:105` computes the complete cost before checking the budget.
- `skilj-graphql/src/limits.rs:135` recursively expands every fragment occurrence without memoization or a traversal budget.
- `skilj-graphql/src/schema.rs:571` installs depth, complexity and expensive-field limits.
- The locked `async-graphql` 7.2.1 dependency also recursively expands fragments in `check_recursive_depth` before validation (`src/schema.rs:739`). Fixing only the custom cost counter is insufficient.

A fragment can spread the next fragment six times. Each added fragment level multiplies traversal work by six, while the input grows by only about 62 bytes. The dependency's recursion limit bounds depth, but allows a large branching expansion within that depth. The request body cap does not prevent this.

The custom counter also expands fragments containing no expensive fields. Even an early return after exceeding the expensive-field count would leave that case exposed. Parsing and traversal run synchronously while the request future is polled, so an ordinary asynchronous timeout does not reliably interrupt them. Enough concurrent requests can occupy the server's runtime workers.

**Reproduction:** construct this pattern, with six spreads at each level:

```graphql
{ ...F0 }
fragment F0 on Query { ...F1 ...F1 ...F1 ...F1 ...F1 ...F1 }
fragment F1 on Query { ...F2 ...F2 ...F2 ...F2 ...F2 ...F2 }
# Continue the pattern, ending with a scalar field.
fragment F2 on Query { ok }
```

The local probe built a dynamic schema with a single `ok: String!` field and the project's cost extension, depth 24, complexity 2000, and expensive-field budget 10. Results from the debug build:

| Fragment levels | Query bytes | Time before rejection |
| --- | --- | --- |
| 6 | 348 | 15 ms |
| 8 | 472 | 538 ms |
| 10 | 596 | 20,660 ms |

All eventually answered `Query is too complex`. The measured times are specific to this machine and debug build; the exponential traversal exists independently of compiler optimization. These were bounded local schema executions, not a load test against a deployed HTTP server. Anonymous requests reach the same schema limits through `execute_as`.

**Remediation:** add a bounded, non-expanding document preflight before handing the query to recursive dependency checks. Treat fragments as a graph, detect cycles, memoize costs and depth, and use saturating arithmetic with a total traversal/expansion budget. Apply it to HTTP, WebSocket and federation execution paths. Patch or upgrade the dependency's affected traversal as needed. Test both expensive and ordinary scalar leaves, repeated fragments, cycles, and unselected operations.

### SEC-02 — Removed JWT signing keys remain trusted in warm caches

Location: `skilj-core/src/access_control/mod.rs:981` (`JwksCache::key_for`).

A cache hit returns its decoding key immediately. Only an unknown `kid` triggers a refresh, subject to the five-second minimum interval. There is no maximum cache age or periodic refresh. Consequently, removing a compromised signing key from the IdP's JWKS does not revoke that key on a running instance that already cached it.

JWT expiration does not bound this exposure when the attacker holds the signing key: the attacker can mint new tokens with future expiration times and the configured issuer/audience. Impersonation additionally requires a subject that resolves to an active Role. A refresh triggered by a different, unknown key, or a restart, can end the exposure; neither is guaranteed to occur within any bounded time.

**Reproduction:** a loopback JWKS server initially returned the repository's RSA test key. A JWT populated a cache. The server then returned an empty key set. After six seconds, the probe signed a new JWT for a different subject under the removed key. The warm cache accepted it without refetching; a new cache fetched the empty set and rejected the same JWT.

```text
removed signing key: warm cache accepted freshly signed subject=superadmin-victim; fresh cache rejected
```

The probe validated JWT verification only; it did not create a Role or claim that an arbitrary unknown subject authenticates to GraphQL.

**Remediation:** enforce a maximum key-set age even on cache hits, with bounded single-flight refresh. Define and test the stale-key policy during IdP outages. Once a successful refresh removes a key, stop accepting it. Preserve the existing miss throttling and fetch timeout. During an incident, refresh or restart every instance after key removal until bounded refresh is implemented.

### SEC-03 — Scoped admin registration can disable owner isolation

Locations:

- `skilj-graphql/src/resolvers/type_registration.rs:44` accepts an admin mapping and caller-selected `ownerTagKey`.
- `skilj-core/src/event_store/mod.rs:3139` checks grant status, level and bounded context, but does not restrict registration changes based on the admin's owner scope.
- `skilj-core/src/event_store/mod.rs:3279` accepts a replacement owner key, including `None`; command registration has the same behavior.
- `skilj-core/src/db/event_types.rs:122` persists the new owner key and field policies.
- `skilj-core/src/event_store/mod.rs:2235` treats an absent owner key as unrestricted.

An admin scoped to owner A can re-register an existing type with the same schema and tag mappings but a null `ownerTagKey`. Subsequent reads using the new type declaration no longer filter out owner B. Existing scoped tokens also lose this dimension of restriction. Registration can also replace private-field declarations, so granting schema administration is broader than granting access to the records currently visible to the admin.

**Reproduction:** the core probe created a mapping scoped to `tenant-A` and a type owned through the `currency` tag. A record tagged `currency=tenant-B` initially failed the owner check. Calling the real registration function with unchanged schema/tags and `owner_tag_key=None` succeeded; the same record then passed the owner check.

```text
scope bypass: tenant-A admin removed ownerTagKey, tenant-B tag now passes
```

This reproduction exercised registration and the read authorization predicate, not a complete database-backed HTTP request.

**Design qualification:** `docs/authorization.md:195` explicitly says type registration, parked deliveries and subject erasure remain bounded-context-wide, and recommends a separate bounded context for a fully confined tenant admin. The finding is therefore that an owner-scoped Admin grant is not an isolation boundary against a hostile admin. The existing token mint/revoke scope checks do not make it one.

**Remediation:** use a separate bounded context for mutually untrusted tenant admins under the current design. If scoped administration is intended to be a security boundary, require a separate trusted schema-management authority for changes to owner and field policies; scoped admins must not be able to weaken those policies. Review the other documented context-wide administrative actions together.

### SEC-04 — `sourceContent` bypasses payload field protection

Locations:

- `skilj-core/src/event_store/mod.rs:4721` protects sensitive fields in `payload`, while `:4744` copies `source_content` unchanged into the event origin.
- `skilj-core/src/db/events.rs:2072` persists that provenance independently of the protected payload.
- `skilj-core/src/event_store/mod.rs:3739` redacts originating command payloads during inspection, but leaves external provenance unchanged.
- `skilj-graphql/src/gql_types.rs:1148` returns external `sourceContent` directly.

If an integration uses `sourceContent` for the original source message, confidential fields occur twice: once in the protected payload and once in unprotected provenance. A caller with an Admin grant permitted to inspect the event can retrieve the original copy even when payload rendering denies access to that field. Admin level does not otherwise imply private-field or sensitive-field access. The unencrypted duplicate also cannot be erased by destroying the payload's subject key.

**Reproduction:** an external event declared `amount` as an `Own` private field and used `{"amount":12345}` for both payload and source content. Inspection under a different Role's admin mapping produced:

```text
origin leak: renderedPayload={"amount":null}, sourceContent={"amount":12345}
```

This demonstrates the private-field disclosure using the real creation and inspection functions. Sensitive-field erasure persistence follows from the separate plaintext storage path; a database-backed erasure reproduction was not run. The bundled Kafka, AMQP and NATS bridges use provenance labels, so this is not a claim that those bridges currently duplicate all message bodies.

**Remediation:** define `sourceContent` as non-confidential provenance and enforce that contract, or store raw source material behind explicit access controls and subject-aware encryption/erasure. Do not assume rendering `payload` protects arbitrary copies elsewhere. Review existing externally ingested rows for confidential provenance before relying on erasure guarantees.

## Coverage and validation

The review surveyed the 18 workspace crates and traced the principal authentication, grant, token, owner-scope, private-field, encryption and HTTP boundaries. It also inspected blob upload/download authorization and storage, broker/Temporal REST integration patterns, SQL identifier/value handling, code generation and macro inputs, local database/key-file handling, CLI token commands, CI permissions/downloads and the deployment template. Review depth varied; this is not a claim that every line or every state transition was examined.

Positive controls observed include asymmetric JWT algorithm selection, required issuer/audience/expiration checks, not-before validation, constant-time token hash comparison, parameter binding and escaped SQL identifiers, payload/body/page limits, subscription lifecycle limits, private-field rendering, and blob download authorization with attachment/nosniff headers.

Validation performed:

- `cargo deny check advisories` refreshed the advisory database and completed successfully: `advisories ok`. This checks the locked dependency graph against that database, not application logic or undisclosed vulnerabilities.
- A bounded GraphQL probe compiled the current `limits.rs` into a dynamic-schema harness using locally built dependencies and reproduced SEC-01.
- A core probe linked locally built workspace libraries and reused repository fixtures to reproduce SEC-02, SEC-03 and SEC-04. SEC-02 used a loopback HTTP JWKS server. Source inspection verified the relevant current implementation paths.
- Probe sources and binaries are in `/tmp/skilj-security-audit/` for this session; they were not added to the production crates.

Not performed: the full workspace build/test/clippy/CGA pipeline, database-backed end-to-end exploit tests, production configuration inspection, external penetration testing, sustained load testing, or fuzzing. At the time of the audit, no security fix had been applied or regression-tested; see the remediation update below. Passing advisories and the inspected controls do not establish absence of further issues.

## Additional observations

- Approval self-separation is explicitly limited for REST/internal proposals: they have no proposing human Role. An admin who can obtain a command token can submit through REST and approve the proposal. Since that admin can also manage approval policy through type registration, this was treated as part of the trusted-admin model, not an additional privilege-escalation finding.
- Blob uploads have a per-object size bound but no per-token storage quota, as already documented in architecture section 239. A stolen upload-capable credential can consume storage until collection catches up.
- `KeyFileProvider::write` uses a predictable sibling `.tmp` name with create/truncate and creation-only mode `0600`. Keep its directory private. A unique, exclusively created temporary file would avoid following a planted symlink or retaining permissive permissions on an existing temporary file.
- The demo uses a checked-in RSA fixture and binds its API to `0.0.0.0`; templates print development credentials and seed an admin. Keep these development shortcuts out of production configurations. The template container itself runs as a non-root numeric UID.
- CI uses read-only repository permissions. Several actions/images remain tag-pinned, and downloaded tools are not uniformly checksum-verified; immutable pins and verified downloads would improve supply-chain assurance.

Prioritize SEC-01, then bound signing-key revocation latency. Resolve the admin trust model and provenance contract before using shared contexts for hostile tenants or claiming subject erasure covers raw source messages.


## Remediation update

The working-tree fixes address all four findings:

- SEC-01: bounded document preflight runs before the dependency's recursive
  validation, on HTTP, subscription and federation schema execution paths.
- SEC-02: successful JWKS sets expire after five minutes; expired keys fail
  closed during refresh errors/throttling. Cold and expired concurrent lookups
  remain single-flight. Existing authenticated WebSockets keep their existing
  JWT-expiration lifecycle; they are not proactively closed on IdP key removal.
- SEC-03: shared type registration and projection rebuild changes require an
  unscoped Admin grant. Core and HTTP tests cover the rejection, and startup
  reconciliation skips scoped grants. Other context-wide administrative powers
  are unchanged; separate contexts remain necessary for fully isolated admins.
- SEC-04: at the user's request, protected event types discard raw source
  content while retaining payloads and metadata. Types with private/sensitive
  fields or subject-encrypted blobs use `[redacted]`. Upgrade cleanup scrubs
  live historical rows; database guards cover policy changes and old writers.
  Metadata must itself be non-confidential. Existing backups/exports are not
  rewritten. Unprotected event types can still retain raw messages for debugging.

The first upgrade can rewrite many provenance columns and generate WAL; allow
maintenance time and disk headroom. Source payloads, timestamps, source identifiers,
caller ids, correlation/causation ids, tags and sequences are retained.

Regression coverage includes the original fragment expansion, anonymous HTTP,
subscription and federation execution, unused operations/fragments, key removal
and IdP outages, scoped schema mutations, reconciliation, and real Postgres
cleanup/guards with concurrent installers and stale writers. The API conformance
transcript remains unchanged. The implementation and operational tradeoffs are
also documented in architecture section 240.


Remediation validation completed:

- 398 Rust tests across core unit/security/persistence suites, GraphQL units,
  anonymous HTTP and schema administration, API conformance, payload limits,
  reconciliation, and subject erasure. Service-dependent tests used a disposable
  Postgres 17 server; the initial embedded-server skip was replaced by a real run.
  The reconciliation suite passed after matching CI's 500-connection setting.
- Workspace clippy with all targets and warnings denied; workspace rustdoc with
  warnings denied; formatting, section-reference and Allium checks.
- 17 CGA regression tests. The pinned core analysis completed with unchanged
  generic-call gaps; its reviewed baseline update is documented in
  `scripts/cga/README.md`. Potential dependency panics remain tracked, not proven
  impossible.

The entire workspace test suite, broker/Temporal containers, and external
penetration/fuzz testing were not run as part of remediation. No deployment or
production cleanup was performed; the cleanup runs when the fixed version brings
a bounded context up to date.
