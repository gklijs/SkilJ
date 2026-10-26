# Authorization flows

How a caller gets from "a request arrived" to "this is allowed", for each way into skilj. The
diagrams are [Mermaid](https://mermaid.js.org/), which GitHub renders in place. This page is an
overview; the reasoning and the tests behind each rule are in
[`architecture.md`](architecture.md) (the section numbers below point there) and the behaviour
itself is in [`specs/skilj.allium`](../specs/skilj.allium), which wins if they disagree.

**Colors in the flowcharts**

```mermaid
flowchart LR
    s(["Start"]):::start
    d{"Decision"}:::decision
    a["Step"]:::step
    ok(["Allowed"]):::allow
    no["Refused"]:::deny
    hid["Hidden, looks absent"]:::hide
    s --> d --> a --> ok
    d --> no
    d --> hid

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
    classDef hide fill:#6b5a1e,stroke:#e6c76f,color:#fff
```

## Contents

1. [The two tracks](#1-the-two-tracks)
2. [Authenticating a GraphQL request](#2-authenticating-a-graphql-request)
3. [A WebSocket's lifetime](#3-a-websockets-lifetime)
4. [Authorizing a GraphQL operation](#4-authorizing-a-graphql-operation)
5. [Submitting a command, end to end](#5-submitting-a-command-end-to-end)
6. [Commands that need approval](#6-commands-that-need-approval)
7. [Fields inside a record you may read](#7-fields-inside-a-record-you-may-read)
8. [Authenticating a REST request](#8-authenticating-a-rest-request)
9. [How access gets there](#9-how-access-gets-there-bootstrap-grants-and-tokens)
10. [Capabilities: who can hand out what](#10-capabilities-who-can-hand-out-what)
11. [The access trail](#11-the-access-trail)

## 1. The two tracks

skilj has two ways in, with two kinds of credential:

| | GraphQL | REST |
|---|---|---|
| For | People and services using the application | Narrow agents and automated callers (§7.1) |
| Credential | A JWT from your identity provider | An admin-issued `<id>.<secret>` token |
| Resolves to | A `Role`, then its `RoleAccessMapping` grants | One `AccessToken`, scoped to one type |
| Scope of access | Whatever the grants allow, per bounded context | Exactly one registered event or command type |
| Who creates the access | A superadmin (Roles) and an admin (grants) | An admin of the bounded context |

```mermaid
flowchart LR
    subgraph GQL["GraphQL: HTTP and WebSocket"]
        direction LR
        JWT["Bearer JWT<br/>signed by your IdP"]:::step --> ROLE["Role<br/>matched on external_subject"]:::step
        ROLE --> GRANT["RoleAccessMapping<br/>level, scope, capabilities"]:::step
    end
    subgraph RST["REST"]
        direction LR
        TOK["Bearer id.secret"]:::step --> AT["AccessToken<br/>one type, optional owner scope"]:::step
    end
    GRANT --> BC[("Bounded context<br/>events, commands, projections")]:::store
    AT --> BC
    SUPER(["Superadmin"]):::start -. "creates Roles,<br/>bounded contexts, grants" .-> GRANT
    ADMIN(["Admin grant"]):::start -. "mints and revokes" .-> AT
    ADMIN -. "registers types" .-> BC

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef store fill:#4a2d6b,stroke:#b79ae0,color:#fff
```

## 2. Authenticating a GraphQL request

Runs on every HTTP request and on every WebSocket `connection_init`. This is the only place
JWTs are validated.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant S as skilj GraphQL
    participant J as JWKS cache
    participant I as Identity provider
    participant DB as Postgres

    C->>S: Authorization: Bearer JWT
    S->>J: key for the token's kid
    opt kid not cached
        J->>I: fetch JWKS (bounded timeout, §82)
        I-->>J: keys
        Note over J: Reactive refresh only. Garbage kids<br/>cannot force repeated fetches (§6)
    end
    J-->>S: verification key
    S->>S: Verify signature and configured algorithm
    S->>S: Require iss, exp and aud (aud must match<br/>a configured audience, §81)
    S->>S: Honour nbf if present (§111)
    alt any check fails
        S-->>C: jwt_verification_failed
    end
    S->>DB: active Role WHERE external_subject = sub<br/>(one indexed lookup, §110)
    alt no active Role
        S-->>C: unauthenticated
    end
    DB-->>S: Role
    S->>DB: this Role's grants only (§124)
    DB-->>S: RoleAccessMapping rows
    S-->>C: Run the request as this Role
```

## 3. A WebSocket's lifetime

Subscriptions authenticate once, in `connection_init`, and are then held to that credential
(§135, §136).

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Connected: upgrade
    Connected --> Authenticated: connection_init with valid JWT
    Connected --> Closed_4408: no connection_init in time (default 10 s)
    Connected --> Closed_auth: connection_init with bad JWT
    Authenticated --> Authenticated: events flow, ping every 30 s answered by pong
    Authenticated --> Closed_4403: JWT expires
    Authenticated --> Dropped: no frame for two ping intervals
    Closed_4408 --> [*]
    Closed_auth --> [*]
    Closed_4403 --> [*]
    Dropped --> [*]

    classDef good fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef bad fill:#8b2635,stroke:#f2868f,color:#fff
    class Authenticated good
    class Closed_4408,Closed_auth,Closed_4403,Dropped bad
```

A client such as `skilj-tui` can refresh an expired token with `--token-command` and reconnect,
resuming from its last sequence (§137, §106).

## 4. Authorizing a GraphQL operation

Authentication only says who the caller is. Each operation then asks what that Role's grants on
the named bounded context allow. Checks run top to bottom and a refusal stops the request.

```mermaid
flowchart TD
    START(["Authenticated Role calls an operation"]):::start --> SUPERQ{"Superadmin-only<br/>operation?"}:::decision
    SUPERQ -- yes --> ISSUPER{"Role.superadmin?"}:::decision
    ISSUPER -- no --> DENY1["Refused"]:::deny
    ISSUPER -- yes --> OKSUPER(["Allowed"]):::allow
    SUPERQ -- no --> HASGRANT{"Active grant on<br/>this bounded context?"}:::decision
    HASGRANT -- no --> DENY2["grant_not_active"]:::deny
    HASGRANT -- yes --> KIND{"What is it?"}:::decision

    KIND -- "read events, commands,<br/>projections" --> LREAD{"level at least Read?"}:::decision
    KIND -- "submit a command" --> LWRITE{"level at least Write?"}:::decision
    KIND -- "register types, mint tokens,<br/>approve, other admin work" --> LADMIN{"level Admin?"}:::decision
    LREAD -- no --> DENY3["insufficient access level"]:::deny
    LWRITE -- no --> DENY3
    LADMIN -- no --> DENY3

    LWRITE -- yes --> ROLEQ{"Command has<br/>requires_role?"}:::decision
    ROLEQ -- "yes, differs from<br/>the caller's Role.name" --> DENY4["insufficient role"]:::deny
    ROLEQ -- "no, or it matches" --> OKCMD(["Continue with<br/>command submission, §5"]):::allow

    LREAD -- yes --> PROJQ{"Reading a projection?"}:::decision
    PROJQ -- no --> OKREAD(["Allowed, then field rules, §7"]):::allow
    PROJQ -- yes --> CAPQ{"Projection requires<br/>a capability?"}:::decision
    CAPQ -- "yes, grant lacks it" --> MISSING["not_found:<br/>looks like it does not exist"]:::hide
    CAPQ -- "no, or held" --> SCOPEQ{"Grant has a scope and the<br/>projection an owner tag?"}:::decision
    SCOPEQ -- "scope does not match" --> DENY5["scope mismatch"]:::deny
    SCOPEQ -- "no, or it matches" --> OKPROJ(["Allowed, then field rules, §7"]):::allow

    LADMIN -- yes --> SCOPEDMINT{"Scoped grant minting<br/>or revoking a token?"}:::decision
    SCOPEDMINT -- "another owner's scope,<br/>or no scope on revoke" --> DENY6["token_scope_beyond_grant"]:::deny
    SCOPEDMINT -- "no, or within own scope" --> OKADMIN(["Allowed"]):::allow

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
    classDef hide fill:#6b5a1e,stroke:#e6c76f,color:#fff
```

The rules, in short:

- **Level** is `Read`, `Write` or `Admin`, each including the one before.
- **Scope** on a grant confines it to one owner's records wherever a projection declares an owner
  tag, for admin grants too (§23). A scoped admin can only mint or revoke tokens within its own
  scope (§92). Other admin surfaces (type registration, parked deliveries, `forgetSubject`) stay
  bounded-context-wide; a tenant that needs a fully confined admin gets its own bounded context.
- **A refused projection looks like a missing one.** A caller without the capability gets the
  same answer, and the same absent type in its per-caller GraphQL schema, as for a name that does
  not exist (§138, §228).
- **Checks that could leak existence run first.** The superadmin check comes before any
  not-found answer, so a bounded context's existence is not an oracle (§139).

## 5. Submitting a command, end to end

Both surfaces authorize the same way before the command reaches `decide()`. Ordering matters:
an unauthorized caller never reaches the decider (§1.3.1).

```mermaid
flowchart TD
    GQLIN(["GraphQL submitCommand<br/>Bearer JWT"]):::start --> AUTHG["Authenticate JWT to a Role, §2"]:::step
    RESTIN(["REST POST /v1/commands/trigger<br/>Bearer id.secret"]):::start --> AUTHR["Resolve CommandToken, §8"]:::step

    AUTHG --> GRANTCHK{"Write grant on the<br/>bounded context?"}:::decision
    AUTHR --> TOKCHK{"Token active and its<br/>type allows REST trigger?"}:::decision
    GRANTCHK -- no --> DENY["Refused"]:::deny
    TOKCHK -- no --> DENY

    GRANTCHK -- yes --> ROLE{"requires_role set and<br/>different from Role.name?"}:::decision
    ROLE -- yes --> DENY
    ROLE -- no --> IDEM
    TOKCHK -- yes --> IDEM["Validate the idempotency key.<br/>Keys are namespaced per client_id (§37)<br/>and the skilj-approval: prefix is reserved"]:::step

    IDEM --> POLICY{"Command type has an<br/>approval policy?"}:::decision
    POLICY -- yes --> PROPOSE(["Stored as a proposal.<br/>decide() is NOT run.<br/>Answer: pendingApproval + proposalId, §6"]):::hide
    POLICY -- no --> LOCK["Take the bounded context's lock,<br/>read the events sharing the command's tags"]:::step
    LOCK --> DECIDE{"decide()"}:::decision
    DECIDE -- Rejected --> REJ(["accepted: false<br/>rejectionKind, rejectionReason.<br/>A business rejection, not an error"]):::deny
    DECIDE -- Accepted --> WRITE["Append events atomically,<br/>sensitive fields encrypted,<br/>update projections, broadcast"]:::step
    WRITE --> DONE(["accepted: true<br/>triggeredEventSequences"]):::allow

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
    classDef hide fill:#6b5a1e,stroke:#e6c76f,color:#fff
```

`dryRunCommand` (GraphQL, Admin only) and `POST /v1/commands/dry-run` (REST) run the same
authorization, then report what the decision would be and write nothing (§185).

## 6. Commands that need approval

A command type can declare that a submission is only a proposal, held until an admin approves it
(issue #98, §214). The decision is made against the state at approval time, not at proposal time.

```mermaid
sequenceDiagram
    autonumber
    participant A as Submitter<br/>(agent or user)
    participant S as skilj
    participant P as command_proposals
    participant H as Approver<br/>(admin grant)

    A->>S: Submit command (normal authorization, §5)
    S->>P: Insert proposal, payload encrypted,<br/>deduplicated on the idempotency key
    S-->>A: accepted: false, pendingApproval: true, proposalId
    H->>S: commandProposals(boundedContext, status)
    S-->>H: Pending proposals (paged)
    H->>S: approveCommand(proposalId)
    S->>S: Admin grant on the context?<br/>approval_role matches Role.name, if set?
    S->>S: Is the approver the proposer?<br/>Refused unless the type allows self-approval
    S->>P: Claim: pending to approving (conditional UPDATE)
    S->>S: Run the ordinary pipeline, as the proposer's client id,<br/>idempotency key skilj-approval:proposalId
    alt decide() accepts
        S->>P: approved, command_external_id
    else decide() rejects
        S->>P: rejected, rejection_reason
    end
    S-->>H: Outcome
```

```mermaid
stateDiagram-v2
    direction LR
    [*] --> pending: submitted
    pending --> approving: an approver claims it
    approving --> approved: decide() accepts
    approving --> rejected: decide() rejects at approval
    approving --> pending: transient failure releases the claim
    pending --> rejected: rejectCommand (withdrawing your own is always allowed)
    pending --> expired: approval deadline passes
    pending --> forgotten: forgetSubject for a subject in its sensitive fields
    approved --> [*]
    rejected --> [*]
    expired --> [*]
    forgotten --> [*]

    classDef good fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef bad fill:#8b2635,stroke:#f2868f,color:#fff
    classDef wait fill:#6b5a1e,stroke:#e6c76f,color:#fff
    class approved good
    class rejected,expired,forgotten bad
    class pending,approving wait
```

## 7. Fields inside a record you may read

Passing the checks above lets you read a record. Individual fields can still be withheld. This
is decided inside the read, not by redacting afterwards.

```mermaid
flowchart TD
    F(["A field in a record the caller may read"]):::start --> KIND{"Field kind"}:::decision

    KIND -- "Sensitive<br/>encrypted per subject" --> S1{"Grant has<br/>can_read_sensitive?"}:::decision
    S1 -- yes --> SOK(["Decrypted"]):::allow
    S1 -- no --> S2{"The field's subject is<br/>the caller's own external_subject?"}:::decision
    S2 -- yes --> SOK
    S2 -- no --> SNO(["null"]):::hide
    SKEY["forgetSubject deletes the subject's key:<br/>nobody can decrypt it any more"]:::deny -.-> SNO

    KIND -- "Private: Own" --> P1{"Caller created<br/>the record?"}:::decision
    KIND -- "Private: Addressed" --> P2{"Caller is the party the<br/>addressee field names?"}:::decision
    KIND -- "Private: Capability" --> P3{"Grant holds the<br/>named capability?"}:::decision
    P1 -- yes --> POK(["Shown"]):::allow
    P2 -- yes --> POK
    P3 -- yes --> POK
    P1 -- no --> PNO(["Hidden"]):::hide
    P2 -- no --> PNO
    P3 -- no --> PNO

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
    classDef hide fill:#6b5a1e,stroke:#e6c76f,color:#fff
```

Notes:

- REST reads fail closed on private fields (§31).
- Parked requests and a rejection's `matchingEvents` are masked by the same rules (§118, §119).
- Private fields are redacted in a read, not in stored projection or snapshot state; §177
  documents what a snapshot keeps.

## 8. Authenticating a REST request

There is no JWT here. A route checks that the token is the right kind for it, and the answer to
a wrong credential is deliberately uninformative: a token id alone reveals nothing (§142).

```mermaid
flowchart TD
    REQ(["REST request"]):::start --> HDR{"Authorization is<br/>Bearer id.secret?"}:::decision
    HDR -- "missing or malformed" --> E401A["401"]:::deny
    HDR -- yes --> LOOKUP["Look the token up by id.<br/>Compare the secret in constant time"]:::step
    LOOKUP --> MATCH{"Secret matches a<br/>token of any kind?"}:::decision
    MATCH -- no --> E401B["401 unrecognised_credential<br/>whatever the id names"]:::deny
    MATCH -- yes --> STATUS{"Token active?"}:::decision
    STATUS -- revoked --> E403B["403 token not active"]:::deny
    STATUS -- yes --> KIND{"Right token kind<br/>for this route?"}:::decision
    KIND -- no --> E403["403 wrong_token_variant"]:::deny
    KIND -- yes --> SC{"Token has an<br/>owner scope?"}:::decision
    SC -- yes --> APPLY["Reads and writes are confined<br/>to that owner's records"]:::step
    SC -- no --> RUN
    APPLY --> RUN(["Run the action on the token's one type"]):::allow

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
```

Which token each route takes (§7.2):

| Route | Token kind |
|---|---|
| `POST /v1/events/external`, `/v1/events/external/batch` | `ExternalEventToken` |
| `POST /v1/events/direct` | `DirectCreationToken` |
| `GET /v1/events`, `GET /v1/events/consume`, `POST /v1/events/consume/ack` | `EventReadToken` |
| `POST /v1/commands/trigger`, `/trigger/batch`, `/dry-run` | `CommandToken` |

## 9. How access gets there: bootstrap, grants and tokens

Nothing is allowed by default. A new bounded context is inert until someone is granted access to
it, on purpose.

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant P as skilj process
    participant SA as Superadmin
    participant AD as Admin grant
    participant R as REST caller

    P->>Op: Logs a one-time bootstrap secret at startup<br/>(only while no superadmin exists, §109)
    Op->>P: createSuperadmin(secret, name, external_subject)
    Note over P: Atomic and single use. The secret ends with the<br/>first claim, and a new one needs a restart
    P-->>SA: Superadmin Role exists

    SA->>P: addBoundedContext
    Note over P: Inert: nobody can use it yet
    SA->>P: createRole, grantRoleAccessMapping(level, scope)
    P-->>AD: Role holds an Admin grant on that context
    Note over P,AD: The app's reconciliation Role needs an Admin grant too.<br/>Without one its types are skipped (skipped_no_access)

    AD->>P: Register event, command and projection types
    AD->>P: Mint a token for one type, with optional owner scope
    P-->>AD: id.secret, shown once
    AD-->>R: Hands over the credential
    R->>P: REST calls with Bearer id.secret
    AD->>P: revokeToken
    Note over P: A missing and a forbidden token<br/>get the same answer (§142)
```

Roles and grants have their own lifecycle. Revoking a Role ends each of its grants, and a
subject can be claimed by only one active Role at a time.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> RoleActive: createRole
    RoleActive --> RoleRevoked: revokeRole
    [*] --> GrantActive: grantRoleAccessMapping
    GrantActive --> GrantActive: capabilities change in place
    GrantActive --> GrantRevoked: revoke grant
    RoleRevoked --> GrantRevoked: ends each of its grants
    RoleRevoked --> [*]
    GrantRevoked --> [*]
```

## 10. Capabilities: who can hand out what

A capability is a named privilege on a grant, and it is the only thing that opens a gated
projection or private field. Nothing implies one: not `Admin`, not superadmin, not a Role's name
(§228).

```mermaid
flowchart TD
    A(["grantCapability or revokeCapability"]):::start --> W{"Who is calling?"}:::decision
    W -- Superadmin --> SUP["May change any grant's capabilities,<br/>its own included"]:::step
    W -- "Admin grant" --> ADM1{"Target grant is in the admin's<br/>own bounded context?"}:::decision
    ADM1 -- no --> D1["grant_not_active"]:::deny
    ADM1 -- yes --> ADM2{"Target is the admin's<br/>own grant?"}:::decision
    ADM2 -- yes --> D2["SelfGrantRefused"]:::deny
    ADM2 -- no --> ADM3{"Capability is in the admin's<br/>delegable set?"}:::decision
    ADM3 -- no --> D3["Refused"]:::deny
    ADM3 -- yes --> OK
    W -- "anyone else" --> D1
    SUP --> OK(["Applied, and recorded in the<br/>same transaction, §11"]):::allow
    SETD["setDelegableCapabilities<br/>superadmin only"]:::step -. "defines" .-> ADM3

    classDef start fill:#1f4e79,stroke:#7fb3e6,color:#fff
    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
```

An admin can run a team without being able to read what the team reads: handing out access and
holding it are separate things. Lowering `delegable` does not reach back to what the admin
already granted. A grant holds at most 16 capabilities and delegates at most 16.

Where a capability is required, and what happens without it:

```mermaid
flowchart LR
    CAP[("Capability on the grant")]:::store
    CAP --> PRJ["Projection.requires_capability"]:::step --> PRJNO["Absent from the caller's GraphQL schema.<br/>Federation never publishes it"]:::hide
    CAP --> FLD["Private field of kind Capability"]:::step --> FLDNO["Field hidden"]:::hide
    CAP --> NONE["A requirement nobody holds"]:::step --> NONENO["Fails closed"]:::deny

    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef store fill:#4a2d6b,stroke:#b79ae0,color:#fff
    classDef deny fill:#8b2635,stroke:#f2868f,color:#fff
    classDef hide fill:#6b5a1e,stroke:#e6c76f,color:#fff
```

## 11. The access trail

Every change to who holds what is also an event in the `admin` bounded context, written in the
same transaction as the change (§229). If the event cannot be written, the change is not made.

```mermaid
flowchart LR
    M["Grant, revoke, capability change,<br/>delegable change, Role revoked,<br/>private-field share or unshare"]:::step --> TX{{"One transaction"}}:::decision
    TX --> ROW[("Grant row changes")]:::store
    TX --> EV[("Event in the admin context<br/>AccessGranted, AccessRevoked,<br/>CapabilityGranted, CapabilityRevoked,<br/>DelegableCapabilitiesSet, AccessBaselined,<br/>PrivateFieldAccessGranted,<br/>PrivateFieldAccessRevoked")]:::store
    EV --> Q["capabilityHistory(roleId, boundedContext, asOf)<br/>superadmin only"]:::step
    Q --> ANS(["Held, level, scope, capabilities, delegable<br/>as of that instant. known: false<br/>before the trail began"]):::allow

    classDef decision fill:#3a3a5c,stroke:#9a9ad0,color:#fff
    classDef step fill:#2d3748,stroke:#718096,color:#fff
    classDef store fill:#4a2d6b,stroke:#b79ae0,color:#fff
    classDef allow fill:#1e6b3a,stroke:#6fcf97,color:#fff
```

- Payloads hold ids only, never names or subjects, so there is nothing a person could ask to
  have forgotten.
- Refused attempts are not recorded: they change nothing.
- At startup each active grant whose events do not fold to held gets one `AccessBaselined`, so
  history starts from what it held at upgrade. That covers a grant with no events and one that
  only has capability events. A grant revoked before that gets no invented history.
- A revoke that finds nothing left to revoke records nothing. A stored event that does not parse
  fails the read rather than being skipped.
- Sharing the private fields of a record is recorded as `PrivateFieldAccessGranted` and
  `PrivateFieldAccessRevoked`. The grantee is the event's Role and the grantor the actor. These
  name no grant, so they do not count in `capabilityHistory`, and a share made before the trail
  began has no event of its own.
- Deleting a bounded context ends its active grants in the trail (`AccessRevoked` with reason
  `bounded_context_deleted`), so `capabilityHistory` stops reporting them as held. Archiving
  ends nothing and records nothing.
- Revoking a Role ends its grants and records each, but leaves the private-field shares it made or
  holds as they are, with no event (the Role can no longer authenticate).
- Not recorded: denied attempts, reads of gated projections, the other admin operations
  (tokens, type registration, and adding or archiving a bounded context).
