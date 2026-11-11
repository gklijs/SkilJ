# SkilJ documentation

Choose a path for the task you are doing. The guides use the wallet template
and courses demo checked into this repository; open documentation from the
Git tag matching your dependency when working with an older release.

## Learn the model

1. [Build and test a wallet](wallet-tutorial.md): events, tags, decisions,
   projections, authorization and an HTTP request in one running application.
2. [Check two entities in one decision](courses-dcb.md): course capacity and
   a student's course limit, with a consistency boundary spanning both.
3. [Design with Event Modeling](event-modeling-hcl.md): map a model to types,
   tags and given/when/then scenarios before implementing domain functions.

## Build an application

| Task | Start here |
| --- | --- |
| Generate a working project | [Wallet tutorial](wallet-tutorial.md), [template README](../templates/skilj-template/README.md) |
| Work with a coding agent (Claude Code, Codex) | [Agent guidance](agent-guidance.md), the generated project's `AGENTS.md` |
| Write and test domain rules | [Wallet tests](../templates/skilj-template/tests/wallet_fixture.rs), [fixture API](https://docs.rs/skilj-test-fixture) |
| Implement traits or configure a server | [skilj API](https://docs.rs/skilj), [domain API](https://docs.rs/skilj-core) |
| Configure roles, tokens and protected fields | [Authorization flows](authorization.md) |
| Consume events over REST | [Event reading](rest-event-reading.md) |
| Store documents or other large bytes | [Payload limits and blobs](large-payloads.md) |
| Coordinate a long-running external process | [Temporal integration](temporal-integration.md) |

## Operate an application

Start with [deployment and upgrades](deployment.md) for database requirements,
bootstrap, reconciliation, shutdown and release boundaries. Use
[performance](performance.md) for measured limits and tuning, and
[environment troubleshooting](troubleshooting.md) for local or CI failures.
Report vulnerabilities through the [security policy](../SECURITY.md).

## Contribute to SkilJ

[CONTRIBUTING](../CONTRIBUTING.md) describes the feedback loop, checks and
coverage limits. The [behavioral specification](../specs/skilj.allium)
defines the contract; the [architecture history](architecture.md) records
implementation decisions and why they changed. You do not need to read that
history in order to build an application.

Architecture sections retain their numbers and anchors for existing links.
Read early proposals as history: §1.3's initial macro choice was revised in
§1.3.3, §6's builder example predates the database URL argument, and §232
replaces the old tag storage layout. Use the linked tested sources and API
documentation for current signatures, and the release changelog for upgrades.
