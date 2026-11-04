#!/usr/bin/env bash
# Validates specs/skilj.allium with `allium check` (cargo install allium-cli).
#
# `allium check` exits 1 on any warning, and the spec carries some that are
# understood and accepted. So this fails on what the checker calls an error,
# and on more warnings than MAX_WARNINGS: a ratchet. A change that fixes a
# warning lowers the number here; one that adds a warning has to either fix
# it or raise the number in the same diff, where a reviewer sees it.
#
# The three accepted ones are the checker's blind spots, not the spec's
# (allium-cli 3.5.3, unchanged in 3.6.1):
# - CommandDecision and EncryptedPayload "not referenced": they are what the
#   black boxes decide() and protect_sensitive_fields() return, and the
#   checker has no way to give a function a result type that it counts (a
#   contract signature's `-> Type` is dropped).
# - AccessToken's `active` "never assigned": every token is created as one
#   of its variants (`CommandToken.created(status: active, ...)`), and the
#   checker doesn't credit a variant's creation to its base entity.
set -euo pipefail

MAX_WARNINGS=3

cd "$(dirname "$0")/.."

report=$(mktemp)
trap 'rm -f "$report"' EXIT
# Exit 1 just means "something was reported"; the report decides.
allium check specs/skilj.allium >"$report" || [ $? -eq 1 ]

python3 - "$report" "$MAX_WARNINGS" <<'PY'
import json, sys
from collections import Counter

diagnostics = json.load(open(sys.argv[1]))["diagnostics"]
limit = int(sys.argv[2])
counts = Counter(d["severity"] for d in diagnostics)
print(f"check-spec: {counts['error']} errors, {counts['warning']} warnings "
      f"(at most {limit}), {counts['info']} info")
for d in diagnostics:
    if d["severity"] in ("error", "warning"):
        where = d["location"]
        print(f"  {d['severity']}: {where['file']}:{where['line']}: {d['message']}")
if counts["error"]:
    sys.exit("check-spec: the spec has errors")
if counts["warning"] > limit:
    sys.exit(f"check-spec: {counts['warning']} warnings, more than the {limit} "
             "accepted; fix the new one or raise MAX_WARNINGS in this script")
PY
