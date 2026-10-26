#!/usr/bin/env bash
# Validates specs/skilj.allium with `allium check` (cargo install allium-cli).
#
# `allium check` exits 1 on any warning, and the spec carries some that are
# understood and accepted (a status the checker cannot see assigned, a value
# only an external surface reads). So this fails on what the checker calls an
# error, and on more warnings than MAX_WARNINGS: a ratchet. A change that
# fixes a warning lowers the number here; one that adds a warning has to
# either fix it or raise the number in the same diff, where a reviewer sees it.
set -euo pipefail

MAX_WARNINGS=14

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
