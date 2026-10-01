#!/usr/bin/env bash
#
# Validates .woodpecker.yml against Woodpecker's own published JSON
# schema - the same schema its linter uses.
#
# Why this exists: `services:` sat under a step for the whole life of the
# file, which is Drone's shape and not Woodpecker's. Woodpecker's linter
# said so on every single run - "Additional property services is not
# allowed" - and a linter finding does not fail a step, so the pipeline
# carried on with no service container and five diagnosis rounds chased
# that. A check that reports and does not block is not a check.
#
# The schema is fetched rather than vendored so it tracks the version
# Codeberg actually runs; a vendored copy would go stale quietly, which is
# the same failure in a different costume. Pin
# WOODPECKER_SCHEMA_URL to a specific tag to validate against a fixed
# version.
#
# Needs python3 with PyYAML and jsonschema (Debian: python3-yaml,
# python3-jsonschema), plus network access for the schema fetch.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

SCHEMA_URL="${WOODPECKER_SCHEMA_URL:-https://raw.githubusercontent.com/woodpecker-ci/woodpecker/main/pipeline/frontend/yaml/linter/schema/schema.json}"
schema="$(mktemp)"
trap 'rm -f "$schema"' EXIT

if ! curl -fsSL --max-time 30 "$SCHEMA_URL" -o "$schema"; then
  echo "ERROR: couldn't fetch Woodpecker's schema from ${SCHEMA_URL}" >&2
  echo "ERROR: not continuing - a check that skips itself when it can't" >&2
  echo "ERROR: run is the check this exists to be." >&2
  exit 1
fi

python3 - "$schema" <<'PY'
import json
import sys

import jsonschema
import yaml

schema = json.load(open(sys.argv[1]))
with open(".woodpecker.yml") as handle:
    config = yaml.safe_load(handle)

validator = jsonschema.Draft7Validator(schema)
errors = sorted(validator.iter_errors(config), key=lambda e: list(e.path))
if errors:
    print("ERROR: .woodpecker.yml does not match Woodpecker's schema:", file=sys.stderr)
    for error in errors:
        # The schema validates `steps` with anyOf/oneOf, so the top-level
        # error is "not valid under any of the given schemas" and carries
        # the whole config as its context. best_match descends into that
        # to the sub-error that actually names the offending key -
        # without it the message is a screenful of YAML and the one fact
        # that matters ("services is not allowed here") is buried in it.
        error = jsonschema.exceptions.best_match([error])
        where = "/".join(str(part) for part in error.absolute_path) or "<top level>"
        message = " ".join(error.message.split())
        if len(message) > 200:
            message = message[:200] + "..."
        print(f"  {where}: {message}", file=sys.stderr)
        for suberror in sorted(error.context, key=lambda e: list(e.path))[:4]:
            sub = " ".join(suberror.message.split())
            if len(sub) > 200:
                sub = sub[:200] + "..."
            print(f"    because: {sub}", file=sys.stderr)
    print(
        "\nWoodpecker's own linter reports this too, but as a warning that does\n"
        "not fail the pipeline - the steps run regardless. So it is worth\n"
        "failing here instead: a step that silently loses its `services:`, or\n"
        "any other key, is a green build that tested nothing.",
        file=sys.stderr,
    )
    sys.exit(1)

print("check-woodpecker-config: OK - .woodpecker.yml matches Woodpecker's schema")
print(f"  steps:    {[step['name'] for step in config['steps']]}")
print(f"  services: {[s['name'] for s in config.get('services', [])] or 'none'}")
PY