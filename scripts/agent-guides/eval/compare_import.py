#!/usr/bin/env python3
"""Compares a project's .skilj.toml with what skilj-codegen imported from
the model it was built from (import-model's static check, check.sh).

Everything the import says must still be there: the bounded context, every
event and command type with each of its fields and their types, every tag,
every command's REST trigger and every rejection the model's
specifications name (the `# Rejected as: ...` comment above a command, as
skilj-codegen writes it). Additions are fine; a removal or a rename is not.

Usage: compare_import.py <imported.skilj.toml> <project.skilj.toml>
Exits 1 and lists what is missing when the project leaves something out.
"""

import re
import sys
import tomllib

REJECTED = re.compile(r"^\s*#\s*Rejected as:\s*(.*)$")
HEADER = re.compile(r"^\s*\[\[\s*(event_type|command_type)\s*\]\]")
NAME = re.compile(r'^\s*name\s*=\s*"([^"]*)"')


def rejections(text):
    """The rejection kinds of each command type, by name.

    A `# Rejected as:` comment belongs to the type whose header follows it
    (with only blank and comment lines between), or else to the type it
    stands in.
    """
    found = {}
    pending = []
    current = None
    for line in text.splitlines():
        match = REJECTED.match(line)
        if match:
            pending += [kind.strip() for kind in match.group(1).split(",") if kind.strip()]
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        header = HEADER.match(line)
        if header:
            current = {"table": header.group(1), "name": None, "kinds": set(pending)}
            pending = []
            found.setdefault("_blocks", []).append(current)
            continue
        if stripped.startswith("["):
            current = None
            pending = []
            continue
        if current is not None:
            current["kinds"].update(pending)
            pending = []
            name = NAME.match(line)
            if name and current["name"] is None:
                current["name"] = name.group(1)
    if current is not None:
        current["kinds"].update(pending)
    kinds = {}
    for block in found.get("_blocks", []):
        if block["table"] == "command_type" and block["name"]:
            kinds.setdefault(block["name"], set()).update(block["kinds"])
    return kinds


def types(model, table):
    return {entry["name"]: entry for entry in model.get(table, [])}


def fields(entry):
    return {field["name"]: field.get("type") for field in entry.get("fields", [])}


def compare(imported_text, project_text):
    """What the project leaves out of the import, as messages."""
    imported = tomllib.loads(imported_text)
    project = tomllib.loads(project_text)
    problems = []
    if project.get("bounded_context") != imported.get("bounded_context"):
        problems.append(
            f"bounded_context is {project.get('bounded_context')!r}, "
            f"the model's is {imported.get('bounded_context')!r}"
        )
    for table in ("event_type", "command_type"):
        ours = types(project, table)
        for name, entry in types(imported, table).items():
            if name not in ours:
                problems.append(f"{table} {name} is missing")
                continue
            have = fields(ours[name])
            for field, kind in fields(entry).items():
                if field not in have:
                    problems.append(f"{table} {name}: field {field} is missing")
                elif have[field] != kind:
                    problems.append(f"{table} {name}: field {field} is {have[field]}, the model's {kind}")
            tags = ours[name].get("tags", {})
            for key, value in entry.get("tags", {}).items():
                if tags.get(key) != value:
                    problems.append(f"{table} {name}: tag {key} = {value!r} is missing")
            if entry.get("rest_trigger_allowed") and not ours[name].get("rest_trigger_allowed"):
                problems.append(f"{table} {name}: rest_trigger_allowed is no longer true")
    ours = rejections(project_text)
    for name, kinds in rejections(imported_text).items():
        missing = kinds - ours.get(name, set())
        if missing:
            problems.append(
                f"command_type {name}: the rejections {', '.join(sorted(missing))} "
                "are no longer annotated (# Rejected as: ...)"
            )
    return problems


def main(argv):
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[-2], file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as imported, open(argv[2], encoding="utf-8") as project:
        problems = compare(imported.read(), project.read())
    for problem in problems:
        print(f"compare-import: {argv[2]}: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
