#!/usr/bin/env python3
"""Check that workspace members do not declare dependency versions themselves.

Every third-party dependency belongs in `[workspace.dependencies]` of the root
`Cargo.toml`; members inherit it with `{ workspace = true }`. One requirement
per crate is what keeps a single version of it in the resolved tree, which
`cargo deny check bans` then enforces.

Run from the repository root:

    python3 .github/scripts/check_workspace_deps.py
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

DEPENDENCY_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")

# Dependencies a member is allowed to declare on its own, with the reason.
# Keep this list as short as possible.
EXEMPTIONS: dict[tuple[str, str], str] = {
    (
        "tests/slo/slo-framework",
        name,
    ): "SLO workload is on OpenTelemetry 0.27 while the ydb examples are on 0.32; "
    "one crate name cannot carry two requirements in the workspace table"
    for name in (
        "opentelemetry",
        "opentelemetry-otlp",
        "opentelemetry-semantic-conventions",
        "opentelemetry_sdk",
    )
}


def dependency_tables(manifest: dict) -> list[tuple[str, dict]]:
    """Return (table name, table) for every dependency table of a manifest."""
    tables = [(table, manifest[table]) for table in DEPENDENCY_TABLES if table in manifest]
    for target, target_manifest in manifest.get("target", {}).items():
        tables += [
            (f"target.{target}.{table}", target_manifest[table])
            for table in DEPENDENCY_TABLES
            if table in target_manifest
        ]
    return tables


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    with (root / "Cargo.toml").open("rb") as handle:
        workspace_manifest = tomllib.load(handle)

    members = workspace_manifest["workspace"]["members"]
    declared = set(workspace_manifest["workspace"].get("dependencies", {}))
    inherited: set[str] = set()
    errors: list[str] = []

    for member in members:
        manifest_path = root / member / "Cargo.toml"
        with manifest_path.open("rb") as handle:
            manifest = tomllib.load(handle)

        for table_name, table in dependency_tables(manifest):
            for name, spec in table.items():
                if isinstance(spec, dict) and spec.get("workspace"):
                    inherited.add(spec.get("package", name))
                    continue
                if (member, name) in EXEMPTIONS:
                    continue
                errors.append(
                    f"{member}/Cargo.toml [{table_name}] {name}: declares its own "
                    f"requirement; move it to [workspace.dependencies] and use "
                    f"`{name} = {{ workspace = true }}`"
                )

    for name in sorted(declared - inherited):
        errors.append(
            f"Cargo.toml [workspace.dependencies] {name}: not inherited by any "
            f"workspace member; remove it"
        )

    if errors:
        print("Workspace dependency check failed:\n", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        print(
            f"\n{len(errors)} problem(s). See the comment above "
            f"[workspace.dependencies] in the root Cargo.toml.",
            file=sys.stderr,
        )
        return 1

    print(
        f"OK: {len(members)} workspace members inherit "
        f"{len(declared)} dependencies from [workspace.dependencies]."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
