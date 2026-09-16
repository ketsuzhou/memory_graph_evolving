"""Shared plumbing for the PG-40..43 cross-repository integration tracers.

Each ``run_<scenario>.py`` module exposes ``TRACER_NAME`` and
``run(spec_root)`` and returns the frozen trace shape::

    {"tracer": str, "ok": bool,
     "steps": [{"name", "ok", "detail"}...],
     "assertions": [{"id", "ok", "detail"}...]}

The Go halves run through each repository's in-repo
``internal/integrationtracer/main`` driver (a tracer driver, never a
composition root); the AReaL halves run in-process against the checked-out
repository added to ``sys.path``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
HOST_REPO = WORKSPACE_ROOT / "memory_graph_evolving" / "pi-group-chat-host"
GMS_REPO = WORKSPACE_ROOT / "memory_graph_evolving" / "graph-memory-service"
AREAL_REPO = WORKSPACE_ROOT / "areal"
GO_BIN = WORKSPACE_ROOT / ".tools" / "go-1.26.8" / "bin" / "go"
GO_CACHE = WORKSPACE_ROOT / ".cache" / "go-build-zcode"


def validate_spec_root(spec_root: Path | str | None) -> Path:
    """Fail closed on a missing or wrong integration root."""
    if spec_root is None:
        raise ValueError("spec_root is required")
    root = Path(spec_root)
    if not root.exists():
        raise FileNotFoundError(f"integration spec root does not exist: {root}")
    if not (root / "tracer_common.py").is_file():
        raise ValueError(f"not a PG-40..43 integration spec root: {root}")
    return root.resolve()


def validate_repos() -> None:
    missing = [
        str(path)
        for path in (HOST_REPO, GMS_REPO, AREAL_REPO, GO_BIN)
        if not path.exists()
    ]
    if missing:
        raise ValueError(f"workspace repositories missing: {', '.join(missing)}")


def run_driver(repo: Path, args: list[str], timeout: int = 600) -> dict:
    """Run one scenario of a repository's integrationtracer driver."""
    validate_repos()
    env = dict(os.environ)
    env["GOCACHE"] = str(GO_CACHE)
    env.setdefault("HOME", str(Path.home()))
    proc = subprocess.run(
        [str(GO_BIN), "run", "./internal/integrationtracer/main", *args],
        cwd=str(repo),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"driver {repo.name} {args[:1]} failed ({proc.returncode}): "
            f"{proc.stderr.strip()[-2000:]}"
        )
    return json.loads(proc.stdout)


def ensure_areal_on_path() -> None:
    validate_repos()
    for entry in (str(AREAL_REPO), "/tmp/pgstub"):
        if entry not in sys.path:
            sys.path.insert(0, entry)


def step(name: str, ok: bool, detail: dict) -> dict:
    """One trace step; ``detail`` must be a structured dict (never a string),
    so consumers can inspect facts instead of parsing prose."""
    if not isinstance(detail, dict):
        raise TypeError(
            f"step {name!r}: detail must be a dict, got {type(detail).__name__}"
        )
    return {"name": name, "ok": bool(ok), "detail": detail}


def assertion(assertion_id: str, ok: bool, detail: str) -> dict:
    """One trace assertion; ``detail`` is the human-readable rationale
    string (the frozen contract reserves structured dicts for steps)."""
    if not isinstance(detail, str):
        raise TypeError(
            f"assertion {assertion_id!r}: detail must be a str, "
            f"got {type(detail).__name__}"
        )
    return {"id": assertion_id, "ok": bool(ok), "detail": detail}


def build_trace(tracer_name: str, steps: list[dict], assertions: list[dict]) -> dict:
    """Assemble the frozen trace shape, failing closed on duplicate step
    names or assertion IDs (a duplicate silently shadows evidence)."""
    names = [entry["name"] for entry in steps]
    ids = [entry["id"] for entry in assertions]
    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    duplicate_ids = sorted({aid for aid in ids if ids.count(aid) > 1})
    if duplicate_names:
        raise ValueError(f"duplicate step names: {', '.join(duplicate_names)}")
    if duplicate_ids:
        raise ValueError(f"duplicate assertion ids: {', '.join(duplicate_ids)}")
    ok = all(entry["ok"] for entry in steps) and all(
        entry["ok"] for entry in assertions
    )
    return {
        "tracer": tracer_name,
        "ok": ok,
        "steps": steps,
        "assertions": assertions,
    }
