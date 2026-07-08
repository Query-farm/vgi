#!/usr/bin/env python3
"""Landing-page conformance checker for VGI workers (all languages).

Drives a running worker's HTTP landing surface and asserts it conforms to the
contract in ``docs/http-landing-contract.md``:

* ``GET {prefix}/describe.json`` validates against ``describe.schema.json`` and
  (optionally) equals a **normalized golden** — the cross-language equality guard.
* ``GET {prefix}/`` serves the pinned shared ``landing.html`` (asset marker), and
  ``?format=json`` returns a JSON status.
* ``GET {prefix}/describe/{catalog}/{schema}/{table}.json`` returns valid columns.

Usage:
    run_landing_conformance.py --url http://localhost:8790 [--golden fixtures/describe.expected.json]
    run_landing_conformance.py --url http://localhost:8790 --golden fixtures/describe.expected.json --update

Exit code is non-zero on any failure. ``--update`` (re)writes the golden from the
live worker (reviewers inspect the diff).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import urllib.request

import jsonschema

_HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_SCHEMA = _HERE / "describe.schema.json"
ASSET_MARKER = "vgi-landing-asset v"

# Fields dropped before comparing to a golden (volatile or language-specific).
_VOLATILE_TOP = ("server_id", "worker", "oauth")


def _get(url: str, accept: str = "application/json") -> tuple[int, bytes, str]:
    req = urllib.request.Request(url, headers={"Accept": accept})
    try:
        with urllib.request.urlopen(req) as resp:  # noqa: S310 — trusted local URL
            return resp.status, resp.read(), resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        return exc.code, exc.read(), exc.headers.get("Content-Type", "")


def normalize(doc: dict) -> dict:
    """Strip volatile/language-specific fields so goldens compare across languages."""
    return {k: v for k, v in doc.items() if k not in _VOLATILE_TOP}


def _canonical(doc: dict) -> str:
    return json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False)


def check(
    base_url: str,
    *,
    schema_path: pathlib.Path = DEFAULT_SCHEMA,
    golden_path: pathlib.Path | None = None,
) -> list[str]:
    """Return a list of failure strings (empty == conformant)."""
    base = base_url.rstrip("/")
    fails: list[str] = []
    schema = json.loads(schema_path.read_text())
    columns_schema = {**schema, **schema["$defs"]["columns"]}

    # 1) describe.json — schema-valid.
    status, body, _ = _get(f"{base}/describe.json")
    if status != 200:
        return [f"GET /describe.json returned HTTP {status}"]
    doc = json.loads(body)
    try:
        jsonschema.validate(doc, schema)
    except jsonschema.ValidationError as exc:
        fails.append(f"describe.json schema violation: {exc.message} at {list(exc.absolute_path)}")

    # 2) GET / serves the pinned shared page; ?format=json is a JSON status.
    status, html, ctype = _get(f"{base}/", accept="text/html")
    if status != 200 or "text/html" not in ctype:
        fails.append(f"GET / (Accept: text/html) returned HTTP {status} {ctype!r}")
    elif ASSET_MARKER not in html.decode("utf-8", "replace"):
        fails.append(f"GET / did not serve the pinned landing.html (missing {ASSET_MARKER!r} marker)")
    status, jbody, _ = _get(f"{base}/?format=json")
    try:
        assert json.loads(jbody).get("status") == "ok"
    except Exception:  # noqa: BLE001
        fails.append("GET /?format=json did not return a JSON status object")

    # 3) Lazy column endpoints — validate one table + one view per schema.
    for cat in doc.get("catalogs", []):
        for sch in cat.get("schemas", []):
            samples = (sch.get("tables") or [])[:1] + (sch.get("views") or [])[:1]
            for obj in samples:
                cstatus, cbody, _ = _get(f"{base}/describe/{cat['name']}/{sch['name']}/{obj['name']}.json")
                if cstatus != 200:
                    fails.append(f"columns {cat['name']}/{sch['name']}/{obj['name']} -> HTTP {cstatus}")
                    continue
                try:
                    jsonschema.validate(json.loads(cbody), columns_schema)
                except jsonschema.ValidationError as exc:
                    fails.append(f"columns {sch['name']}.{obj['name']} schema violation: {exc.message}")

    # 4) Golden diff (cross-language equality).
    if golden_path is not None:
        got = _canonical(normalize(doc))
        want = golden_path.read_text() if golden_path.exists() else None
        if want is None:
            fails.append(f"golden {golden_path} does not exist (run with --update)")
        elif got.strip() != want.strip():
            import difflib

            diff = list(
                difflib.unified_diff(
                    want.strip().splitlines(), got.strip().splitlines(), "golden", "live", lineterm=""
                )
            )
            snippet = "\n".join(diff[:40])
            fails.append(f"describe.json does not match golden {golden_path.name}:\n{snippet}")

    return fails


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", required=True, help="Worker base URL, e.g. http://localhost:8790")
    ap.add_argument("--schema", type=pathlib.Path, default=DEFAULT_SCHEMA)
    ap.add_argument("--golden", type=pathlib.Path, default=None)
    ap.add_argument("--update", action="store_true", help="Write the normalized golden from the live worker")
    args = ap.parse_args()

    if args.update:
        if args.golden is None:
            ap.error("--update requires --golden")
        _, body, _ = _get(f"{args.url.rstrip('/')}/describe.json")
        args.golden.parent.mkdir(parents=True, exist_ok=True)
        args.golden.write_text(_canonical(normalize(json.loads(body))) + "\n")
        print(f"wrote golden {args.golden}")
        return 0

    fails = check(args.url, schema_path=args.schema, golden_path=args.golden)
    if fails:
        print(f"FAIL ({len(fails)}):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS: landing conformance OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
