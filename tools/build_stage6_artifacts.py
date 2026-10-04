"""Build deterministic, allowlisted Stage 6 runtime archives."""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any

from featureforge.canonical import canonical_json, digest

ROOT = Path(__file__).resolve().parents[1]
MAXIMUM_BYTES = 10 * 1024 * 1024
WORKER_FILES = (
    "src/featureforge/__init__.py",
    "src/featureforge/aws_runtime.py",
    "src/featureforge/canonical.py",
    "src/featureforge/control_worker.py",
    "src/featureforge/expected.py",
    "src/featureforge/managed.py",
    "src/featureforge/managed_admission.py",
    "src/featureforge/model.py",
    "src/featureforge/online.py",
    "src/featureforge/store.py",
    "src/featureforge/stage6_live.py",
)
GLUE_FILES = (
    "src/featureforge/__init__.py",
    "src/featureforge/canonical.py",
    "src/featureforge/managed.py",
    "src/featureforge/model.py",
    "src/featureforge/spark_runtime.py",
    "src/featureforge/store.py",
    "src/featureforge/temporal.py",
)


def _archive_name(source: str) -> str:
    prefix = "src/"
    if not source.startswith(prefix):
        raise ValueError("runtime archive source must be under src")
    return source[len(prefix) :]


def build_zip(output: Path, files: tuple[str, ...]) -> dict[str, Any]:
    if len(files) != len(set(files)):
        raise ValueError("artifact allowlist contains duplicates")
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for relative in sorted(files):
            path = (ROOT / relative).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file():
                raise ValueError(f"unsafe or missing artifact input: {relative}")
            info = zipfile.ZipInfo(_archive_name(relative), date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            info.create_system = 3
            archive.writestr(
                info,
                path.read_bytes(),
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )
    data = output.read_bytes()
    if len(data) > MAXIMUM_BYTES:
        raise ValueError("runtime artifact exceeds the bounded maximum")
    return {
        "files": list(sorted(files)),
        "maximum_bytes": MAXIMUM_BYTES,
        "name": output.name,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def build(output_dir: Path) -> dict[str, Any]:
    artifacts = [
        build_zip(output_dir / "featureforge-control-worker.zip", WORKER_FILES),
        build_zip(output_dir / "featureforge-glue-library.zip", GLUE_FILES),
    ]
    manifest: dict[str, Any] = {
        "artifacts": artifacts,
        "builder": "tools/build_stage6_artifacts.py",
        "contract": "featureforge-stage6-artifact-manifest-v1",
        "project": "featureforge-ml-feature-platform",
        "stage": 6,
    }
    manifest["manifest_digest"] = digest(manifest)
    (output_dir / "artifact-manifest.json").write_text(
        canonical_json(manifest) + "\n", encoding="utf-8"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(json.dumps(build(args.output_dir), sort_keys=True))


if __name__ == "__main__":
    main()
