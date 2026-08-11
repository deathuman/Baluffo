from __future__ import annotations

from pathlib import Path

from src.storage_metrics import duration_ms, record_json_write


def records_json_storage_metrics(target: Path | str) -> bool:
    target_path = Path(target)
    return target_path.suffix == ".json" or target_path.name.endswith(".json.gz")


def record_json_text_write(
    *,
    path: Path,
    target: Path,
    text: str,
    write_started_at: float,
    uncompressed_size_bytes: int | None = None,
) -> None:
    if not records_json_storage_metrics(target):
        return
    target = Path(target).expanduser()
    if uncompressed_size_bytes is None:
        uncompressed_size_bytes = len(text.encode("utf-8"))
    try:
        # codeql[py/path-injection] Storage metrics inspect trusted local runtime artifacts.
        compressed_size_bytes = target.stat().st_size
    except OSError:
        compressed_size_bytes = uncompressed_size_bytes
    record_json_write(
        path=path,
        target=target,
        storage_kind="gzip" if target.suffix == ".gz" else "json",
        serialization_duration_ms=0,
        atomic_replace_duration_ms=duration_ms(write_started_at),
        compressed_size_bytes=compressed_size_bytes,
        uncompressed_size_bytes=uncompressed_size_bytes,
        replaced=True,
        data_dir=target.parent,
    )
