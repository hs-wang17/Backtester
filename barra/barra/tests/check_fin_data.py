from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import polars as pl

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from config import FIN_TABLE_REQUIRED_COLS, PATH_FD_DATA, TABLES  # noqa: E402


def _build_cache_index(cache_dir: Path) -> dict[str, list[Path]]:
    by_table: dict[str, list[Path]] = {}
    for p in cache_dir.iterdir():
        if not p.is_file() or p.suffix != ".feather":
            continue
        parts = p.stem.rsplit("_", 2)
        if len(parts) != 3:
            continue
        table, _start, _end = parts
        by_table.setdefault(table, []).append(p)
    return by_table


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Check cached fin-data tables contain all columns needed by factor code. "
            "Uses `config.FIN_TABLE_REQUIRED_COLS` and scans `PATH_FD_DATA/barra_fd_cache`."
        )
    )
    parser.add_argument("--strict", action="store_true", help="exit 1 if any missing")
    args = parser.parse_args()

    cache_dir = Path(PATH_FD_DATA) / "barra_fd_cache"
    if not cache_dir.is_dir():
        raise SystemExit(f"cache dir not found: {cache_dir}")

    by_table = _build_cache_index(cache_dir)
    missing_any = False

    for table in TABLES:
        required = FIN_TABLE_REQUIRED_COLS.get(table, [])
        candidates = by_table.get(table, [])
        if not candidates:
            print(f"[MISSING CACHE] {table}")
            missing_any = True
            continue
        cache_file = max(candidates, key=lambda x: x.stat().st_mtime)
        lf = pl.scan_ipc(str(cache_file), memory_map=True)
        cols = set(lf.collect_schema().names())

        missing = [c for c in required if c not in cols]
        if missing:
            print(f"[MISSING COLS] {table} -> {cache_file.name}")
            print(f"  missing: {missing}")
            missing_any = True
        else:
            print(f"[OK] {table} -> {cache_file.name} ({len(cols)} cols)")

    if args.strict and missing_any:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
