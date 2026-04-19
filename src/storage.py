"""CSV storage.

File layout:   data/<asset_class>/<friendly_symbol>_<granularity>.csv
Columns:       datetime,open,high,low,close,volume

`last_timestamp()` tail-reads the CSV (not a full parse) so resuming is
fast even for multi-GB files. `append_df()` drops rows <= the stored
last timestamp, so repeated runs with overlapping windows are idempotent.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]
_TAIL_BYTES = 8192


def csv_path(data_dir: Path, asset_class: str, symbol: str, granularity: str) -> Path:
    return Path(data_dir) / asset_class / f"{symbol}_{granularity}.csv"


def last_timestamp(path: Path) -> pd.Timestamp | None:
    """Return the last row's datetime (UTC), or None if the file is missing,
    empty, or header-only. Raises if the last row exists but can't be parsed
    (we'd rather fail loudly than silently re-download everything)."""
    if not path.exists():
        return None
    size = path.stat().st_size
    if size == 0:
        return None

    with path.open("rb") as f:
        if size <= _TAIL_BYTES:
            chunk = f.read()
        else:
            f.seek(-_TAIL_BYTES, os.SEEK_END)
            chunk = f.read()

    lines = [ln for ln in chunk.splitlines() if ln.strip()]
    if not lines:
        return None
    last = lines[-1].decode("utf-8", errors="replace")
    if last.lower().startswith("datetime"):
        return None  # header-only file

    ts_str = last.split(",", 1)[0]
    return pd.to_datetime(ts_str, utc=True, errors="raise")


def append_df(path: Path, df: pd.DataFrame) -> int:
    """Append rows with datetime > existing last timestamp. Returns rows written."""
    if df is None or df.empty:
        return 0
    df = df[COLUMNS].sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    last = last_timestamp(path)
    if last is not None:
        df = df[df["datetime"] > last]
    if df.empty:
        return 0

    write_header = not path.exists() or path.stat().st_size == 0
    df.to_csv(path, mode="a", header=write_header, index=False)
    return len(df)
