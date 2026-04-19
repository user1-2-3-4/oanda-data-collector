"""Pipeline orchestration.

`build_jobs` expands the config into a flat list of work items, one per
(asset_class, symbol, granularity) combo, optionally filtered by schedule
(hourly/daily), asset class, or granularity. `run_job` fetches and stores
a single item incrementally.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd
import yaml

from .oanda_api import OandaClient
from .storage import append_df, csv_path, last_timestamp


@dataclass
class Job:
    asset_class: str
    symbol: str          # friendly name (used in CSV filename)
    instrument: str      # Oanda instrument code (used in API)
    granularity: str
    initial_lookback_days: int


def load_config(config_dir: Path) -> tuple[dict, dict]:
    config_dir = Path(config_dir)
    with (config_dir / "instruments.yaml").open() as f:
        instruments = yaml.safe_load(f) or {}
    with (config_dir / "schedules.yaml").open() as f:
        schedules = yaml.safe_load(f) or {}
    return instruments, schedules


def build_jobs(
    instruments: dict,
    schedules: dict,
    schedule_filter: str | None = None,
    asset_filter: str | None = None,
    granularity_filter: str | None = None,
) -> list[Job]:
    jobs: list[Job] = []
    grans = schedules.get("granularities", {}) or {}
    for asset_class, symbol_map in (instruments or {}).items():
        if asset_filter and asset_class != asset_filter:
            continue
        for symbol, oanda_code in (symbol_map or {}).items():
            for gran, gcfg in grans.items():
                if granularity_filter and gran != granularity_filter:
                    continue
                if schedule_filter and gcfg.get("update_schedule") != schedule_filter:
                    continue
                jobs.append(Job(
                    asset_class=asset_class,
                    symbol=symbol,
                    instrument=oanda_code,
                    granularity=gran,
                    initial_lookback_days=int(gcfg.get("initial_lookback_days", 365)),
                ))
    return jobs


def run_job(client: OandaClient, data_dir: Path, job: Job) -> dict:
    """Fetch from last stored timestamp (or initial lookback for new files)
    and append. Returns a summary dict."""
    path = csv_path(data_dir, job.asset_class, job.symbol, job.granularity)
    last = last_timestamp(path)
    if last is not None:
        start_ts = last  # storage dedup handles the boundary row
        mode = "incremental"
    else:
        start_ts = pd.Timestamp.utcnow() - timedelta(days=job.initial_lookback_days)
        mode = "initial"
    end_ts = pd.Timestamp.utcnow()

    df = client.fetch_range(job.instrument, job.granularity, start_ts, end_ts)
    written = append_df(path, df)
    return {
        "asset_class":  job.asset_class,
        "symbol":       job.symbol,
        "instrument":   job.instrument,
        "granularity":  job.granularity,
        "mode":         mode,
        "fetched_from": str(start_ts),
        "rows_fetched": len(df),
        "rows_written": written,
        "path":         str(path),
    }
