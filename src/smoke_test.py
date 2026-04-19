"""Smoke test.

Fetches a small recent window for every configured combination without
touching the production data directory. The goal is to catch problems
(invalid symbol, wrong granularity for an instrument, auth issues,
account permission mismatches) before they disrupt a long production run.

Exit code 1 if any combo fails. Run this before enabling the hourly
workflow and after any config change.
"""
from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

from .oanda_api import OandaClient
from .pipeline import build_jobs, load_config


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config-dir", default="config", type=Path)
    ap.add_argument("--environment", default="practice", choices=["practice", "live"])
    ap.add_argument("--lookback-hours", type=float, default=72.0,
                    help="How far back the smoke-test window extends (default 72h -- "
                         "enough to catch the previous weekend for forex)")
    ap.add_argument("--asset-class", default=None, help="Restrict to one asset class")
    ap.add_argument("--granularity", default=None, help="Restrict to one granularity")
    ap.add_argument("--safe-candles", type=int, default=500)
    ap.add_argument("--pause-seconds", type=float, default=0.2)
    ap.add_argument("--base-sleep", type=float, default=0.5)
    ap.add_argument("--max-retries", type=int, default=3)
    args = ap.parse_args()

    instruments, schedules = load_config(args.config_dir)
    jobs = build_jobs(
        instruments, schedules,
        asset_filter=args.asset_class,
        granularity_filter=args.granularity,
    )
    if not jobs:
        print("No jobs matched the filters. Check config/instruments.yaml and config/schedules.yaml.")
        return 2

    client = OandaClient(
        environment=args.environment,
        safe_candles=args.safe_candles,
        pause_seconds=args.pause_seconds,
        base_sleep=args.base_sleep,
        max_retries=args.max_retries,
    )

    end_ts = pd.Timestamp.utcnow()
    start_ts = end_ts - timedelta(hours=args.lookback_hours)
    print(f"Smoke-testing {len(jobs)} combos over {args.lookback_hours}h window")

    passes, warnings, failures = 0, 0, []
    for j in jobs:
        label = f"{j.asset_class}/{j.symbol}/{j.granularity}"
        try:
            df = client.fetch_range(j.instrument, j.granularity, start_ts, end_ts)
            if df.empty:
                # Empty can be legitimate (weekend on forex, illiquid bond, etc.)
                # Flag it so you can investigate but don't fail the run.
                print(f"  WARN  {label}: 0 rows returned (market closed or illiquid?)")
                warnings += 1
            else:
                last = df['datetime'].iloc[-1]
                print(f"  OK    {label}: {len(df)} rows, last={last}")
                passes += 1
        except Exception as e:
            print(f"  FAIL  {label}: {e}")
            failures.append((label, str(e)))

    total = len(jobs)
    print(f"\n{passes} OK, {warnings} warn, {len(failures)} fail ({total} total)")
    if failures:
        print("\nFailures:")
        for label, err in failures:
            print(f"  {label}: {err}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
