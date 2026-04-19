"""Production update entry point.

Reads config, iterates (asset_class, symbol, granularity) jobs matching
the --schedule filter, incrementally fetches new candles since the last
stored timestamp, and appends them to CSV. Failures on individual jobs
are logged and do not abort the run (unless --fail-fast).

Example CI invocation (hourly timeframes):
    python -m src.update --schedule hourly --environment practice
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .oanda_api import OandaClient
from .pipeline import build_jobs, load_config, run_job


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config-dir", default="config", type=Path)
    ap.add_argument("--data-dir",   default="data",   type=Path)
    ap.add_argument("--environment", default="practice", choices=["practice", "live"])
    ap.add_argument("--schedule", default=None, choices=[None, "hourly", "daily", "manual"],
                    help="Run only jobs whose update_schedule matches. Default: all jobs.")
    ap.add_argument("--asset-class", default=None, help="Restrict to a single asset class")
    ap.add_argument("--granularity", default=None, help="Restrict to a single granularity")
    ap.add_argument("--safe-candles", type=int, default=2500)
    ap.add_argument("--pause-seconds", type=float, default=0.3)
    ap.add_argument("--base-sleep", type=float, default=0.5)
    ap.add_argument("--max-retries", type=int, default=4)
    ap.add_argument("--fail-fast", action="store_true",
                    help="Stop on first error (default: continue and report at end)")
    ap.add_argument("--summary-json", type=Path, default=None,
                    help="Write run summary as JSON to this path (for CI artifacts)")
    args = ap.parse_args()

    instruments, schedules = load_config(args.config_dir)
    jobs = build_jobs(
        instruments, schedules,
        schedule_filter=args.schedule,
        asset_filter=args.asset_class,
        granularity_filter=args.granularity,
    )
    print(f"Scheduled {len(jobs)} jobs (schedule={args.schedule or 'all'}, "
          f"asset={args.asset_class or 'all'}, granularity={args.granularity or 'all'})")
    if not jobs:
        print("No jobs matched filters; nothing to do.")
        return 0

    client = OandaClient(
        environment=args.environment,
        safe_candles=args.safe_candles,
        pause_seconds=args.pause_seconds,
        base_sleep=args.base_sleep,
        max_retries=args.max_retries,
    )

    results = []
    failures = []
    total_written = 0

    for j in jobs:
        label = f"{j.asset_class}/{j.symbol}/{j.granularity}"
        print(f"\n[{label}]")
        try:
            r = run_job(client, args.data_dir, j)
            total_written += r["rows_written"]
            results.append(r)
            print(f"  -> wrote {r['rows_written']} rows ({r['mode']}, fetched {r['rows_fetched']})")
        except Exception as e:
            print(f"  !! FAILED: {e}")
            failures.append({"label": label, "error": str(e)})
            if args.fail_fast:
                break

    print(f"\n===== Summary =====")
    print(f"Jobs run:      {len(results)}/{len(jobs)}")
    print(f"Rows written:  {total_written}")
    print(f"Failures:      {len(failures)}")
    for f in failures:
        print(f"  - {f['label']}: {f['error']}")

    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(json.dumps(
            {"results": results, "failures": failures, "rows_written": total_written},
            indent=2, default=str,
        ))

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
