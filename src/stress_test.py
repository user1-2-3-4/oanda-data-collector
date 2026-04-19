"""Rate-limit stress test.

Fires repeated candle requests with retries DISABLED so rate-limit events
surface instead of being masked. Use this to validate your choice of
`safe_candles`, `pause_seconds`, and `max_retries` before a long
production run.

Typical usage:
    python -m src.stress_test --count 300 --pause-seconds 0

Look for:
    * Any HTTP 429 in the log -> you're pushing harder than Oanda allows
    * Latency p95 stable vs rising -> rising means you're queueing at Oanda
    * Achieved req/s vs configured -> your bottleneck

Once you know where the ceiling is, set `update.py` pause_seconds to
stay well below it (default 0.3s leaves plenty of headroom).
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from datetime import timedelta

import pandas as pd
import requests

from .oanda_api import OANDA_HOSTS, _resolve_token, _to_oanda_time


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--instrument", default="EUR_USD")
    ap.add_argument("--granularity", default="M1")
    ap.add_argument("--count", type=int, default=200,
                    help="Number of requests to fire")
    ap.add_argument("--pause-seconds", type=float, default=0.0,
                    help="Inter-request sleep (0 = as fast as possible)")
    ap.add_argument("--environment", default="practice", choices=["practice", "live"])
    args = ap.parse_args()

    token = _resolve_token()
    url = f"{OANDA_HOSTS[args.environment]}/v3/instruments/{args.instrument}/candles"
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })

    end_ts = pd.Timestamp.utcnow()
    start_ts = end_ts - timedelta(hours=2)
    params = {
        "price": "M",
        "granularity": args.granularity,
        "from": _to_oanda_time(start_ts),
        "to":   _to_oanda_time(end_ts),
        "smooth": "false",
        "includeFirst": "true",
    }

    latencies = []
    status_counts: dict[int, int] = {}
    errors: list[tuple[int, str]] = []
    rate_limited_at: list[int] = []

    t_start = time.monotonic()
    for i in range(args.count):
        t0 = time.monotonic()
        try:
            resp = session.get(url, params=params, timeout=30)
            latencies.append(time.monotonic() - t0)
            status_counts[resp.status_code] = status_counts.get(resp.status_code, 0) + 1
            if resp.status_code == 429:
                rate_limited_at.append(i)
                ra = resp.headers.get("Retry-After", "-")
                print(f"  #{i:04d} 429 (Retry-After={ra})")
            elif resp.status_code >= 400:
                print(f"  #{i:04d} HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as e:
            errors.append((i, str(e)))
            print(f"  #{i:04d} exception: {e}")
        if args.pause_seconds > 0:
            time.sleep(args.pause_seconds)
    elapsed = time.monotonic() - t_start

    print(f"\n----- Stress test results -----")
    print(f"Requests:   {args.count}")
    print(f"Duration:   {elapsed:.2f}s")
    print(f"Throughput: {args.count/elapsed:.2f} req/s")
    print(f"Status codes: {dict(sorted(status_counts.items()))}")
    print(f"429s:       {len(rate_limited_at)}"
          + (f"  (first at request #{rate_limited_at[0]})" if rate_limited_at else ""))
    print(f"Exceptions: {len(errors)}")

    if latencies:
        s = sorted(latencies)
        def pct(p):
            return s[min(len(s)-1, int(p * len(s)))]
        print(f"Latency (s): min={s[0]:.3f}  p50={pct(0.5):.3f}  "
              f"p95={pct(0.95):.3f}  p99={pct(0.99):.3f}  max={s[-1]:.3f}")

    # Return non-zero if we saw any rate-limit or error -- lets CI flag
    # regressions if the stress test is run as part of a dedicated workflow.
    return 1 if (rate_limited_at or errors) else 0


if __name__ == "__main__":
    sys.exit(main())
