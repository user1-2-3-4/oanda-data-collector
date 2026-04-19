"""Low-level Oanda v20 API access.

Direct refactor of the original `fetch_oanda_candles_chunk` and
`fetch_oanda_history` functions into a class, with three deliberate
changes:

  1. Credentials come from the OANDA_API_TOKEN env var (required by CI);
     falls back to getpass() only for local interactive use.
  2. All rate-limit knobs (safe_candles, max_retries, base_sleep,
     pause_seconds) are constructor args so they can be tuned per-run
     and stress-tested from the CLI.
  3. `includeFirst` flips to False after the first chunk in a range
     fetch, so we don't re-request the boundary candle on every chunk.
     (Storage layer still de-dupes defensively.)
"""
from __future__ import annotations

import os
import time
from datetime import timedelta
from typing import Optional

import pandas as pd
import requests

OANDA_HOSTS = {
    "practice": "https://api-fxpractice.oanda.com",
    "live":     "https://api-fxtrade.oanda.com",
}

# Granularity -> minutes per candle. Used to size chunk spans.
GRANULARITY_MINUTES = {
    "M1": 1, "M5": 5, "M15": 15, "M30": 30,
    "H1": 60, "H2": 120, "H4": 240,
    "D":  1440, "W": 10080,
}

COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]


def _resolve_token() -> str:
    """Env var first (CI-friendly), getpass fallback for local use."""
    for var in ("OANDA_API_TOKEN", "OANDA_ACCESS_TOKEN"):
        token = os.environ.get(var)
        if token:
            return token
    try:
        from getpass import getpass
        return getpass("Enter your OANDA API key: ")
    except Exception as e:
        raise RuntimeError(
            "OANDA_API_TOKEN not set and no interactive prompt available"
        ) from e


def _utc(ts) -> pd.Timestamp:
    """Coerce any input to tz-aware UTC pandas Timestamp."""
    ts = pd.Timestamp(ts)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _to_oanda_time(ts) -> str:
    return _utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


class OandaClient:
    def __init__(
        self,
        environment: str = "practice",
        api_token: Optional[str] = None,
        safe_candles: int = 2500,     # well below Oanda's 5000 hard cap
        max_retries: int = 4,
        base_sleep: float = 0.5,       # exponential backoff base
        pause_seconds: float = 0.3,    # inter-chunk pause
        timeout_sec: float = 30.0,
    ):
        if environment not in OANDA_HOSTS:
            raise ValueError(f"environment must be one of {list(OANDA_HOSTS)}")
        self.base_url = OANDA_HOSTS[environment]
        self.token = api_token or _resolve_token()
        self.safe_candles = safe_candles
        self.max_retries = max_retries
        self.base_sleep = base_sleep
        self.pause_seconds = pause_seconds
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        })

    # ---------------------------------------------------------------
    # Single-chunk fetch with retry (kept close to original logic)
    # ---------------------------------------------------------------
    def fetch_chunk(
        self,
        instrument: str,
        granularity: str,
        start_ts,
        end_ts,
        price: str = "M",
        include_first: bool = True,
    ) -> pd.DataFrame:
        url = f"{self.base_url}/v3/instruments/{instrument}/candles"
        params = {
            "price": price,
            "granularity": granularity,
            "from": _to_oanda_time(start_ts),
            "to":   _to_oanda_time(end_ts),
            "smooth": "false",
            "includeFirst": "true" if include_first else "false",
        }

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_sec)
                if resp.status_code == 200:
                    return self._parse_candles(resp.json().get("candles", []))
                if resp.status_code in (429, 500, 502, 503, 504):
                    # Respect Retry-After if Oanda sent one, otherwise back off.
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else self.base_sleep * (2 ** attempt)
                    print(f"  retry {attempt+1}/{self.max_retries} HTTP {resp.status_code}; waiting {wait:.2f}s")
                    time.sleep(wait)
                    continue
                # Non-retryable 4xx - surface body text, then raise
                print(f"  HTTP {resp.status_code}: {resp.text[:300]}")
                resp.raise_for_status()
            except requests.RequestException as e:
                last_err = e
                wait = self.base_sleep * (2 ** attempt)
                print(f"  retry {attempt+1}/{self.max_retries} exception: {e}; waiting {wait:.2f}s")
                time.sleep(wait)

        raise RuntimeError(
            f"fetch_chunk failed after {self.max_retries} retries for "
            f"{instrument} {granularity} {start_ts}->{end_ts}: {last_err}"
        )

    @staticmethod
    def _parse_candles(candles: list[dict]) -> pd.DataFrame:
        rows = []
        for c in candles:
            if not c.get("complete", False):
                continue
            block = c.get("mid") or c.get("bid") or c.get("ask")
            if not block:
                continue
            rows.append({
                "datetime": pd.to_datetime(c["time"], utc=True),
                "open":   float(block["o"]),
                "high":   float(block["h"]),
                "low":    float(block["l"]),
                "close":  float(block["c"]),
                "volume": float(c.get("volume", 0)),
            })
        if not rows:
            return pd.DataFrame(columns=COLUMNS)
        return pd.DataFrame(rows).sort_values("datetime").drop_duplicates("datetime")

    # ---------------------------------------------------------------
    # Range fetch: paginate across chunks
    # ---------------------------------------------------------------
    def fetch_range(
        self,
        instrument: str,
        granularity: str,
        start_ts,
        end_ts,
        price: str = "M",
    ) -> pd.DataFrame:
        if granularity not in GRANULARITY_MINUTES:
            raise ValueError(f"Unsupported granularity {granularity!r}")
        tf_min = GRANULARITY_MINUTES[granularity]
        start_ts = _utc(start_ts)
        end_ts   = _utc(end_ts)

        # Clamp end to the most recent *complete* candle so we don't request
        # the in-progress candle (which would be skipped anyway but wastes a call).
        cutoff = pd.Timestamp.utcnow() - timedelta(minutes=tf_min)
        if end_ts > cutoff:
            end_ts = cutoff
        if start_ts >= end_ts:
            return pd.DataFrame(columns=COLUMNS)

        chunk_span = timedelta(minutes=tf_min * self.safe_candles)
        all_chunks = []
        cur_start = start_ts
        include_first = True
        while cur_start < end_ts:
            cur_end = min(cur_start + chunk_span, end_ts)
            print(f"  {instrument} {granularity}: {cur_start} -> {cur_end}")
            chunk = self.fetch_chunk(
                instrument, granularity,
                cur_start, cur_end,
                price=price,
                include_first=include_first,
            )
            include_first = False

            if not chunk.empty:
                all_chunks.append(chunk)
                cur_start = chunk["datetime"].iloc[-1] + timedelta(minutes=tf_min)
            else:
                # Weekend / market closed / sparse data -- advance to next chunk
                cur_start = cur_end + timedelta(minutes=tf_min)

            if self.pause_seconds:
                time.sleep(self.pause_seconds)

        if not all_chunks:
            return pd.DataFrame(columns=COLUMNS)
        df = pd.concat(all_chunks, ignore_index=True)
        df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        return df[(df["datetime"] >= start_ts) & (df["datetime"] <= end_ts)].copy()
