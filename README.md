# Oanda Multi-Asset Data Collector

Incremental downloader for Oanda v20 candle data across Forex, Metals,
Indices, Commodities, and Bonds. Designed to run periodically via
GitHub Actions, storing CSVs in the repo.

## Layout

```
.
├── config/
│   ├── instruments.yaml     # friendly symbol -> Oanda instrument code, by asset class
│   └── schedules.yaml       # per-granularity lookback + update cadence
├── src/
│   ├── oanda_api.py         # low-level client: chunked fetch + retry
│   ├── storage.py           # CSV append (idempotent, tail-reads last timestamp)
│   ├── pipeline.py          # job expansion + orchestration
│   ├── update.py            # production entry point (CLI)
│   ├── smoke_test.py        # small-sample test, run before every config change
│   └── stress_test.py       # deliberate rate-limit probe
├── data/
│   └── <asset_class>/<symbol>_<granularity>.csv
└── .github/workflows/
    ├── smoke-test.yml
    ├── update-hourly.yml
    └── update-daily.yml
```

CSV columns: `datetime,open,high,low,close,volume` (UTC, incomplete candles dropped).

## Setup

1. **Create an Oanda API token** in your Oanda account dashboard
   (practice or live).
2. **Store it as a GitHub Secret** named `OANDA_API_TOKEN` in the repo.
3. Install dependencies: `pip install -r requirements.txt`

For local use, the scripts also read `OANDA_API_TOKEN` from your shell
env, falling back to a `getpass` prompt if unset.

## Running

### Smoke test (always run this first)

```bash
export OANDA_API_TOKEN=...
python -m src.smoke_test --environment practice
```

Expected output: one line per (asset_class, symbol, granularity) combo
marked `OK`, `WARN` (empty -- weekend/illiquid), or `FAIL`. Investigate
every `FAIL` before enabling the production workflow -- the usual cause
is a symbol that your account tier doesn't have access to.

Restrict to a subset:

```bash
python -m src.smoke_test --asset-class indices --granularity H1
```

### Incremental update

```bash
python -m src.update --schedule hourly   # intraday timeframes
python -m src.update --schedule daily    # H4/D/W
python -m src.update                      # everything
```

The script reads the last stored timestamp per CSV and fetches only
what's new. First run per combo backfills to `initial_lookback_days`
from `schedules.yaml`.

### Stress test (before committing any rate-limit changes)

```bash
python -m src.stress_test --count 300 --pause-seconds 0
```

This bypasses retries and fires as fast as possible, surfacing any 429s
that would normally be hidden. The output tells you:
- Achieved req/s vs Oanda's ~120/s ceiling
- Whether 429s appeared and after how many requests
- Latency distribution

If you hit 429s, widen `pause_seconds` in `update.py` or reduce
`safe_candles` (smaller chunks = more requests but smaller responses).

## Scheduled runs

Two workflows pick up `update_schedule: hourly` and `daily` from
`schedules.yaml` respectively:

- `.github/workflows/update-hourly.yml` — runs :10 every hour
- `.github/workflows/update-daily.yml`  — runs 23:15 UTC daily

Both commit new rows back to the repo on `data/**`. They share a
concurrency group so they can't overlap, and they `git pull --rebase`
before pushing to handle any window where both have uncommitted work.

The `smoke-test.yml` workflow runs on every PR that touches `src/**`,
`config/**`, `requirements.txt`, or the workflows themselves.

## Rate-limit safety measures

Oanda's v20 REST API allows up to ~120 requests/second per connection,
with a hard cap of 5000 candles per response. The defaults here stay
well under both:

| Knob             | Default | Why                                            |
| ---------------- | ------- | ---------------------------------------------- |
| `safe_candles`   | 2500    | Half of the 5000 cap (original script's value) |
| `pause_seconds`  | 0.3     | ~3 req/s ceiling from inter-chunk pause alone  |
| `max_retries`    | 4       | Covers transient 429/5xx                       |
| `base_sleep`     | 0.5     | Exponential backoff: 0.5, 1, 2, 4, 8s          |
| HTTP timeout     | 30s     | Generous to avoid spurious retries             |

The client also honours Oanda's `Retry-After` header when present on
429 responses, which is the correct behaviour per the v20 docs.

## Adding new symbols

1. Add the entry to `config/instruments.yaml` under the appropriate
   asset class. Left side = friendly name (CSV filename), right side =
   Oanda instrument code.
2. Run the smoke test, restricted to the new symbols:
   ```bash
   python -m src.smoke_test --asset-class <class>
   ```
3. Commit both the config change and the newly-generated data files
   (the first production run will backfill to `initial_lookback_days`).

## Repo size

CSV data committed to the repo grows over time. Realistic estimates at
full config:
- M1 across ~40 symbols, 60-day rolling: ~400 MB/year growth
- H1 and slower: modest

If size becomes an issue, options:
- Move data commits to a dedicated `data` branch with shallow history
- Use Git LFS for large CSVs
- Store CSVs as release assets instead of in-tree

## Notes

- All timestamps stored are UTC RFC3339.
- `volume` is Oanda's tick count, not traded volume -- it's a proxy.
- CFDs from Oanda are MID-priced by default here; change
  `price="M"` to `"B"`, `"A"`, or `"MBA"` in `oanda_api.py` if you
  need bid/ask.
- This script preserves the original pattern of dropping incomplete
  candles. The most recent bar will therefore lag the current time by
  one bar width until it closes.
