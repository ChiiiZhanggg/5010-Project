
## Files

| file | what's in it |
|---|---|
| `episodes.parquet` | 12 experiments (4 regimes x 3 options). Main input. |
| `stock_prices.parquet` | SPY daily OHLCV + `logret`, `rv_20d`, `rv_60d` |
| `stock_intraday_1h.parquet` | SPY hourly, last ~730 days, `America/New_York` |
| `stock_intraday_15m.parquet` | SPY 15-min, last ~60 days |
| `vix.parquet` | `vix`, `vix3m`, `vix6m` (decimals) |
| `risk_free.parquet` | `r` from ^IRX (decimal, ffilled) |
| `dividends.parquet` | SPY ex-div history (full, back to 1993) |
| `option_chain_snapshot.parquet` | today's SPY chain, for IV smile only |
| `data_quality_report.md` | coverage + sanity checks |

Date columns are `datetime64`!!



## Regime windows

- **calm** — 2021-03-01 to 2021-12-31, post-COVID low vol
- **stress** — 2020-02-01 to 2020-05-31, COVID crash + bounce
- **highvol** — 2022-04-01 to 2022-10-31, Fed hikes
- **recent** — 2025-10-01 to 2026-04-21


## Things to know

- intraday only covers `recent`; older regimes are daily-only
- VIX values are already decimals (0.1629, not 16.29)
- `dividends.parquet` goes back to 1993 — filter by date before using
- parity check in the QA report tends to FAIL on wide call-side spreads;
  that's microstructure, not a bug
- SPY options are American but near-ATM early exercise is negligible, so
  BS treats them as European here
- don't forward-fill SPY daily prices into non-trading days — the pipeline
  leaves gaps on purpose

## Layout

```
data/
  config.py
  pull_data.py  clean_data.py  build_episodes.py  quality_report.py  run_all.py
  README.md
  REVIEW.md  REVIEW_ROUND2.md
  raw/        (gitignored)
  output/
    episodes.parquet
    stock_prices.parquet  stock_intraday_1h.parquet  stock_intraday_15m.parquet
    vix.parquet  risk_free.parquet  dividends.parquet
    option_chain_snapshot.parquet
    data_quality_report.md
```
