# Data Quality Report

Generated: 2026-04-22T03:24:52.387281+00:00

## File Summary
- **stock_prices.parquet**: rows=1,583 range=[2020-01-02 00:00:00 .. 2026-04-21 00:00:00]
- **stock_intraday_1h.parquet**: rows=5,068 range=[2023-05-23 09:30:00-04:00 .. 2026-04-21 15:30:00-04:00]
- **stock_intraday_15m.parquet**: rows=1,521 range=[2026-01-26 09:30:00-05:00 .. 2026-04-21 15:45:00-04:00]
- **vix.parquet**: rows=1,583 range=[2020-01-02 00:00:00 .. 2026-04-21 00:00:00]
- **risk_free.parquet**: rows=1,583 range=[2020-01-02 00:00:00 .. 2026-04-21 00:00:00]
- **dividends.parquet**: rows=134 range=[1993-03-19 00:00:00 .. 2026-03-20 00:00:00]
  - full history back to 1993; filter by date if needed
- **option_chain_snapshot.parquet**: rows=327 range=[2026-04-21 00:00:00 .. 2026-04-21 00:00:00]
- **episodes.parquet**: rows=12 range=[2020-02-03 00:00:00 .. 2025-10-01 00:00:00]

## Episodes Validation

- Expected 12 rows (4 regimes x 3 options)
- Actual 12 rows, 12 unique combos
- PASS

## Regime Coverage (SPY daily)

| regime | window | nyse | rows | missing | rv_20d | rv_60d |
|---|---|---:|---:|---:|---:|---:|
| calm | 2021-03-01 .. 2021-12-31 | 214 | 214 | 0 | 0.1228 | 0.1248 |
| stress | 2020-02-01 .. 2020-05-31 | 82 | 82 | 0 | 0.4502 | 0.6029 |
| highvol | 2022-04-01 .. 2022-10-31 | 147 | 147 | 0 | 0.2486 | 0.2490 |
| recent | 2025-10-01 .. 2026-04-21 | 139 | 139 | 0 | 0.1274 | 0.1186 |

## NaN Ratios

- VIX: {'vix': '0.0000%', 'vix3m': '0.0000%', 'vix6m': '0.0000%'}
- r: 0.0000%
  r range: [-0.0010, 0.0535]

## Episodes Summary (12 rows)

| episode_id | regime | cp | money | inception | S0 | K | expiration | T_days | T_years | iv_vix | iv_vix3m | r | div | q |
|---|---|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|
| SPY_calm_C_ATM | calm | C | ATM | 2021-03-01 | 389.58 | 390.00 | 2021-05-28 | 63 | 0.2500 | 0.2335 | 0.2678 | 0.0003 | 1.2780 | 1.3143% |
| SPY_calm_C_OTM | calm | C | OTM | 2021-03-01 | 389.58 | 409.00 | 2021-05-28 | 63 | 0.2500 | 0.2335 | 0.2678 | 0.0003 | 1.2780 | 1.3143% |
| SPY_calm_P_ATM | calm | P | ATM | 2021-03-01 | 389.58 | 390.00 | 2021-05-28 | 63 | 0.2500 | 0.2335 | 0.2678 | 0.0003 | 1.2780 | 1.3143% |
| SPY_stress_C_ATM | stress | C | ATM | 2020-02-03 | 324.12 | 324.00 | 2020-05-01 | 62 | 0.2460 | 0.1797 | 0.1804 | 0.0152 | 1.4060 | 1.7636% |
| SPY_stress_C_OTM | stress | C | OTM | 2020-02-03 | 324.12 | 340.00 | 2020-05-01 | 62 | 0.2460 | 0.1797 | 0.1804 | 0.0152 | 1.4060 | 1.7636% |
| SPY_stress_P_ATM | stress | P | ATM | 2020-02-03 | 324.12 | 324.00 | 2020-05-01 | 62 | 0.2460 | 0.1797 | 0.1804 | 0.0152 | 1.4060 | 1.7636% |
| SPY_highvol_C_ATM | highvol | C | ATM | 2022-04-01 | 452.92 | 453.00 | 2022-07-01 | 62 | 0.2460 | 0.1963 | 0.2421 | 0.0050 | 1.5770 | 1.4162% |
| SPY_highvol_C_OTM | highvol | C | OTM | 2022-04-01 | 452.92 | 476.00 | 2022-07-01 | 62 | 0.2460 | 0.1963 | 0.2421 | 0.0050 | 1.5770 | 1.4162% |
| SPY_highvol_P_ATM | highvol | P | ATM | 2022-04-01 | 452.92 | 453.00 | 2022-07-01 | 62 | 0.2460 | 0.1963 | 0.2421 | 0.0050 | 1.5770 | 1.4162% |
| SPY_recent_C_ATM | recent | C | ATM | 2025-10-01 | 668.45 | 668.00 | 2025-12-26 | 60 | 0.2381 | 0.1629 | 0.1876 | 0.0385 | 1.9930 | 1.2437% |
| SPY_recent_C_OTM | recent | C | OTM | 2025-10-01 | 668.45 | 702.00 | 2025-12-26 | 60 | 0.2381 | 0.1629 | 0.1876 | 0.0385 | 1.9930 | 1.2437% |
| SPY_recent_P_ATM | recent | P | ATM | 2025-10-01 | 668.45 | 668.00 | 2025-12-26 | 60 | 0.2381 | 0.1629 | 0.1876 | 0.0385 | 1.9930 | 1.2437% |

## Intraday Coverage by Regime

| regime | 1h | 15m |
|---|---:|---:|
| calm | 0 | 0 |
| stress | 0 | 0 |
| highvol | 0 | 0 |
| recent | 955 | 1521 |

## Put-Call Parity Spot Check
- exp=2026-05-22, snap=2026-04-21, T=0.0849y, S=704.08, r=0.0360, PV_div=0.0000
- test: |C - P - (S - K*exp(-rT) - PV_div)| < $1.41

| strike | C_mid | P_mid | LHS | RHS | diff | pass |
|---:|---:|---:|---:|---:|---:|:---:|
| 704.00 | 16.33 | 11.32 | 5.015 | 2.228 | 2.787 | FAIL |
| 705.00 | 16.16 | 11.66 | 4.490 | 1.231 | 3.259 | FAIL |
| 703.00 | 16.95 | 10.98 | 5.980 | 3.225 | 2.755 | FAIL |
| 706.00 | 14.84 | 12.02 | 2.820 | 0.234 | 2.586 | FAIL |
| 702.00 | 17.32 | 10.64 | 6.670 | 4.222 | 2.448 | FAIL |

0/5 within $1.41. Systematic fails usually mean wide call-side bid-ask on the snapshot.

## Known Limitations

- intraday only covers recent regime (yfinance caps 730d/60d)
- ^VIX6M can have gaps in early 2020
- ^IRX ffilled; r=0.05 fallback if fully missing
- option_chain_snapshot is one-day only, not historical
- SPY American early-exercise ignored (near-ATM, negligible)
