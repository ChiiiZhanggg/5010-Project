"""Write data/output/data_quality_report.md."""

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from config import REGIMES

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"


def _nyse(start, end):
    cal = mcal.get_calendar("NYSE")
    return pd.DatetimeIndex(
        cal.schedule(start_date=pd.Timestamp(start).date(),
                     end_date=pd.Timestamp(end).date()).index.normalize()
    )


def _sline(name, df, tcol):
    if df is None or len(df) == 0:
        return f"- **{name}**: empty"
    tmin = pd.to_datetime(df[tcol]).min()
    tmax = pd.to_datetime(df[tcol]).max()
    return f"- **{name}**: rows={len(df):,} range=[{tmin} .. {tmax}]"


def episodes_validation(ep, lines):
    lines.append("## Episodes Validation")
    lines.append("")

    expected = {(r, cp, m) for r in REGIMES
                for cp, m in [("C", "ATM"), ("C", "OTM"), ("P", "ATM")]}
    actual = set(zip(ep["regime"], ep["cp_flag"], ep["moneyness"]))
    missing = expected - actual
    extra = actual - expected

    lines.append(f"- Expected {len(expected)} rows (4 regimes x 3 options)")
    lines.append(f"- Actual {len(ep)} rows, {len(actual)} unique combos")
    if missing:
        lines.append(f"- FAIL: missing {sorted(missing)}")
    if extra:
        lines.append(f"- FAIL: extra {sorted(extra)}")
    if not missing and not extra and len(ep) == len(expected):
        lines.append("- PASS")

    required = ["S0", "strike", "T_days", "T_years", "iv_inception_vix",
                "r_inception", "div_amount_in_window", "pv_div_inception",
                "q_inception"]
    missing_cols = [c for c in required if c not in ep.columns]
    if missing_cols:
        lines.append(f"- FAIL: missing columns {missing_cols}")
    lines.append("")


def parity_check(chain, spot, r, div_df, lines):
    # C - P = S - K*exp(-rT) - PV(div); treat SPY as European near ATM
    lines.append("## Put-Call Parity Spot Check")
    if chain is None or len(chain) == 0 or spot is None or r is None:
        lines.append("- skipped (missing inputs)")
        return

    exp = pd.to_datetime(chain["expiration"].iloc[0])
    snap = pd.to_datetime(chain["snapshot_date"].iloc[0])
    T = max((exp - snap).days, 1) / 365.0

    pv_div = 0.0
    if div_df is not None and len(div_df):
        d = div_df.copy()
        d["ex_date"] = pd.to_datetime(d["ex_date"]).dt.normalize()
        inw = d[(d["ex_date"] > snap.normalize()) & (d["ex_date"] <= exp.normalize())]
        if len(inw):
            y2d = (inw["ex_date"] - snap.normalize()).dt.days.to_numpy() / 365.0
            pv_div = float((inw["amount"].to_numpy() * np.exp(-r * y2d)).sum())

    calls = chain[chain["cp_flag"] == "C"].set_index("strike")
    puts = chain[chain["cp_flag"] == "P"].set_index("strike")
    common = sorted(set(calls.index) & set(puts.index))
    if not common:
        lines.append("- no common strikes")
        return

    arr = np.array(common)
    idx = np.argsort(np.abs(arr - spot))
    sample = [float(arr[i]) for i in idx[:5]]
    thresh = max(0.50, 0.002 * spot)

    lines.append(f"- exp={exp.date()}, snap={snap.date()}, T={T:.4f}y, "
                 f"S={spot:.2f}, r={r:.4f}, PV_div={pv_div:.4f}")
    lines.append(f"- test: |C - P - (S - K*exp(-rT) - PV_div)| < ${thresh:.2f}")
    lines.append("")
    lines.append("| strike | C_mid | P_mid | LHS | RHS | diff | pass |")
    lines.append("|---:|---:|---:|---:|---:|---:|:---:|")

    def mid(row):
        b, a = row.get("bid", np.nan), row.get("ask", np.nan)
        if pd.notna(b) and pd.notna(a) and b > 0 and a > 0:
            return float((b + a) / 2.0)
        last = row.get("last", np.nan)
        return float(last) if pd.notna(last) else np.nan

    ok_n = tot = 0
    for K in sample:
        c, p = mid(calls.loc[K]), mid(puts.loc[K])
        if not (np.isfinite(c) and np.isfinite(p)):
            lines.append(f"| {K:.2f} | {c} | {p} | n/a | n/a | n/a | SKIP |")
            continue
        tot += 1

        lhs = c - p
        rhs = spot - K * math.exp(-r * T) - pv_div
        diff = lhs - rhs
        ok = abs(diff) < thresh
        ok_n += int(ok)
        lines.append(f"| {K:.2f} | {c:.2f} | {p:.2f} | "
                     f"{lhs:.3f} | {rhs:.3f} | {diff:.3f} | "
                     f"{'PASS' if ok else 'FAIL'} |")
    lines.append("")
    lines.append(f"{ok_n}/{tot} within ${thresh:.2f}. Systematic fails usually "
                 f"mean wide call-side bid-ask on the snapshot.")


def main():
    print("=== quality_report.py ===")
    lines = ["# Data Quality Report", "",
             f"Generated: {pd.Timestamp.now(tz='UTC').isoformat()}", ""]

    prices = pd.read_parquet(OUT / "stock_prices.parquet")
    vix = pd.read_parquet(OUT / "vix.parquet")
    rf = pd.read_parquet(OUT / "risk_free.parquet")
    div = pd.read_parquet(OUT / "dividends.parquet")

    try:
        i1h = pd.read_parquet(OUT / "stock_intraday_1h.parquet")
    except FileNotFoundError:
        i1h = None
    try:
        i15m = pd.read_parquet(OUT / "stock_intraday_15m.parquet")
    except FileNotFoundError:
        i15m = None
    try:
        chain = pd.read_parquet(OUT / "option_chain_snapshot.parquet")
    except FileNotFoundError:
        chain = None
    episodes = pd.read_parquet(OUT / "episodes.parquet")

    lines.append("## File Summary")
    lines.append(_sline("stock_prices.parquet", prices, "date"))
    lines.append(_sline("stock_intraday_1h.parquet", i1h, "datetime")
                 if i1h is not None else "- **stock_intraday_1h.parquet**: MISSING")
    lines.append(_sline("stock_intraday_15m.parquet", i15m, "datetime")
                 if i15m is not None else "- **stock_intraday_15m.parquet**: MISSING")
    lines.append(_sline("vix.parquet", vix, "date"))
    lines.append(_sline("risk_free.parquet", rf, "date"))

    if len(div):
        lines.append(_sline("dividends.parquet", div, "ex_date"))
        lines.append("  - full history back to 1993; filter by date if needed")
    else:
        lines.append("- **dividends.parquet**: empty")

    if chain is not None and len(chain):
        lines.append(_sline("option_chain_snapshot.parquet", chain, "snapshot_date"))
    else:
        lines.append("- **option_chain_snapshot.parquet**: MISSING")
    lines.append(_sline("episodes.parquet", episodes, "inception_date"))
    lines.append("")

    episodes_validation(episodes, lines)

    lines.append("## Regime Coverage (SPY daily)")
    lines.append("")
    lines.append("| regime | window | nyse | rows | missing | rv_20d | rv_60d |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")

    prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()
    for regime, (s, e) in REGIMES.items():
        sess = _nyse(s, e)
        sub = prices[(prices["date"] >= s) & (prices["date"] <= e)]
        rv20 = sub["rv_20d"].mean() if len(sub) else float("nan")
        rv60 = sub["rv_60d"].mean() if len(sub) else float("nan")
        lines.append(f"| {regime} | {s} .. {e} | {len(sess)} | "
                     f"{len(sub)} | {len(sess)-len(sub)} | "
                     f"{rv20:.4f} | {rv60:.4f} |")
    lines.append("")

    lines.append("## NaN Ratios")
    lines.append("")
    vn = {c: f"{vix[c].isna().mean():.4%}" for c in ("vix", "vix3m", "vix6m")
          if c in vix.columns}
    lines.append(f"- VIX: {vn}")
    lines.append(f"- r: {rf['r'].isna().mean():.4%}")
    lines.append(f"  r range: [{rf['r'].min():.4f}, {rf['r'].max():.4f}]")
    lines.append("")

    lines.append("## Episodes Summary (12 rows)")
    lines.append("")
    lines.append("| episode_id | regime | cp | money | inception | S0 | K | "
                 "expiration | T_days | T_years | iv_vix | iv_vix3m | r | div | q |")
    lines.append("|---|---|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|")
    for _, r in episodes.iterrows():
        lines.append(
            f"| {r['episode_id']} | {r['regime']} | {r['cp_flag']} | "
            f"{r['moneyness']} | {pd.Timestamp(r['inception_date']).date()} | "
            f"{r['S0']:.2f} | {r['strike']:.2f} | "
            f"{pd.Timestamp(r['expiration']).date()} | "
            f"{r['T_days']} | {r['T_years']:.4f} | "
            f"{r['iv_inception_vix']:.4f} | {r['iv_inception_vix3m']:.4f} | "
            f"{r['r_inception']:.4f} | {r['div_amount_in_window']:.4f} | "
            f"{r['q_inception']:.4%} |"
        )
    lines.append("")

    lines.append("## Intraday Coverage by Regime")
    lines.append("")
    lines.append("| regime | 1h | 15m |")
    lines.append("|---|---:|---:|")
    for regime, (s, e) in REGIMES.items():
        n1 = n15 = 0
        if i1h is not None and len(i1h):
            d = pd.to_datetime(i1h["datetime"]).dt.tz_convert("America/New_York").dt.normalize()
            n1 = int(((d >= pd.Timestamp(s, tz="America/New_York")) &
                      (d <= pd.Timestamp(e, tz="America/New_York"))).sum())
        if i15m is not None and len(i15m):
            d = pd.to_datetime(i15m["datetime"]).dt.tz_convert("America/New_York").dt.normalize()
            n15 = int(((d >= pd.Timestamp(s, tz="America/New_York")) &
                       (d <= pd.Timestamp(e, tz="America/New_York"))).sum())
        lines.append(f"| {regime} | {n1} | {n15} |")
    lines.append("")

    spot = None
    r_last = float(rf.sort_values("date")["r"].iloc[-1]) if len(rf) else None
    if chain is not None and len(chain):
        spot = float(prices.sort_values("date")["close"].iloc[-1])
    parity_check(chain, spot, r_last, div, lines)
    lines.append("")

    lines.append("## Known Limitations")
    lines.append("")
    lines.append("- intraday only covers recent regime (yfinance caps 730d/60d)")
    lines.append("- ^VIX6M can have gaps in early 2020")
    lines.append("- ^IRX ffilled; r=0.05 fallback if fully missing")
    lines.append("- option_chain_snapshot is one-day only, not historical")
    lines.append("- SPY American early-exercise ignored (near-ATM, negligible)")
    lines.append("")

    path = OUT / "data_quality_report.md"
    path.write_text("\n".join(lines))
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
