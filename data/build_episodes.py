"""Build episodes.parquet: 4 regimes x 3 options = 12 rows."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from config import REGIMES

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"

MIN_TRADING_DAYS = 60
FRIDAY_SCAN_DAYS = 70


def _nyse(start, end):
    cal = mcal.get_calendar("NYSE")
    return pd.DatetimeIndex(
        cal.schedule(start_date=start.date(), end_date=end.date()).index.normalize()
    )


def _first_session_after(target, sessions):
    later = sessions[sessions >= target]
    if not len(later):
        raise ValueError(f"no NYSE session >= {target.date()}")
    return later[0]


def _friday_at_least(start, n, sessions):
    # first Friday >= n trading days after start AND itself a session
    # (Good Friday would otherwise hand back a closed-market expiry)
    future = sessions[sessions > start]
    if len(future) < n:
        raise ValueError(f"not enough sessions after {start.date()}")

    earliest = future[n - 1]
    sset = set(sessions)
    cur = earliest

    for _ in range(FRIDAY_SCAN_DAYS):
        if cur.weekday() == 4 and cur in sset:
            return cur
        cur += pd.Timedelta(days=1)
    raise ValueError(f"no session-Friday within {FRIDAY_SCAN_DAYS}d of {earliest.date()}")


def main():
    print("=== build_episodes.py ===")
    prices = pd.read_parquet(OUT / "stock_prices.parquet")
    vix = pd.read_parquet(OUT / "vix.parquet")
    rf = pd.read_parquet(OUT / "risk_free.parquet")
    div = pd.read_parquet(OUT / "dividends.parquet")

    for d in (prices, vix, rf):
        d["date"] = pd.to_datetime(d["date"]).dt.normalize()
    if len(div):
        div["ex_date"] = pd.to_datetime(div["ex_date"]).dt.normalize()

    sessions = _nyse(pd.Timestamp("2019-12-01"),
                     prices["date"].max() + pd.Timedelta(days=400))

    price_map = prices.set_index("date")["close"].to_dict()
    vix_map = vix.set_index("date")[["vix", "vix3m"]].to_dict("index")
    r_map = rf.set_index("date")["r"].to_dict()
    r_sorted = rf.sort_values("date").reset_index(drop=True)

    def lookup_vix(d):
        row = vix_map.get(d)
        if row is None:
            prior = vix[vix["date"] <= d].tail(1)
            if not len(prior):
                return float("nan"), float("nan")
            return float(prior["vix"].iloc[0]), float(prior["vix3m"].iloc[0])
        return float(row["vix"]), float(row["vix3m"])

    def lookup_r(d):
        if d in r_map and not pd.isna(r_map[d]):
            return float(r_map[d])
        prior = r_sorted[r_sorted["date"] <= d].tail(1)
        return float(prior["r"].iloc[0]) if len(prior) else 0.05

    rows = []
    for regime, (w_start, _) in REGIMES.items():
        inception = _first_session_after(pd.Timestamp(w_start), sessions)
        if inception not in price_map:
            nearby = sessions[(sessions >= inception) &
                              (sessions <= inception + pd.Timedelta(days=7))]
            inception = next((d for d in nearby if d in price_map), inception)

        S0 = float(price_map.get(inception, float("nan")))
        if np.isnan(S0):
            raise RuntimeError(f"no SPY close near {inception.date()}")

        expiration = _friday_at_least(inception, MIN_TRADING_DAYS, sessions)
        t_days = int(((sessions > inception) & (sessions <= expiration)).sum())
        t_years = t_days / 252.0

        iv_vix, iv_vix3m = lookup_vix(inception)
        r0 = lookup_r(inception)

        dw = (div[(div["ex_date"] > inception) & (div["ex_date"] <= expiration)]
              if len(div) else div.iloc[0:0])
        div_amt = float(dw["amount"].sum()) if len(dw) else 0.0
        div_dates = ",".join(d.strftime("%Y-%m-%d") for d in dw["ex_date"]) if len(dw) else ""

        if len(dw):
            y2d = (dw["ex_date"] - inception).dt.days.to_numpy() / 365.0
            pv_div = float((dw["amount"].to_numpy() * np.exp(-r0 * y2d)).sum())
        else:
            pv_div = 0.0
        q = (float(-np.log(1 - pv_div / S0) / t_years)
             if t_years > 0 and S0 > 0 and 0 < pv_div < S0 else 0.0)

        for cp, money, strike in [("C", "ATM", round(S0)),
                                  ("C", "OTM", round(S0 * 1.05)),
                                  ("P", "ATM", round(S0))]:
            rows.append({
                "episode_id": f"SPY_{regime}_{cp}_{money}",
                "regime": regime, "ticker": "SPY", "cp_flag": cp, "moneyness": money,
                "inception_date": inception, "S0": S0, "strike": float(strike),
                "expiration": expiration, "T_days": t_days, "T_years": t_years,
                "iv_inception_vix": iv_vix, "iv_inception_vix3m": iv_vix3m,
                "r_inception": r0,
                "div_amount_in_window": div_amt, "div_ex_dates": div_dates,
                "pv_div_inception": pv_div, "q_inception": q,
            })
            print(f"SPY_{regime}_{cp}_{money}: S0={S0:.2f} K={strike} "
                  f"exp={expiration.date()} T={t_days} iv={iv_vix:.3f} "
                  f"r={r0:.4f} div=${div_amt:.3f}")

    df = pd.DataFrame(rows)
    df.to_parquet(OUT / "episodes.parquet", index=False)
    print(f"wrote {len(df)} episodes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
