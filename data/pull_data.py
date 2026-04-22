"""Download raw data to data/raw/*.pkl."""

import pickle
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

RAW = Path(__file__).resolve().parent / "raw"
RAW.mkdir(parents=True, exist_ok=True)
START = "2020-01-01"


def _save(obj, name):
    p = RAW / name
    with open(p, "wb") as f:
        pickle.dump(obj, f)
    print(f"  saved {name} ({p.stat().st_size/1024:.1f} KB)")


def _flatten(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def pull_spy_daily():
    print("[pull] SPY daily ...")
    end = (date.today() + timedelta(days=1)).isoformat()

    df = yf.download("SPY", start=START, end=end, auto_adjust=False,
                     progress=False, actions=False)
    _save(_flatten(df).reset_index(), "spy_daily.pkl")


def pull_spy_intraday():
    t = yf.Ticker("SPY")

    print("[pull] SPY 1h (730d) ...")
    _save(t.history(period="730d", interval="1h",
                    auto_adjust=False, actions=False).reset_index(),
          "spy_intraday_1h.pkl")

    print("[pull] SPY 15m (60d) ...")
    _save(t.history(period="60d", interval="15m",
                    auto_adjust=False, actions=False).reset_index(),
          "spy_intraday_15m.pkl")


def pull_vix():
    print("[pull] VIX family ...")
    end = (date.today() + timedelta(days=1)).isoformat()

    frames = {}
    for t in ["^VIX", "^VIX3M", "^VIX6M"]:
        try:
            d = _flatten(yf.download(t, start=START, end=end, auto_adjust=False,
                                     progress=False, actions=False))
            if "Close" in d.columns and len(d):
                frames[t] = d[["Close"]].rename(columns={"Close": t})
            else:
                print(f"  WARN: {t} empty")
                frames[t] = pd.DataFrame(columns=[t])
        except Exception as e:
            print(f"  WARN: {t}: {e}")
            frames[t] = pd.DataFrame(columns=[t])

    _save(pd.concat(frames.values(), axis=1).reset_index(), "vix.pkl")


def pull_risk_free():
    print("[pull] ^IRX ...")
    end = (date.today() + timedelta(days=1)).isoformat()

    df = _flatten(yf.download("^IRX", start=START, end=end, auto_adjust=False,
                              progress=False, actions=False))
    df = df.reset_index()[["Date", "Close"]].rename(columns={"Close": "IRX"})
    _save(df, "irx.pkl")


def pull_dividends():
    print("[pull] SPY dividends ...")
    d = yf.Ticker("SPY").dividends.reset_index()
    d.columns = ["ex_date", "amount"]
    d["ticker"] = "SPY"
    _save(d, "spy_dividends.pkl")


def pull_option_chain():
    print("[pull] SPY option chain ...")
    tkr = yf.Ticker("SPY")
    exps = list(tkr.options) if tkr.options else []
    today = date.today()

    chosen = None
    for e in exps:
        try:
            dte = (datetime.strptime(e, "%Y-%m-%d").date() - today).days
        except ValueError:
            continue
        if 30 <= dte <= 60:
            chosen = e
            break

    if chosen is None and exps:
        best = None
        for e in exps:
            try:
                dte = (datetime.strptime(e, "%Y-%m-%d").date() - today).days
            except ValueError:
                continue
            if dte <= 0:
                continue
            diff = abs(dte - 45)
            if best is None or diff < best[0]:
                best = (diff, e)
        if best:
            chosen = best[1]

    result = {"snapshot_date": today.isoformat(), "expiration": None,
              "calls": None, "puts": None, "spot": None}
    if chosen is None:
        print("  WARN: no valid expirations")
        _save(result, "spy_option_chain.pkl")
        return

    ch = tkr.option_chain(chosen)
    calls, puts = ch.calls.copy(), ch.puts.copy()
    calls["cp_flag"] = "C"
    puts["cp_flag"] = "P"

    hist = tkr.history(period="5d", auto_adjust=False, actions=False)
    spot = float(hist["Close"].iloc[-1]) if len(hist) else None

    result.update(expiration=chosen, calls=calls, puts=puts, spot=spot)
    print(f"  chose {chosen}, spot={spot}, calls={len(calls)}, puts={len(puts)}")
    _save(result, "spy_option_chain.pkl")


def main():
    print(f"=== pull_data.py -> {RAW} ===")
    pull_spy_daily()
    pull_spy_intraday()
    pull_vix()
    pull_risk_free()
    pull_dividends()
    pull_option_chain()
    return 0


if __name__ == "__main__":
    sys.exit(main())
