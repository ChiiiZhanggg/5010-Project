"""Raw pickles -> clean parquet in data/output/."""

import pickle
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "raw"
OUT = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)

DAILY_START = pd.Timestamp("2020-01-01")
FALLBACK_R = 0.05


def _load(name):
    with open(RAW / name, "rb") as f:
        return pickle.load(f)


def _nyse(start, end):
    cal = mcal.get_calendar("NYSE")
    return pd.DatetimeIndex(
        cal.schedule(start_date=start.date(), end_date=end.date()).index.normalize()
    )


def clean_daily_spy():
    print("[clean] SPY daily ...")
    df = _load("spy_daily.pkl")
    df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()

    df = df.rename(columns={
        "Date": "date", "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
    })
    df["ticker"] = "SPY"
    df = df[["date", "ticker", "open", "high", "low", "close",
             "volume", "adj_close"]].dropna(subset=["close"])
    
    df = df.sort_values("date").reset_index(drop=True)

    sessions = _nyse(DAILY_START, df["date"].max())
    missing = [d for d in sessions if d not in set(df["date"])]
    if missing:
        print(f"  info: {len(missing)} NYSE sessions missing; e.g. "
              f"{[m.date().isoformat() for m in missing[:5]]}")

    df["logret"] = np.log(df["close"] / df["close"].shift(1))
    df["rv_20d"] = df["logret"].rolling(20).std() * np.sqrt(252)
    df["rv_60d"] = df["logret"].rolling(60).std() * np.sqrt(252)

    df.to_parquet(OUT / "stock_prices.parquet", index=False)
    print(f"  wrote stock_prices.parquet rows={len(df)}")


def clean_intraday(name_in, name_out):
    print(f"[clean] {name_in} ...")
    try:
        df = _load(name_in)
    except FileNotFoundError:
        print(f"  skip: {name_in} missing")
        return
    if df is None or len(df) == 0:
        print(f"  skip: {name_in} empty")
        return

    ts_col = next((c for c in ("Datetime", "Date", "index") if c in df.columns),
                  df.columns[0])
    df = df.rename(columns={ts_col: "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"], utc=False)

    if df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize(
            "America/New_York", nonexistent="shift_forward", ambiguous="NaT")
    else:
        df["datetime"] = df["datetime"].dt.tz_convert("America/New_York")

    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low",
                            "Close": "close", "Volume": "volume"})
    keep = ["datetime", "open", "high", "low", "close", "volume"]
    for c in keep:
        if c not in df.columns:
            df[c] = np.nan
    df = df[keep].dropna(subset=["close"])
    df["ticker"] = "SPY"
    df = df[["datetime", "ticker", "open", "high", "low", "close", "volume"]]
    df = df.sort_values("datetime").reset_index(drop=True)

    df.to_parquet(OUT / name_out, index=False)
    print(f"  wrote {name_out} rows={len(df)}")


def clean_vix():
    print("[clean] VIX ...")
    df = _load("vix.pkl")
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    df = df.rename(columns={"Date": "date", "^VIX": "vix",
                            "^VIX3M": "vix3m", "^VIX6M": "vix6m"})

    for col in ("vix", "vix3m", "vix6m"):
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce") / 100.0

    df = df[["date", "vix", "vix3m", "vix6m"]].sort_values("date").reset_index(drop=True)
    df.to_parquet(OUT / "vix.parquet", index=False)
    print(f"  wrote vix.parquet rows={len(df)}")


def clean_risk_free():
    print("[clean] ^IRX ...")
    df = _load("irx.pkl")
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    df["IRX"] = pd.to_numeric(df["IRX"], errors="coerce")
    df = df.rename(columns={"Date": "date"}).sort_values("date").reset_index(drop=True)

    df["r"] = (df["IRX"] / 100.0).ffill()
    if df["r"].isna().any():
        print(f"  fallback r={FALLBACK_R} for {df['r'].isna().sum()} rows")
        df["r"] = df["r"].fillna(FALLBACK_R)

    df = df[["date", "r"]]
    df.to_parquet(OUT / "risk_free.parquet", index=False)
    print(f"  wrote risk_free.parquet rows={len(df)}, "
          f"r range=[{df['r'].min():.4f}, {df['r'].max():.4f}]")


def clean_dividends():
    print("[clean] dividends ...")
    df = _load("spy_dividends.pkl")
    if len(df) == 0:
        df = pd.DataFrame(columns=["ex_date", "ticker", "amount"])
    else:
        df["ex_date"] = pd.to_datetime(df["ex_date"]).dt.tz_localize(None).dt.normalize()
        df = df[["ex_date", "ticker", "amount"]]
    df.to_parquet(OUT / "dividends.parquet", index=False)
    print(f"  wrote dividends.parquet rows={len(df)}")


def clean_option_chain():
    print("[clean] option chain ...")
    raw = _load("spy_option_chain.pkl")
    if raw is None or raw.get("calls") is None or raw.get("puts") is None:
        print("  skip: empty")
        return

    def _norm(d):
        out = pd.DataFrame()
        out["strike"] = pd.to_numeric(d.get("strike"), errors="coerce")
        out["bid"] = pd.to_numeric(d.get("bid"), errors="coerce")
        out["ask"] = pd.to_numeric(d.get("ask"), errors="coerce")
        out["last"] = pd.to_numeric(d.get("lastPrice"), errors="coerce")
        out["volume"] = pd.to_numeric(d.get("volume"), errors="coerce")
        out["oi"] = pd.to_numeric(d.get("openInterest"), errors="coerce")
        out["iv"] = pd.to_numeric(d.get("impliedVolatility"), errors="coerce")
        out["cp_flag"] = d["cp_flag"].values
        return out

    df = pd.concat([_norm(raw["calls"]), _norm(raw["puts"])], ignore_index=True)
    df["expiration"] = pd.Timestamp(raw["expiration"])
    df["snapshot_date"] = pd.Timestamp(raw["snapshot_date"])

    df = df[["expiration", "strike", "cp_flag", "bid", "ask", "last",
             "volume", "oi", "iv", "snapshot_date"]]
    df.to_parquet(OUT / "option_chain_snapshot.parquet", index=False)
    print(f"  wrote option_chain_snapshot.parquet rows={len(df)}")


def main():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        print(f"clean_data.py -> {OUT} done")
        clean_daily_spy()
        clean_intraday("spy_intraday_1h.pkl", "stock_intraday_1h.parquet")
        clean_intraday("spy_intraday_15m.pkl", "stock_intraday_15m.parquet")
        clean_vix()
        clean_risk_free()
        clean_dividends()
        clean_option_chain()
    return 0


if __name__ == "__main__":
    sys.exit(main())
