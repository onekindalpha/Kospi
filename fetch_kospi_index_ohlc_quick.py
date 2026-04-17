import argparse
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", default="", help="YYYYMMDD; default=today")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    end_str = args.end if args.end else datetime.now().strftime("%Y%m%d")
    end_dt = datetime.strptime(end_str, "%Y%m%d")
    fetch_end = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    df = fdr.DataReader("KS11", args.start, fetch_end)
    if df.empty:
        raise RuntimeError("KS11 data is empty")

    need = ["Open", "High", "Low", "Close"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise RuntimeError(f"Missing OHLC columns: {miss}; actual={list(df.columns)}")

    out = df.reset_index()[["Date", "Open", "High", "Low", "Close"]].copy()
    out.columns = ["date", "open", "high", "low", "close"]
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y%m%d")
    out = out[out["date"] <= end_str].copy()

    if out.empty:
        raise RuntimeError("No rows to save")

    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"saved: {args.out}")
    print(f"last date: {out['date'].iloc[-1]}")
    print(out.tail(5).to_string(index=False))


if __name__ == "__main__":
    main()
