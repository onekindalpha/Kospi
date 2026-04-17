#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

API_BASE = "https://data-dbg.krx.co.kr/svc/apis/sto"
ENDPOINTS = {
    "KOSPI": "/stk_bydd_trd",
    "KOSDAQ": "/ksq_bydd_trd",
    "KONEX": "/knx_bydd_trd",
}


def _krx_post(session: requests.Session, auth_key: str, endpoint: str, payload: dict, timeout: int = 15) -> dict:
    url = API_BASE + endpoint
    headers = {
        "AUTH_KEY": auth_key.strip(),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    r = session.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"KRX API Error: {r.status_code} / {r.text[:300]}")
    try:
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"JSON parse failed: {e} / body={r.text[:300]}")
    if isinstance(data, dict) and data.get("respCode") not in (None, "000", 0, "0"):
        raise RuntimeError(f"KRX API respCode error: {data.get('respCode')} / {data.get('respMsg')}")
    return data


def fetch_daily_trade(session: requests.Session, auth_key: str, bas_dd: str, market: str) -> pd.DataFrame:
    market = market.upper()
    if market not in ENDPOINTS:
        raise ValueError(f"unsupported market: {market}")
    data = _krx_post(session, auth_key, ENDPOINTS[market], {"basDd": bas_dd})
    rows = data.get("OutBlock_1", [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for c in [
        "TDD_CLSPRC", "CMPPREVDD_PRC", "FLUC_RT",
        "TDD_OPNPRC", "TDD_HGPRC", "TDD_LWPRC",
        "ACC_TRDVOL", "ACC_TRDVAL", "MKTCAP", "LIST_SHRS",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False), errors="coerce")

    rename_map = {
        "BAS_DD": "Date",
        "ISU_CD": "Code",
        "ISU_NM": "Name",
        "MKT_NM": "MarketName",
        "SECT_TP_NM": "SectorType",
        "TDD_OPNPRC": "Open",
        "TDD_HGPRC": "High",
        "TDD_LWPRC": "Low",
        "TDD_CLSPRC": "Close",
        "ACC_TRDVOL": "Volume",
        "ACC_TRDVAL": "Value",
        "MKTCAP": "MarketCap",
        "LIST_SHRS": "ListShares",
        "CMPPREVDD_PRC": "PrevDiff",
        "FLUC_RT": "FlucRate",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["Market"] = market
    return df


def classify_breadth(df: pd.DataFrame) -> tuple[int, int, int]:
    if df.empty:
        return 0, 0, 0
    if "PrevDiff" in df.columns:
        diff = pd.to_numeric(df["PrevDiff"], errors="coerce").fillna(0)
        adv = int((diff > 0).sum())
        decl = int((diff < 0).sum())
        unch = int((diff == 0).sum())
        return adv, decl, unch
    if "FlucRate" in df.columns:
        fluc = pd.to_numeric(df["FlucRate"], errors="coerce").fillna(0)
        adv = int((fluc > 0).sum())
        decl = int((fluc < 0).sum())
        unch = int((fluc == 0).sum())
        return adv, decl, unch
    raise RuntimeError(f"Cannot classify breadth: columns={list(df.columns)}")


def build_breadth(auth_key: str, start: str, end: str, market: str, base_value: float, sleep_sec: float = 0.0, progress_every: int = 10) -> pd.DataFrame:
    dates = pd.bdate_range(pd.to_datetime(start), pd.to_datetime(end))
    total = len(dates)
    rows = []
    ad_line = base_value
    session = requests.Session()

    print(f"[INFO] market={market} dates={start}..{end} business_days={total}", flush=True)

    for i, dt in enumerate(dates, start=1):
        bas_dd = dt.strftime("%Y%m%d")
        try:
            df = fetch_daily_trade(session, auth_key, bas_dd, market)
            if df.empty:
                print(f"[INFO] {bas_dd} empty/holiday", flush=True)
                continue
            adv, decl, unch = classify_breadth(df)
            ad_diff = adv - decl
            ad_line += ad_diff
            rows.append({
                "date": bas_dd,
                "advances": adv,
                "declines": decl,
                "unchanged": unch,
                "ad_diff": ad_diff,
                "ad_line": ad_line,
            })
            if i == 1 or i % progress_every == 0 or i == total:
                print(f"[OK] {i}/{total} {bas_dd} adv={adv} decl={decl} unch={unch} ad_line={ad_line}", flush=True)
        except Exception as e:
            print(f"[WARN] {i}/{total} {bas_dd} skipped: {e}", file=sys.stderr, flush=True)
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No breadth rows built. Check AUTH_KEY approval, endpoint permission, and date range.")

    breadth = (out["advances"] / (out["advances"] + out["declines"]).replace(0, pd.NA)).astype(float)
    out["breadth_thrust_ema10"] = breadth.ewm(span=10, adjust=False).mean()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["KOSPI", "KOSDAQ", "KONEX"])
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    ap.add_argument("--out", required=True)
    ap.add_argument("--base", type=float, default=50000.0)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--progress-every", type=int, default=10)
    args = ap.parse_args()

    auth_key = os.environ.get("KRX_AUTH_KEY", "").strip()
    if not auth_key:
        raise SystemExit("KRX_AUTH_KEY environment variable not set")

    out = build_breadth(auth_key, args.start, args.end, args.market, args.base, args.sleep, args.progress_every)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"saved: {out_path}", flush=True)
    print(out.tail(5).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
