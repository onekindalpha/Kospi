#!/usr/bin/env python3
"""
GitHub Actions에서 매일 실행 — KRX API로 KOSPI/KOSDAQ breadth 수집 후 CSV 저장
환경변수: KRX_AUTH_KEY
"""
import os, sys
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests

API_BASE    = "https://data-dbg.krx.co.kr/svc/apis/sto"
ENDPOINTS   = {"KOSPI": "/stk_bydd_trd", "KOSDAQ": "/ksq_bydd_trd"}
FDR_SYMBOLS = {"KOSPI": "KS11", "KOSDAQ": "KQ11"}
DATA_DIR    = Path("data")
DATA_DIR.mkdir(exist_ok=True)

AUTH_KEY = os.environ.get("KRX_AUTH_KEY", "")
if not AUTH_KEY:
    print("ERROR: KRX_AUTH_KEY 환경변수 없음")
    sys.exit(1)


def fetch_krx_page(market: str, bas_dd: str) -> dict:
    url = API_BASE + ENDPOINTS[market]
    headers = {
        "AUTH_KEY": AUTH_KEY.strip(),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.post(url, headers=headers, json={"basDd": bas_dd}, timeout=30)
    r.raise_for_status()
    return r.json()


def collect_breadth(market: str, start_str: str, end_str: str,
                    base_value: float = 50000.0) -> pd.DataFrame:
    start = datetime.strptime(start_str, "%Y%m%d")
    end   = datetime.strptime(end_str,   "%Y%m%d")
    rows  = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # 평일만
            bas_dd = d.strftime("%Y%m%d")
            try:
                data = fetch_krx_page(market, bas_dd)
                items = data.get("OutBlock_1", [])
                if items:
                    def _val(x):
                        # 대시보드와 동일 — PrevDiff 또는 FlucRate 필드
                        for key in ("CMPPREVDD_PRC", "PrevDiff", "FLUC_RT", "FlucRate"):
                            v = x.get(key)
                            if v is not None:
                                try:
                                    return float(str(v).replace(",", ""))
                                except Exception:
                                    pass
                        return 0.0
                    adv = sum(1 for x in items if _val(x) > 0)
                    dec = sum(1 for x in items if _val(x) < 0)
                    unc = len(items) - adv - dec
                    rows.append({"date": int(bas_dd), "advances": adv, "declines": dec, "unchanged": unc})
                    print(f"  {bas_dd}: +{adv} -{dec} ={unc}")
            except Exception as e:
                print(f"  {bas_dd} skip: {e}")
        d += timedelta(days=1)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    df["ad_diff"] = df["advances"] - df["declines"]
    df["ad_line"] = base_value + df["ad_diff"].cumsum()
    return df


def fetch_index_ohlc(market: str, start_str: str, end_str: str) -> pd.DataFrame:
    try:
        import FinanceDataReader as fdr
        sym = FDR_SYMBOLS[market]
        raw = fdr.DataReader(sym, start_str, end_str).reset_index()
        raw.columns = [c.lower() for c in raw.columns]
        raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y%m%d").astype(int)
        return raw[["date", "open", "high", "low", "close"]].dropna()
    except Exception as e:
        print(f"  index fetch 실패: {e}")
        return pd.DataFrame()


def main():
    today    = datetime.today()
    end_str  = today.strftime("%Y%m%d")
    # 기존 CSV가 있으면 마지막 날짜 이후부터만 수집
    for market in ["KOSPI", "KOSDAQ"]:
        csv_path   = DATA_DIR / f"{market.lower()}_breadth.csv"
        idx_path   = DATA_DIR / f"{market.lower()}_index.csv"

        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            last_date = int(existing["date"].max())
            start_dt  = datetime.strptime(str(last_date), "%Y%m%d") + timedelta(days=1)
            start_str = start_dt.strftime("%Y%m%d")
            print(f"[{market}] 기존 {len(existing)}행, {last_date} 이후 추가 수집")
        else:
            # 최초 실행: 2년치
            start_str = (today - timedelta(days=730)).strftime("%Y%m%d")
            existing  = None
            print(f"[{market}] 최초 수집 {start_str}~{end_str}")

        if start_str > end_str:
            print(f"[{market}] 이미 최신 상태")
            continue

        new_df = collect_breadth(market, start_str, end_str)
        if new_df.empty:
            print(f"[{market}] 새 데이터 없음")
            continue

        if existing is not None:
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates("date").sort_values("date").reset_index(drop=True)
            # ad_line 재계산 (base 유지)
            base = float(existing["ad_line"].iloc[0]) - float(existing["ad_diff"].iloc[0])
            combined["ad_line"] = base + combined["ad_diff"].cumsum()
        else:
            combined = new_df

        combined.to_csv(csv_path, index=False)
        print(f"[{market}] breadth CSV 저장 ({len(combined)}행) → {csv_path}")

        # 지수 OHLC
        idx_new = fetch_index_ohlc(market, start_str, end_str)
        if not idx_new.empty:
            if idx_path.exists():
                idx_existing = pd.read_csv(idx_path)
                idx_combined = pd.concat([idx_existing, idx_new], ignore_index=True)
                idx_combined = idx_combined.drop_duplicates("date").sort_values("date").reset_index(drop=True)
            else:
                idx_combined = idx_new
            idx_combined.to_csv(idx_path, index=False)
            print(f"[{market}] index CSV 저장 ({len(idx_combined)}행) → {idx_path}")


if __name__ == "__main__":
    main()
