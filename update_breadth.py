#!/usr/bin/env python3
"""
GitHub Actions에서 매일 실행
1) KOSPI/KOSDAQ breadth (advances/declines) → data/{market}_breadth.csv
2) 지수 OHLC                                → data/{market}_index.csv
3) 전종목 종가 누적                          → data/{market}_prices.csv
4) 일별 NH-NL (52주 기준)                   → data/{market}_nhnl_daily.csv
5) 주간 NH-NL (W-FRI 집계)                  → data/{market}_nhnl.csv

환경변수: KRX_AUTH_KEY
"""
import os, sys
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests

API_BASE  = "https://data-dbg.krx.co.kr/svc/apis/sto"
ENDPOINTS = {"KOSPI": "/stk_bydd_trd", "KOSDAQ": "/ksq_bydd_trd"}
FDR_SYMS  = {"KOSPI": "KS11", "KOSDAQ": "KQ11"}
DATA_DIR  = Path("data")
DATA_DIR.mkdir(exist_ok=True)

AUTH_KEY = os.environ.get("KRX_AUTH_KEY", "").strip()
if not AUTH_KEY:
    print("ERROR: KRX_AUTH_KEY 환경변수 없음")
    sys.exit(1)


# ── KRX API 호출 ──────────────────────────────────────────────
def fetch_krx(market: str, bas_dd: str) -> list:
    url = API_BASE + ENDPOINTS[market]
    headers = {
        "AUTH_KEY": AUTH_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.post(url, headers=headers, json={"basDd": bas_dd}, timeout=30)
    r.raise_for_status()
    return r.json().get("OutBlock_1", [])


def is_common_stock(item: dict) -> bool:
    """우선주/ETF/ETN/리츠 등 제외 — 보통주만"""
    nm = str(item.get("ISU_NM", "") or item.get("ISU_ABBRV", ""))
    cd = str(item.get("ISU_SRT_CD", "") or item.get("ISU_CD", ""))
    if cd and not cd.endswith("0"):
        return False
    bad = ["우", "ETF", "ETN", "리츠", "스팩", "SPAC", "인프라"]
    return not any(b in nm for b in bad)


# ── 1) Breadth ────────────────────────────────────────────────
def collect_breadth(market: str, start_str: str, end_str: str,
                    base_value: float = 50000.0) -> pd.DataFrame:
    start = datetime.strptime(start_str, "%Y%m%d")
    end   = datetime.strptime(end_str,   "%Y%m%d")
    rows  = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            bas_dd = d.strftime("%Y%m%d")
            try:
                items = fetch_krx(market, bas_dd)
                if items:
                    def _val(x):
                        for key in ("CMPPREVDD_PRC", "FLUC_RT"):
                            v = x.get(key)
                            if v is not None:
                                try: return float(str(v).replace(",", ""))
                                except: pass
                        return 0.0
                    adv = sum(1 for x in items if _val(x) > 0)
                    dec = sum(1 for x in items if _val(x) < 0)
                    unc = len(items) - adv - dec
                    rows.append({"date": int(bas_dd), "advances": adv,
                                 "declines": dec, "unchanged": unc})
                    print(f"  breadth {bas_dd}: +{adv} -{dec} ={unc}")
            except Exception as e:
                print(f"  breadth {bas_dd} skip: {e}")
        d += timedelta(days=1)

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    df["ad_diff"] = df["advances"] - df["declines"]
    df["ad_line"] = base_value + df["ad_diff"].cumsum()
    return df


# ── 2) 지수 OHLC ──────────────────────────────────────────────
def fetch_index_ohlc(market: str, start_str: str, end_str: str) -> pd.DataFrame:
    try:
        import FinanceDataReader as fdr
        raw = fdr.DataReader(FDR_SYMS[market], start_str, end_str).reset_index()
        raw.columns = [c.lower() for c in raw.columns]
        raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y%m%d").astype(int)
        return raw[["date", "open", "high", "low", "close"]].dropna()
    except Exception as e:
        print(f"  index fetch 실패: {e}")
        return pd.DataFrame()


# ── 3+4) 전종목 종가 누적 + 일별 NH-NL 계산 ─────────────────
def update_prices_and_nhnl(market: str, bas_dd: str):
    """
    bas_dd 하루치 전종목 종가를 prices CSV에 추가하고
    252거래일치가 쌓이면 그날 NH-NL을 계산해 nhnl_daily CSV에 추가.
    """
    prices_path = DATA_DIR / f"{market.lower()}_prices.csv"
    nhnl_d_path = DATA_DIR / f"{market.lower()}_nhnl_daily.csv"

    # 이미 오늘 nhnl_daily에 있으면 스킵
    if nhnl_d_path.exists():
        _ex = pd.read_csv(nhnl_d_path)
        if int(bas_dd) in _ex["date"].values:
            print(f"  [{market}] nhnl_daily {bas_dd} 이미 존재, 스킵")
            return

    # 오늘 종가 fetch
    try:
        items = fetch_krx(market, bas_dd)
    except Exception as e:
        print(f"  [{market}] prices fetch 실패 {bas_dd}: {e}")
        return

    if not items:
        print(f"  [{market}] {bas_dd} 데이터 없음 (휴장일?)")
        return

    today_rows = []
    for item in items:
        if not is_common_stock(item):
            continue
        cd = str(item.get("ISU_SRT_CD", "") or item.get("ISU_CD", "")).strip()
        cl = item.get("TDD_CLSPRC", "")
        try:
            close_val = float(str(cl).replace(",", ""))
        except:
            continue
        if cd and close_val > 0:
            today_rows.append({"date": int(bas_dd), "code": cd, "close": close_val})

    if not today_rows:
        print(f"  [{market}] {bas_dd} 유효 종목 없음")
        return

    today_df = pd.DataFrame(today_rows)
    print(f"  [{market}] {bas_dd} 종목 수: {len(today_df)}")

    # 기존 prices에 오늘 추가
    if prices_path.exists():
        prices = pd.read_csv(prices_path)
        prices = prices[prices["date"] != int(bas_dd)]
        prices = pd.concat([prices, today_df], ignore_index=True)
    else:
        prices = today_df

    prices = prices.sort_values(["code", "date"]).reset_index(drop=True)
    prices.to_csv(prices_path, index=False)

    # NH-NL 계산: 252거래일치 있는 종목만
    dates_avail = sorted(prices["date"].unique())
    if len(dates_avail) < 252:
        print(f"  [{market}] 누적 {len(dates_avail)}일 — 252일 미만, NH-NL 계산 보류")
        return

    # 오늘 기준 직전 252 거래일
    ref_dates = dates_avail[-252:]
    panel = prices[prices["date"].isin(ref_dates)].copy()

    # 종목별 252일 모두 있는 것만
    cnt = panel.groupby("code")["date"].count()
    valid_codes = cnt[cnt >= 252].index
    panel = panel[panel["code"].isin(valid_codes)]

    if panel.empty:
        print(f"  [{market}] 유효 종목 없어 NH-NL 스킵")
        return

    # 직전 251일 기준 최고/최저 → 오늘 종가와 비교
    prev_dates = ref_dates[:-1]
    prev_panel = panel[panel["date"].isin(prev_dates)]
    prev_high = prev_panel.groupby("code")["close"].max()
    prev_low  = prev_panel.groupby("code")["close"].min()

    today_close = panel[panel["date"] == int(bas_dd)].set_index("code")["close"]
    today_close = today_close[today_close.index.isin(valid_codes)]

    nh = int((today_close > prev_high.reindex(today_close.index)).fillna(False).sum())
    nl = int((today_close < prev_low.reindex(today_close.index)).fillna(False).sum())
    nhnl = nh - nl
    print(f"  [{market}] NH-NL {bas_dd}: NH={nh} NL={nl} NHNL={nhnl:+}")

    new_row = pd.DataFrame([{"date": int(bas_dd), "new_highs": nh,
                              "new_lows": nl, "nhnl": nhnl}])

    if nhnl_d_path.exists():
        nhnl_daily = pd.read_csv(nhnl_d_path)
        nhnl_daily = nhnl_daily[nhnl_daily["date"] != int(bas_dd)]
        nhnl_daily = pd.concat([nhnl_daily, new_row], ignore_index=True)
    else:
        nhnl_daily = new_row

    nhnl_daily = nhnl_daily.sort_values("date").reset_index(drop=True)
    nhnl_daily.to_csv(nhnl_d_path, index=False)
    print(f"  [{market}] nhnl_daily 저장 ({len(nhnl_daily)}행)")


# ── 5) 주간 NH-NL 재집계 ─────────────────────────────────────
def rebuild_weekly_nhnl(market: str):
    nhnl_d_path = DATA_DIR / f"{market.lower()}_nhnl_daily.csv"
    nhnl_w_path = DATA_DIR / f"{market.lower()}_nhnl.csv"

    if not nhnl_d_path.exists():
        return

    daily = pd.read_csv(nhnl_d_path)
    daily["dt"] = pd.to_datetime(daily["date"].astype(str), format="%Y%m%d")

    weekly = (daily.set_index("dt")
              .resample("W-FRI")[["new_highs", "new_lows", "nhnl"]]
              .sum()
              .reset_index())
    weekly["date"] = weekly["dt"].dt.strftime("%Y%m%d").astype(int)
    weekly = weekly[["date", "dt", "new_highs", "new_lows", "nhnl"]]
    weekly = weekly.sort_values("date").reset_index(drop=True)

    # 기존 주간 CSV가 있으면 병합 (daily 재계산분이 없는 오래된 주는 old 유지)
    if nhnl_w_path.exists():
        old_w = pd.read_csv(nhnl_w_path)
        daily_dates_weekly = set(weekly["date"].values)
        old_w = old_w[~old_w["date"].isin(daily_dates_weekly)]
        weekly = pd.concat([old_w, weekly], ignore_index=True)
        weekly = weekly.sort_values("date").reset_index(drop=True)

    weekly.to_csv(nhnl_w_path, index=False)
    print(f"  [{market}] nhnl 주간 재집계 저장 ({len(weekly)}행)")


# ── main ──────────────────────────────────────────────────────
def main():
    today   = datetime.today()
    end_str = today.strftime("%Y%m%d")

    for market in ["KOSPI", "KOSDAQ"]:
        print(f"\n{'='*40}")
        print(f"[{market}] 처리 시작")

        # ── breadth ──
        csv_path = DATA_DIR / f"{market.lower()}_breadth.csv"
        idx_path = DATA_DIR / f"{market.lower()}_index.csv"

        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            last_date = int(existing["date"].max())
            start_dt  = datetime.strptime(str(last_date), "%Y%m%d") + timedelta(days=1)
            start_str = start_dt.strftime("%Y%m%d")
            print(f"[{market}] breadth: 기존 {len(existing)}행, {last_date} 이후 추가")
        else:
            start_str = (today - timedelta(days=730)).strftime("%Y%m%d")
            existing  = None
            print(f"[{market}] breadth: 최초 수집 {start_str}~{end_str}")

        if start_str <= end_str:
            new_df = collect_breadth(market, start_str, end_str)
            if not new_df.empty:
                if existing is not None:
                    combined = pd.concat([existing, new_df], ignore_index=True)
                    combined = combined.drop_duplicates("date").sort_values("date").reset_index(drop=True)
                    base = float(existing["ad_line"].iloc[0]) - float(existing["ad_diff"].iloc[0])
                    combined["ad_line"] = base + combined["ad_diff"].cumsum()
                else:
                    combined = new_df
                combined.to_csv(csv_path, index=False)
                print(f"[{market}] breadth CSV 저장 ({len(combined)}행)")

        # ── index OHLC ──
        if start_str <= end_str:
            idx_new = fetch_index_ohlc(market, start_str, end_str)
            if not idx_new.empty:
                if idx_path.exists():
                    idx_ex = pd.read_csv(idx_path)
                    idx_combined = pd.concat([idx_ex, idx_new], ignore_index=True)
                    idx_combined = idx_combined.drop_duplicates("date").sort_values("date").reset_index(drop=True)
                else:
                    idx_combined = idx_new
                idx_combined.to_csv(idx_path, index=False)
                print(f"[{market}] index CSV 저장 ({len(idx_combined)}행)")

        # ── prices + nhnl_daily ──
        # 오늘 포함 최근 3거래일 시도 (장마감 전 실행 시 어제치라도 수집)
        for offset in range(3):
            _d = today - timedelta(days=offset)
            if _d.weekday() < 5:
                update_prices_and_nhnl(market, _d.strftime("%Y%m%d"))

        # ── 주간 NH-NL 재집계 ──
        rebuild_weekly_nhnl(market)

    print("\n완료")


if __name__ == "__main__":
    main()
