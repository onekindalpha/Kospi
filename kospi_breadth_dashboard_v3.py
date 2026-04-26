#!/usr/bin/env python3
from __future__ import annotations
# KOSPI / KOSDAQ Breadth Dashboard (Streamlit)
# 실행: streamlit run kospi_breadth_dashboard_v1.py
# GitHub raw CSV URL (로컬에서 data/ 폴더 push 후 Cloud에서 읽음)
GITHUB_RAW = "https://raw.githubusercontent.com/onekindalpha/Kospi/main/data"
GITHUB_BREADTH = {
    "KOSPI":  f"{GITHUB_RAW}/kospi_breadth.csv",
    "KOSDAQ": f"{GITHUB_RAW}/kosdaq_breadth.csv",
}
GITHUB_INDEX = {
    "KOSPI":  f"{GITHUB_RAW}/kospi_index.csv",
    "KOSDAQ": f"{GITHUB_RAW}/kosdaq_index.csv",
}
GITHUB_NHNL = {
    "KOSPI":  f"{GITHUB_RAW}/kospi_nhnl.csv",
    "KOSDAQ": f"{GITHUB_RAW}/kosdaq_nhnl.csv",
}
GITHUB_NHNL_DAILY = {
    "KOSPI":  f"{GITHUB_RAW}/kospi_nhnl_daily.csv",
    "KOSDAQ": f"{GITHUB_RAW}/kosdaq_nhnl_daily.csv",
}

import hashlib
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import platform
import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

# ── 한글 폰트 설정 ──
def _setup_korean_font():
    import matplotlib.font_manager as fm
    import subprocess
    sys_name = platform.system()
    if sys_name == "Darwin":
        plt.rcParams["font.family"] = "AppleGothic"
    elif sys_name == "Windows":
        plt.rcParams["font.family"] = "Malgun Gothic"
    else:
        # Linux (Streamlit Cloud): NanumGothic 설치 시도
        nanum = [f.name for f in fm.fontManager.ttflist if "Nanum" in f.name]
        if nanum:
            plt.rcParams["font.family"] = nanum[0]
        else:
            try:
                subprocess.run(
                    ["apt-get", "install", "-y", "-q", "fonts-nanum"],
                    check=True, capture_output=True
                )
                fm._load_fontmanager(try_read_cache=False)
                nanum2 = [f.name for f in fm.fontManager.ttflist if "Nanum" in f.name]
                if nanum2:
                    plt.rcParams["font.family"] = nanum2[0]
            except Exception:
                # 폰트 설치 실패 시 차트 레이블을 영어로 대체 (아래 make_chart_img 참조)
                pass
    plt.rcParams["axes.unicode_minus"] = False

_setup_korean_font()
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import traceback
import streamlit as st

try:
    from mplfinance.original_flavor import candlestick_ohlc
    MPL_OK = True
except ImportError:
    MPL_OK = False

try:
    import FinanceDataReader as fdr
    FDR_OK = True
except ImportError:
    FDR_OK = False

# ──────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────
API_BASE = "https://data-dbg.krx.co.kr/svc/apis/sto"
KRX_ENDPOINTS  = {"KOSPI": "/stk_bydd_trd", "KOSDAQ": "/ksq_bydd_trd"}
FDR_SYMBOLS    = {"KOSPI": "KS11",          "KOSDAQ": "KQ11"}
CACHE_DIR      = Path("./breadth_cache")

STATUS_MAP = {
    "BULLISH_CONFIRMATION":         ("✅ 상승 확인",           "가격·A/D선 모두 고점 근접 (동행)",                   "#2e7d32"),
    "BULLISH_DIVERGENCE":           ("🔴 심각한 A/D 미확인",   "가격 고점인데 A/D선이 크게 뒤처짐",                  "#c62828"),
    "BULLISH_DIVERGENCE_CANDIDATE": ("🟠 A/D 초기 경고",       "가격이 A/D선보다 빠르게 회복 중",                    "#ef6c00"),
    "RECOVERY_IN_PROGRESS":         ("🟡 회복 진행 중",         "가격 고점 재공략 중, 브레드스 미확인",                "#f9a825"),
    "DOWNSIDE_DIVERGENCE_CANDIDATE":("🟢 하락 다이버전스",      "가격 저점 근접, A/D선은 저점 미확인",                 "#00838f"),
    "NORMAL_WEAKNESS":              ("⚫ 전반적 약세",           "가격·A/D선 모두 저점 근접",                          "#455a64"),
    "NEUTRAL":                      ("⬜ 중립",                 "뚜렷한 신호 없음",                                   "#757575"),
}

# ──────────────────────────────────────────────────────────────
# NH-NL 캐시 경로
# ──────────────────────────────────────────────────────────────
NHNL_CACHE_DIR = Path("./nhnl_cache_v2")

def _nhnl_cache_path(market: str, date_str: str) -> Path:
    NHNL_CACHE_DIR.mkdir(exist_ok=True)
    return NHNL_CACHE_DIR / f"nhnl_v2_{market}_{date_str}.csv"

def load_nhnl_cache(market: str, date_str: str) -> pd.DataFrame | None:
    p = _nhnl_cache_path(market, date_str)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, dtype={"date": str})
    except Exception:
        return None
    # 예전 잘못 생성된 짧은 캐시(예: 5주치)는 자동 무시
    if df.empty or len(df) < 20:
        return None
    return df

def save_nhnl_cache(df: pd.DataFrame, market: str, date_str: str):
    p = _nhnl_cache_path(market, date_str)
    df.to_csv(p, index=False)


def _is_common_stock_krx(df: pd.DataFrame) -> pd.Series:
    """
    책 취지에 맞게 보통주 중심으로 필터링한다.
    우선주는 이름/단축코드 패턴으로 최대한 제거한다.
    ETF/ETN/ELW/스팩/리츠/펀드/인버스/레버리지도 제외한다.
    """
    if df.empty:
        return pd.Series(dtype=bool)

    name_col = next((c for c in ["ISU_ABBRV", "ISU_NM", "Name", "name"] if c in df.columns), None)
    code_col = next((c for c in ["ISU_SRT_CD", "Code", "Symbol", "code"] if c in df.columns), None)

    name = df[name_col].astype(str).fillna("") if name_col else pd.Series([""] * len(df), index=df.index)
    code = df[code_col].astype(str).fillna("") if code_col else pd.Series([""] * len(df), index=df.index)

    exclude_pat = (
        r"(?:우$|우B$|우C$|[0-9]우$|스팩|리츠|REIT|ETF|ETN|ELW|KODEX|TIGER|KOSEF|KBSTAR|ARIRANG|HANARO|"
        r"SOL|ACE|TIMEFOLIO|TREX|SMART|FOCUS|마이티|TRUE|QV|RISE|레버리지|인버스|선물|채권|"
        r"펀드|액티브|TDF|TRF|BLN|회사채|국고채)"
    )
    bad_name = name.str.contains(exclude_pat, case=False, regex=True, na=False)

    # KRX 보통주 외의 특수코드/우선주/기타 증권 일부 제거 보조
    bad_code = code.str.endswith(("K", "L", "M", "N"))  # 예외적 코드 방어
    return ~(bad_name | bad_code)


def compute_nhnl_pykrx(market: str, end_date: str, prog=None, auth_key: str = "", chart_start_date: str | None = None) -> pd.DataFrame:
    """
    책 기준 NH-NL 구현:
    - 보통주 중심
    - 종가 기준
    - 52주(252거래일) 신고가/신저가 돌파 종목 수
    - 주간 합계(W-FRI)
    데이터 소스는 pykrx/FDR 대신 KRX 일별 전체종목 스냅샷 사용.
    """
    if not auth_key or not str(auth_key).strip():
        raise RuntimeError("NH-NL은 현재 KRX API AUTH_KEY 기반으로 계산합니다. 사이드바의 KRX AUTH_KEY를 입력하세요.")

    end_dt = pd.to_datetime(end_date, format="%Y%m%d")
    if chart_start_date:
        chart_start_dt = pd.to_datetime(chart_start_date, format="%Y%m%d")
        start_dt = chart_start_dt - timedelta(days=420)
    else:
        start_dt = end_dt - timedelta(days=800)
    dates = pd.bdate_range(start_dt, end_dt)
    session = requests.Session()

    daily_frames = []
    total = len(dates)
    for i, dt in enumerate(dates, 1):
        bas_dd = dt.strftime("%Y%m%d")
        try:
            raw = _fetch_daily(session, auth_key, bas_dd, market)
        except Exception:
            continue
        if raw is None or raw.empty:
            continue

        code_col = next((c for c in ["ISU_SRT_CD", "ISU_CD", "Code", "Symbol"] if c in raw.columns), None)
        name_col = next((c for c in ["ISU_ABBRV", "ISU_NM", "Name"] if c in raw.columns), None)
        close_col = next((c for c in ["TDD_CLSPRC", "Close", "close"] if c in raw.columns), None)

        if code_col is None or close_col is None:
            continue

        df = raw.copy()
        df["date"] = bas_dd
        df["code"] = df[code_col].astype(str).str.extract(r"(\d+)")[0].str.zfill(6)
        df["name"] = df[name_col].astype(str) if name_col else ""
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        df = df.dropna(subset=["code", "close"])
        df = df[_is_common_stock_krx(df)].copy()
        if not df.empty:
            daily_frames.append(df[["date", "code", "name", "close"]])

        if prog:
            prog.progress(i / total, text=f"NH-NL 계산용 KRX 수집 중… {bas_dd} ({i}/{total})")

    if not daily_frames:
        raise RuntimeError("NH-NL 계산용 KRX 일별 종목 데이터가 없습니다.")

    panel = pd.concat(daily_frames, ignore_index=True)
    panel["dt"] = pd.to_datetime(panel["date"], format="%Y%m%d")
    panel = panel.sort_values(["code", "dt"]).drop_duplicates(["code", "dt"], keep="last")

    # 종목별 거래일 수 기준으로 너무 짧은 히스토리는 제외
    valid_counts = panel.groupby("code")["dt"].size()
    valid_codes = valid_counts[valid_counts >= 260].index
    panel = panel[panel["code"].isin(valid_codes)].copy()
    if panel.empty:
        raise RuntimeError("52주 판정에 필요한 히스토리를 가진 종목이 없습니다.")

    def _mark_breakouts(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("dt").copy()
        prev_high = g["close"].rolling(252, min_periods=252).max().shift(1)
        prev_low = g["close"].rolling(252, min_periods=252).min().shift(1)
        g["new_high"] = ((g["close"] > prev_high) & prev_high.notna()).astype(int)
        g["new_low"] = ((g["close"] < prev_low) & prev_low.notna()).astype(int)
        return g[["dt", "new_high", "new_low"]]

    marked = panel.groupby("code", group_keys=False).apply(_mark_breakouts).reset_index(drop=True)
    daily = marked.groupby("dt", as_index=False)[["new_high", "new_low"]].sum()
    daily["nhnl"] = daily["new_high"] - daily["new_low"]

    weekly = daily.set_index("dt").resample("W-FRI").sum().reset_index()
    weekly = weekly.rename(columns={"new_high": "new_highs", "new_low": "new_lows"})
    weekly["date"] = weekly["dt"].dt.strftime("%Y%m%d")
    weekly = weekly[["date", "dt", "new_highs", "new_lows", "nhnl"]]
    weekly = weekly.sort_values("dt").reset_index(drop=True)
    cutoff_dt = start_dt + pd.Timedelta(days=365)
    weekly = weekly[weekly["dt"] >= cutoff_dt].reset_index(drop=True)
    if chart_start_date:
        chart_start_dt = pd.to_datetime(chart_start_date, format="%Y%m%d")
        weekly = weekly[weekly["dt"] >= chart_start_dt].reset_index(drop=True)

    # 너무 앞쪽 워밍업 구간 제거
    cutoff = pd.to_datetime(start_dt) + pd.Timedelta(days=365)
    weekly = weekly[weekly["dt"] >= cutoff].reset_index(drop=True)
    return weekly


def compute_nhnl_fdr(market: str, end_date: str, prog=None, auth_key: str = "") -> pd.DataFrame:
    return compute_nhnl_pykrx(market=market, end_date=end_date, prog=prog, auth_key=auth_key)
# ──────────────────────────────────────────────────────────────
# 파일 캐시 유틸
# ──────────────────────────────────────────────────────────────
def _cache_path(market: str, start: str, end: str, base: float) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    key = f"{market}_{start}_{end}_{int(base)}"
    return CACHE_DIR / f"{key}.csv"

def load_cache(market: str, start: str, end: str, base: float) -> pd.DataFrame | None:
    p = _cache_path(market, start, end, base)
    if p.exists():
        df = pd.read_csv(p, dtype={"date": str})
        return df
    return None

def save_cache(df: pd.DataFrame, market: str, start: str, end: str, base: float) -> None:
    p = _cache_path(market, start, end, base)
    df.to_csv(p, index=False)

def list_caches() -> list[Path]:
    CACHE_DIR.mkdir(exist_ok=True)
    return sorted(CACHE_DIR.glob("*.csv"))

# ──────────────────────────────────────────────────────────────
# KRX API
# ──────────────────────────────────────────────────────────────
def _krx_post(session, auth_key, endpoint, payload):
    url = API_BASE + endpoint
    headers = {"AUTH_KEY": auth_key.strip(), "Content-Type": "application/json",
                "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    r = session.post(url, headers=headers, json=payload, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"KRX {r.status_code}: {r.text[:200]}")
    data = r.json()
    if isinstance(data, dict) and data.get("respCode") not in (None, "000", 0, "0"):
        raise RuntimeError(f"KRX respCode {data.get('respCode')}: {data.get('respMsg')}")
    return data

def _fetch_daily(session, auth_key, bas_dd, market):
    data = _krx_post(session, auth_key, KRX_ENDPOINTS[market], {"basDd": bas_dd})
    rows = data.get("OutBlock_1", [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for c in ["TDD_CLSPRC", "CMPPREVDD_PRC", "FLUC_RT",
              "TDD_OPNPRC", "TDD_HGPRC", "TDD_LWPRC"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False), errors="coerce")
    return df.rename(columns={"BAS_DD": "Date", "CMPPREVDD_PRC": "PrevDiff", "FLUC_RT": "FlucRate"})

def _classify_breadth(df):
    if df.empty:
        return 0, 0, 0
    col = "PrevDiff" if "PrevDiff" in df.columns else "FlucRate"
    v = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return int((v > 0).sum()), int((v < 0).sum()), int((v == 0).sum())

def build_breadth(auth_key, start, end, market, base_value=50000.0):
    dates = pd.bdate_range(pd.to_datetime(start), pd.to_datetime(end))
    rows, ad_line = [], base_value
    session = requests.Session()
    prog = st.progress(0, text="KRX 브레드스 수집 중…")
    for i, dt in enumerate(dates, 1):
        bas_dd = dt.strftime("%Y%m%d")
        try:
            df = _fetch_daily(session, auth_key, bas_dd, market)
            if not df.empty:
                adv, decl, unch = _classify_breadth(df)
                ad_line += adv - decl
                rows.append({"date": bas_dd, "advances": adv, "declines": decl,
                             "unchanged": unch, "ad_diff": adv - decl, "ad_line": ad_line})
        except Exception as e:
            st.warning(f"{bas_dd} 스킵: {e}")
        prog.progress(i / len(dates), text=f"수집 중… {bas_dd} ({i}/{len(dates)})")
    prog.empty()
    if not rows:
        raise RuntimeError("수집된 데이터 없음")
    out = pd.DataFrame(rows)
    br = (out["advances"] / (out["advances"] + out["declines"]).replace(0, pd.NA)).astype(float)
    out["breadth_thrust_ema10"] = br.ewm(span=10, adjust=False).mean()
    return out

# ──────────────────────────────────────────────────────────────
# GitHub raw CSV 로드
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=1800)
def load_from_github(market: str) -> pd.DataFrame:
    """GitHub에 push된 CSV(breadth + index 머지)를 읽어 반환"""
    import requests as _req
    b_url = GITHUB_BREADTH[market]
    i_url = GITHUB_INDEX[market]

    resp_b = _req.get(b_url, timeout=15)
    if resp_b.status_code != 200:
        raise RuntimeError(f"GitHub breadth CSV 없음 ({resp_b.status_code})\n{b_url}\n→ 로컬에서 update_and_push.sh 실행 후 push 해주세요.")
    breadth = pd.read_csv(io.StringIO(resp_b.text), dtype={"date": str})

    resp_i = _req.get(i_url, timeout=15)
    if resp_i.status_code == 200:
        idx = pd.read_csv(io.StringIO(resp_i.text), dtype={"date": str})
        avail_cols = [c for c in ["date","open","high","low","close"] if c in idx.columns]
        df = breadth.merge(idx[avail_cols], on="date", how="left")
    else:
        # index CSV 없음 → breadth만 사용 (지수 그래프 없이 NH-NL만 표시)
        df = breadth.copy()

    df = df.sort_values("date").reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False, ttl=1800)
def load_nhnl_daily_from_github(market: str):
    """GitHub에 push된 NH-NL 일별 CSV를 읽어 반환 (없으면 None)"""
    import requests as _req
    if market not in GITHUB_NHNL_DAILY:
        return None
    url = GITHUB_NHNL_DAILY[market]
    try:
        resp = _req.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        df = pd.read_csv(io.StringIO(resp.text))
        df["date"] = df["date"].astype(int)
        df["dt"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")
        return df.sort_values("date").reset_index(drop=True)
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=1800)
def load_nhnl_from_github(market: str):
    """GitHub에 push된 NH-NL CSV를 읽어 반환 (없으면 None)"""
    import requests as _req
    if market not in GITHUB_NHNL:
        return None
    url = GITHUB_NHNL[market]
    try:
        resp = _req.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        df = pd.read_csv(io.StringIO(resp.text), dtype={"date": str})
        if df.empty:
            return None
        return df
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────
# 지수 OHLC
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_index_ohlc(market, start, end):
    if not FDR_OK:
        raise RuntimeError("finance-datareader 미설치")
    symbol = FDR_SYMBOLS[market]
    end_dt = datetime.strptime(end, "%Y%m%d") + timedelta(days=1)
    raw = fdr.DataReader(symbol, start, end_dt.strftime("%Y-%m-%d"))
    if raw.empty:
        raise RuntimeError(f"{symbol} 데이터 없음")
    raw.columns = [str(c).strip().title() for c in raw.columns]
    df = raw.reset_index()
    df.columns = [str(c).strip().title() for c in df.columns]
    date_col = next((c for c in df.columns if c.lower() in ("date", "datetime")), None)
    if not date_col:
        raise RuntimeError(f"날짜 컬럼 없음: {list(df.columns)}")
    def _find(*candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise RuntimeError(f"{candidates} 컬럼 없음: {list(df.columns)}")
    out = pd.DataFrame({
        "date":  pd.to_datetime(df[date_col]).dt.strftime("%Y%m%d"),
        "open":  pd.to_numeric(df[_find("Open")],  errors="coerce"),
        "high":  pd.to_numeric(df[_find("High")],  errors="coerce"),
        "low":   pd.to_numeric(df[_find("Low")],   errors="coerce"),
        "close": pd.to_numeric(df[_find("Close", "Adj Close")], errors="coerce"),
    })
    return out[out["date"] <= end].dropna().reset_index(drop=True)

# ──────────────────────────────────────────────────────────────
# 판정 로직
# ──────────────────────────────────────────────────────────────
def classify(price_off_high, ad_off_high, gap,
             price_off_low, ad_off_low,
             price_thr=2.0, ad_thr=3.0, gap_warn=1.5, gap_danger=2.5):
    # 직관적 부호: - = 고점 아래, + = 고점 위
    # gap = adOff - priceOff: + = A/D 선행(좋음), - = A/D 지연(나쁨)
    ph = price_off_high >= -price_thr
    ah = ad_off_high    >= -ad_thr
    pl = price_off_low  <= price_thr
    al = ad_off_low     <= ad_thr
    if ph and ah and gap >= -1.0:            return "BULLISH_CONFIRMATION"
    if ph and gap <= -gap_danger:            return "BULLISH_DIVERGENCE"
    if gap <= -gap_warn:                     return "BULLISH_DIVERGENCE_CANDIDATE"
    if gap < -1.0:                           return "RECOVERY_IN_PROGRESS"
    if pl and not al:                        return "DOWNSIDE_DIVERGENCE_CANDIDATE"
    if pl and al:                            return "NORMAL_WEAKNESS"
    return "NEUTRAL"

def compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger):
    closes   = df["close"].values.astype(float)
    ad_lines = df["ad_line"].values.astype(float)
    window   = closes[-lookback:]
    peak_idx      = window.argmax()
    days_ago      = lookback - 1 - peak_idx
    price_high    = window[peak_idx]
    ad_at_peak    = ad_lines[-(days_ago + 1)]
    price_low     = closes[-lookback:].min()
    ad_low        = ad_lines[-lookback:].min()
    last_close    = closes[-1]
    last_ad       = ad_lines[-1]

    # 직관적 부호: - = 아래, + = 위
    price_off = (last_close - price_high)  / abs(price_high)  * 100 if price_high  else float("nan")
    ad_off    = (last_ad    - ad_at_peak)  / abs(ad_at_peak)  * 100 if ad_at_peak  else float("nan")
    gap       = ad_off - price_off
    price_off_low = (last_close - price_low) / abs(price_low) * 100 if price_low else float("nan")
    ad_off_low    = (last_ad    - ad_low)    / abs(ad_low)    * 100 if ad_low    else float("nan")

    peak_date  = str(df["date"].iloc[-(days_ago + 1)])
    peak_label = "오늘" if days_ago == 0 else f"{days_ago}일전 ({peak_date})"
    status_key = classify(price_off, ad_off, gap, price_off_low, ad_off_low,
                          price_thr, ad_thr, gap_warn, gap_danger)
    verdict, note, color = STATUS_MAP[status_key]
    return dict(peak_label=peak_label, price_off=price_off, ad_off=ad_off, gap=gap,
                verdict=verdict, note=note, color=color,
                last_close=last_close, last_ad=last_ad,
                price_high=price_high, ad_at_peak=ad_at_peak)

# ──────────────────────────────────────────────────────────────
# H_a / H_b / L_a / L_b 계산 (파인스크립트 로직 그대로)
# ──────────────────────────────────────────────────────────────
def compute_hlab(df: pd.DataFrame, high_bars: int = 60, low_bars: int = 130) -> dict:
    """
    파인스크립트 v16과 동일한 로직:
    H_b = 최근 high_bars 구간 고점
    H_a = 그 이전 high_bars 구간 고점
    L_b = 최근 low_bars 구간 저점
    L_a = 그 이전 low_bars 구간 저점
    """
    closes  = df["close"].values.astype(float)
    ad_line = df["ad_line"].values.astype(float)
    dts     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")
    n = len(closes)

    def _safe_slice(arr, end_idx, length):
        start = max(0, end_idx - length)
        return arr[start:end_idx], start

    # ── Pine script v16 로직 그대로 ──────────────────────────────
    # H_b = ta.highest(close, highBars)         → 최근 high_bars 구간 고점
    # H_a = ta.highest(close[highBars], highBars) → highBars 이전부터 highBars 구간 고점
    # (H_b 위치와 무관하게 항상 현재에서 high_bars 이전 시점이 기준)

    # H_b: 최근 high_bars 구간 (현재 포함)
    hb_window, hb_start = _safe_slice(closes, n, high_bars)
    if len(hb_window) == 0:
        hb_window = closes; hb_start = 0
    hb_idx_local = int(np.argmax(hb_window))
    hb_idx = hb_start + hb_idx_local
    hb_val = closes[hb_idx]
    hb_dt  = dts.iloc[hb_idx]
    hb_ad  = ad_line[hb_idx]

    # H_a: 현재에서 high_bars 이전 시점을 기준으로 high_bars 길이 탐색
    # Pine: close[highBars] 시점부터 highBars 개 = 인덱스 (n - 2*high_bars) ~ (n - high_bars)
    ha_end   = max(0, n - high_bars)          # high_bars 이전 시점 (exclusive end)
    ha_window, ha_start = _safe_slice(closes, ha_end, high_bars)
    if len(ha_window) > 0:
        ha_idx_local = int(np.argmax(ha_window))
        ha_idx = ha_start + ha_idx_local
        ha_val = closes[ha_idx]
        ha_dt  = dts.iloc[ha_idx]
        ha_ad  = ad_line[ha_idx]
    else:
        ha_val, ha_dt, ha_ad, ha_idx = hb_val, hb_dt, hb_ad, hb_idx

    # L_b: 최근 low_bars 구간 (현재 포함)
    lb_window, lb_start = _safe_slice(closes, n, low_bars)
    if len(lb_window) == 0:
        lb_window = closes; lb_start = 0
    lb_idx_local = int(np.argmin(lb_window))
    lb_idx = lb_start + lb_idx_local
    lb_val = closes[lb_idx]
    lb_dt  = dts.iloc[lb_idx]
    lb_ad  = ad_line[lb_idx]

    # L_a: 현재에서 low_bars 이전 시점을 기준으로 low_bars 길이 탐색
    # Pine: close[lowBars] 시점부터 lowBars 개
    la_end   = max(0, n - low_bars)
    la_window, la_start = _safe_slice(closes, la_end, low_bars)
    if len(la_window) > 0:
        la_idx_local = int(np.argmin(la_window))
        la_idx = la_start + la_idx_local
        la_val = closes[la_idx]
        la_dt  = dts.iloc[la_idx]
        la_ad  = ad_line[la_idx]
    else:
        la_val, la_dt, la_ad, la_idx = lb_val, lb_dt, lb_ad, lb_idx

    # 불일치 판정
    bear_div     = bool(hb_val > ha_val and hb_ad < ha_ad)
    bear_div_pct = abs((ha_ad - hb_ad) / ha_ad * 100) if (bear_div and ha_ad != 0) else 0.0
    bull_div     = bool(lb_val < la_val and lb_ad > la_ad)
    bull_div_pct = abs((lb_ad - la_ad) / la_ad * 100) if (bull_div and la_ad != 0) else 0.0

    return dict(
        hb_val=hb_val, hb_dt=hb_dt, hb_ad=hb_ad,
        ha_val=ha_val, ha_dt=ha_dt, ha_ad=ha_ad,
        lb_val=lb_val, lb_dt=lb_dt, lb_ad=lb_ad,
        la_val=la_val, la_dt=la_dt, la_ad=la_ad,
        bear_div=bear_div, bear_div_pct=bear_div_pct,
        bull_div=bull_div, bull_div_pct=bull_div_pct,
    )

# ──────────────────────────────────────────────────────────────
# 차트 — domain 수동 분할 (make_subplots 미사용)
# 모든 trace가 xaxis="x" 하나를 공유 → 세로선이 전체 높이 관통
# yaxis(위 캔들) domain=[0.42,1.0], yaxis2(아래 A/D) domain=[0.0,0.38]
# yaxis2에 spikesnap="data" → A/D Line에 자석 가로선
# ──────────────────────────────────────────────────────────────
def make_plotly_chart(df: pd.DataFrame, market: str, sig: dict,
                      chart_months: int, hlab: dict) -> tuple[go.Figure, dict]:

    end_dt   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    mask     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt
    pf       = df[mask].copy().reset_index(drop=True)
    pf["dt"] = pd.to_datetime(pf["date"].astype(str), format="%Y%m%d")

    hb_color = "rgba(255,80,80,0.95)"  if hlab["bear_div"] else "rgba(160,160,160,0.8)"
    ha_color = "rgba(255,140,140,0.6)" if hlab["bear_div"] else "rgba(120,120,120,0.5)"
    lb_color = "rgba(38,210,160,0.95)" if hlab["bull_div"] else "rgba(160,160,160,0.8)"
    la_color = "rgba(38,210,160,0.6)"  if hlab["bull_div"] else "rgba(120,120,120,0.5)"

    price_low  = float(pf["low"].min())
    price_high = float(pf["high"].max())
    price_span = max(price_high - price_low, abs(price_high) * 0.02, 1.0)
    y1_range = [price_low - price_span * 0.08, price_high + price_span * 0.15]

    ad_vals = pf["ad_line"].astype(float)
    ad_min = float(ad_vals.min())
    ad_max = float(ad_vals.max())
    ad_span = max(ad_max - ad_min, max(abs(ad_max), 1.0) * 0.02, 1.0)
    y2_range = [ad_min - ad_span * 0.10, ad_max + ad_span * 0.10]

    # 파인스크립트 v16과 동일한 3단계 기준 (warnPct=0.5, dangerPct=2.0)
    _warn_pct   = 0.5
    _danger_pct = 2.0
    if hlab["bear_div"]:
        _p = hlab["bear_div_pct"]
        if _p >= _danger_pct:
            div_text  = f"🔴 부정적 불일치 (위험) {_p:.1f}%"
            div_color = "#c62828"
        elif _p >= _warn_pct:
            div_text  = f"🟠 부정적 불일치 (주의) {_p:.1f}%"
            div_color = "#ef6c00"
        else:
            div_text  = f"🟡 초기 부정적 불일치 {_p:.1f}%"
            div_color = "#f9a825"
    elif hlab["bull_div"]:
        _p = hlab["bull_div_pct"]
        if _p >= _warn_pct:
            div_text  = f"🟢 긍정적 불일치 (바닥 신호) {_p:.1f}%"
            div_color = "#26d2a0"
        else:
            div_text  = f"🔵 초기 긍정적 불일치 {_p:.1f}%"
            div_color = "#1e88e5"
    else:
        div_text, div_color = "불일치 없음", "#aaaaaa"

    fig = go.Figure()

    # ── 위 패널 캔들 (yaxis="y1", domain 0.42~1.0)
    fig.add_trace(go.Candlestick(
        x=pf["dt"], open=pf["open"], high=pf["high"], low=pf["low"], close=pf["close"],
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        name=market, showlegend=False,
        xaxis="x", yaxis="y1",
    ))

    # ── 아래 패널 A/D Line (yaxis="y2", domain 0.0~0.49)
    fig.add_trace(go.Scatter(
        x=pf["dt"], y=ad_vals,
        line=dict(color="#1e88e5", width=2.0), name="A/D Line",
        hoverinfo="y",
        xaxis="x", yaxis="y2",
    ))

    # ── 아래 패널: 가격을 A/D 스케일로 매핑 (Pine: priceMapped)
    _close = pf["close"].astype(float)
    _price_min = float(_close.min())
    _price_max = float(_close.max())
    _ad_min_pf = float(ad_vals.min())
    _ad_max_pf = float(ad_vals.max())
    if _price_max != _price_min:
        _price_mapped = _ad_min_pf + (_close - _price_min) / (_price_max - _price_min) * (_ad_max_pf - _ad_min_pf)
    else:
        _price_mapped = ad_vals
    fig.add_trace(go.Scatter(
        x=pf["dt"], y=_price_mapped,
        line=dict(color="rgba(180,180,180,0.5)", width=1.0), name="Price (scaled)",
        hoverinfo="skip",
        xaxis="x", yaxis="y2",
    ))

    # 위 패널 수평선 (yref="y1") — 레이블 왼쪽에 표시
    for val, color, dash, ann in [
        (hlab["hb_val"], hb_color, "dash", f"H_b {hlab['hb_val']:,.0f}"),
        (hlab["ha_val"], ha_color, "dot",  f"H_a {hlab['ha_val']:,.0f}"),
        (hlab["lb_val"], lb_color, "dash", f"L_b {hlab['lb_val']:,.0f}"),
        (hlab["la_val"], la_color, "dot",  f"L_a {hlab['la_val']:,.0f}"),
    ]:
        fig.add_shape(type="line", x0=pf["dt"].iloc[0], x1=pf["dt"].iloc[-1],
                      y0=val, y1=val, xref="x", yref="y1",
                      line=dict(color=color, dash=dash, width=1.2))
        fig.add_annotation(x=pf["dt"].iloc[0], y=val, xref="x", yref="y1",
                           text=ann, font=dict(color=color, size=10),
                           xanchor="right", showarrow=False)

    # 아래 패널 수평선 (yref="y2")
    for val, color, dash, ann in [
        (hlab["hb_ad"], hb_color, "dash", f"A/D H_b {hlab['hb_ad']:,.0f}"),
        (hlab["ha_ad"], ha_color, "dot",  f"A/D H_a {hlab['ha_ad']:,.0f}"),
        (hlab["lb_ad"], lb_color, "dash", f"A/D L_b {hlab['lb_ad']:,.0f}"),
        (hlab["la_ad"], la_color, "dot",  f"A/D L_a {hlab['la_ad']:,.0f}"),
    ]:
        fig.add_shape(type="line", x0=pf["dt"].iloc[0], x1=pf["dt"].iloc[-1],
                      y0=val, y1=val, xref="x", yref="y2",
                      line=dict(color=color, dash=dash, width=1.0))
        fig.add_annotation(x=pf["dt"].iloc[0], y=val, xref="x", yref="y2",
                           text=ann, font=dict(color=color, size=9),
                           xanchor="right", showarrow=False)

    # ── Pine 라벨: H_a/H_b/L_a/L_b 포인트에 라벨 + 연결 대시선
    # H_a 라벨 (아래 패널 A/D 위치)
    fig.add_annotation(
        x=hlab["ha_dt"], y=hlab["ha_ad"], xref="x", yref="y2",
        text=f"H_a<br>{hlab['ha_val']:,.0f}",
        showarrow=True, arrowhead=2, ax=0, ay=-25,
        font=dict(color=ha_color, size=10),
        bgcolor="rgba(60,60,60,0.8)", bordercolor=ha_color, borderwidth=1,
    )
    fig.add_annotation(
        x=hlab["hb_dt"], y=hlab["hb_ad"], xref="x", yref="y2",
        text=f"H_b<br>{hlab['hb_val']:,.0f}",
        showarrow=True, arrowhead=2, ax=0, ay=-25,
        font=dict(color=hb_color, size=10),
        bgcolor="rgba(60,60,60,0.8)", bordercolor=hb_color, borderwidth=1,
    )
    fig.add_annotation(
        x=hlab["la_dt"], y=hlab["la_ad"], xref="x", yref="y2",
        text=f"L_a<br>{hlab['la_val']:,.0f}",
        showarrow=True, arrowhead=2, ax=0, ay=25,
        font=dict(color=la_color, size=10),
        bgcolor="rgba(60,60,60,0.8)", bordercolor=la_color, borderwidth=1,
    )
    fig.add_annotation(
        x=hlab["lb_dt"], y=hlab["lb_ad"], xref="x", yref="y2",
        text=f"L_b<br>{hlab['lb_val']:,.0f}",
        showarrow=True, arrowhead=2, ax=0, ay=25,
        font=dict(color=lb_color, size=10),
        bgcolor="rgba(60,60,60,0.8)", bordercolor=lb_color, borderwidth=1,
    )
    # 연결 대시선 (항상 표시, 불일치 시 강조색)
    fig.add_shape(type="line",
        x0=hlab["ha_dt"], y0=hlab["ha_ad"], x1=hlab["hb_dt"], y1=hlab["hb_ad"],
        xref="x", yref="y2",
        line=dict(color=hb_color, width=2, dash="dash"))
    fig.add_shape(type="line",
        x0=hlab["la_dt"], y0=hlab["la_ad"], x1=hlab["lb_dt"], y1=hlab["lb_ad"],
        xref="x", yref="y2",
        line=dict(color=lb_color, width=2, dash="dash"))

    # ── Pine: 맨 오른쪽 끝 판정 라벨
    _last_dt  = pf["dt"].iloc[-1]
    _last_ad  = float(ad_vals.iloc[-1])
    fig.add_annotation(
        x=_last_dt, y=_last_ad, xref="x", yref="y2",
        text=f"{div_text}",
        showarrow=False, xanchor="left",
        font=dict(color="white", size=11),
        bgcolor=div_color, bordercolor=div_color, borderwidth=1,
        xshift=8,
    )

    # A/D 데이터 lookup: ISO 날짜문자열 → float (JS 자석선에 사용)
    ad_lookup = {
        dt.strftime("%Y-%m-%d"): float(v)
        for dt, v in zip(pf["dt"], ad_vals)
    }

    fig.update_layout(
        template="plotly_dark", height=660,
        title=dict(text=f"{market} — {div_text}", font=dict(size=14, color=div_color)),
        # hovermode="x": 같은 x의 모든 trace에 동시 hover → y2 spike도 위 패널 hover로 트리거됨
        hovermode="x",
        hoverlabel=dict(bgcolor="#1e1e2e", font_color="#ffffff", font_size=12, bordercolor="#555"),
        legend=dict(orientation="h", y=1.01, x=0),
        margin=dict(l=10, r=90, t=55, b=35),
        xaxis=dict(
            domain=[0, 1],
            rangeslider=dict(visible=False),
            showspikes=True, spikemode="across", spikesnap="cursor",
            spikethickness=1, spikecolor="rgba(200,200,200,0.7)", spikedash="solid",
            tickformat="%Y/%m/%d", tickangle=-45, tickfont=dict(size=11),
            showline=True, mirror=True,
        ),
        yaxis=dict(
            title="지수", domain=[0.50, 1.0], range=y1_range,
            showspikes=True, spikemode="across", spikesnap="cursor",
            spikethickness=1, spikecolor="rgba(200,200,200,0.4)", spikedash="solid",
            showline=True, mirror=True,
        ),
        yaxis2=dict(
            title="A/D Line", domain=[0.0, 0.49], range=y2_range,
            showspikes=True, spikemode="across", spikesnap="data",
            spikethickness=2, spikecolor="rgba(255,255,255,1.0)", spikedash="solid",
            anchor="x",
        ),
    )
    return fig, ad_lookup

# ──────────────────────────────────────────────────────────────
# 메인 앱
# ──────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="국장 브레드스 대시보드",
                       page_icon="📊", layout="wide")
    st.title("📊 국장 A/D Line 브레드스 대시보드")
    st.caption("KRX 상승·하락 종목 수 기반 / 스탠 와인스태인 브레드스 분석")

    # ── 사이드바 ──────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ 설정")
        market = st.selectbox("마켓", ["KOSPI", "KOSDAQ"])

        mode = st.radio("데이터 소스", ["☁️ GitHub (빠름)", "🔑 KRX API (직접 수집)"],
                        index=0,
                        help="GitHub: Actions가 매일 자동 업데이트한 CSV 사용\nKRX API: 직접 수집 (AUTH_KEY 필요)")

        if mode == "🔑 KRX API (직접 수집)":
            auth_key = st.text_input("KRX AUTH_KEY",
                                     value=os.environ.get("KRX_AUTH_KEY", ""),
                                     type="password")
            c1, c2 = st.columns(2)
            today = datetime.today()
            start_dt = c1.date_input("시작일", value=today - timedelta(days=730))
            end_dt   = c2.date_input("종료일", value=today)
            base_value = st.number_input("A/D Line 시작값", value=50000.0, step=1000.0)
        else:
            auth_key = ""
            today = datetime.today()
            start_dt = today - timedelta(days=730)
            end_dt   = today

        fetch_btn = st.button("🔄 데이터 불러오기", type="primary", width='stretch')
        if mode == "🔑 KRX API (직접 수집)":
            st.caption("💡 새로 불러오고 싶으면 아래 캐시를 지우고 불러오세요.")

        st.divider()
        st.subheader("분석 파라미터")
        lookback     = st.slider("Lookback (일)",      20, 252, 126)
        chart_months = st.slider("차트 표시 기간 (월)", 1,  24,  6)
        high_bars    = st.slider("고점 탐색 구간 H_b (일)", 10, 500, 30)
        low_bars     = st.slider("저점 탐색 구간 L_b (일)", 10, 500, 30)
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0,  step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0,  step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5,  step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5,  step=0.1)

        st.divider()
        st.subheader("💾 저장된 캐시")
        caches = list_caches()
        if caches:
            for p in caches:
                col_a, col_b = st.columns([3, 1])
                col_a.caption(p.name)
                if col_b.button("🗑", key=str(p)):
                    p.unlink()
                    st.rerun()
        else:
            st.caption("저장된 캐시 없음")

    # ── 데이터 불러오기 ──────────────────────────────
    if not fetch_btn and "df_merged" not in st.session_state:
        st.info("👈 사이드바에서 마켓 선택 후 **데이터 불러오기** 버튼을 눌러주세요.")
        return

    if fetch_btn:
        st.session_state.pop(f"nhnl_{market}", None)
        if mode == "☁️ GitHub (빠름)":
            try:
                with st.spinner("GitHub에서 CSV 읽는 중…"):
                    df = load_from_github(market)
                    nhnl_df = load_nhnl_from_github(market)
                    nhnl_daily_df = load_nhnl_daily_from_github(market)
                st.success(f"✅ GitHub 로드 완료 — {len(df)}일치 / 최신: {df['date'].iloc[-1]}")
                st.session_state[f"nhnl_{market}"] = nhnl_df if nhnl_df is not None and not nhnl_df.empty else None
                st.session_state[f"nhnl_daily_{market}"] = nhnl_daily_df if nhnl_daily_df is not None and not nhnl_daily_df.empty else None
                if nhnl_df is None or nhnl_df.empty:
                    st.info("GitHub 빠른 모드에서는 저장된 NH-NL CSV가 있을 때만 NH-NL 탭을 표시합니다.")
            except Exception as e:
                st.error(f"GitHub 로드 실패: {e}")
                return
        else:
            if not auth_key:
                st.error("KRX AUTH_KEY를 입력해주세요.")
                return
            start_str = start_dt.strftime("%Y%m%d")
            end_str   = end_dt.strftime("%Y%m%d")
            cached = load_cache(market, start_str, end_str, 50000.0)
            nhnl_cached = load_nhnl_cache(market, end_str)
            try:
                if cached is not None:
                    st.success(f"✅ 캐시에서 로드 ({market} {start_str}~{end_str})")
                    df = cached
                else:
                    with st.spinner("지수 OHLC 수집 중…"):
                        index_df = fetch_index_ohlc(market, start_str, end_str)
                    breadth_df = build_breadth(auth_key, start_str, end_str, market, 50000.0)
                    df = breadth_df.merge(
                        index_df[["date","open","high","low","close"]],
                        on="date", how="inner"
                    ).sort_values("date").reset_index(drop=True)
                    save_cache(df, market, start_str, end_str, 50000.0)
                    st.success(f"✅ A/D 데이터 수집 완료 — {len(df)}일치")

                if nhnl_cached is not None and not nhnl_cached.empty:
                    nhnl_df = nhnl_cached
                    st.success(f"✅ NH-NL 캐시 로드 — {len(nhnl_df)}주치")
                else:
                    prog3 = st.progress(0, text="NH-NL 계산용 KRX 수집 중…")
                    nhnl_df = compute_nhnl_pykrx(
                        market,
                        end_str,
                        prog=prog3,
                        auth_key=auth_key,
                        chart_start_date=start_str,
                    )
                    prog3.empty()
                    if nhnl_df is not None and not nhnl_df.empty:
                        save_nhnl_cache(nhnl_df, market, end_str)
                        st.success(f"✅ NH-NL 계산 완료 — {len(nhnl_df)}주치")
                st.session_state[f"nhnl_{market}"] = nhnl_df if nhnl_df is not None and not nhnl_df.empty else None
            except Exception as e:
                st.error(f"데이터 수집 실패: {type(e).__name__}: {e}")
                return

        st.session_state["df_merged"] = df
        st.session_state["df_market"] = market

    # 마켓이 바뀌면 세션 초기화
    if st.session_state.get("df_market") != market:
        st.session_state.pop("df_merged", None)
        st.info("마켓이 변경됐어요. 데이터 불러오기를 다시 눌러주세요.")
        return

    # ── 차트 및 판정 출력 ───────────────────────────
    df = st.session_state["df_merged"]

    if len(df) < lookback:
        st.warning(f"데이터 부족: {len(df)}행 (lookback={lookback})")
        return

    sig  = compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger)
    hlab = compute_hlab(df, high_bars=high_bars, low_bars=low_bars)
    last = df.iloc[-1]

    # ── 탭 구성 ──
    # st.tabs 는 서버측에서 active tab을 제어/유지할 수 없어서
    # 버튼 클릭 시 rerun 되면 첫 탭으로 돌아가 보일 수 있음.
    TAB_LABELS = ["📈 A/D Line", "⚡ 모멘텀", "🏔 NH-NL"]
    if "active_tab" not in st.session_state:
        st.session_state["active_tab"] = TAB_LABELS[0]

    _default_idx = TAB_LABELS.index(st.session_state.get("active_tab", TAB_LABELS[0]))
    if hasattr(st, "segmented_control"):
        active_tab = st.segmented_control(
            "분석 탭",
            TAB_LABELS,
            selection_mode="single",
            default=TAB_LABELS[_default_idx],
            key="active_tab_selector",
        )
    else:
        active_tab = st.radio(
            "분석 탭",
            TAB_LABELS,
            index=_default_idx,
            horizontal=True,
            key="active_tab_selector",
        )
    st.session_state["active_tab"] = active_tab

    # ══════════════════════════════════════════════
    # TAB 1: 기존 A/D Line 분석
    # ══════════════════════════════════════════════
    if active_tab == "📈 A/D Line":
        # ── Pine 테이블 그대로 재현 ──────────────────────────────
        _ha_date = hlab["ha_dt"].strftime("%-m/%-d") if hasattr(hlab["ha_dt"], "strftime") else str(hlab["ha_dt"])
        _hb_date = hlab["hb_dt"].strftime("%-m/%-d") if hasattr(hlab["hb_dt"], "strftime") else str(hlab["hb_dt"])
        _la_date = hlab["la_dt"].strftime("%-m/%-d") if hasattr(hlab["la_dt"], "strftime") else str(hlab["la_dt"])
        _lb_date = hlab["lb_dt"].strftime("%-m/%-d") if hasattr(hlab["lb_dt"], "strftime") else str(hlab["lb_dt"])

        # 판정 (Pine 로직 동일)
        _bear = hlab["bear_div"]
        _bull = hlab["bull_div"]
        _bdp  = hlab["bear_div_pct"]
        _bup  = hlab["bull_div_pct"]
        if _bear and _bdp >= 2.0:
            _status = "🔴 부정적 불일치 (위험)"
            _note   = f"H_b 신고점 / A/D {_bdp:.2f}% 뒤처짐"
            _scolor = "#c62828"
        elif _bear and _bdp >= 0.5:
            _status = "🟠 부정적 불일치 (주의)"
            _note   = f"H_b 신고점 / A/D {_bdp:.2f}% 뒤처짐"
            _scolor = "#ef6c00"
        elif _bear:
            _status = "🟡 초기 부정적 불일치"
            _note   = "H_b 신고점 / A/D 소폭 뒤처짐"
            _scolor = "#f9a825"
        elif _bull and _bup >= 0.5:
            _status = "🟢 긍정적 불일치 (바닥 신호)"
            _note   = f"L_b 신저점 / A/D {_bup:.2f}% 더 올라옴"
            _scolor = "#26d2a0"
        elif _bull:
            _status = "🔵 초기 긍정적 불일치"
            _note   = "L_b 신저점 / A/D 소폭 상승"
            _scolor = "#1565c0"
        else:
            _status = "중립"
            _note   = "불일치 없음"
            _scolor = "#757575"

        # 상단 판정 배너
        st.markdown(
            f'<div style="background:{_scolor};padding:12px 18px;border-radius:8px;margin:4px 0 8px 0">'
            f'<b style="font-size:1.2em;color:white">{_status}</b>'
            f'&nbsp;&nbsp;<span style="color:#ffffffcc">{_note}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # 상단 metrics: 최근날짜 / 종가 / 오늘 AD차이
        c1, c2, c3 = st.columns(3)
        c1.metric("최근 날짜", pd.to_datetime(str(last["date"]), format="%Y%m%d").strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가", f"{float(last['close']):,.2f}")
        c3.metric("오늘 A/D 차이", f"{int(last['ad_diff']):+,}")

        try:
            fig_main, ad_lookup = make_plotly_chart(df, market, sig, chart_months, hlab)

            # ── A/D 자석 가로선: Plotly HTML export + JS 내장 방식 ──────────
            # st.plotly_chart 대신 fig를 HTML로 export한 후 st.components로 렌더링.
            # 같은 iframe 안에 Plotly JS가 있어서 window.parent 없이 직접 이벤트 접근.
            import plotly.io as _pio
            _ad_json = json.dumps(ad_lookup)
            _fig_html = _pio.to_html(
                fig_main,
                full_html=False,
                include_plotlyjs="cdn",
                div_id="ad_main_chart",
                config={"responsive": True, "displayModeBar": False},
            )
            _magnet_js = f"""
<script>
(function() {{
  const adData = {_ad_json};

  function toDateKey(xVal) {{
    if (typeof xVal === 'number') {{
      const d = new Date(xVal);
      return d.getFullYear() + '-'
        + String(d.getMonth()+1).padStart(2,'0') + '-'
        + String(d.getDate()).padStart(2,'0');
    }}
    return String(xVal).substring(0, 10);
  }}

  function init() {{
    const gd = document.getElementById('ad_main_chart');
    if (!gd || !gd._fullLayout) {{ setTimeout(init, 300); return; }}

    gd.on('plotly_hover', function(data) {{
      if (!data || !data.points || !data.points.length) return;
      const dateKey = toDateKey(data.points[0].x);
      const adVal = adData[dateKey];
      if (adVal === undefined) return;
      const shapes = (gd.layout.shapes || []).filter(s => s.name !== '_ad_magnet');
      shapes.push({{
        name: '_ad_magnet',
        type: 'line',
        xref: 'paper', x0: 0, x1: 1,
        yref: 'y2', y0: adVal, y1: adVal,
        line: {{ color: 'rgba(255,255,255,0.95)', width: 2, dash: 'solid' }},
      }});
      Plotly.relayout(gd, {{ shapes: shapes }});
    }});

    gd.on('plotly_unhover', function() {{
      const shapes = (gd.layout.shapes || []).filter(s => s.name !== '_ad_magnet');
      Plotly.relayout(gd, {{ shapes: shapes }});
    }});
  }}

  // Plotly CDN 로드 완료 후 실행
  if (typeof Plotly !== 'undefined') {{
    setTimeout(init, 200);
  }} else {{
    document.addEventListener('plotly_loaded', function() {{ setTimeout(init, 200); }});
    setTimeout(init, 1500);  // fallback
  }}
}})();
</script>
"""
            _full_html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ margin: 0; padding: 0; background: transparent; }}
    #ad_main_chart {{ width: 100%; }}
  </style>
</head>
<body>
{_fig_html}
{_magnet_js}
</body>
</html>
"""
            import streamlit.components.v1 as _stc
            _stc.html(_full_html, height=690, scrolling=False)

        except Exception as e:
            st.error(f"차트 렌더링 실패: {e}")

        # Pine 테이블 재현: 차트 아래
        st.markdown("---")
        _ad_hb_flag = "  ⚠" if _bear else "  ✓"
        _ad_lb_flag = "  ⚠" if _bull else "  ✓"
        col_h, col_l = st.columns(2)
        with col_h:
            st.markdown(f"""
| 항목 | 값 |
|---|---|
| H_a 예전 고점 ({_ha_date}) | {hlab['ha_val']:,.2f} |
| A/D @ H_a | {hlab['ha_ad']:,.0f} |
| H_b 최근 고점 ({_hb_date}) | {hlab['hb_val']:,.2f} |
| A/D @ H_b | {hlab['hb_ad']:,.0f}{_ad_hb_flag} |
| A/D 괴리 % | {_bdp:.2f}% |
| 판정 | {_status} |
""")
        with col_l:
            st.markdown(f"""
| 항목 | 값 |
|---|---|
| L_a 예전 저점 ({_la_date}) | {hlab['la_val']:,.2f} |
| A/D @ L_a | {hlab['la_ad']:,.0f} |
| L_b 최근 저점 ({_lb_date}) | {hlab['lb_val']:,.2f} |
| A/D @ L_b | {hlab['lb_ad']:,.0f}{_ad_lb_flag} |
| A/D 괴리 % | {_bup:.2f}% |
| 판정 | {_status} |
""")
        st.markdown("---")

        with st.expander("📋 원시 데이터 보기"):
            show = df.copy()
            show["date"] = pd.to_datetime(show["date"].astype(str), format="%Y%m%d").dt.strftime("%Y-%m-%d")
            cols = [c for c in ["date","advances","declines","unchanged",
                      "ad_diff","ad_line","close","breadth_thrust_ema10"] if c in show.columns]
            st.dataframe(
                show[cols].sort_values("date", ascending=False).reset_index(drop=True),
                width='stretch',
            )
            csv = show.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("📥 CSV 다운로드", csv,
                               f"{market}_breadth.csv", "text/csv")

    # ══════════════════════════════════════════════
    # TAB 2: MI 탄력지수 (스탠 와인스태인 책 정의)
    # ══════════════════════════════════════════════
    elif active_tab == "⚡ 모멘텀":
        st.subheader("⚡ MI 탄력지수 (Momentum Index)")
        st.caption(
            "스탠 와인스태인 책 정의: 등락종목수 차이(AD)의 200일 롤링 평균. "
            "0선 위 = 시장 강세, 0선 아래 = 시장 약세."
        )

        mi_window = st.slider("MA 기간 (기본 200일)", 50, 300, 200, step=10, key="mi_win")

        end_dt2   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
        start_dt2 = end_dt2 - pd.DateOffset(months=chart_months)
        mask2 = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt2
        pf2   = df[mask2].copy().reset_index(drop=True)
        pf2["dt"] = pd.to_datetime(pf2["date"].astype(str), format="%Y%m%d")

        ad_diff_s  = pd.Series(df["ad_diff"].values.astype(float))
        mi_full    = ad_diff_s.rolling(mi_window).mean()   # 책 정의: N일 단순 롤링 평균

        mi_plot    = mi_full.iloc[mask2.values].reset_index(drop=True)

        last_mi    = mi_full.iloc[-1]
        prev_mi    = mi_full.iloc[-2] if len(mi_full) >= 2 else last_mi
        if pd.isna(last_mi):
            mi_verdict = "⚪ 데이터 부족"
            mi_color   = "#757575"
        elif last_mi > 0 and last_mi > prev_mi:
            mi_verdict = "🟢 강세 상승"
            mi_color   = "#2e7d32"
        elif last_mi > 0:
            mi_verdict = "🟡 강세 둔화"
            mi_color   = "#f9a825"
        elif last_mi < 0 and last_mi < prev_mi:
            mi_verdict = "🔴 약세 하락"
            mi_color   = "#c62828"
        else:
            mi_verdict = "🟠 약세 회복 중"
            mi_color   = "#ef6c00"

        m1, m2, m3 = st.columns(3)
        m1.metric(f"MI ({mi_window}일 평균)", f"{last_mi:+.1f}" if not pd.isna(last_mi) else "N/A")
        m2.metric("전일 대비", f"{(last_mi - prev_mi):+.1f}" if not pd.isna(last_mi) else "N/A")
        m3.metric("판정", mi_verdict)

        fig_mi = go.Figure()
        fig_mi.add_trace(go.Bar(
            x=pf2["dt"], y=mi_plot,
            marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in mi_plot.fillna(0)],
            name=f"MI ({mi_window}일 평균)", opacity=0.85
        ))
        fig_mi.add_hline(y=0, line_color="gray", line_dash="dot",
                         annotation_text="기준선(0)")
        fig_mi.update_layout(
            title=f"{market} MI 탄력지수 — AD차이 {mi_window}일 롤링 평균 (스탠 와인스태인)",
            template="plotly_dark", height=420,
            legend=dict(orientation="h", y=1.05),
            yaxis_title="MI 값 (AD 평균)"
        )
        st.plotly_chart(fig_mi, width='stretch')

        if len(df) < mi_window:
            st.warning(f"⚠️ 데이터 {len(df)}일 — {mi_window}일 MA 계산에 데이터가 부족합니다. "
                       f"수집 기간을 늘리거나 MA 기간을 줄여주세요.")

    # ══════════════════════════════════════════════
    # TAB 3: NH-NL
    # ══════════════════════════════════════════════
    elif active_tab == "🏔 NH-NL":
        st.subheader("🏔 고점-저점 수치 (신고가 - 신저가 종목 수)")
        st.caption(
            "스탠 와인스태인 책 정의: 매주 신고가 기록 종목 수 - 신저가 기록 종목 수. "
            "KRX API 일별 전체 종목 스냅샷으로 52주 신고가/신저가를 판별해 주간 집계합니다."
        )

        nhnl_df = st.session_state.get(f"nhnl_{market}")
        if nhnl_df is None or nhnl_df.empty:
            if mode == "☁️ GitHub (빠름)":
                st.info("GitHub 빠른 모드에서는 저장된 NH-NL CSV가 있을 때만 NH-NL을 표시합니다. 데이터 불러오기 시 함께 로드됩니다.")
            else:
                st.info("KRX 직접 수집 모드에서는 '데이터 불러오기'를 누를 때 NH-NL도 함께 계산합니다.")
        if nhnl_df is not None and not nhnl_df.empty:
            from plotly.subplots import make_subplots as _msp2
            nhnl_df["dt"] = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d")
            _today_ts = pd.Timestamp(datetime.today().date())
            # W-FRI 집계 시 이번주 금요일 날짜로 찍힘 → 오늘+7일까지 허용 (미래 공백 방지는 x축 range로 처리)
            end_dt3   = nhnl_df["dt"].max()
            start_dt3 = end_dt3 - pd.DateOffset(months=chart_months)
            pf3       = nhnl_df[(nhnl_df["dt"] >= start_dt3) & (nhnl_df["dt"] <= end_dt3)].copy().reset_index(drop=True)

            # 4주 MA 전체 기준 계산
            ns_all   = pd.Series(nhnl_df["nhnl"].values.astype(float))
            nma_all  = ns_all.rolling(4).mean()
            nma_plot = nma_all.iloc[(nhnl_df["dt"] >= start_dt3).values].reset_index(drop=True)

            # ── 최신 NH-NL: nhnl_daily_df 우선, 없으면 주간 CSV 마지막행 ──
            _nhnl_daily_local = st.session_state.get(f"nhnl_daily_{market}")
            _today_date_int   = int(datetime.today().strftime("%Y%m%d"))
            _nh_label = ""  # 출처 표시용

            if _nhnl_daily_local is not None and not _nhnl_daily_local.empty:
                # 오늘 또는 가장 최근 거래일 행 사용
                _daily_sorted = _nhnl_daily_local.sort_values("date")
                _last_daily   = _daily_sorted.iloc[-1]
                last_nhnl = int(_last_daily["nhnl"])
                last_nh   = int(_last_daily["new_highs"])
                last_nl   = int(_last_daily["new_lows"])
                _nh_label = f"일별 ({str(int(_last_daily['date']))[4:6]}/{str(int(_last_daily['date']))[6:8]})"
            else:
                # 주간 CSV 마지막행 — W-FRI 집계값
                last_nhnl = int(ns_all.iloc[-1])
                last_nh   = int(nhnl_df["new_highs"].iloc[-1])
                last_nl   = int(nhnl_df["new_lows"].iloc[-1])
                _last_wk_date = nhnl_df["dt"].iloc[-1]
                _nh_label = f"주간({_last_wk_date.strftime('%m/%d')} 집계)"

            # 판정: 4주 MA 기울기 (초기값 — 아래 보정에서 덮어씀)
            lma = nma_all.iloc[-1]; pma = nma_all.iloc[-2] if len(nma_all) >= 2 else lma
            nhnl_ma_vals = nma_all.dropna()
            slope = np.polyfit(np.arange(len(nhnl_ma_vals)), nhnl_ma_vals.values, 1)[0] if len(nhnl_ma_vals) >= 2 else 0.0
            if pd.isna(lma):            nhnl_verdict, trend_color = "⚪ 부족",   "#757575"
            elif lma > 0 and lma > pma: nhnl_verdict, trend_color = "🟢 강세",   "#2e7d32"
            elif lma > 0:               nhnl_verdict, trend_color = "🟡 둔화",   "#f9a825"
            elif lma < 0 and lma < pma: nhnl_verdict, trend_color = "🔴 약세",   "#c62828"
            else:                       nhnl_verdict, trend_color = "🟠 회복중", "#ef6c00"

            # 마지막 수집 주 날짜 — W-FRI 레이블은 해당 주 금요일이지만
            # 실제 수집 시점은 그 이전일 수 있으므로 오늘 기준으로 보정
            _last_data_dt = nhnl_df["dt"].max()
            _today_ts2 = pd.Timestamp(datetime.today().date())
            # 실제 수집된 마지막 거래일 = min(W-FRI 날짜, 오늘)
            _actual_last = min(_last_data_dt, _today_ts2)
            # 해당 주 월요일
            _actual_mon = _actual_last - pd.Timedelta(days=_actual_last.weekday())
            _last_data_str = f"{_actual_mon.strftime('%Y/%m/%d')} ~ {_actual_last.strftime('%Y/%m/%d')}"
            if _nhnl_daily_local is not None and not _nhnl_daily_local.empty:
                _last_daily_dt = pd.to_datetime(str(int(_nhnl_daily_local.sort_values("date")["date"].iloc[-1])), format="%Y%m%d")
                _last_daily_str = _last_daily_dt.strftime("%Y/%m/%d")
            else:
                _last_daily_str = "일별 데이터 없음"
            st.markdown(
                f"📅 최종 수집 주: **{_last_data_str}** (주간 집계)  \
"
                f"📆 최종 수집 일: **{_last_daily_str}** (일별 NH-NL) | NH-NL 출처: **{_nh_label}**"
            )

            h1, h2, h3, h4 = st.columns(4)
            h1.metric("신고가 종목 수", f"{last_nh:,}", help=f"출처: {_nh_label}")
            h2.metric("신저가 종목 수", f"{last_nl:,}", help=f"출처: {_nh_label}")
            h3.metric("NH-NL",          f"{last_nhnl:+,}", help=f"출처: {_nh_label}")

            # 일간 판정: 최근 일별 NH-NL 전일 대비
            _daily_verdict = "일별 데이터 없음"
            if _nhnl_daily_local is not None and len(_nhnl_daily_local) >= 2:
                _daily_sorted2 = _nhnl_daily_local.sort_values("date")
                _d_last = int(_daily_sorted2["nhnl"].iloc[-1])
                _d_prev = int(_daily_sorted2["nhnl"].iloc[-2])
                _daily_verdict = "⚠️ 일간 브레드스↓" if _d_last < _d_prev else "🟢 일간 개선/유지"
            st.caption(f"📌 일간 판정: **{_daily_verdict}** | 주간 판정은 아래 추세선 기준")

            # 지수 같은 기간
            _has_index = all(c in df.columns for c in ["close", "high", "low"])
            pf_idx3 = df[pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt3].copy()
            pf_idx3["dt"] = pd.to_datetime(pf_idx3["date"].astype(str), format="%Y%m%d")
            if _has_index:
                pf_idx3 = pf_idx3.dropna(subset=["close"])

            # 판정 보정: 지수 방향 vs NH-NL 방향 + Pine 기준 ±200 임계값
            # Pine script 기준: |nhnl| > 200 = 강한 신호, 0~200 = 보통
            _STRONG = 200  # Pine script 임계값
            _idx_recent = pf_idx3.tail(20)
            _idx_up = (_has_index and len(_idx_recent) >= 2 and
                       float(_idx_recent["close"].iloc[-1]) > float(_idx_recent["close"].iloc[0]))
            _nhnl_up = (len(nhnl_df) >= 2 and
                        float(nhnl_df["nhnl"].iloc[-1]) >= float(nhnl_df["nhnl"].iloc[-2]))
            _strong_bull = last_nhnl > _STRONG   # Pine: nhnl > 200
            _strong_bear = last_nhnl < -_STRONG  # Pine: nhnl < -200

            # 주간 판정: KOSDAQ은 수동 상승추세선 기준으로 우선 판정
            _weekly_trend_desc = ""
            if market == "KOSDAQ" and _has_index and not pf_idx3.empty:
                def _trend_nearest_close(_target_dt):
                    _center = pd.Timestamp(_target_dt)
                    _diffs = (pf_idx3["dt"] - _center).abs()
                    _i = _diffs.argmin()
                    return pf_idx3["dt"].iloc[_i], float(pf_idx3["close"].iloc[_i])

                def _line_value_at(_dt_a, _y_a, _dt_b, _y_b, _target_dt):
                    _days = max((_dt_b - _dt_a).days, 1)
                    _slope = (_y_b - _y_a) / _days
                    return _y_a + _slope * max((pd.Timestamp(_target_dt) - _dt_a).days, 0)

                # 1차: 03/06→04/03, 2차: 04/10→04/24. 최신 주간 판정은 2차선을 우선 사용.
                _ta1, _ya1 = _trend_nearest_close("2026-03-06")
                _tb1, _yb1 = _trend_nearest_close("2026-04-03")
                _ta2, _ya2 = _trend_nearest_close("2026-04-10")
                _tb2, _yb2 = _trend_nearest_close("2026-04-24")
                _last_idx_dt = pf_idx3["dt"].iloc[-1]
                _last_idx_close = float(pf_idx3["close"].iloc[-1])
                _trend_ref = _line_value_at(_ta2, _ya2, _tb2, _yb2, _last_idx_dt)
                _trend_gap_pct = (_last_idx_close - _trend_ref) / abs(_trend_ref) * 100 if _trend_ref else 0.0
                if _last_idx_close >= _trend_ref * 0.995:
                    nhnl_verdict, trend_color = "🟢 주간 추세 유지", "#43a047"
                else:
                    nhnl_verdict, trend_color = "⚠️ 주간 추세선 이탈", "#ef6c00"
                _weekly_trend_desc = f"상승추세선 기준: 현재 지수와 추세선 괴리 {_trend_gap_pct:+.2f}%"

            if not _weekly_trend_desc and not pd.isna(lma):
                if _idx_up and lma > 0 and _strong_bull and _nhnl_up:
                    nhnl_verdict, trend_color = "🟢 강한상승",  "#2e7d32"   # Pine: 강한 상승 브레드스
                elif _idx_up and lma > 0 and _nhnl_up:
                    nhnl_verdict, trend_color = "🟢 양호",      "#43a047"   # Pine: 양호
                elif _idx_up and lma > 0 and not _nhnl_up:
                    nhnl_verdict, trend_color = "⚠️ 브레드스↓", "#ef6c00"   # 지수↑이지만 NH-NL 약화
                elif _idx_up and lma > 0:
                    nhnl_verdict, trend_color = "🟡 둔화중",    "#f9a825"
                elif not _idx_up and lma > 0 and _nhnl_up:
                    nhnl_verdict, trend_color = "🔵 선행회복",  "#1e88e5"   # NH-NL 먼저 회복
                elif _strong_bear and lma < 0 and lma < pma:
                    nhnl_verdict, trend_color = "🔴 강한하락",  "#b71c1c"   # Pine: 강한 하락 브레드스
                elif lma < 0 and lma < pma:
                    nhnl_verdict, trend_color = "🔴 약세",      "#c62828"   # Pine: 주의
                elif lma < 0:
                    nhnl_verdict, trend_color = "🟠 회복중",    "#ef6c00"
                else:
                    nhnl_verdict, trend_color = "🟡 혼조",      "#f9a825"   # Pine: 혼조

            h4.metric("판정", nhnl_verdict)
            # 판정 기준 안내 (Pine script ±200 임계값 기준)
            _verdict_desc = {
                "🟢 강한상승":  "NH-NL>200, MA+, 지수↑ (강한 상승 브레드스)",
                "🟢 양호":      "NH-NL+, MA+, 지수↑ (양호)",
                "⚠️ 브레드스↓": "지수↑이나 NH-NL 전주 대비 감소 (약화 경고)",
                "🟢 주간 추세 유지": "지수가 지정 상승추세선 위에 있음",
                "⚠️ 주간 추세선 이탈": "지수가 지정 상승추세선을 하회",
                "🟡 둔화중":    "지수↑이나 MA 상승세 약화",
                "🔵 선행회복":  "NH-NL 회복 중, 지수 아직 하락",
                "🔴 강한하락":  "NH-NL<-200, MA-, 지수↓ (강한 하락 브레드스)",
                "🔴 약세":      "MA-, MA 하락 중 (주의)",
                "🟠 회복중":    "MA- 이나 하락세 둔화",
                "🟡 혼조":      "MA 방향 불명확 (혼조)",
                "⚪ 부족":      "데이터 부족",
            }
            _desc = _verdict_desc.get(nhnl_verdict, "")
            if _desc:
                _extra_desc = f" | {_weekly_trend_desc}" if _weekly_trend_desc else ""
                st.caption(f"ℹ️ {_desc}{_extra_desc} | Pine ±200 기준 적용")

            # domain 수동 분할 — make_subplots 미사용
            # 모든 trace가 xaxis="x" 공유 → 세로선이 전체 높이 관통
            fig_hl = go.Figure()

            # 위 패널: 지수 곡선 (yaxis="y1", domain 0.45~1.0)
            if _has_index and not pf_idx3.empty:
                fig_hl.add_trace(go.Scatter(
                    x=pf_idx3["dt"], y=pf_idx3["close"],
                    line=dict(color="rgba(200,200,200,0.9)", width=1.8),
                    name=f"{market} 지수",
                    xaxis="x", yaxis="y1",
                ))
            else:
                fig_hl.add_trace(go.Scatter(
                    x=[], y=[],
                    name=f"{market} 지수 (데이터 없음)",
                    xaxis="x", yaxis="y1",
                ))

            # 아래 패널: NH-NL 곡선 — hover 시 "집계 구간: M/D(월)~M/D(금)" 표시
            # W-FRI 집계: dt가 해당 주 금요일 → 월요일은 dt-4일
            _nhnl_mon = pf3["dt"] - pd.Timedelta(days=4)
            _nhnl_fri = pf3["dt"]
            _week_labels = [
                f"{m.strftime('%-m/%-d')}(월)~{f.strftime('%-m/%-d')}(금)"
                for m, f in zip(_nhnl_mon, _nhnl_fri)
            ]
            fig_hl.add_trace(go.Scatter(
                x=pf3["dt"], y=pf3["nhnl"].astype(float),
                mode="lines+markers",
                line=dict(color="#26a69a", width=1.8),
                marker=dict(size=6, color="#26a69a", symbol="circle"),
                name="NH-NL",
                customdata=_week_labels,
                hovertemplate="집계구간: %{customdata}<br>NH-NL: %{y:+,}<extra></extra>",
                xaxis="x", yaxis="y2",
            ))

            # ── 이번 주 NH-NL 예상치 ──────────────────────────────
            # nhnl_daily CSV가 있으면 → 이번 주 실제 일별 누적으로 예상
            # 없으면 → 직전 주 일평균으로 추정
            _forecast_error = None
            try:
                _today = pd.Timestamp(datetime.today().date())
                _this_mon = _today - pd.Timedelta(days=_today.weekday())
                _this_fri = _this_mon + pd.Timedelta(days=4)
                # 오늘이 금요일이면 예상 종점을 내일(토)로 살짝 밀어서 선이 보이게
                _forecast_end = _this_fri + pd.Timedelta(days=1) if _today >= _this_fri else _this_fri

                # 직전 주 마지막 주간값 → 점선 시작점
                _prev_weekly = nhnl_df[nhnl_df["dt"] < _this_mon].copy().reset_index(drop=True)
                if _prev_weekly.empty:
                    _prev_weekly = nhnl_df.head(1).copy()
                _last_wk_dt   = pd.Timestamp(_prev_weekly["dt"].iloc[-1])
                _last_wk_nhnl = float(_prev_weekly["nhnl"].iloc[-1])

                # 이번 주 주간행 (W-FRI = _this_fri 로 찍힌 행)
                _this_week_row = nhnl_df[nhnl_df["dt"] == _this_fri]

                # 일별 CSV 우선, 없으면 주간행 활용, 없으면 직전주 추정
                nhnl_daily_df = st.session_state.get(f"nhnl_daily_{market}")
                if nhnl_daily_df is not None and not nhnl_daily_df.empty:
                    _this_week_daily = nhnl_daily_df[
                        (nhnl_daily_df["dt"] >= _this_mon) & (nhnl_daily_df["dt"] <= _today)
                    ].copy()
                else:
                    _this_week_daily = pd.DataFrame()

                if not _this_week_daily.empty:
                    # 일별 CSV 있음 → 실제 합산
                    _days_done   = len(_this_week_daily)
                    _current_sum = int(_this_week_daily["nhnl"].sum())
                    _daily_avg   = _current_sum / _days_done
                    _est_nhnl    = int(_daily_avg * 5)
                    _today_x     = pd.Timestamp(_this_week_daily["dt"].iloc[-1])
                    _est_label   = (f"이번 주 예상 (실제 {_days_done}일 기반)<br>"
                                    f"현재 누적: {_current_sum:+,} → 금요일 예상: {_est_nhnl:+,}")
                    _x_pts = [_last_wk_dt, _today_x, _this_fri]
                    _y_pts = [_last_wk_nhnl, _current_sum, _est_nhnl]

                elif not _this_week_row.empty:
                    # 주간 CSV에 이번 주 행 있음 → 합산값 + breadth 경과일수로 예상
                    _current_sum = int(_this_week_row["nhnl"].iloc[-1])
                    _df_dt = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")
                    _days_done = max(int((_df_dt >= _this_mon).sum()), 1)
                    _daily_avg = _current_sum / _days_done
                    _est_nhnl  = int(_daily_avg * 5)
                    _est_label = (f"이번 주 예상 ({_days_done}일 집계 기반)<br>"
                                  f"현재 누적: {_current_sum:+,} → 금요일 예상: {_est_nhnl:+,}")
                    _x_pts = [_last_wk_dt, _today, _this_fri]
                    _y_pts = [_last_wk_nhnl, _current_sum, _est_nhnl]

                else:
                    # 이번 주 데이터 없음 → 직전주 일평균 추정
                    _daily_avg   = _last_wk_nhnl / 5.0
                    _days_done   = min(int(_today.weekday()) + 1, 5)
                    _current_sum = int(_daily_avg * _days_done)
                    _est_nhnl    = int(_daily_avg * 5)
                    _est_label   = (f"이번 주 예상 (직전주 추정)<br>"
                                    f"{_days_done}일 경과 추정: {_current_sum:+,} → 금요일: {_est_nhnl:+,}")
                    _x_pts = [_last_wk_dt, _today, _this_fri]
                    _y_pts = [_last_wk_nhnl, _current_sum, _est_nhnl]

                # 오늘이 금요일 다음이면 확정 → 예상치 불필요
                _this_fri_confirmed = _today > _this_fri
                if not _this_fri_confirmed:
                    # ── 시나리오 3개 ──────────────────────────────────
                    # 직전 4주 일평균들로 낙관/중립/비관 계산
                    _recent4 = _prev_weekly.tail(4)["nhnl"].values / 5.0  # 주간값 ÷ 5 = 일평균
                    _avg_opt  = int(float(max(_recent4)) * 5)   # 낙관: 직전 4주 중 최고
                    _avg_base = _est_nhnl                        # 중립: 현재 페이스
                    _avg_pes  = int(float(min(_recent4)) * 5)   # 비관: 직전 4주 중 최저

                    _scenarios = [
                        # (label, est, color, dash, marker_symbol)
                        ("🟢 낙관", _avg_opt,  "rgba(100,220,130,0.85)", "dot",    "triangle-up"),
                        ("🟡 중립", _avg_base, "rgba(255,220,100,0.90)", "dashdot","circle"),
                        ("🔴 비관", _avg_pes,  "rgba(255,100,100,0.80)", "dash",   "triangle-down"),
                    ]
                    for _slabel, _sest, _scol, _sdash, _ssym in _scenarios:
                        # 직전 주 확정값 → 금요일 예상값 — 점선 1개
                        fig_hl.add_trace(go.Scatter(
                            x=[_last_wk_dt, _forecast_end],
                            y=[_last_wk_nhnl, _sest],
                            mode="lines+markers",
                            line=dict(color=_scol, width=2.0, dash=_sdash),
                            marker=dict(size=[0, 11], color=_scol, symbol=_ssym),
                            name=f"{_slabel} {_sest:+,}",
                            hovertemplate=(f"{_slabel}<br>금요일 예상: {_sest:+,}<extra></extra>"),
                            xaxis="x", yaxis="y2",
                        ))

                    # 음영: 비관~낙관 범위
                    _lo = min(_avg_pes, _avg_opt)
                    _hi = max(_avg_pes, _avg_opt)
                    fig_hl.add_trace(go.Scatter(
                        x=[_last_wk_dt, _forecast_end, _forecast_end, _last_wk_dt],
                        y=[_last_wk_nhnl, _hi, _lo, _last_wk_nhnl],
                        fill="toself",
                        fillcolor="rgba(255,220,100,0.06)",
                        line=dict(color="rgba(0,0,0,0)"),
                        showlegend=False, hoverinfo="skip",
                        xaxis="x", yaxis="y2",
                    ))

                    # ── 3/31 저점 → 각 시나리오 예상점 지지선 ──────────
                    # pf3는 주간(W-FRI)이라 3/31이 없음
                    # 3/24~4/11 구간에서 NH-NL 최저 주간값 찾기
                    _search_start = pd.Timestamp("2026-03-24")
                    _search_end   = pd.Timestamp("2026-04-11")
                    _search_range = pf3[(pf3["dt"] >= _search_start) & (pf3["dt"] <= _search_end)]
                    if not _search_range.empty:
                        _min_idx = _search_range["nhnl"].idxmin()
                        _ref_nhnl_dt  = pd.Timestamp(pf3.loc[_min_idx, "dt"])
                        _ref_nhnl_val = float(pf3.loc[_min_idx, "nhnl"])
                    else:
                        _ref_row = pf3[pf3["dt"] <= pd.Timestamp("2026-04-11")].tail(1)
                        _ref_nhnl_dt  = pd.Timestamp(_ref_row["dt"].iloc[-1])
                        _ref_nhnl_val = float(_ref_row["nhnl"].iloc[-1])
                    if True:
                        for _slabel, _sest, _scol, _sdash, _ssym in _scenarios:
                            fig_hl.add_trace(go.Scatter(
                                x=[_ref_nhnl_dt, _forecast_end],
                                y=[_ref_nhnl_val, _sest],
                                mode="lines",
                                line=dict(color=_scol, width=1.2, dash=_sdash),
                                showlegend=False,
                                hovertemplate=(f"{_slabel} 지지선<br>"
                                               f"3/31 NH-NL: {_ref_nhnl_val:+,.0f} → {_sest:+,}<extra></extra>"),
                                xaxis="x", yaxis="y2",
                            ))
            except Exception as _fe:
                _forecast_error = str(_fe)

            if _forecast_error:
                st.caption(f"⚠ 예상치 계산 오류: {_forecast_error}")

            # ── 추세선 헬퍼 ──────────────────────────────────────
            def _extend_line(dt1, y1, dt2, y2, ext_days=10):
                if dt1 == dt2: return [dt1, dt2], [y1, y2]
                _slope = (y2 - y1) / max((dt2 - dt1).days, 1)
                dt_ext = dt2 + pd.Timedelta(days=ext_days)
                y_ext  = y2 + _slope * ext_days
                return [dt1, dt2, dt_ext], [y1, y2, y_ext]

            def _nhnl_val_at_dt(target_dt):
                diffs = (pf3["dt"] - target_dt).abs()
                i = diffs.argmin()
                if diffs.iloc[i] > pd.Timedelta(days=10):
                    return None, None
                return pf3["dt"].iloc[i], float(pf3["nhnl"].iloc[i])

            def _idx_val_at_dt(target_dt):
                diffs = (pf_idx3["dt"] - pd.Timestamp(target_dt)).abs()
                i = diffs.argmin()
                return pf_idx3["dt"].iloc[i], float(pf_idx3["close"].iloc[i])

            def _idx_close_low_at_dt(target_dt, window=5):
                """target_dt 근처 window일 범위에서 종가가 가장 낮은 날짜+종가 (상승추세선용)"""
                center = pd.Timestamp(target_dt)
                mask = (pf_idx3["dt"] >= center - pd.Timedelta(days=window)) & \
                       (pf_idx3["dt"] <= center + pd.Timedelta(days=window))
                sub = pf_idx3[mask]
                if sub.empty:
                    i = (pf_idx3["dt"] - center).abs().argmin()
                    return pf_idx3["dt"].iloc[i], float(pf_idx3["close"].iloc[i])
                i = sub["close"].idxmin()
                return pf_idx3["dt"].loc[i], float(pf_idx3["close"].loc[i])

            def _idx_close_high_at_dt(target_dt, window=5):
                """target_dt 근처 window일 범위에서 종가가 가장 높은 날짜+종가 (하락추세선용)"""
                center = pd.Timestamp(target_dt)
                mask = (pf_idx3["dt"] >= center - pd.Timedelta(days=window)) & \
                       (pf_idx3["dt"] <= center + pd.Timedelta(days=window))
                sub = pf_idx3[mask]
                if sub.empty:
                    i = (pf_idx3["dt"] - center).abs().argmin()
                    return pf_idx3["dt"].iloc[i], float(pf_idx3["close"].iloc[i])
                i = sub["close"].idxmax()
                return pf_idx3["dt"].loc[i], float(pf_idx3["close"].loc[i])

            # ── 수동 추세선: 마켓별로 다르게 적용 ──
            # (날짜A, 날짜B, 색, 연장일, 기준: "low"=상승추세선 / "high"=하락추세선, idx_only=지수패널만)
            # 날짜는 정확한 거래일로 지정 → window=0으로 exact match
            _manual_lines_by_market = {
                "KOSPI": [
                    # (날짜A, 날짜B, 색, 연장일, 기준, idx_only)
                    ("2026-02-13", "2026-02-26", "rgba(255,200,50,0.95)",  10, "low",  False),  # 상승추세선 (저점종가)
                    ("2026-02-26", "2026-03-18", "rgba(255,100,100,0.90)", 10, "high", False),  # 하락추세선 (고점종가)
                    ("2026-03-04", "2026-03-31", "rgba(100,200,255,0.90)", 14, "low",  False),  # 지지선 (저점종가)
                ],
                "KOSDAQ": [],   # 코스닥 추세선 없음 (못 그려서 제거)
            }
            _manual_lines = _manual_lines_by_market.get(market, [])

            def _nearest_close(target_dt):
                """지정 날짜에 가장 가까운 거래일의 종가 반환 (탐색 없음, 정확한 날짜 사용)"""
                center = pd.Timestamp(target_dt)
                diffs = (pf_idx3["dt"] - center).abs()
                i = diffs.argmin()
                return pf_idx3["dt"].iloc[i], float(pf_idx3["close"].iloc[i])

            for _da, _db, _col, _ext, _basis, _idx_only in _manual_lines:
                if not _has_index or pf_idx3.empty:
                    _dt_a = pd.Timestamp(_da)
                    _dt_b = pd.Timestamp(_db)
                    _y_a = _y_b = None
                else:
                    _dt_a, _y_a = _nearest_close(_da)
                    _dt_b, _y_b = _nearest_close(_db)

                _, _ny_a = _nhnl_val_at_dt(_dt_a)
                _, _ny_b = _nhnl_val_at_dt(_dt_b)

                # 위 패널: 지수 추세선 (y1)
                if _y_a is not None and _y_b is not None:
                    _xs, _ys = _extend_line(_dt_a, _y_a, _dt_b, _y_b, ext_days=_ext)
                    fig_hl.add_trace(go.Scatter(
                        x=_xs, y=_ys, mode="lines",
                        line=dict(color=_col, width=2, dash="dot"),
                        showlegend=False, xaxis="x", yaxis="y1",
                    ))
                    # 시작/끝 날짜 레이블 (선 위에 작게)
                    fig_hl.add_annotation(x=_dt_a, y=_y_a, text=_dt_a.strftime("%m/%d"),
                        font=dict(size=9, color=_col), showarrow=False,
                        yshift=-14, xref="x", yref="y1")
                    fig_hl.add_annotation(x=_dt_b, y=_y_b, text=_dt_b.strftime("%m/%d"),
                        font=dict(size=9, color=_col), showarrow=False,
                        yshift=-14, xref="x", yref="y1")

                # 아래 패널: NH-NL 추세선 (y2) — 선만, 마커/텍스트 없음
                if not _idx_only and _ny_a is not None and _ny_b is not None:
                    _xs2, _ys2 = _extend_line(_dt_a, _ny_a, _dt_b, _ny_b, ext_days=_ext)
                    fig_hl.add_trace(go.Scatter(
                        x=_xs2, y=_ys2, mode="lines",
                        line=dict(color=_col, width=2, dash="dot"),
                        showlegend=False, xaxis="x", yaxis="y2",
                    ))

            # ── 지수 전용 상승추세선 (y1만) — 마켓별 ──
            _up_lines_by_market = {
                "KOSPI":  [("2026-03-31", "2026-04-07", "rgba(100,255,150,0.90)", 21)],  # 원래 있던 코스피 상승추세선
                "KOSDAQ": [
                    ("2026-03-06", "2026-04-03", "rgba(100,255,150,0.85)", 21),
                    ("2026-04-10", "2026-04-24", "rgba(80,220,120,0.95)", 14),
                ],
            }
            _up_lines_idx_only = _up_lines_by_market.get(market, [])

            for _da, _db, _col, _ext in _up_lines_idx_only:
                if not _has_index or pf_idx3.empty:
                    continue
                _dt_a, _y_a = _nearest_close(_da)
                _dt_b, _y_b = _nearest_close(_db)
                _xs, _ys = _extend_line(_dt_a, _y_a, _dt_b, _y_b, ext_days=_ext)
                fig_hl.add_trace(go.Scatter(
                    x=_xs, y=_ys, mode="lines",
                    line=dict(color=_col, width=2, dash="dot"),
                    showlegend=False, xaxis="x", yaxis="y1",
                ))
                fig_hl.add_annotation(x=_dt_a, y=_y_a, text=_dt_a.strftime("%m/%d"),
                    font=dict(size=9, color=_col), showarrow=False,
                    yshift=-14, xref="x", yref="y1")
                fig_hl.add_annotation(x=_dt_b, y=_y_b, text=_dt_b.strftime("%m/%d"),
                    font=dict(size=9, color=_col), showarrow=False,
                    yshift=-14, xref="x", yref="y1")

            # 0선 / ±500 기준선 (y2 패널)
            for _y, _color, _dash, _width in [
                (0,    "rgba(255,255,255,0.3)", "solid", 0.8),
                (500,  "rgba(100,220,100,0.5)", "dash",  1.0),
                (-500, "rgba(255,100,100,0.5)", "dash",  1.0),
            ]:
                fig_hl.add_shape(type="line",
                    xref="paper", x0=0, x1=1,
                    yref="y2", y0=_y, y1=_y,
                    line=dict(color=_color, dash=_dash, width=_width),
                    layer="below",
                )

            fig_hl.update_layout(
                template="plotly_dark", height=560,
                title=dict(text=f"{market} NH-NL — {nhnl_verdict}",
                           font=dict(size=13, color=trend_color)),
                hovermode="x",
                hoverlabel=dict(bgcolor="#1e1e2e", font_color="white",
                               font_size=12, bordercolor="#444"),
                margin=dict(l=10, r=60, t=45, b=35),
                legend=dict(orientation="h", y=1.01),
                # 단일 xaxis — 세로선이 도메인 0~1 전체 관통
                xaxis=dict(
                    domain=[0, 1],
                    range=[start_dt3, _today_ts + pd.Timedelta(days=9)],
                    showspikes=True, spikemode="across", spikesnap="cursor",
                    spikethickness=1, spikecolor="rgba(200,200,200,0.8)", spikedash="solid",
                    tickformat="%Y/%m/%d", dtick=7*24*60*60*1000,
                    tickangle=-45, tickfont=dict(size=10),
                ),
                yaxis=dict(title="지수", domain=[0.58, 1.0],
                           showspikes=True, spikemode="across", spikesnap="cursor",
                           spikethickness=1, spikecolor="rgba(200,200,200,0.4)"),
                yaxis2=dict(title="NH-NL", domain=[0.0, 0.42], zeroline=False, anchor="x",
                            showspikes=True, spikemode="across", spikesnap="cursor",
                            spikethickness=1, spikecolor="rgba(200,200,200,0.4)"),
            )
            st.plotly_chart(fig_hl, width='stretch')
            st.caption(
                "📌 예상선 계산 방식 — "
                "🟢 **낙관**: 직전 4주 중 최고 주 일평균 × 5 | "
                "🟡 **중립**: 이번 주 현재 누적 ÷ 경과일 × 5 | "
                "🔴 **비관**: 직전 4주 중 최저 주 일평균 × 5"
            )

            # 원시 데이터 — 일별 우선, 없으면 주간
            with st.expander("📋 원시 데이터 보기", expanded=False):
                _nhnl_daily_raw = st.session_state.get(f"nhnl_daily_{market}")
                if _nhnl_daily_raw is not None and not _nhnl_daily_raw.empty:
                    _daily_disp = _nhnl_daily_raw.copy()
                    _daily_disp["날짜"] = pd.to_datetime(_daily_disp["date"].astype(str), format="%Y%m%d").dt.strftime("%Y/%m/%d")
                    _daily_disp = _daily_disp.rename(columns={"new_highs":"신고가 수","new_lows":"신저가 수","nhnl":"NH-NL"})
                    _daily_disp = _daily_disp[["날짜","신고가 수","신저가 수","NH-NL"]].sort_values("날짜", ascending=False).reset_index(drop=True)
                    st.caption("📅 일별 데이터")
                    st.dataframe(_daily_disp, use_container_width=True, height=400)
                else:
                    display_df = pf3[["dt","new_highs","new_lows","nhnl"]].copy()
                    display_df = display_df.rename(columns={"dt":"날짜","new_highs":"신고가 수","new_lows":"신저가 수","nhnl":"NH-NL"})
                    display_df["날짜"] = display_df["날짜"].dt.strftime("%Y/%m/%d")
                    display_df = display_df.sort_values("날짜", ascending=False).reset_index(drop=True)
                    st.caption("📅 주간 데이터 (일별 데이터 없음)")
                    st.dataframe(display_df, use_container_width=True, height=300)

if __name__ == "__main__":
    main()
