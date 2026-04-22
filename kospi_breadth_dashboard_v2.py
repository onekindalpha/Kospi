#!/usr/bin/env python3
from __future__ import annotations
# KOSPI / KOSDAQ Breadth Dashboard v2 (Streamlit)
# 실행: streamlit run kospi_breadth_dashboard_v2.py
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

import io
import os
from datetime import datetime, timedelta
from pathlib import Path

import platform
import matplotlib
matplotlib.use("Agg")

def _setup_korean_font():
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import subprocess
    sys_name = platform.system()
    if sys_name == "Darwin":
        plt.rcParams["font.family"] = "AppleGothic"
    elif sys_name == "Windows":
        plt.rcParams["font.family"] = "Malgun Gothic"
    else:
        nanum = [f.name for f in fm.fontManager.ttflist if "Nanum" in f.name]
        if nanum:
            plt.rcParams["font.family"] = nanum[0]
        else:
            try:
                subprocess.run(["apt-get","install","-y","-q","fonts-nanum"],
                               check=True, capture_output=True)
                fm._load_fontmanager(try_read_cache=False)
                nanum2 = [f.name for f in fm.fontManager.ttflist if "Nanum" in f.name]
                if nanum2:
                    plt.rcParams["font.family"] = nanum2[0]
            except Exception:
                pass
    plt.rcParams["axes.unicode_minus"] = False

_setup_korean_font()

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

try:
    import FinanceDataReader as fdr
    FDR_OK = True
except ImportError:
    FDR_OK = False

# ──────────────────────────────────────────────────────────────
API_BASE      = "https://data-dbg.krx.co.kr/svc/apis/sto"
KRX_ENDPOINTS = {"KOSPI": "/stk_bydd_trd", "KOSDAQ": "/ksq_bydd_trd"}
FDR_SYMBOLS   = {"KOSPI": "KS11",          "KOSDAQ": "KQ11"}
CACHE_DIR     = Path("./breadth_cache")
NHNL_CACHE_DIR= Path("./nhnl_cache_v2")

HOVER_STYLE = dict(bgcolor="#1a1a2e", font_color="#ffffff",
                   font_size=12, bordercolor="#555555")

STATUS_MAP = {
    "BULLISH_CONFIRMATION":          ("✅ 상승 확인",          "가격·A/D선 모두 고점 근접 (동행)",        "#2e7d32"),
    "BULLISH_DIVERGENCE":            ("🔴 심각한 A/D 미확인", "가격 고점인데 A/D선이 크게 뒤처짐",       "#c62828"),
    "BULLISH_DIVERGENCE_CANDIDATE":  ("🟠 A/D 초기 경고",     "가격이 A/D선보다 빠르게 회복 중",         "#ef6c00"),
    "RECOVERY_IN_PROGRESS":          ("🟡 회복 진행 중",       "가격 고점 재공략 중, 브레드스 미확인",    "#f9a825"),
    "DOWNSIDE_DIVERGENCE_CANDIDATE": ("🟢 하락 다이버전스",    "가격 저점 근접, A/D선은 저점 미확인",     "#00838f"),
    "NORMAL_WEAKNESS":               ("⚫ 전반적 약세",         "가격·A/D선 모두 저점 근접",               "#455a64"),
    "NEUTRAL":                       ("⬜ 중립",               "뚜렷한 신호 없음",                        "#757575"),
}

# ──────────────────────────────────────────────────────────────
# 캐시 유틸
# ──────────────────────────────────────────────────────────────
def _cache_path(market, start, end, base):
    CACHE_DIR.mkdir(exist_ok=True)
    return CACHE_DIR / f"{market}_{start}_{end}_{int(base)}.csv"

def load_cache(market, start, end, base):
    p = _cache_path(market, start, end, base)
    return pd.read_csv(p, dtype={"date": str}) if p.exists() else None

def save_cache(df, market, start, end, base):
    df.to_csv(_cache_path(market, start, end, base), index=False)

def list_caches():
    CACHE_DIR.mkdir(exist_ok=True)
    return sorted(CACHE_DIR.glob("*.csv"))

def _nhnl_cache_path(market, date_str):
    NHNL_CACHE_DIR.mkdir(exist_ok=True)
    return NHNL_CACHE_DIR / f"nhnl_v2_{market}_{date_str}.csv"

def load_nhnl_cache(market, date_str):
    p = _nhnl_cache_path(market, date_str)
    if not p.exists():
        return None
    df = pd.read_csv(p, dtype={"date": str})
    return df if len(df) >= 20 else None

def save_nhnl_cache(df, market, date_str):
    df.to_csv(_nhnl_cache_path(market, date_str), index=False)

# ──────────────────────────────────────────────────────────────
# GitHub 로드
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=1800)
def load_from_github(market: str) -> pd.DataFrame:
    b_resp = requests.get(GITHUB_BREADTH[market], timeout=15)
    if b_resp.status_code != 200:
        raise RuntimeError(
            f"GitHub breadth CSV 없음 ({b_resp.status_code})\n"
            f"{GITHUB_BREADTH[market]}\n"
            "→ GitHub Actions가 아직 실행 안 됐거나 data/ 폴더가 push 안 된 상태입니다."
        )
    breadth = pd.read_csv(io.StringIO(b_resp.text), dtype={"date": str})

    i_resp = requests.get(GITHUB_INDEX[market], timeout=15)
    if i_resp.status_code != 200:
        raise RuntimeError(
            f"GitHub index CSV 없음 ({i_resp.status_code})\n"
            f"{GITHUB_INDEX[market]}"
        )
    idx = pd.read_csv(io.StringIO(i_resp.text), dtype={"date": str})

    df = breadth.merge(idx[["date","open","high","low","close"]], on="date", how="inner")
    return df.sort_values("date").reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=1800)
def load_nhnl_from_github(market: str) -> pd.DataFrame | None:
    """GitHub에 저장된 NH-NL CSV를 읽어 반환. 없으면 None."""
    url = GITHUB_NHNL[market]
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        df = pd.read_csv(io.StringIO(resp.text), dtype={"date": str})
        if df.empty or len(df) < 5:
            return None
        return df
    except Exception:
        return None

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
    for c in ["TDD_CLSPRC","CMPPREVDD_PRC","FLUC_RT","TDD_OPNPRC","TDD_HGPRC","TDD_LWPRC"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",","",regex=False), errors="coerce")
    return df.rename(columns={"BAS_DD":"Date","CMPPREVDD_PRC":"PrevDiff","FLUC_RT":"FlucRate"})

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
# 지수 OHLC
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_index_ohlc(market, start, end):
    if not FDR_OK:
        raise RuntimeError("finance-datareader 미설치")
    end_dt = datetime.strptime(end, "%Y%m%d") + timedelta(days=1)
    raw = fdr.DataReader(FDR_SYMBOLS[market], start, end_dt.strftime("%Y-%m-%d"))
    if raw.empty:
        raise RuntimeError(f"{FDR_SYMBOLS[market]} 데이터 없음")
    raw.columns = [str(c).strip().title() for c in raw.columns]
    df = raw.reset_index()
    df.columns = [str(c).strip().title() for c in df.columns]
    date_col = next(c for c in df.columns if c.lower() in ("date","datetime"))
    def _find(*cands):
        for c in cands:
            if c in df.columns: return c
        raise RuntimeError(f"컬럼 없음: {cands}")
    out = pd.DataFrame({
        "date":  pd.to_datetime(df[date_col]).dt.strftime("%Y%m%d"),
        "open":  pd.to_numeric(df[_find("Open")],  errors="coerce"),
        "high":  pd.to_numeric(df[_find("High")],  errors="coerce"),
        "low":   pd.to_numeric(df[_find("Low")],   errors="coerce"),
        "close": pd.to_numeric(df[_find("Close","Adj Close")], errors="coerce"),
    })
    return out[out["date"] <= end].dropna().reset_index(drop=True)

# ──────────────────────────────────────────────────────────────
# 공통주 필터 (NH-NL 용)
# ──────────────────────────────────────────────────────────────
def _is_common_stock_krx(df):
    if df.empty:
        return pd.Series(dtype=bool)
    name_col = next((c for c in ["ISU_ABBRV","ISU_NM","Name","name"] if c in df.columns), None)
    code_col = next((c for c in ["ISU_SRT_CD","Code","Symbol","code"] if c in df.columns), None)
    name = df[name_col].astype(str).fillna("") if name_col else pd.Series([""]*len(df), index=df.index)
    code = df[code_col].astype(str).fillna("") if code_col else pd.Series([""]*len(df), index=df.index)
    exclude_pat = (
        r"(?:우$|우B$|우C$|[0-9]우$|스팩|리츠|REIT|ETF|ETN|ELW|KODEX|TIGER|KOSEF|KBSTAR|ARIRANG|"
        r"HANARO|SOL|ACE|TIMEFOLIO|TREX|SMART|FOCUS|마이티|TRUE|QV|RISE|레버리지|인버스|선물|채권|"
        r"펀드|액티브|TDF|TRF|BLN|회사채|국고채)"
    )
    bad_name = name.str.contains(exclude_pat, case=False, regex=True, na=False)
    bad_code = code.str.endswith(("K","L","M","N"))
    return ~(bad_name | bad_code)

# ──────────────────────────────────────────────────────────────
# NH-NL 계산 (KRX API 직접)
# ──────────────────────────────────────────────────────────────
def compute_nhnl_pykrx(market, end_date, prog=None, auth_key="", chart_start_date=None):
    if not auth_key or not str(auth_key).strip():
        raise RuntimeError("NH-NL은 KRX API AUTH_KEY가 필요합니다. 사이드바에서 입력해주세요.")
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
        code_col  = next((c for c in ["ISU_SRT_CD","ISU_CD","Code","Symbol"] if c in raw.columns), None)
        name_col  = next((c for c in ["ISU_ABBRV","ISU_NM","Name"] if c in raw.columns), None)
        close_col = next((c for c in ["TDD_CLSPRC","Close","close"] if c in raw.columns), None)
        if code_col is None or close_col is None:
            continue
        df = raw.copy()
        df["date"]  = bas_dd
        df["code"]  = df[code_col].astype(str).str.extract(r"(\d+)")[0].str.zfill(6)
        df["name"]  = df[name_col].astype(str) if name_col else ""
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        df = df.dropna(subset=["code","close"])
        df = df[_is_common_stock_krx(df)].copy()
        if not df.empty:
            daily_frames.append(df[["date","code","name","close"]])
        if prog:
            prog.progress(i / total, text=f"NH-NL 계산용 KRX 수집 중… {bas_dd} ({i}/{total})")

    if not daily_frames:
        raise RuntimeError("NH-NL 계산용 KRX 일별 종목 데이터가 없습니다.")

    panel = pd.concat(daily_frames, ignore_index=True)
    panel["dt"] = pd.to_datetime(panel["date"], format="%Y%m%d")
    panel = panel.sort_values(["code","dt"]).drop_duplicates(["code","dt"], keep="last")
    valid_counts = panel.groupby("code")["dt"].size()
    valid_codes  = valid_counts[valid_counts >= 260].index
    panel = panel[panel["code"].isin(valid_codes)].copy()
    if panel.empty:
        raise RuntimeError("52주 판정에 필요한 히스토리를 가진 종목이 없습니다.")

    def _mark(g):
        g = g.sort_values("dt").copy()
        ph = g["close"].rolling(252, min_periods=252).max().shift(1)
        pl = g["close"].rolling(252, min_periods=252).min().shift(1)
        g["new_high"] = ((g["close"] > ph) & ph.notna()).astype(int)
        g["new_low"]  = ((g["close"] < pl) & pl.notna()).astype(int)
        return g[["dt","new_high","new_low"]]

    marked = panel.groupby("code", group_keys=False).apply(_mark).reset_index(drop=True)
    daily  = marked.groupby("dt", as_index=False)[["new_high","new_low"]].sum()
    daily["nhnl"] = daily["new_high"] - daily["new_low"]
    weekly = daily.set_index("dt").resample("W-FRI").sum().reset_index()
    weekly = weekly.rename(columns={"new_high":"new_highs","new_low":"new_lows"})
    weekly["date"] = weekly["dt"].dt.strftime("%Y%m%d")
    weekly = weekly[["date","dt","new_highs","new_lows","nhnl"]].sort_values("dt").reset_index(drop=True)
    cutoff = pd.to_datetime(start_dt) + pd.Timedelta(days=365)
    weekly = weekly[weekly["dt"] >= cutoff].reset_index(drop=True)
    if chart_start_date:
        cs_dt = pd.to_datetime(chart_start_date, format="%Y%m%d")
        weekly = weekly[weekly["dt"] >= cs_dt].reset_index(drop=True)
    return weekly

# ──────────────────────────────────────────────────────────────
# 판정 로직
# ──────────────────────────────────────────────────────────────
def classify(price_off_high, ad_off_high, gap, price_off_low, ad_off_low,
             price_thr=2.0, ad_thr=3.0, gap_warn=1.5, gap_danger=2.5):
    ph = price_off_high >= -price_thr
    ah = ad_off_high    >= -ad_thr
    pl = price_off_low  <= price_thr
    al = ad_off_low     <= ad_thr
    if ph and ah and gap >= -1.0:    return "BULLISH_CONFIRMATION"
    if ph and gap <= -gap_danger:    return "BULLISH_DIVERGENCE"
    if gap <= -gap_warn:             return "BULLISH_DIVERGENCE_CANDIDATE"
    if gap < -1.0:                   return "RECOVERY_IN_PROGRESS"
    if pl and not al:                return "DOWNSIDE_DIVERGENCE_CANDIDATE"
    if pl and al:                    return "NORMAL_WEAKNESS"
    return "NEUTRAL"

def compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger):
    closes   = df["close"].values.astype(float)
    ad_lines = df["ad_line"].values.astype(float)
    window   = closes[-lookback:]
    peak_idx = window.argmax()
    days_ago = lookback - 1 - peak_idx
    price_high = window[peak_idx]
    ad_at_peak = ad_lines[-(days_ago + 1)]
    last_close = closes[-1]; last_ad = ad_lines[-1]
    price_low = closes[-lookback:].min(); ad_low = ad_lines[-lookback:].min()
    price_off = (last_close - price_high) / abs(price_high) * 100 if price_high else float("nan")
    ad_off    = (last_ad   - ad_at_peak)  / abs(ad_at_peak) * 100 if ad_at_peak else float("nan")
    gap       = ad_off - price_off
    price_off_low = (last_close - price_low) / abs(price_low) * 100 if price_low else float("nan")
    ad_off_low    = (last_ad   - ad_low)    / abs(ad_low)    * 100 if ad_low    else float("nan")
    peak_date  = str(df["date"].iloc[-(days_ago+1)])
    peak_label = "오늘" if days_ago == 0 else f"{days_ago}일전 ({peak_date})"
    status_key = classify(price_off, ad_off, gap, price_off_low, ad_off_low,
                          price_thr, ad_thr, gap_warn, gap_danger)
    verdict, note, color = STATUS_MAP[status_key]
    return dict(peak_label=peak_label, price_off=price_off, ad_off=ad_off, gap=gap,
                verdict=verdict, note=note, color=color,
                last_close=last_close, last_ad=last_ad,
                price_high=price_high, ad_at_peak=ad_at_peak)

def compute_hlab(df, high_bars=60, low_bars=130):
    closes  = df["close"].values.astype(float)
    ad_line = df["ad_line"].values.astype(float)
    dts     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")
    n = len(closes)
    def _slice(arr, end_idx, length):
        start = max(0, end_idx - length)
        return arr[start:end_idx], start
    hb_win, hb_s = _slice(closes, n, high_bars)
    hb_i = hb_s + int(np.argmax(hb_win))
    hb_val, hb_dt, hb_ad = closes[hb_i], dts.iloc[hb_i], ad_line[hb_i]
    ha_win, ha_s = _slice(closes, hb_s + int(np.argmax(hb_win)), high_bars)
    if len(ha_win) > 0:
        ha_i = ha_s + int(np.argmax(ha_win))
        ha_val, ha_dt, ha_ad = closes[ha_i], dts.iloc[ha_i], ad_line[ha_i]
    else:
        ha_val, ha_dt, ha_ad = hb_val, hb_dt, hb_ad
    lb_win, lb_s = _slice(closes, n, low_bars)
    lb_i = lb_s + int(np.argmin(lb_win))
    lb_val, lb_dt, lb_ad = closes[lb_i], dts.iloc[lb_i], ad_line[lb_i]
    la_win, la_s = _slice(closes, lb_s + int(np.argmin(lb_win)), low_bars)
    if len(la_win) > 0:
        la_i = la_s + int(np.argmin(la_win))
        la_val, la_dt, la_ad = closes[la_i], dts.iloc[la_i], ad_line[la_i]
    else:
        la_val, la_dt, la_ad = lb_val, lb_dt, lb_ad
    bear_div = bool(hb_val > ha_val and hb_ad < ha_ad)
    bear_pct = abs((ha_ad - hb_ad) / ha_ad * 100) if (bear_div and ha_ad != 0) else 0.0
    bull_div = bool(lb_val < la_val and lb_ad > la_ad)
    bull_pct = abs((lb_ad - la_ad) / la_ad * 100) if (bull_div and la_ad != 0) else 0.0
    return dict(hb_val=hb_val, hb_dt=hb_dt, hb_ad=hb_ad,
                ha_val=ha_val, ha_dt=ha_dt, ha_ad=ha_ad,
                lb_val=lb_val, lb_dt=lb_dt, lb_ad=lb_ad,
                la_val=la_val, la_dt=la_dt, la_ad=la_ad,
                bear_div=bear_div, bear_div_pct=bear_pct,
                bull_div=bull_div, bull_div_pct=bull_pct)

# ──────────────────────────────────────────────────────────────
# 차트
# ──────────────────────────────────────────────────────────────
def make_plotly_chart(df, market, sig, chart_months, hlab):
    end_dt   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    mask     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt
    pf       = df[mask].copy().reset_index(drop=True)
    pf["dt"] = pd.to_datetime(pf["date"].astype(str), format="%Y%m%d")

    hb_color = "rgba(255,80,80,0.95)"  if hlab["bear_div"] else "rgba(160,160,160,0.8)"
    ha_color = "rgba(255,140,140,0.6)" if hlab["bear_div"] else "rgba(120,120,120,0.5)"
    lb_color = "rgba(38,210,160,0.95)" if hlab["bull_div"] else "rgba(160,160,160,0.8)"
    la_color = "rgba(38,210,160,0.6)"  if hlab["bull_div"] else "rgba(120,120,120,0.5)"

    price_low  = float(pf["low"].min()); price_high = float(pf["high"].max())
    price_span = max(price_high - price_low, abs(price_high)*0.02, 1.0)
    y1_range   = [price_low - price_span*0.08, price_high + price_span*0.12]
    ad_min = float(pf["ad_line"].min()); ad_max = float(pf["ad_line"].max())
    ad_span = max(ad_max - ad_min, max(abs(ad_max),1.0)*0.02, 1.0)
    y2_range = [ad_min - ad_span*0.1, ad_max + ad_span*0.1]

    if hlab["bear_div"]:
        div_text, div_color = f"⚠ 부정적 불일치 {hlab['bear_div_pct']:.1f}%", "#ff5050"
    elif hlab["bull_div"]:
        div_text, div_color = f"✓ 긍정적 불일치 {hlab['bull_div_pct']:.1f}%", "#26d2a0"
    else:
        div_text, div_color = "불일치 없음", "#aaaaaa"

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=pf["dt"], open=pf["open"], high=pf["high"], low=pf["low"], close=pf["close"],
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        name=market, showlegend=False, xaxis="x", yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        x=pf["dt"], y=pf["ad_line"].astype(float),
        line=dict(color="#1e88e5", width=2.0), name="A/D Line",
        xaxis="x", yaxis="y2",
    ))
    ad_vals = pf["ad_line"].astype(float)
    pr_min, pr_max = pf["close"].min(), pf["close"].max()
    price_mapped = (ad_vals.min() + (pf["close"] - pr_min) / (pr_max - pr_min) *
                    (ad_vals.max() - ad_vals.min()) if pr_max != pr_min else ad_vals)
    fig.add_trace(go.Scatter(
        x=pf["dt"], y=price_mapped,
        line=dict(color="rgba(180,180,180,0.35)", width=1.0),
        name="가격(참조)", showlegend=False, xaxis="x", yaxis="y2",
    ))
    for val, color, dash, ann in [
        (hlab["hb_val"], hb_color, "dash", f"H_b {hlab['hb_val']:,.0f}"),
        (hlab["ha_val"], ha_color, "dot",  f"H_a {hlab['ha_val']:,.0f}"),
        (hlab["lb_val"], lb_color, "dash", f"L_b {hlab['lb_val']:,.0f}"),
        (hlab["la_val"], la_color, "dot",  f"L_a {hlab['la_val']:,.0f}"),
    ]:
        fig.add_shape(type="line", x0=pf["dt"].iloc[0], x1=pf["dt"].iloc[-1],
                      y0=val, y1=val, xref="x", yref="y1",
                      line=dict(color=color, dash=dash, width=1.2))
        fig.add_annotation(x=pf["dt"].iloc[-1], y=val, xref="x", yref="y1",
                           text=ann, font=dict(color=color, size=10),
                           xanchor="left", showarrow=False)
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
    if hlab["bear_div"]:
        mid = hlab["ha_dt"] + (hlab["hb_dt"] - hlab["ha_dt"]) / 2
        fig.add_shape(type="line",
            x0=hlab["ha_dt"], y0=hlab["ha_ad"], x1=hlab["hb_dt"], y1=hlab["hb_ad"],
            xref="x", yref="y2", line=dict(color="rgba(255,80,80,0.9)", width=2, dash="dash"))
        fig.add_annotation(x=mid, y=(hlab["ha_ad"]+hlab["hb_ad"])/2,
                           xref="x", yref="y2", text=f"⚠ {hlab['bear_div_pct']:.1f}%",
                           font=dict(color="#ff5050", size=12), showarrow=False)
    if hlab["bull_div"]:
        mid = hlab["la_dt"] + (hlab["lb_dt"] - hlab["la_dt"]) / 2
        fig.add_shape(type="line",
            x0=hlab["la_dt"], y0=hlab["la_ad"], x1=hlab["lb_dt"], y1=hlab["lb_ad"],
            xref="x", yref="y2", line=dict(color="rgba(38,210,160,0.9)", width=2, dash="dash"))
        fig.add_annotation(x=mid, y=(hlab["la_ad"]+hlab["lb_ad"])/2,
                           xref="x", yref="y2", text=f"✓ {hlab['bull_div_pct']:.1f}%",
                           font=dict(color="#26d2a0", size=12), showarrow=False)
    fig.update_layout(
        template="plotly_dark", height=660,
        title=dict(text=f"{market} — {div_text}", font=dict(size=14, color=div_color)),
        hovermode="x",
        hoverlabel=HOVER_STYLE,
        legend=dict(orientation="h", y=1.01, x=0),
        margin=dict(l=10, r=90, t=55, b=10),
        xaxis=dict(rangeslider=dict(visible=False),
                   showspikes=True, spikemode="across", spikesnap="cursor",
                   spikethickness=1, spikecolor="rgba(200,200,200,0.7)", spikedash="solid",
                   tickformat="%Y/%m/%d", tickangle=-45, tickfont=dict(size=8), domain=[0,1],
                   showline=True, mirror=True),
        yaxis =dict(title="지수",   domain=[0.52,1.0], range=y1_range,
                    showspikes=True, spikemode="across", spikesnap="cursor",
                    spikethickness=1, spikecolor="rgba(200,200,200,0.4)", spikedash="solid",
                    showline=True, mirror=True),
        yaxis2=dict(title="A/D Line", domain=[0.0,0.48], range=y2_range,
                    showspikes=True, spikemode="across", spikesnap="data",
                    spikethickness=1, spikecolor="rgba(200,200,200,0.6)", spikedash="dot",
                    showline=True, mirror=True),
    )
    return fig

# ──────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="국장 브레드스 대시보드", page_icon="📊", layout="wide")
    st.title("📊 국장 A/D Line 브레드스 대시보드")
    st.caption("KRX 상승·하락 종목 수 기반 / 스탠 와인스태인 브레드스 분석")

    with st.sidebar:
        st.header("⚙️ 설정")
        market = st.selectbox("마켓", ["KOSPI","KOSDAQ"])
        mode   = st.radio("데이터 소스",
                          ["☁️ GitHub (빠름)", "🔑 KRX API (직접 수집)"],
                          index=0,
                          help="GitHub: Actions가 매일 자동 업데이트한 CSV 사용\nKRX API: 직접 수집 (AUTH_KEY 필요)")

        if mode == "🔑 KRX API (직접 수집)":
            auth_key = st.text_input("KRX AUTH_KEY",
                                     value=os.environ.get("KRX_AUTH_KEY",""),
                                     type="password")
            c1, c2 = st.columns(2)
            today    = datetime.today()
            start_dt = c1.date_input("시작일", value=today - timedelta(days=730))
            end_dt   = c2.date_input("종료일", value=today)
            base_value = st.number_input("A/D Line 시작값", value=50000.0, step=1000.0)
        else:
            auth_key = ""

        fetch_btn = st.button("🔄 데이터 불러오기", type="primary", use_container_width=True)
        if mode == "🔑 KRX API (직접 수집)":
            st.caption("💡 새로 불러오고 싶으면 아래 캐시를 지우고 불러오세요.")

        st.divider()
        st.subheader("분석 파라미터")
        lookback     = st.slider("Lookback (일)",         20, 252, 126)
        chart_months = st.slider("차트 표시 기간 (월)",    1,  24,   6)
        high_bars    = st.slider("고점 탐색 구간 H_b (일)", 10, 500,  60)
        low_bars     = st.slider("저점 탐색 구간 L_b (일)", 10, 500, 130)
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0, step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0, step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5, step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5, step=0.1)

        st.divider()
        st.subheader("💾 저장된 캐시")
        caches = list_caches()
        if caches:
            for p in caches:
                ca, cb = st.columns([3,1])
                ca.caption(p.name)
                if cb.button("🗑", key=str(p)):
                    p.unlink(); st.rerun()
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
                    df      = load_from_github(market)
                    nhnl_df = load_nhnl_from_github(market)
                st.success(f"✅ GitHub 로드 완료 — {len(df)}일치 / 최신: {df['date'].iloc[-1]}")
                if nhnl_df is not None and not nhnl_df.empty:
                    st.session_state[f"nhnl_{market}"] = nhnl_df
                    st.success(f"✅ NH-NL GitHub 로드 완료 — {len(nhnl_df)}주치")
                else:
                    st.session_state[f"nhnl_{market}"] = None
                    st.info(
                        "ℹ️ GitHub에 NH-NL CSV가 아직 없습니다.\n\n"
                        f"→ `{GITHUB_NHNL[market]}`\n\n"
                        "GitHub Actions가 아직 실행 안 됐거나, "
                        "update_breadth.yml에 NH-NL 생성 스텝이 추가되지 않은 상태입니다. "
                        "**KRX API 직접 수집 모드**로 먼저 계산한 뒤 repo에 push하면 이후 GitHub 모드에서도 볼 수 있습니다."
                    )
            except Exception as e:
                st.error(f"GitHub 로드 실패: {e}")
                return

        else:  # KRX API 직접
            if not auth_key:
                st.error("KRX AUTH_KEY를 입력해주세요.")
                return
            start_str = start_dt.strftime("%Y%m%d")
            end_str   = end_dt.strftime("%Y%m%d")
            cached      = load_cache(market, start_str, end_str, 50000.0)
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
                    prog3  = st.progress(0, text="NH-NL 계산용 KRX 수집 중…")
                    nhnl_df = compute_nhnl_pykrx(market, end_str, prog=prog3,
                                                  auth_key=auth_key, chart_start_date=start_str)
                    prog3.empty()
                    if nhnl_df is not None and not nhnl_df.empty:
                        save_nhnl_cache(nhnl_df, market, end_str)
                        st.success(f"✅ NH-NL 계산 완료 — {len(nhnl_df)}주치")
                st.session_state[f"nhnl_{market}"] = (
                    nhnl_df if nhnl_df is not None and not nhnl_df.empty else None
                )
            except Exception as e:
                st.error(f"데이터 수집 실패: {type(e).__name__}: {e}")
                return

        st.session_state["df_merged"] = df
        st.session_state["df_market"] = market

    if st.session_state.get("df_market") != market:
        st.session_state.pop("df_merged", None)
        st.info("마켓이 변경됐어요. 데이터 불러오기를 다시 눌러주세요.")
        return

    df = st.session_state["df_merged"]
    if len(df) < lookback:
        st.warning(f"데이터 부족: {len(df)}행 (lookback={lookback})")
        return

    sig  = compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger)
    hlab = compute_hlab(df, high_bars=high_bars, low_bars=low_bars)
    last = df.iloc[-1]

    # ── 탭 ──
    TAB_LABELS = ["📈 A/D Line", "⚡ 모멘텀", "🏔 NH-NL"]
    if "active_tab" not in st.session_state:
        st.session_state["active_tab"] = TAB_LABELS[0]
    _default_idx = TAB_LABELS.index(st.session_state.get("active_tab", TAB_LABELS[0]))

    if hasattr(st, "segmented_control"):
        active_tab = st.segmented_control("분석 탭", TAB_LABELS, selection_mode="single",
                                          default=TAB_LABELS[_default_idx],
                                          key="active_tab_selector")
    else:
        active_tab = st.radio("분석 탭", TAB_LABELS, index=_default_idx,
                              horizontal=True, key="active_tab_selector")
    st.session_state["active_tab"] = active_tab

    # ══ TAB 1: A/D Line ══
    if active_tab == "📈 A/D Line":
        gap_color = "#00897b" if sig["gap"] >= 0 else "#c62828"
        gap_arrow = "▲" if sig["gap"] >= 0 else "▼"
        st.markdown(
            f'<div style="text-align:center;padding:6px 0 2px 0">'
            f'<span style="font-size:0.85em;color:#aaaaaa">괴리 (A/D − 가격)</span><br>'
            f'<span style="font-size:2.6em;font-weight:900;color:{gap_color}">'
            f'{gap_arrow} {sig["gap"]:+.2f}%</span>'
            f'<span style="font-size:0.8em;color:#aaaaaa;margin-left:8px">'
            f'기준: {sig["peak_label"]}</span>'
            f'</div>', unsafe_allow_html=True)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("최근 날짜", pd.to_datetime(str(last["date"]), format="%Y%m%d").strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가", f"{float(last['close']):,.2f}")
        c3.metric("오늘 AD 차이",   f"{int(last['ad_diff']):+,}")
        c4.metric("가격 고점 대비", f"{sig['price_off']:.2f}%")
        c5.metric("A/D 고점 대비",  f"{sig['ad_off']:.2f}%")
        st.markdown(
            f'<div style="background:{sig["color"]};padding:12px 18px;border-radius:8px;margin:8px 0">'
            f'<b style="font-size:1.2em;color:white">{sig["verdict"]}</b>'
            f'&nbsp;&nbsp;<span style="color:#ffffffcc">{sig["note"]}</span>'
            f'&nbsp;&nbsp;<span style="color:#ffffffaa;font-size:0.9em">기준: {sig["peak_label"]}</span>'
            f'</div>', unsafe_allow_html=True)
        try:
            st.plotly_chart(make_plotly_chart(df, market, sig, chart_months, hlab),
                            use_container_width=True)
        except Exception as e:
            st.error(f"차트 렌더링 실패: {e}")

        with st.expander("📋 원시 데이터 보기"):
            show = df.copy()
            show["date"] = pd.to_datetime(show["date"].astype(str), format="%Y%m%d").dt.strftime("%Y-%m-%d")
            cols = [c for c in ["date","advances","declines","unchanged",
                                "ad_diff","ad_line","close","breadth_thrust_ema10"] if c in show.columns]
            st.dataframe(show[cols].sort_values("date", ascending=False).reset_index(drop=True),
                         use_container_width=True)
            csv = show.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("📥 CSV 다운로드", csv, f"{market}_breadth.csv", "text/csv")

    # ══ TAB 2: MI ══
    elif active_tab == "⚡ 모멘텀":
        st.subheader("⚡ MI 탄력지수 (Momentum Index)")
        st.caption("스탠 와인스태인 책 정의: 등락종목수 차이(AD)의 200일 롤링 평균. "
                   "0선 위 = 시장 강세, 0선 아래 = 시장 약세.")
        mi_window = st.slider("MA 기간 (기본 200일)", 50, 300, 200, step=10, key="mi_win")

        end_dt2   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
        start_dt2 = end_dt2 - pd.DateOffset(months=chart_months)
        mask2     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt2
        pf2       = df[mask2].copy().reset_index(drop=True)
        pf2["dt"] = pd.to_datetime(pf2["date"].astype(str), format="%Y%m%d")

        ad_diff_s = pd.Series(df["ad_diff"].values.astype(float))
        mi_full   = ad_diff_s.rolling(mi_window).mean()
        mi_plot   = mi_full.iloc[mask2.values].reset_index(drop=True)
        last_mi   = mi_full.iloc[-1]
        prev_mi   = mi_full.iloc[-2] if len(mi_full) >= 2 else last_mi

        if pd.isna(last_mi):
            mi_verdict, mi_color = "⚪ 데이터 부족",    "#757575"
        elif last_mi > 0 and last_mi > prev_mi:
            mi_verdict, mi_color = "🟢 강세 상승",      "#2e7d32"
        elif last_mi > 0:
            mi_verdict, mi_color = "🟡 강세 둔화",      "#f9a825"
        elif last_mi < 0 and last_mi < prev_mi:
            mi_verdict, mi_color = "🔴 약세 하락",      "#c62828"
        else:
            mi_verdict, mi_color = "🟠 약세 회복 중",   "#ef6c00"

        m1, m2, m3 = st.columns(3)
        m1.metric(f"MI ({mi_window}일 평균)", f"{last_mi:+.1f}" if not pd.isna(last_mi) else "N/A")
        m2.metric("전일 대비", f"{(last_mi-prev_mi):+.1f}" if not pd.isna(last_mi) else "N/A")
        m3.metric("판정", mi_verdict)

        fig_mi = go.Figure()
        fig_mi.add_trace(go.Bar(
            x=pf2["dt"], y=mi_plot,
            marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in mi_plot.fillna(0)],
            name=f"MI ({mi_window}일 평균)", opacity=0.85,
        ))
        fig_mi.add_hline(y=0, line_color="gray", line_dash="dot", annotation_text="기준선(0)")
        fig_mi.update_layout(
            title=f"{market} MI 탄력지수 — AD차이 {mi_window}일 롤링 평균 (스탠 와인스태인)",
            template="plotly_dark", height=420,
            hovermode="x",
            hoverlabel=HOVER_STYLE,
            legend=dict(orientation="h", y=1.05),
            yaxis_title="MI 값 (AD 평균)",
        )
        st.plotly_chart(fig_mi, use_container_width=True)
        if len(df) < mi_window:
            st.warning(f"⚠️ 데이터 {len(df)}일 — {mi_window}일 MA 계산에 데이터가 부족합니다.")

    # ══ TAB 3: NH-NL ══
    elif active_tab == "🏔 NH-NL":
        st.subheader("🏔 고점-저점 수치 (신고가 - 신저가 종목 수)")
        st.caption("스탠 와인스태인 책 정의: 매주 신고가 기록 종목 수 - 신저가 기록 종목 수. "
                   "KRX API 일별 전체 종목 스냅샷으로 52주 신고가/신저가를 판별해 주간 집계합니다.")

        nhnl_df = st.session_state.get(f"nhnl_{market}")

        if nhnl_df is None or nhnl_df.empty:
            if mode == "☁️ GitHub (빠름)":
                st.warning(
                    "GitHub에 NH-NL CSV가 없습니다.\n\n"
                    "**해결 방법:** KRX API 직접 수집 모드로 한 번 실행하면 로컬에 캐시가 생깁니다. "
                    "그 CSV를 `data/kospi_nhnl.csv` 로 repo에 push하면 이후 GitHub 모드에서도 볼 수 있습니다."
                )
            else:
                st.info("KRX 직접 수집 모드에서는 '데이터 불러오기'를 누를 때 NH-NL도 함께 계산합니다.")
            return

        from plotly.subplots import make_subplots as _msp2
        nhnl_df["dt"] = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d")
        idx_end_dt = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
        end_dt3    = max(nhnl_df["dt"].max(), idx_end_dt)
        start_dt3  = end_dt3 - pd.DateOffset(months=chart_months)
        pf3        = nhnl_df[nhnl_df["dt"] >= start_dt3].copy().reset_index(drop=True)

        if pf3.empty:
            st.warning("선택한 기간에 NH-NL 데이터가 없습니다.")
            return

        last_nhnl = int(pf3["nhnl"].iloc[-1])
        last_nh   = int(pf3["new_highs"].iloc[-1])
        last_nl   = int(pf3["new_lows"].iloc[-1])

        # ── 책 기준 판정 ──────────────────────────────────────
        # 1) NH-NL 이전 고점 대비 불일치 (지수 고점 vs NH-NL 고점)
        nhnl_vals  = pf3["nhnl"].astype(float)
        idx_vals   = None
        pf_idx3    = df[pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt3].copy()
        pf_idx3["dt"] = pd.to_datetime(pf_idx3["date"].astype(str), format="%Y%m%d")

        # 지수 고점 2개 찾기 (앞/뒤 절반씩)
        mid = len(pf3) // 2
        nhnl_first_half = nhnl_vals.iloc[:mid]
        nhnl_second_half = nhnl_vals.iloc[mid:]
        peak1_nhnl = nhnl_first_half.max() if len(nhnl_first_half) > 0 else 0
        peak2_nhnl = nhnl_second_half.max() if len(nhnl_second_half) > 0 else 0
        bearish_div = bool(peak2_nhnl < peak1_nhnl and last_nhnl > 0)  # 지수 고점 갱신했는데 NH-NL 못 따라옴

        # 책 기준 판정
        if last_nhnl >= 500:
            nhnl_verdict, trend_color = "🟢 강세 확인",  "#2e7d32"
        elif last_nhnl > 0 and bearish_div:
            nhnl_verdict, trend_color = "🟠 불일치 경고", "#ef6c00"
        elif last_nhnl > 0:
            nhnl_verdict, trend_color = "🟡 약한 강세",  "#f9a825"
        elif last_nhnl < 0:
            nhnl_verdict, trend_color = "🔴 약세",       "#c62828"
        else:
            nhnl_verdict, trend_color = "⚪ 중립",       "#757575"

        h1, h2, h3, h4 = st.columns(4)
        h1.metric("신고가 종목 수", f"{last_nh:,}")
        h2.metric("신저가 종목 수", f"{last_nl:,}")
        h3.metric("NH-NL",          f"{last_nhnl:+,}")
        h4.metric("판정",            nhnl_verdict)

        # ── 자동 추세선: 지수 고점/저점 날짜 기준으로 NH-NL도 같은 구간 비교 ──
        def _find_peaks_idx(series, order=None):
            """국소 최대값 인덱스 목록 반환 (scipy 없으면 단순 방식)"""
            arr = series.values
            o = order or max(2, len(arr) // 15)
            try:
                from scipy.signal import argrelextrema
                idxs = argrelextrema(arr, np.greater, order=o)[0]
            except ImportError:
                # scipy 없으면 단순 롤링 방식
                idxs = []
                for i in range(o, len(arr) - o):
                    if arr[i] == arr[max(0,i-o):i+o+1].max():
                        idxs.append(i)
                idxs = np.array(idxs)
            return idxs

        def _find_troughs_idx(series, order=None):
            arr = series.values
            o = order or max(2, len(arr) // 15)
            try:
                from scipy.signal import argrelextrema
                idxs = argrelextrema(arr, np.less, order=o)[0]
            except ImportError:
                idxs = []
                for i in range(o, len(arr) - o):
                    if arr[i] == arr[max(0,i-o):i+o+1].min():
                        idxs.append(i)
                idxs = np.array(idxs)
            return idxs

        # 지수에서 고점/저점 날짜 2개 추출
        idx_series = pd.Series(pf_idx3["close"].values)
        idx_peak_idxs   = _find_peaks_idx(idx_series)
        idx_trough_idxs = _find_troughs_idx(idx_series)

        # NH-NL은 주간 데이터라 일별 지수와 인덱스가 다름 → 날짜 기준으로 매핑
        def _nhnl_val_at_dt(target_dt):
            """NH-NL 데이터에서 target_dt에 가장 가까운 값 반환"""
            if pf3.empty:
                return None, None
            diffs = (pf3["dt"] - target_dt).abs()
            i = diffs.argmin()
            return pf3["dt"].iloc[i], float(pf3["nhnl"].iloc[i])

        # 고점 추세선용: 지수 고점 최근 2개
        peak_pairs   = None
        trough_pairs = None

        if len(idx_peak_idxs) >= 2:
            i1, i2 = idx_peak_idxs[-2], idx_peak_idxs[-1]
            if i1 < len(pf_idx3) and i2 < len(pf_idx3):
                dt1 = pf_idx3["dt"].iloc[i1]; y_idx1 = pf_idx3["close"].iloc[i1]
                dt2 = pf_idx3["dt"].iloc[i2]; y_idx2 = pf_idx3["close"].iloc[i2]
                ndt1, ny1 = _nhnl_val_at_dt(dt1)
                ndt2, ny2 = _nhnl_val_at_dt(dt2)
                peak_pairs = (dt1, y_idx1, dt2, y_idx2, ndt1, ny1, ndt2, ny2)

        if len(idx_trough_idxs) >= 2:
            i1, i2 = idx_trough_idxs[-2], idx_trough_idxs[-1]
            if i1 < len(pf_idx3) and i2 < len(pf_idx3):
                dt1 = pf_idx3["dt"].iloc[i1]; y_idx1 = pf_idx3["close"].iloc[i1]
                dt2 = pf_idx3["dt"].iloc[i2]; y_idx2 = pf_idx3["close"].iloc[i2]
                ndt1, ny1 = _nhnl_val_at_dt(dt1)
                ndt2, ny2 = _nhnl_val_at_dt(dt2)
                trough_pairs = (dt1, y_idx1, dt2, y_idx2, ndt1, ny1, ndt2, ny2)

        # ── 차트 ──────────────────────────────────────────────
        fig_hl = _msp2(rows=2, cols=1, shared_xaxes=True,
                       row_heights=[0.45, 0.55], vertical_spacing=0.03)

        # 위 패널: 지수
        fig_hl.add_trace(go.Scatter(
            x=pf_idx3["dt"], y=pf_idx3["close"],
            line=dict(color="rgba(220,220,220,0.95)", width=1.5),
            name=f"{market} 지수",
        ), row=1, col=1)

        # 자동 추세선: 지수 고점 연결 + 같은 날짜 NH-NL 마커
        if peak_pairs:
            dt1, y1, dt2, y2, ndt1, ny1, ndt2, ny2 = peak_pairs
            fig_hl.add_trace(go.Scatter(
                x=[dt1, dt2], y=[y1, y2],
                mode="lines+markers",
                line=dict(color="rgba(255,180,50,0.9)", width=2, dash="dot"),
                marker=dict(size=8, color="rgba(255,180,50,1.0)", symbol="triangle-up"),
                name="고점 추세선",
            ), row=1, col=1)

        # 자동 추세선: 지수 저점 연결
        if trough_pairs:
            dt1, y1, dt2, y2, ndt1, ny1, ndt2, ny2 = trough_pairs
            fig_hl.add_trace(go.Scatter(
                x=[dt1, dt2], y=[y1, y2],
                mode="lines+markers",
                line=dict(color="rgba(100,200,255,0.9)", width=2, dash="dot"),
                marker=dict(size=8, color="rgba(100,200,255,1.0)", symbol="triangle-down"),
                name="저점 추세선",
            ), row=1, col=1)

        # 아래 패널: NH-NL
        fig_hl.add_trace(go.Scatter(
            x=pf3["dt"], y=nhnl_vals,
            line=dict(color="rgba(220,220,220,0.95)", width=1.5),
            name="NH-NL",
            fill="tozeroy",
            fillcolor="rgba(100,180,255,0.15)",
        ), row=2, col=1)

        # 아래 패널: 지수 고점 날짜 기준 NH-NL 추세선 (같은 구간 비교)
        if peak_pairs:
            dt1, y1, dt2, y2, ndt1, ny1, ndt2, ny2 = peak_pairs
            if ny1 is not None and ny2 is not None:
                fig_hl.add_trace(go.Scatter(
                    x=[ndt1, ndt2], y=[ny1, ny2],
                    mode="lines+markers",
                    line=dict(color="rgba(255,180,50,0.9)", width=2, dash="dot"),
                    marker=dict(size=8, color="rgba(255,180,50,1.0)", symbol="triangle-up"),
                    name="고점 구간 NH-NL",
                    showlegend=False,
                ), row=2, col=1)

        if trough_pairs:
            dt1, y1, dt2, y2, ndt1, ny1, ndt2, ny2 = trough_pairs
            if ny1 is not None and ny2 is not None:
                fig_hl.add_trace(go.Scatter(
                    x=[ndt1, ndt2], y=[ny1, ny2],
                    mode="lines+markers",
                    line=dict(color="rgba(100,200,255,0.9)", width=2, dash="dot"),
                    marker=dict(size=8, color="rgba(100,200,255,1.0)", symbol="triangle-down"),
                    name="저점 구간 NH-NL",
                    showlegend=False,
                ), row=2, col=1)

        # 0선 / ±500 기준선
        fig_hl.add_hline(y=0, line_color="rgba(200,200,200,0.6)", line_dash="solid",
                         line_width=1, row=2, col=1,
                         annotation_text="  0", annotation_font_color="rgba(200,200,200,0.7)",
                         annotation_position="left")
        fig_hl.add_hline(y=500, line_color="rgba(100,220,100,0.5)", line_dash="dash",
                         line_width=1, row=2, col=1,
                         annotation_text="  +500", annotation_font_color="rgba(100,220,100,0.7)",
                         annotation_position="left")
        fig_hl.add_hline(y=-500, line_color="rgba(255,100,100,0.5)", line_dash="dash",
                         line_width=1, row=2, col=1,
                         annotation_text="  -500", annotation_font_color="rgba(255,100,100,0.7)",
                         annotation_position="left")

        x_min = max(pf3["dt"].min(), pf_idx3["dt"].min()) if not pf_idx3.empty else pf3["dt"].min()

        fig_hl.update_layout(
            template="plotly_dark", height=650,
            title=dict(text=f"{market} NH-NL (고점-저점 수치) — {nhnl_verdict}",
                       font=dict(size=13, color=trend_color)),
            hovermode="x",
            hoverlabel=HOVER_STYLE,
            margin=dict(l=60, r=20, t=45, b=10),
            xaxis_rangeslider_visible=False,
            legend=dict(orientation="h", y=1.01),
            yaxis =dict(title=f"{market} 지수"),
            yaxis2=dict(title="NH-NL", zeroline=False),
        )
        fig_hl.update_traces(xaxis="x")
        fig_hl.update_xaxes(
            range=[x_min, end_dt3],
            showspikes=True, spikemode="across", spikesnap="cursor",
            spikethickness=1, spikecolor="rgba(200,200,200,0.6)", spikedash="solid",
            tickformat="%Y/%m", dtick="M1",
            tickangle=-45, tickfont=dict(size=9),
            showgrid=True, gridcolor="rgba(80,80,80,0.4)",
        )
        fig_hl.update_yaxes(
            showspikes=True, spikethickness=1,
            spikecolor="rgba(200,200,200,0.4)",
            showgrid=True, gridcolor="rgba(80,80,80,0.3)",
        )
        # 직접 추세선 그리기 버튼
        config = {
            "modeBarButtonsToAdd": ["drawline", "drawopenpath", "eraseshape"],
            "modeBarButtonsToRemove": [],
            "displayModeBar": True,
            "scrollZoom": True,
        }
        st.plotly_chart(fig_hl, use_container_width=True, config=config)

if __name__ == "__main__":
    main()
