#!/usr/bin/env python3
from __future__ import annotations
# US Market Breadth Dashboard — 스탠 와인스태인 방식
# 데이터: yfinance (검증된 심볼만 사용)
# AD Line: 다우30 / NASDAQ100 구성종목 일별 등락 집계로 계산

import io
import requests as _requests
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False

# ──────────────────────────────────────────────────────────────
# S&P500 (NYSE 대표) / NASDAQ100 구성종목
# Wikipedia에서 최신 목록 가져옴. 실패 시 하드코딩 fallback 사용.
# ──────────────────────────────────────────────────────────────

# S&P500 fallback (NYSE 전체 대표)
SP500_FALLBACK = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","BRK-B","AVGO","JPM",
    "LLY","UNH","V","XOM","MA","JNJ","PG","HD","COST","MRK","ABBV","BAC",
    "NFLX","KO","CRM","PEP","AMD","TMO","WMT","ORCL","MCD","LIN","CSCO",
    "GE","ABT","ACN","IBM","TXN","CAT","INTU","GS","AMGN","SPGI","DHR",
    "AXP","NOW","RTX","VZ","ISRG","NEE","HON","PFE","MS","BX","BKNG","LOW",
    "UBER","UNP","PM","TJX","AMAT","QCOM","ELV","ETN","PLD","SYK","C","BA",
    "BSX","DE","REGN","VRTX","MDT","CB","ADI","PANW","MU","GILD","ADP","CVS",
    "WM","SO","CME","MMC","PGR","ZTS","SCHW","AMT","CI","DUK","ITW","AON",
    "NOC","APD","FI","ICE","SHW","MCO","EOG","MCK","USB","TGT","EMR","HCA",
    "MMM","WFC","BDX","LRCX","MO","ECL","KLAC","F","SNPS","CDNS","FCX","NSC",
]

# NASDAQ100 fallback
NDX100_FALLBACK = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","COST",
    "NFLX","AMD","ADBE","QCOM","INTU","TXN","AMGN","ISRG","BKNG","VRTX",
    "MU","LRCX","PANW","KLAC","MRVL","AMAT","SNPS","CDNS","ABNB","CRWD",
    "MELI","ORLY","REGN","FTNT","CTAS","PCAR","MNST","CPRT","DXCM","TEAM",
    "KDP","ODFL","ROST","WDAY","PAYX","IDXX","EXC","FAST","GEHC","DLTR",
    "BIIB","VRSK","CTSH","ZS","ANSS","ALGN","ON","CEG","DDOG","TTWO","MRNA",
]

@st.cache_data(show_spinner=False, ttl=86400)
def get_sp500_tickers() -> list[str]:
    """Wikipedia에서 S&P500 티커 목록 가져오기. 실패 시 fallback."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        syms = df["Symbol"].tolist()
        # BRK.B → BRK-B 형식 변환
        syms = [s.replace(".", "-") for s in syms if isinstance(s, str)]
        return [s for s in syms if len(s) <= 5]
    except Exception:
        return SP500_FALLBACK

@st.cache_data(show_spinner=False, ttl=86400)
def get_ndx100_tickers() -> list[str]:
    """Wikipedia에서 NASDAQ100 티커 목록 가져오기. 실패 시 fallback."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        for t in tables:
            cols = [str(c).lower() for c in t.columns]
            if any("ticker" in c or "symbol" in c for c in cols):
                col = t.columns[[i for i, c in enumerate(cols) if "ticker" in c or "symbol" in c][0]]
                syms = t[col].tolist()
                syms = [s.replace(".", "-") for s in syms if isinstance(s, str)]
                return [s for s in syms if len(s) <= 5]
    except Exception:
        pass
    return NDX100_FALLBACK

MARKET_CFG = {
    "NYSE": {
        "get_tickers": get_sp500_tickers,   # S&P500 = NYSE 대표 지수
        "idx_sym":     "^GSPC",             # S&P500 지수
        "cmp_sym":     "SPY",               # 비교용 ETF
        "label":       "NYSE (S&P500 기준)",
        "yf_pd_sym":   "SPY",
        "div_fallback": 0.015,
    },
    "NASDAQ": {
        "get_tickers": get_ndx100_tickers,  # NASDAQ100
        "idx_sym":     "^IXIC",             # NASDAQ Composite
        "cmp_sym":     "QQQ",               # 비교용 ETF
        "label":       "NASDAQ (NDX100 기준)",
        "yf_pd_sym":   "QQQ",
        "div_fallback": 0.006,
    },
}

STATUS_MAP = {
    "BULLISH_CONFIRMATION":          ("✅ 상승 확인",       "가격·A/D선 모두 고점 동행",      "#2e7d32"),
    "BULLISH_DIVERGENCE":            ("🔴 부정적 불일치",    "가격 고점 / A/D선 크게 뒤처짐",  "#c62828"),
    "BULLISH_DIVERGENCE_CANDIDATE":  ("🟠 초기 경고",        "가격이 A/D선보다 빠르게 회복",   "#ef6c00"),
    "RECOVERY_IN_PROGRESS":          ("🟡 회복 진행 중",      "고점 재공략 중, A/D 미확인",     "#f9a825"),
    "DOWNSIDE_DIVERGENCE_CANDIDATE": ("🟢 긍정적 불일치",     "가격 저점 / A/D선은 더 올라옴",  "#00838f"),
    "NORMAL_WEAKNESS":               ("⚫ 전반적 약세",        "가격·A/D선 모두 저점",           "#455a64"),
    "NEUTRAL":                       ("⬜ 중립",              "뚜렷한 신호 없음",                "#757575"),
}

# ──────────────────────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────────────────────
def _yf_download(syms: list[str], start: str, end: str) -> pd.DataFrame:
    """yf.download()로 여러 심볼 종가 일괄 수집. YYYYMMDD → DataFrame(날짜×심볼)"""
    s = pd.to_datetime(start, format="%Y%m%d").strftime("%Y-%m-%d")
    e = (pd.to_datetime(end,   format="%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    raw = yf.download(syms, start=s, end=e, auto_adjust=True, progress=False, threads=True)
    if raw is None or raw.empty:
        raise RuntimeError("yfinance download 결과 없음")
    # Close 추출
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw["Close"]
    else:
        close = raw[["Close"]] if "Close" in raw.columns else raw
    if hasattr(close.index, "tz") and close.index.tz is not None:
        close.index = close.index.tz_localize(None)
    else:
        close.index = pd.to_datetime(close.index)
    return close.sort_index()

def _yf_ticker_history(sym: str, start: str, end: str) -> pd.Series:
    """단일 심볼 종가 Series"""
    s = pd.to_datetime(start, format="%Y%m%d").strftime("%Y-%m-%d")
    e = (pd.to_datetime(end,   format="%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    raw = yf.Ticker(sym).history(start=s, end=e, auto_adjust=True)
    if raw is None or raw.empty:
        raise RuntimeError(f"yfinance {sym} 데이터 없음")
    if hasattr(raw.index, "tz") and raw.index.tz is not None:
        raw.index = raw.index.tz_localize(None)
    else:
        raw.index = pd.to_datetime(raw.index)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    return pd.to_numeric(raw["Close"], errors="coerce").dropna().sort_index()

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_breadth(market: str, start: str, end: str, base: float = 50000.0) -> pd.DataFrame:
    """
    S&P500(NYSE) / NASDAQ100 구성종목 일별 등락으로 AD Line 계산.
    advances = 전일 대비 상승 종목 수, declines = 하락 종목 수.
    """
    if not YF_OK:
        raise RuntimeError("yfinance 미설치")
    cfg     = MARKET_CFG[market]
    tickers = cfg["get_tickers"]()

    close_df = _yf_download(tickers, start, end)
    # 전일 대비 등락
    ret = close_df.pct_change()
    advances = (ret > 0).sum(axis=1)
    declines = (ret < 0).sum(axis=1)
    ad_diff  = (advances - declines).astype(float)

    df = pd.DataFrame({
        "advances": advances.values,
        "declines": declines.values,
        "ad_diff":  ad_diff.values,
    }, index=close_df.index)
    df["ad_line"] = base + df["ad_diff"].cumsum()
    df["date"]    = df.index.strftime("%Y%m%d")
    return df.reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_index(market: str, start: str, end: str) -> pd.DataFrame:
    """지수 OHLC — ^DJI / ^IXIC (yfinance에서 확실히 작동)"""
    if not YF_OK:
        raise RuntimeError("yfinance 미설치")
    cfg = MARKET_CFG[market]
    s = pd.to_datetime(start, format="%Y%m%d").strftime("%Y-%m-%d")
    e = (pd.to_datetime(end,   format="%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    raw = yf.Ticker(cfg["idx_sym"]).history(start=s, end=e, auto_adjust=True)
    if raw is None or raw.empty:
        raise RuntimeError(f"{cfg['idx_sym']} 데이터 없음")
    if hasattr(raw.index, "tz") and raw.index.tz is not None:
        raw.index = raw.index.tz_localize(None)
    else:
        raw.index = pd.to_datetime(raw.index)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    out = pd.DataFrame({
        "date":  raw.index.strftime("%Y%m%d"),
        "open":  pd.to_numeric(raw["Open"],  errors="coerce"),
        "high":  pd.to_numeric(raw["High"],  errors="coerce"),
        "low":   pd.to_numeric(raw["Low"],   errors="coerce"),
        "close": pd.to_numeric(raw["Close"], errors="coerce"),
    })
    return out.dropna(subset=["close"]).reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_nhnl(market: str, start: str, end: str) -> pd.DataFrame | None:
    """52주 신고가/신저가: 구성종목 rolling 260일 고점/저점 대비 오늘 종가."""
    if not YF_OK:
        return None
    cfg     = MARKET_CFG[market]
    tickers = cfg["get_tickers"]()
    # NH-NL 계산은 1년치 추가 데이터 필요
    ext_start = (pd.to_datetime(start, format="%Y%m%d") - timedelta(days=400)).strftime("%Y%m%d")
    try:
        close_df = _yf_download(tickers, ext_start, end)
        if close_df.empty:
            return None
        roll_high = close_df.rolling(260, min_periods=200).max()
        roll_low  = close_df.rolling(260, min_periods=200).min()
        new_highs = (close_df >= roll_high).sum(axis=1)
        new_lows  = (close_df <= roll_low).sum(axis=1)
        # 원래 start 이후만
        start_dt = pd.to_datetime(start, format="%Y%m%d")
        new_highs = new_highs[new_highs.index >= start_dt]
        new_lows  = new_lows[new_lows.index  >= start_dt]
        df = pd.DataFrame({"new_highs": new_highs, "new_lows": new_lows}).dropna()
        weekly_hi = df["new_highs"].resample("W-FRI").last()
        weekly_lo = df["new_lows"].resample("W-FRI").last()
        weekly    = pd.DataFrame({"new_highs": weekly_hi, "new_lows": weekly_lo}).dropna()
        weekly["nhnl"] = weekly["new_highs"] - weekly["new_lows"]
        weekly["date"] = weekly.index.strftime("%Y%m%d")
        return weekly.reset_index(drop=True)
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=86400)
def fetch_pd(market: str, months: int):
    """P/D = 지수 종가 ÷ 연간 실제 배당금 (rolling 365일 합산)"""
    if not YF_OK:
        return None, "yfinance 미설치"
    try:
        cfg     = MARKET_CFG[market]
        end_d   = datetime.today()
        start_d = end_d - timedelta(days=max(365 * 6, months * 35))
        ticker  = yf.Ticker(cfg["yf_pd_sym"])
        ph = ticker.history(start=start_d.strftime("%Y-%m-%d"),
                            end=end_d.strftime("%Y-%m-%d"), auto_adjust=True)
        if ph is None or ph.empty:
            return None, "가격 데이터 없음"
        if hasattr(ph.index, "tz") and ph.index.tz is not None:
            ph.index = ph.index.tz_localize(None)
        else:
            ph.index = pd.to_datetime(ph.index)
        if isinstance(ph.columns, pd.MultiIndex):
            ph.columns = ph.columns.get_level_values(0)
        close_s = pd.to_numeric(ph["Close"], errors="coerce").dropna().sort_index()
        divs = ticker.dividends
        if divs is not None and not divs.empty:
            if hasattr(divs.index, "tz") and divs.index.tz is not None:
                divs.index = divs.index.tz_localize(None)
            else:
                divs.index = pd.to_datetime(divs.index)
            divs_d  = divs.reindex(close_s.index, fill_value=0.0)
            ann_div = divs_d.rolling(365, min_periods=1).sum()
        else:
            ann_div = close_s * cfg["div_fallback"]
        wc  = close_s.resample("W-FRI").last().dropna()
        wd  = ann_div.resample("W-FRI").last().reindex(wc.index).ffill().replace(0, float("nan"))
        pdr = wc / wd
        out = pd.DataFrame({
            "date": wc.index.strftime("%Y%m%d"),
            "close": wc.values, "dividend_est": wd.values, "pd_ratio": pdr.values,
        }).dropna(subset=["pd_ratio"])
        out["div_yield"] = out["dividend_est"] / out["close"]
        out["dt"] = pd.to_datetime(out["date"], format="%Y%m%d")
        return out.reset_index(drop=True), None
    except Exception as ex:
        return None, str(ex)

# ──────────────────────────────────────────────────────────────
# 판정
# ──────────────────────────────────────────────────────────────
def classify(poh, aoh, gap, pol, aol, pt=2.0, at=3.0, gw=1.5, gd=2.5):
    if poh >= -pt and aoh >= -at and gap >= -1.0: return "BULLISH_CONFIRMATION"
    if poh >= -pt and gap <= -gd:                 return "BULLISH_DIVERGENCE"
    if gap <= -gw:                                return "BULLISH_DIVERGENCE_CANDIDATE"
    if gap < -1.0:                                return "RECOVERY_IN_PROGRESS"
    if pol <= pt and not (aol <= at):             return "DOWNSIDE_DIVERGENCE_CANDIDATE"
    if pol <= pt and aol <= at:                   return "NORMAL_WEAKNESS"
    return "NEUTRAL"

def compute_hlab(df: pd.DataFrame, high_bars: int = 60, low_bars: int = 130) -> dict:
    closes  = df["close"].values.astype(float)
    ad_line = df["ad_line"].values.astype(float)
    dts     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d")
    n = len(closes)

    def _safe_slice(arr, end_idx, length):
        start = max(0, end_idx - length)
        return arr[start:end_idx], start

    hb_window, hb_start = _safe_slice(closes, n, high_bars)
    hb_idx_local = int(np.argmax(hb_window))
    hb_idx = hb_start + hb_idx_local
    hb_val, hb_dt, hb_ad = closes[hb_idx], dts.iloc[hb_idx], ad_line[hb_idx]

    ha_window, ha_start = _safe_slice(closes, hb_start + hb_idx_local, high_bars)
    if len(ha_window) > 0:
        ha_idx_local = int(np.argmax(ha_window))
        ha_idx = ha_start + ha_idx_local
        ha_val, ha_dt, ha_ad = closes[ha_idx], dts.iloc[ha_idx], ad_line[ha_idx]
    else:
        ha_val, ha_dt, ha_ad = hb_val, hb_dt, hb_ad

    lb_window, lb_start = _safe_slice(closes, n, low_bars)
    lb_idx_local = int(np.argmin(lb_window))
    lb_idx = lb_start + lb_idx_local
    lb_val, lb_dt, lb_ad = closes[lb_idx], dts.iloc[lb_idx], ad_line[lb_idx]

    la_window, la_start = _safe_slice(closes, lb_start + lb_idx_local, low_bars)
    if len(la_window) > 0:
        la_idx_local = int(np.argmin(la_window))
        la_idx = la_start + la_idx_local
        la_val, la_dt, la_ad = closes[la_idx], dts.iloc[la_idx], ad_line[la_idx]
    else:
        la_val, la_dt, la_ad = lb_val, lb_dt, lb_ad

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

def compute_signals(df, lookback, pt, at, gw, gd):
    closes = df["close"].values.astype(float)
    ads    = df["ad_line"].values.astype(float)
    w      = closes[-lookback:]
    pi     = w.argmax(); da = lookback - 1 - pi
    ph     = w[pi]; ap = ads[-(da + 1)]
    lc     = closes[-1]; la = ads[-1]
    pl     = closes[-lookback:].min(); al = ads[-lookback:].min()
    poff   = (lc - ph) / abs(ph) * 100 if ph else float("nan")
    aoff   = (la - ap) / abs(ap) * 100 if ap else float("nan")
    gap    = aoff - poff
    poll   = (lc - pl) / abs(pl) * 100 if pl else float("nan")
    aoll   = (la - al) / abs(al) * 100 if al else float("nan")
    peak_d = str(df["date"].iloc[-(da + 1)])
    plbl   = "오늘" if da == 0 else f"{da}일전 ({peak_d})"
    sk     = classify(poff, aoff, gap, poll, aoll, pt, at, gw, gd)
    v, n, c = STATUS_MAP[sk]
    return dict(peak_label=plbl, price_off=poff, ad_off=aoff, gap=gap,
                verdict=v, note=n, color=c, last_close=lc, last_ad=la,
                price_high=ph, ad_at_peak=ap)

# ──────────────────────────────────────────────────────────────
# 차트 — Plotly (호버 세로선 + H_a/H_b/L_a/L_b)
# ──────────────────────────────────────────────────────────────
def make_plotly_chart(df, market, sig, chart_months, hlab) -> go.Figure:
    from plotly.subplots import make_subplots

    end_dt   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    mask     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt
    pf       = df[mask].copy().reset_index(drop=True)
    pf["dt"] = pd.to_datetime(pf["date"].astype(str), format="%Y%m%d")

    hb_color = "rgba(255,80,80,0.95)"  if hlab["bear_div"] else "rgba(160,160,160,0.8)"
    ha_color = "rgba(255,140,140,0.6)" if hlab["bear_div"] else "rgba(120,120,120,0.5)"
    lb_color = "rgba(38,210,160,0.95)" if hlab["bull_div"] else "rgba(160,160,160,0.8)"
    la_color = "rgba(38,210,160,0.6)"  if hlab["bull_div"] else "rgba(120,120,120,0.5)"

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.52, 0.48], vertical_spacing=0.03,
                        subplot_titles=(f"{MARKET_CFG[market]['label']}", "A/D Line (가격 겹침)"))

    fig.add_trace(go.Candlestick(
        x=pf["dt"], open=pf["open"], high=pf["high"], low=pf["low"], close=pf["close"],
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        name=market, showlegend=False,
    ), row=1, col=1)

    for val, color, dash, label in [
        (hlab["hb_val"], hb_color, "dash", f"H_b {hlab['hb_val']:,.2f}"),
        (hlab["ha_val"], ha_color, "dot",  f"H_a {hlab['ha_val']:,.2f}"),
        (hlab["lb_val"], lb_color, "dash", f"L_b {hlab['lb_val']:,.2f}"),
        (hlab["la_val"], la_color, "dot",  f"L_a {hlab['la_val']:,.2f}"),
    ]:
        fig.add_hline(y=val, line_color=color, line_dash=dash, line_width=1.5,
                      annotation_text=label, annotation_font_color=color,
                      annotation_font_size=11, row=1, col=1)

    fig.add_trace(go.Scatter(
        x=pf["dt"], y=pf["ad_line"].astype(float),
        line=dict(color="#1e88e5", width=2.5), name="A/D Line",
    ), row=2, col=1)

    ad_min = pf["ad_line"].min(); ad_max = pf["ad_line"].max()
    pr_min = pf["close"].min();   pr_max = pf["close"].max()
    if pr_max != pr_min:
        price_mapped = ad_min + (pf["close"] - pr_min) / (pr_max - pr_min) * (ad_max - ad_min)
    else:
        price_mapped = pf["ad_line"]
    fig.add_trace(go.Scatter(
        x=pf["dt"], y=price_mapped,
        line=dict(color="rgba(180,180,180,0.45)", width=1.2),
        name="가격(겹침)",
    ), row=2, col=1)

    for val, color, dash, label in [
        (hlab["hb_ad"], hb_color, "dash", f"A/D@H_b {hlab['hb_ad']:,.0f}"),
        (hlab["ha_ad"], ha_color, "dot",  f"A/D@H_a {hlab['ha_ad']:,.0f}"),
        (hlab["lb_ad"], lb_color, "dash", f"A/D@L_b {hlab['lb_ad']:,.0f}"),
        (hlab["la_ad"], la_color, "dot",  f"A/D@L_a {hlab['la_ad']:,.0f}"),
    ]:
        fig.add_hline(y=val, line_color=color, line_dash=dash, line_width=1.5,
                      annotation_text=label, annotation_font_color=color,
                      annotation_font_size=10, row=2, col=1)

    if hlab["bear_div"]:
        fig.add_shape(type="line",
            x0=hlab["ha_dt"], y0=hlab["ha_ad"], x1=hlab["hb_dt"], y1=hlab["hb_ad"],
            line=dict(color="rgba(255,80,80,0.85)", width=2, dash="dash"), row=2, col=1)
        mid_dt = hlab["ha_dt"] + (hlab["hb_dt"] - hlab["ha_dt"]) / 2
        fig.add_annotation(x=mid_dt, y=(hlab["ha_ad"]+hlab["hb_ad"])/2,
                           text=f"⚠ {hlab['bear_div_pct']:.1f}%",
                           font=dict(color="rgba(255,80,80,0.9)", size=11),
                           showarrow=False, row=2, col=1)
    if hlab["bull_div"]:
        fig.add_shape(type="line",
            x0=hlab["la_dt"], y0=hlab["la_ad"], x1=hlab["lb_dt"], y1=hlab["lb_ad"],
            line=dict(color="rgba(38,210,160,0.85)", width=2, dash="dash"), row=2, col=1)
        mid_dt = hlab["la_dt"] + (hlab["lb_dt"] - hlab["la_dt"]) / 2
        fig.add_annotation(x=mid_dt, y=(hlab["la_ad"]+hlab["lb_ad"])/2,
                           text=f"✓ {hlab['bull_div_pct']:.1f}%",
                           font=dict(color="rgba(38,210,160,0.9)", size=11),
                           showarrow=False, row=2, col=1)

    div_text = (f"  ⚠ 부정적 불일치 {hlab['bear_div_pct']:.1f}%" if hlab["bear_div"]
                else f"  ✓ 긍정적 불일치 {hlab['bull_div_pct']:.1f}%" if hlab["bull_div"] else "")

    fig.update_layout(
        template="plotly_dark", height=680,
        title=dict(text=f"{MARKET_CFG[market]['label']} — {sig['verdict']}{div_text}", font_size=13),
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.01, x=0),
        margin=dict(l=10, r=80, t=55, b=10),
    )
    fig.update_xaxes(
        showspikes=True, spikemode="across", spikesnap="cursor",
        spikethickness=1, spikecolor="#aaa", spikedash="dot",
        tickformat="%m/%d", dtick=7*24*60*60*1000,
        tickangle=-45, tickfont=dict(size=9),
    )
    fig.update_yaxes(showspikes=True, spikethickness=1, spikecolor="#aaa")
    return fig

# ──────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="미장 브레드스 대시보드", page_icon="🇺🇸", layout="wide")
    st.title("🇺🇸 미국 시장 브레드스 대시보드")
    st.caption("NYSE(다우30) / NASDAQ100 — 스탠 와인스태인 브레드스 분석")

    with st.sidebar:
        st.header("⚙️ 설정")
        market   = st.selectbox("마켓", ["NYSE", "NASDAQ"])
        today    = datetime.today()
        start_dt = st.date_input("시작일", value=today - timedelta(days=730))
        end_dt   = st.date_input("종료일", value=today)
        fetch_btn = st.button("🔄 데이터 불러오기", type="primary", use_container_width=True)
        st.divider()
        st.subheader("분석 파라미터")
        lookback     = st.slider("Lookback (일)",      20, 252, 126)
        chart_months = st.slider("차트 표시 기간 (월)", 1,  24,   6)
        high_bars    = st.slider("고점 탐색 H_b (일)",  10, 500, 60)
        low_bars     = st.slider("저점 탐색 L_b (일)",  10, 500, 130)
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0, step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0, step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5, step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5, step=0.1)

    if not fetch_btn and "us_df_merged" not in st.session_state:
        st.info("👈 사이드바에서 마켓 선택 후 **데이터 불러오기** 버튼을 눌러주세요.")
        return

    if fetch_btn:
        if not YF_OK:
            st.error("yfinance 미설치"); return
        start_str = start_dt.strftime("%Y%m%d")
        end_str   = end_dt.strftime("%Y%m%d")
        try:
            with st.spinner("구성종목 수집 중… (30~60초 소요)"):
                breadth_df = fetch_breadth(market, start_str, end_str)
            with st.spinner("지수 OHLC 수집 중…"):
                index_df = fetch_index(market, start_str, end_str)
            df = breadth_df.merge(
                index_df[["date", "open", "high", "low", "close"]], on="date", how="inner"
            ).sort_values("date").reset_index(drop=True)
            st.success(f"✅ {market} 완료 — {len(df)}일치 / 최신: {df['date'].iloc[-1]}")
            st.session_state["us_df_merged"] = df
            st.session_state["us_df_market"] = market
            with st.spinner("NH-NL 계산 중…"):
                nhnl_df = fetch_nhnl(market, start_str, end_str)
            st.session_state["us_nhnl"] = nhnl_df
        except Exception as e:
            st.error(f"데이터 수집 실패: {e}"); return

    if st.session_state.get("us_df_market") != market:
        st.session_state.pop("us_df_merged", None)
        st.info("마켓이 변경됐습니다. 데이터 불러오기를 다시 눌러주세요."); return

    df      = st.session_state["us_df_merged"]
    nhnl_df = st.session_state.get("us_nhnl")
    if len(df) < lookback:
        st.warning(f"데이터 부족: {len(df)}행"); return

    sig  = compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger)
    hlab = compute_hlab(df, high_bars=high_bars, low_bars=low_bars)
    last = df.iloc[-1]
    tab1, tab2, tab3 = st.tabs(["📈 A/D Line", "⚡ MI 탄력지수", "🏔 NH-NL"])

    with tab1:
        gc = "#00897b" if sig["gap"] >= 0 else "#c62828"
        ga = "▲" if sig["gap"] >= 0 else "▼"
        st.markdown(
            f'<div style="text-align:center;padding:6px 0 2px 0">'
            f'<span style="font-size:0.85em;color:#aaa">괴리 (A/D − 가격)</span><br>'
            f'<span style="font-size:2.6em;font-weight:900;color:{gc}">{ga} {sig["gap"]:+.2f}%</span>'
            f'<span style="font-size:0.8em;color:#aaa;margin-left:8px">기준: {sig["peak_label"]}</span></div>',
            unsafe_allow_html=True)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("최근 날짜", pd.to_datetime(str(last["date"]), format="%Y%m%d").strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가", f"{float(last['close']):,.2f}")
        c3.metric("오늘 AD 차이",   f"{float(last['ad_diff']):+,.0f}")
        c4.metric("가격 고점 대비", f"{sig['price_off']:.2f}%")
        c5.metric("A/D 고점 대비",  f"{sig['ad_off']:.2f}%")
        st.markdown(
            f'<div style="background:{sig["color"]};padding:12px 18px;border-radius:8px;margin:8px 0">'
            f'<b style="font-size:1.2em;color:white">{sig["verdict"]}</b>'
            f'&nbsp;&nbsp;<span style="color:#ffffffcc">{sig["note"]}</span></div>',
            unsafe_allow_html=True)
        try:
            st.plotly_chart(make_plotly_chart(df, market, sig, chart_months, hlab), use_container_width=True)
        except Exception as e:
            st.error(f"차트 오류: {e}")
        with st.expander("📋 원시 데이터"):
            show = df.copy()
            show["date"] = pd.to_datetime(show["date"].astype(str), format="%Y%m%d").dt.strftime("%Y-%m-%d")
            st.dataframe(show[["date", "ad_diff", "ad_line", "close"]].sort_values("date", ascending=False).reset_index(drop=True), use_container_width=True)

    with tab2:
        st.subheader("⚡ MI 탄력지수 (Momentum Index)")
        st.caption("스탠 와인스태인: 등락종목수 차이(AD)의 200일 롤링 평균. 0선 위=강세.")
        mi_w  = st.slider("MA 기간", 50, 300, 200, step=10, key="us_mi")
        end2  = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
        mask2 = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= end2 - pd.DateOffset(months=chart_months)
        pf2   = df[mask2].copy(); pf2["dt"] = pd.to_datetime(pf2["date"].astype(str), format="%Y%m%d")
        ads   = pd.Series(df["ad_diff"].values.astype(float))
        mif   = ads.rolling(mi_w).mean()
        mip   = mif.iloc[mask2.values].reset_index(drop=True)
        lm    = mif.iloc[-1]; pm = mif.iloc[-2] if len(mif) >= 2 else lm
        if pd.isna(lm):              mv, mc = "⚪ 데이터 부족", "#757575"
        elif lm > 0 and lm > pm:    mv, mc = "🟢 강세 상승", "#2e7d32"
        elif lm > 0:                 mv, mc = "🟡 강세 둔화", "#f9a825"
        elif lm < 0 and lm < pm:    mv, mc = "🔴 약세 하락", "#c62828"
        else:                        mv, mc = "🟠 약세 회복 중", "#ef6c00"
        m1, m2, m3 = st.columns(3)
        m1.metric(f"MI ({mi_w}일)", f"{lm:+.1f}" if not pd.isna(lm) else "N/A")
        m2.metric("전일 대비", f"{lm - pm:+.1f}" if not pd.isna(lm) else "N/A")
        m3.metric("판정", mv)
        fig_mi = go.Figure()
        fig_mi.add_trace(go.Bar(x=pf2["dt"], y=mip,
            marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in mip.fillna(0)],
            name=f"MI ({mi_w}일)", opacity=0.85))
        fig_mi.add_hline(y=0, line_color="gray", line_dash="dot", annotation_text="기준선(0)")
        fig_mi.update_layout(title=f"{market} MI 탄력지수", template="plotly_dark", height=420, yaxis_title="MI")
        st.plotly_chart(fig_mi, use_container_width=True)

    with tab3:
        st.subheader("🏔 NH-NL (52주 신고가 - 신저가 종목 수)")
        st.caption("구성종목 기준 260거래일 롤링 신고가/신저가 종목 수 (주봉).")
        if nhnl_df is not None and not nhnl_df.empty:
            end3  = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d").max()
            mask3 = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d") >= end3 - pd.DateOffset(months=chart_months)
            pf3   = nhnl_df[mask3].copy(); pf3["dt"] = pd.to_datetime(pf3["date"].astype(str), format="%Y%m%d")
            ns    = pd.Series(nhnl_df["nhnl"].values.astype(float))
            nma   = ns.rolling(10).mean().iloc[mask3.values].reset_index(drop=True)
            ln    = int(ns.iloc[-1]); lh = int(nhnl_df["new_highs"].iloc[-1]); ll = int(nhnl_df["new_lows"].iloc[-1])
            nv    = ("🟢 강세" if ln > 0 else "🔴 약세")
            n1, n2, n3, n4 = st.columns(4)
            n1.metric("신고가 종목", f"{lh}"); n2.metric("신저가 종목", f"{ll}")
            n3.metric("NH-NL", f"{ln:+}");    n4.metric("판정", nv)
            fig_n = go.Figure()
            fig_n.add_trace(go.Bar(x=pf3["dt"], y=pf3["nhnl"],
                marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in pf3["nhnl"]], name="NH-NL", opacity=0.8))
            fig_n.add_trace(go.Scatter(x=pf3["dt"], y=nma, line=dict(color="orange", width=1.5), name="10주 MA"))
            fig_n.add_hline(y=0, line_color="gray", line_dash="dot")
            fig_n.update_layout(title=f"{market} NH-NL (주봉)", template="plotly_dark", height=420, yaxis_title="NH-NL")
            st.plotly_chart(fig_n, use_container_width=True)
        else:
            st.warning("NH-NL 데이터를 가져오지 못했습니다.")


if __name__ == "__main__":
    main()
