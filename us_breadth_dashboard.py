#!/usr/bin/env python3
from __future__ import annotations
# US Market (NYSE / NASDAQ) Breadth Dashboard (Streamlit)
# 스탠 와인스태인 방식: A/D Line, MI 탄력지수, NH-NL, P/D 비율
# 실행: streamlit run us_breadth_dashboard.py

import io
import os
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

try:
    from mplfinance.original_flavor import candlestick_ohlc
    MPL_OK = True
except ImportError:
    MPL_OK = False

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False

try:
    import FinanceDataReader as fdr
    FDR_OK = True
except ImportError:
    FDR_OK = False

# ──────────────────────────────────────────────────────────────
# 심볼 맵
# ADV/DECL: yfinance  ^ADV/^DECL (NYSE),  ^ADVQ/^DECLQ (NASDAQ)
# NH-NL:    yfinance  ^NHNL (NYSE 52주 신고가-신저가 net)
# 지수:     yfinance  ^DJI / ^IXIC
# ──────────────────────────────────────────────────────────────
MARKET_CFG = {
    "NYSE": {
        "adv":   "^ADV",
        "decl":  "^DECL",
        "nhi":   "^HGH",    # NYSE 52주 신고가
        "nlo":   "^LOW",    # NYSE 52주 신저가
        "index": "^DJI",
        "div_fallback": 0.020,
        "label": "NYSE / 다우존스",
    },
    "NASDAQ": {
        "adv":   "^ADVQ",
        "decl":  "^DECLQ",
        "nhi":   "^HGHQ",   # NASDAQ 52주 신고가
        "nlo":   "^LOWQ",   # NASDAQ 52주 신저가
        "index": "^IXIC",
        "div_fallback": 0.007,
        "label": "NASDAQ",
    },
}

STATUS_MAP = {
    "BULLISH_CONFIRMATION":          ("✅ 상승 확인",        "가격·A/D선 모두 고점 동행",       "#2e7d32"),
    "BULLISH_DIVERGENCE":            ("🔴 부정적 불일치",     "가격 고점 / A/D선 크게 뒤처짐",   "#c62828"),
    "BULLISH_DIVERGENCE_CANDIDATE":  ("🟠 초기 경고",         "가격이 A/D선보다 빠르게 회복",    "#ef6c00"),
    "RECOVERY_IN_PROGRESS":          ("🟡 회복 진행 중",       "고점 재공략 중, A/D 미확인",      "#f9a825"),
    "DOWNSIDE_DIVERGENCE_CANDIDATE": ("🟢 긍정적 불일치",      "가격 저점 / A/D선은 더 올라옴",   "#00838f"),
    "NORMAL_WEAKNESS":               ("⚫ 전반적 약세",         "가격·A/D선 모두 저점",            "#455a64"),
    "NEUTRAL":                       ("⬜ 중립",               "뚜렷한 신호 없음",                 "#757575"),
}

# ──────────────────────────────────────────────────────────────
# yfinance 헬퍼
# ──────────────────────────────────────────────────────────────
def _yf_close(sym: str, start: str, end: str) -> pd.Series:
    """yfinance로 종가 Series 반환. start/end는 YYYYMMDD"""
    s = pd.to_datetime(start, format="%Y%m%d").strftime("%Y-%m-%d")
    e = (pd.to_datetime(end, format="%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    raw = yf.download(sym, start=s, end=e, auto_adjust=True, progress=False)
    if raw.empty:
        raise RuntimeError(f"yfinance {sym} 데이터 없음")
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = ["_".join(c).strip() for c in raw.columns]
    col = next((c for c in raw.columns if "close" in c.lower()), None)
    if col is None:
        col = raw.columns[0]
    s_out = raw[col].dropna()
    s_out.index = pd.to_datetime(s_out.index)
    return s_out.sort_index()

# ──────────────────────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_breadth(market: str, start: str, end: str, base: float = 50000.0) -> pd.DataFrame:
    """yfinance ^ADV/^DECL (NYSE) 또는 ^ADVQ/^DECLQ (NASDAQ)"""
    if not YF_OK:
        raise RuntimeError("yfinance 미설치")
    cfg = MARKET_CFG[market]
    adv_s  = _yf_close(cfg["adv"],  start, end)
    decl_s = _yf_close(cfg["decl"], start, end)
    df = pd.DataFrame({"advances": adv_s, "declines": decl_s}).dropna()
    df["ad_diff"] = df["advances"] - df["declines"]
    df["ad_line"] = base + df["ad_diff"].cumsum()
    df["date"]    = df.index.strftime("%Y%m%d")
    return df.reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_index(market: str, start: str, end: str) -> pd.DataFrame:
    """yfinance 지수 OHLC"""
    if not YF_OK:
        raise RuntimeError("yfinance 미설치")
    cfg = MARKET_CFG[market]
    s = pd.to_datetime(start, format="%Y%m%d").strftime("%Y-%m-%d")
    e = (pd.to_datetime(end, format="%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    raw = yf.download(cfg["index"], start=s, end=e, auto_adjust=True, progress=False)
    if raw.empty:
        raise RuntimeError(f"{cfg['index']} 데이터 없음")
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = ["_".join(c).strip() for c in raw.columns]
    raw.columns = [c.lower().split("_")[0] for c in raw.columns]
    raw.index = pd.to_datetime(raw.index)
    out = pd.DataFrame({
        "date":  raw.index.strftime("%Y%m%d"),
        "open":  pd.to_numeric(raw.get("open",  raw.iloc[:, 0]), errors="coerce"),
        "high":  pd.to_numeric(raw.get("high",  raw.iloc[:, 0]), errors="coerce"),
        "low":   pd.to_numeric(raw.get("low",   raw.iloc[:, 0]), errors="coerce"),
        "close": pd.to_numeric(raw.get("close", raw.iloc[:, 0]), errors="coerce"),
    })
    return out.dropna(subset=["close"]).reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_nhnl(market: str, start: str, end: str) -> pd.DataFrame | None:
    """
    yfinance ^HGH/^LOW (NYSE) 또는 ^HGHQ/^LOWQ (NASDAQ) →
    일별 신고가/신저가 종목 수 → 주봉(금요일) 합산
    """
    if not YF_OK:
        return None
    cfg = MARKET_CFG[market]
    try:
        nhi_s = _yf_close(cfg["nhi"], start, end)
        nlo_s = _yf_close(cfg["nlo"], start, end)
        df = pd.DataFrame({"new_highs": nhi_s, "new_lows": nlo_s}).dropna()
        df.index = pd.to_datetime(df.index)
        weekly = df.resample("W-FRI").sum()
        weekly = weekly[weekly["new_highs"] > 0]
        weekly["nhnl"] = weekly["new_highs"] - weekly["new_lows"]
        weekly["date"] = weekly.index.strftime("%Y%m%d")
        return weekly.reset_index(drop=True)
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=86400)
def fetch_pd(market: str, months: int):
    """
    P/D = 지수 종가 ÷ 연간 실제 배당금.
    yfinance ticker.history + ticker.dividends로 실제 배당금 시계열 수집.
    rolling 365일 합산으로 연간 배당금 계산 → 시간에 따라 움직이는 P/D 차트.
    """
    if not YF_OK:
        return None, "yfinance 미설치"
    try:
        cfg     = MARKET_CFG[market]
        end_d   = datetime.today()
        start_d = end_d - timedelta(days=max(365 * 6, months * 35))
        ticker  = yf.Ticker(cfg["index"])

        # 종가
        price_raw = ticker.history(start=start_d.strftime("%Y-%m-%d"),
                                   end=end_d.strftime("%Y-%m-%d"), auto_adjust=True)
        if price_raw.empty:
            return None, f"{cfg['index']} 가격 데이터 없음"
        price_raw.index = pd.to_datetime(price_raw.index).tz_localize(None)
        close_s = price_raw["Close"].sort_index()

        # 실제 배당금 시계열
        divs = ticker.dividends
        if divs is not None and not divs.empty:
            divs.index = pd.to_datetime(divs.index).tz_localize(None)
            divs = divs.sort_index()
            divs_daily  = divs.reindex(close_s.index, fill_value=0.0)
            annual_div  = divs_daily.rolling(365, min_periods=1).sum()
        else:
            annual_div = close_s * cfg["div_fallback"]

        # 주봉 리샘플
        weekly_close = close_s.resample("W-FRI").last().dropna()
        weekly_div   = annual_div.resample("W-FRI").last().reindex(weekly_close.index).ffill()
        weekly_div   = weekly_div.replace(0, float("nan"))
        pd_ratio     = weekly_close / weekly_div

        out = pd.DataFrame({
            "date":         weekly_close.index.strftime("%Y%m%d"),
            "close":        weekly_close.values,
            "dividend_est": weekly_div.values,
            "pd_ratio":     pd_ratio.values,
        }).dropna(subset=["pd_ratio"])
        out["div_yield"] = out["dividend_est"] / out["close"]
        return out.reset_index(drop=True), None
    except Exception as ex:
        return None, str(ex)

# ──────────────────────────────────────────────────────────────
# 판정
# ──────────────────────────────────────────────────────────────
def classify(price_off_high, ad_off_high, gap,
             price_off_low, ad_off_low,
             price_thr=2.0, ad_thr=3.0, gap_warn=1.5, gap_danger=2.5):
    ph = price_off_high >= -price_thr
    ah = ad_off_high    >= -ad_thr
    pl = price_off_low  <= price_thr
    al = ad_off_low     <= ad_thr
    if ph and ah and gap >= -1.0:     return "BULLISH_CONFIRMATION"
    if ph and gap <= -gap_danger:     return "BULLISH_DIVERGENCE"
    if gap <= -gap_warn:              return "BULLISH_DIVERGENCE_CANDIDATE"
    if gap < -1.0:                    return "RECOVERY_IN_PROGRESS"
    if pl and not al:                 return "DOWNSIDE_DIVERGENCE_CANDIDATE"
    if pl and al:                     return "NORMAL_WEAKNESS"
    return "NEUTRAL"

def compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger):
    closes   = df["close"].values.astype(float)
    ad_lines = df["ad_line"].values.astype(float)
    window   = closes[-lookback:]
    peak_idx = window.argmax()
    days_ago = lookback - 1 - peak_idx
    price_high  = window[peak_idx]
    ad_at_peak  = ad_lines[-(days_ago + 1)]
    last_close  = closes[-1]
    last_ad     = ad_lines[-1]
    price_low   = closes[-lookback:].min()
    ad_low      = ad_lines[-lookback:].min()

    price_off     = (last_close - price_high) / abs(price_high) * 100 if price_high else float("nan")
    ad_off        = (last_ad   - ad_at_peak)  / abs(ad_at_peak) * 100 if ad_at_peak else float("nan")
    gap           = ad_off - price_off
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
# 차트
# ──────────────────────────────────────────────────────────────
def make_chart_img(df, market, sig, chart_months):
    end_dt   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    mask     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt
    pf       = df[mask].copy().reset_index(drop=True)
    pf["dt"] = pd.to_datetime(pf["date"].astype(str), format="%Y%m%d")

    ohlc       = pf[["dt","open","high","low","close"]].copy()
    ohlc["dn"] = ohlc["dt"].map(mdates.date2num)
    ohlc_vals  = ohlc[["dn","open","high","low","close"]].values

    days_ago = int(sig["peak_label"].split("일전")[0]) if "일전" in sig["peak_label"] else 0
    peak_dt  = pd.to_datetime(str(df["date"].iloc[-(days_ago + 1)]), format="%Y%m%d")
    peak_dn  = mdates.date2num(peak_dt)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), sharex=True,
                                   gridspec_kw={"height_ratios": [1.4, 1]},
                                   facecolor="#0e1117")
    for ax in (ax1, ax2):
        ax.set_facecolor("#0e1117")
        ax.tick_params(colors="#aaaaaa")
        ax.spines[:].set_color("#333333")

    if MPL_OK:
        candlestick_ohlc(ax1, ohlc_vals, width=0.6,
                         colorup="#26a69a", colordown="#ef5350", alpha=0.9)
    else:
        ax1.plot(pf["dt"], pf["close"].astype(float), color="#26a69a", linewidth=1.5)

    ax1.set_title(MARKET_CFG[market]["label"], color="#e0e0e0", fontsize=13)
    ax1.set_ylabel("Index", color="#aaaaaa")
    ax1.grid(True, color="#1e2530", linewidth=0.5)
    ax1.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    ax1.axhline(y=sig["price_high"], color="orange", linestyle="--", linewidth=1.2, alpha=0.8,
                label=f"Peak {sig['price_high']:,.2f}")
    ax1.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", labelcolor="#e0e0e0")

    ax2.plot(pf["dt"], pf["ad_line"].astype(float), color="#1565c0", linewidth=1.8)
    ax2.set_ylabel("A/D Line", color="#aaaaaa")
    ax2.set_title("A/D Line", color="#e0e0e0", fontsize=11)
    ax2.grid(True, color="#1e2530", linewidth=0.5)
    ax2.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    ax2.axhline(y=sig["ad_at_peak"], color="orange", linestyle="--", linewidth=1.2, alpha=0.8,
                label=f"A/D at Peak {sig['ad_at_peak']:,.0f}")
    ax2.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", labelcolor="#e0e0e0")

    locator   = mdates.AutoDateLocator()
    formatter = mdates.DateFormatter("%Y-%m")
    ax2.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)
    fig.autofmt_xdate(rotation=30, ha="right")

    box_txt = (f"Peak: {sig['peak_label']}\n"
               f"Price vs Peak: {sig['price_off']:.2f}%\n"
               f"A/D vs Peak:   {sig['ad_off']:.2f}%\n"
               f"Gap:           {sig['gap']:.2f}%")
    ax1.text(0.01, 0.97, box_txt, transform=ax1.transAxes,
             va="top", ha="left", fontsize=10, color="white", family="monospace",
             bbox=dict(boxstyle="round,pad=0.5", facecolor=sig["color"], alpha=0.9))

    plt.tight_layout(pad=1.5)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()

# ──────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="미장 브레드스 대시보드",
                       page_icon="🇺🇸", layout="wide")
    st.title("🇺🇸 미국 시장 브레드스 대시보드")
    st.caption("NYSE / NASDAQ — 스탠 와인스태인 브레드스 분석")

    with st.sidebar:
        st.header("⚙️ 설정")
        market = st.selectbox("마켓", ["NYSE", "NASDAQ"])

        today    = datetime.today()
        start_dt = st.date_input("시작일", value=today - timedelta(days=730))
        end_dt   = st.date_input("종료일", value=today)
        fetch_btn = st.button("🔄 데이터 불러오기", type="primary", use_container_width=True)

        st.divider()
        st.subheader("분석 파라미터")
        lookback     = st.slider("Lookback (일)",       20, 252, 126)
        chart_months = st.slider("차트 표시 기간 (월)",  1,  24,   6)
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0,  step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0,  step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5,  step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5,  step=0.1)

    if not fetch_btn and "us_df_merged" not in st.session_state:
        st.info("👈 사이드바에서 마켓 선택 후 **데이터 불러오기** 버튼을 눌러주세요.")
        return

    if fetch_btn:
        if not YF_OK:
            st.error("yfinance 미설치: pip install yfinance")
            return
        start_str = start_dt.strftime("%Y%m%d")
        end_str   = end_dt.strftime("%Y%m%d")
        try:
            with st.spinner("브레드스 데이터 수집 중…"):
                breadth_df = fetch_breadth(market, start_str, end_str)
            with st.spinner("지수 OHLC 수집 중…"):
                index_df   = fetch_index(market, start_str, end_str)
            df = breadth_df.merge(
                index_df[["date","open","high","low","close"]],
                on="date", how="inner"
            ).sort_values("date").reset_index(drop=True)
            st.success(f"✅ {market} 수집 완료 — {len(df)}일치 / 최신: {df['date'].iloc[-1]}")
            st.session_state["us_df_merged"] = df
            st.session_state["us_df_market"] = market

            with st.spinner("NH-NL 수집 중…"):
                nhnl_df = fetch_nhnl(market, start_str, end_str)
            st.session_state["us_nhnl"] = nhnl_df
        except Exception as e:
            st.error(f"데이터 수집 실패: {e}")
            return

    if st.session_state.get("us_df_market") != market:
        st.session_state.pop("us_df_merged", None)
        st.info("마켓이 변경됐습니다. 데이터 불러오기를 다시 눌러주세요.")
        return

    df      = st.session_state["us_df_merged"]
    nhnl_df = st.session_state.get("us_nhnl")

    if len(df) < lookback:
        st.warning(f"데이터 부족: {len(df)}행")
        return

    sig  = compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger)
    last = df.iloc[-1]

    tab1, tab2, tab3, tab4 = st.tabs(["📈 A/D Line", "⚡ MI 탄력지수", "🏔 NH-NL", "📊 P/D 비율"])

    # ══ TAB 1: A/D Line ══
    with tab1:
        gap_color = "#00897b" if sig["gap"] >= 0 else "#c62828"
        gap_arrow = "▲" if sig["gap"] >= 0 else "▼"
        st.markdown(
            f'<div style="text-align:center;padding:6px 0 2px 0">'
            f'<span style="font-size:0.85em;color:#aaaaaa">괴리 (A/D − 가격)</span><br>'
            f'<span style="font-size:2.6em;font-weight:900;color:{gap_color}">'
            f'{gap_arrow} {sig["gap"]:+.2f}%</span>'
            f'<span style="font-size:0.8em;color:#aaaaaa;margin-left:8px">'
            f'기준: {sig["peak_label"]}</span></div>',
            unsafe_allow_html=True,
        )
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("최근 날짜",
                  pd.to_datetime(str(last["date"]), format="%Y%m%d").strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가",  f"{float(last['close']):,.2f}")
        c3.metric("오늘 AD 차이",    f"{int(last['ad_diff']):+,}")
        c4.metric("가격 고점 대비",  f"{sig['price_off']:.2f}%")
        c5.metric("A/D 고점 대비",   f"{sig['ad_off']:.2f}%")

        st.markdown(
            f'<div style="background:{sig["color"]};padding:12px 18px;border-radius:8px;margin:8px 0">'
            f'<b style="font-size:1.2em;color:white">{sig["verdict"]}</b>'
            f'&nbsp;&nbsp;<span style="color:#ffffffcc">{sig["note"]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        try:
            img = make_chart_img(df, market, sig, chart_months)
            st.image(img, use_container_width=True)
        except Exception as e:
            st.error(f"차트 렌더링 실패: {e}")

        with st.expander("📋 원시 데이터"):
            show = df.copy()
            show["date"] = pd.to_datetime(show["date"].astype(str), format="%Y%m%d").dt.strftime("%Y-%m-%d")
            st.dataframe(
                show[["date","advances","declines","ad_diff","ad_line","close"]]
                .sort_values("date", ascending=False).reset_index(drop=True),
                use_container_width=True,
            )

    # ══ TAB 2: MI 탄력지수 ══
    with tab2:
        st.subheader("⚡ MI 탄력지수 (Momentum Index)")
        st.caption("스탠 와인스태인 책 정의: 등락종목수 차이(AD)의 200일 롤링 평균. 0선 위 = 강세.")

        mi_window = st.slider("MA 기간 (기본 200일)", 50, 300, 200, step=10, key="us_mi_win")

        end_dt2   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
        start_dt2 = end_dt2 - pd.DateOffset(months=chart_months)
        mask2     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt2
        pf2       = df[mask2].copy().reset_index(drop=True)
        pf2["dt"] = pd.to_datetime(pf2["date"].astype(str), format="%Y%m%d")

        ad_diff_s = pd.Series(df["ad_diff"].values.astype(float))
        mi_full   = ad_diff_s.rolling(mi_window).mean()
        mi_plot   = mi_full.iloc[mask2.values].reset_index(drop=True)

        last_mi = mi_full.iloc[-1]
        prev_mi = mi_full.iloc[-2] if len(mi_full) >= 2 else last_mi

        if pd.isna(last_mi):
            mi_verdict, mi_color = "⚪ 데이터 부족", "#757575"
        elif last_mi > 0 and last_mi > prev_mi:
            mi_verdict, mi_color = "🟢 강세 상승", "#2e7d32"
        elif last_mi > 0:
            mi_verdict, mi_color = "🟡 강세 둔화", "#f9a825"
        elif last_mi < 0 and last_mi < prev_mi:
            mi_verdict, mi_color = "🔴 약세 하락", "#c62828"
        else:
            mi_verdict, mi_color = "🟠 약세 회복 중", "#ef6c00"

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
        fig_mi.add_hline(y=0, line_color="gray", line_dash="dot", annotation_text="기준선(0)")
        fig_mi.update_layout(
            title=f"{market} MI 탄력지수 — AD차이 {mi_window}일 롤링 평균",
            template="plotly_dark", height=420, yaxis_title="MI 값"
        )
        st.plotly_chart(fig_mi, use_container_width=True)

        if len(df) < mi_window:
            st.warning(f"⚠️ 데이터 {len(df)}일 — {mi_window}일 평균에 데이터 부족.")

    # ══ TAB 3: NH-NL ══
    with tab3:
        st.subheader("🏔 NH-NL (52주 신고가 - 신저가 종목 수)")
        st.caption("스탠 와인스태인 책 정의: 매주 신고가 종목 수 - 신저가 종목 수 (주봉 집계).")

        if nhnl_df is not None and not nhnl_df.empty:
            end_dt3   = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d").max()
            start_dt3 = end_dt3 - pd.DateOffset(months=chart_months)
            mask3     = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d") >= start_dt3
            pf3       = nhnl_df[mask3].copy().reset_index(drop=True)
            pf3["dt"] = pd.to_datetime(pf3["date"].astype(str), format="%Y%m%d")

            nhnl_s       = pd.Series(nhnl_df["nhnl"].values.astype(float))
            nhnl_plot    = pf3["nhnl"]
            nhnl_ma_plot = nhnl_s.rolling(10).mean().iloc[mask3.values].reset_index(drop=True)

            last_nhnl = int(nhnl_s.iloc[-1])
            last_nh   = int(nhnl_df["new_highs"].iloc[-1])
            last_nl   = int(nhnl_df["new_lows"].iloc[-1])
            nhnl_verdict = ("🟢 강세"      if last_nhnl > 100 else
                            "🟢 약한 강세" if last_nhnl > 0   else
                            "🔴 약세"      if last_nhnl < -100 else "🟠 약한 약세")

            n1, n2, n3, n4 = st.columns(4)
            n1.metric("신고가 종목", f"{last_nh:,}")
            n2.metric("신저가 종목", f"{last_nl:,}")
            n3.metric("NH-NL",       f"{last_nhnl:+,}")
            n4.metric("판정",         nhnl_verdict)

            fig_nhnl = go.Figure()
            fig_nhnl.add_trace(go.Bar(
                x=pf3["dt"], y=nhnl_plot,
                marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in nhnl_plot],
                name="NH-NL", opacity=0.8
            ))
            fig_nhnl.add_trace(go.Scatter(
                x=pf3["dt"], y=nhnl_ma_plot,
                line=dict(color="orange", width=1.5), name="10주 MA"
            ))
            fig_nhnl.add_hline(y=0, line_color="gray", line_dash="dot")
            fig_nhnl.update_layout(
                title=f"{market} NH-NL (52주 신고가 - 신저가, 주봉)",
                template="plotly_dark", height=420, yaxis_title="NH-NL 종목 수"
            )
            st.plotly_chart(fig_nhnl, use_container_width=True)
        else:
            st.warning("NH-NL 데이터를 가져오지 못했습니다. 데이터 불러오기를 다시 눌러주세요.")

    # ══ TAB 4: P/D 비율 ══
    with tab4:
        st.subheader("📊 P/D 비율 (Price ÷ Dividend)")
        st.caption(
            "스탠 와인스태인 책 정의: 지수 종가 ÷ 연간 배당금 추정치 (종가 × 연배당수익률). "
            "26 이상 = 위험 / 14~17 = 정상 구간."
        )

        pd_ma_n        = st.slider("MA 기간 (주)", 4, 52, 13, key="us_pd_ma")
        pd_danger_high = st.number_input("위험 기준 (이상)", value=26.0, step=1.0, key="us_pd_dh")
        pd_normal_low  = st.number_input("정상 하단",        value=14.0, step=1.0, key="us_pd_nl")

        with st.spinner("P/D 데이터 로딩 중…"):
            pd_df, pd_err = fetch_pd(market, chart_months)

        if pd_err:
            st.error(f"P/D 오류: {pd_err}")
        elif pd_df is None or pd_df.empty:
            st.warning("P/D 데이터를 가져오지 못했습니다.")
        else:
            end_pd   = pd_df["date"].max()
            start_pd = end_pd - pd.DateOffset(months=chart_months)
            pf4      = pd_df[pd_df["date"] >= start_pd].copy().reset_index(drop=True)

            pd_ratio_s = pd_df["pd_ratio"]
            pd_ma_s    = pd_ratio_s.rolling(pd_ma_n).mean()
            pd_plot    = pf4["pd_ratio"]
            pd_ma_plot = pd_ratio_s.rolling(pd_ma_n).mean().iloc[
                           pd_df["date"] >= start_pd
                         ].reset_index(drop=True)

            last_pd        = pd_ratio_s.iloc[-1]
            last_div_yield = pd_df["div_yield"].iloc[-1]
            last_close     = pd_df["close"].iloc[-1]

            pd_verdict = ("🔴 위험"   if not pd.isna(last_pd) and last_pd >= pd_danger_high else
                          "🟡 주의"   if not pd.isna(last_pd) and last_pd >= pd_danger_high * 0.85 else
                          "🟢 정상"   if not pd.isna(last_pd) and last_pd >= pd_normal_low  else
                          "🟢 저평가" if not pd.isna(last_pd) else "⚪ N/A")

            p1, p2, p3, p4 = st.columns(4)
            p1.metric("지수 종가",    f"{last_close:,.2f}")
            p2.metric("연배당수익률", f"{last_div_yield*100:.2f}%")
            p3.metric("P/D 비율",     f"{last_pd:.1f}" if not pd.isna(last_pd) else "N/A")
            p4.metric("판정",          pd_verdict)

            st.info("📌 배당금(D) = 지수 종가 × 연간 배당수익률. P/D = 종가 ÷ 배당금. "
                    "**26 이상 = 위험**, **14~17 = 정상** (스탠 와인스태인).")

            fig_pd = go.Figure()
            fig_pd.add_hrect(
                y0=pd_danger_high,
                y1=max(pd_plot.dropna().max() + 2 if len(pd_plot.dropna()) > 0 else pd_danger_high + 2,
                       pd_danger_high + 2),
                fillcolor="red", opacity=0.06, line_width=0,
                annotation_text="위험 구간", annotation_position="top left"
            )
            fig_pd.add_hrect(
                y0=0, y1=pd_normal_low,
                fillcolor="teal", opacity=0.06, line_width=0,
                annotation_text="저평가 구간", annotation_position="bottom left"
            )
            fig_pd.add_trace(go.Scatter(
                x=pf4["date"], y=pd_plot,
                line=dict(color="#42a5f5", width=2), name="P/D 비율"
            ))
            fig_pd.add_trace(go.Scatter(
                x=pf4["date"], y=pd_ma_plot,
                line=dict(color="orange", width=1.5, dash="dash"),
                name=f"P/D {pd_ma_n}주 MA"
            ))
            fig_pd.add_hline(y=pd_danger_high, line_color="red",  line_dash="dash",
                             annotation_text=f"위험({pd_danger_high:.0f})")
            fig_pd.add_hline(y=pd_normal_low,  line_color="teal", line_dash="dash",
                             annotation_text=f"정상하단({pd_normal_low:.0f})")
            fig_pd.update_layout(
                title=f"{market} P/D 비율 (주봉, 스탠 와인스태인)",
                template="plotly_dark", height=420,
                legend=dict(orientation="h", y=1.05),
                yaxis_title="P/D 비율"
            )
            st.plotly_chart(fig_pd, use_container_width=True)


if __name__ == "__main__":
    main()
