#!/usr/bin/env python3
from __future__ import annotations
# US Market (NYSE / NASDAQ) Breadth Dashboard (Streamlit)
# 스탠 와인스태인 방식: A/D Line, MI 탄력지수, NH-NL, P/D 비율
# 데이터 소스: pandas_datareader (Stooq) + yfinance (P/D 배당금)

import io
from datetime import datetime, timedelta

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
    import pandas_datareader.data as web
    PDR_OK = True
except ImportError:
    PDR_OK = False

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False

# ──────────────────────────────────────────────────────────────
# Stooq 심볼
# $ADVN/$DECLN: NYSE 상승/하락 종목 수
# $ADVQ/$DECLQ: NASDAQ 상승/하락 종목 수
# $HIGN/$LOWN:  NYSE 52주 신고가/신저가
# $HIGNQ/$LOWNQ: NASDAQ 52주 신고가/신저가
# ^DJI: 다우존스, ^NDQ: NASDAQ Composite
# ──────────────────────────────────────────────────────────────
MARKET_CFG = {
    "NYSE": {
        "adv":  "$ADVN",   "decl": "$DECLN",
        "nhi":  "$HIGN",   "nlo":  "$LOWN",
        "idx":  "^DJI",
        "div_fallback": 0.020,
        "label": "NYSE / 다우존스",
    },
    "NASDAQ": {
        "adv":  "$ADVQ",   "decl": "$DECLQ",
        "nhi":  "$HIGNQ",  "nlo":  "$LOWNQ",
        "idx":  "^NDQ",
        "div_fallback": 0.007,
        "label": "NASDAQ",
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
# 데이터 수집 헬퍼
# ──────────────────────────────────────────────────────────────
def _stooq(sym: str, start: str, end: str) -> pd.Series:
    """pandas_datareader Stooq로 종가 Series 반환. start/end: YYYYMMDD"""
    if not PDR_OK:
        raise RuntimeError("pandas-datareader 미설치")
    s = pd.to_datetime(start, format="%Y%m%d")
    e = pd.to_datetime(end,   format="%Y%m%d")
    df = web.DataReader(sym, "stooq", s, e)
    if df is None or df.empty:
        raise RuntimeError(f"Stooq {sym} 데이터 없음")
    df = df.sort_index()
    col = "Close" if "Close" in df.columns else df.columns[0]
    return pd.to_numeric(df[col], errors="coerce").dropna()

def _stooq_ohlc(sym: str, start: str, end: str) -> pd.DataFrame:
    """pandas_datareader Stooq로 OHLC DataFrame 반환"""
    if not PDR_OK:
        raise RuntimeError("pandas-datareader 미설치")
    s = pd.to_datetime(start, format="%Y%m%d")
    e = pd.to_datetime(end,   format="%Y%m%d")
    df = web.DataReader(sym, "stooq", s, e)
    if df is None or df.empty:
        raise RuntimeError(f"Stooq {sym} 데이터 없음")
    df = df.sort_index()
    df.columns = [c.lower() for c in df.columns]
    df["date"] = df.index.strftime("%Y%m%d")
    for c in ["open","high","low","close"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["date","open","high","low","close"]].dropna(subset=["close"]).reset_index(drop=True)

# ──────────────────────────────────────────────────────────────
# 캐시 함수
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_breadth(market: str, start: str, end: str, base: float = 50000.0) -> pd.DataFrame:
    cfg   = MARKET_CFG[market]
    adv_s = _stooq(cfg["adv"],  start, end)
    dec_s = _stooq(cfg["decl"], start, end)
    df = pd.DataFrame({"advances": adv_s, "declines": dec_s}).dropna()
    df["ad_diff"] = df["advances"] - df["declines"]
    df["ad_line"] = base + df["ad_diff"].cumsum()
    df["date"]    = df.index.strftime("%Y%m%d")
    return df.reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_index(market: str, start: str, end: str) -> pd.DataFrame:
    return _stooq_ohlc(MARKET_CFG[market]["idx"], start, end)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_nhnl(market: str, start: str, end: str) -> pd.DataFrame | None:
    cfg = MARKET_CFG[market]
    try:
        nhi_s = _stooq(cfg["nhi"], start, end)
        nlo_s = _stooq(cfg["nlo"], start, end)
        df = pd.DataFrame({"new_highs": nhi_s, "new_lows": nlo_s}).dropna()
        weekly = df.resample("W-FRI").sum()
        weekly = weekly[weekly["new_highs"] > 0]
        weekly["nhnl"] = weekly["new_highs"] - weekly["new_lows"]
        weekly["date"] = weekly.index.strftime("%Y%m%d")
        return weekly.reset_index(drop=True)
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=86400)
def fetch_pd(market: str, months: int):
    """P/D = 지수 종가 ÷ 연간 실제 배당금 (yfinance ticker.dividends)"""
    if not YF_OK:
        return None, "yfinance 미설치"
    try:
        cfg    = MARKET_CFG[market]
        end_d  = datetime.today()
        start_d = end_d - timedelta(days=max(365 * 6, months * 35))
        yf_sym = "^DJI" if market == "NYSE" else "^IXIC"
        ticker = yf.Ticker(yf_sym)

        price_raw = ticker.history(start=start_d.strftime("%Y-%m-%d"),
                                   end=end_d.strftime("%Y-%m-%d"), auto_adjust=True)
        if price_raw.empty:
            return None, f"{yf_sym} 가격 없음"
        price_raw.index = pd.to_datetime(price_raw.index).tz_localize(None)
        close_s = price_raw["Close"].sort_index()

        divs = ticker.dividends
        if divs is not None and not divs.empty:
            divs.index = pd.to_datetime(divs.index).tz_localize(None)
            divs_daily = divs.reindex(close_s.index, fill_value=0.0)
            annual_div = divs_daily.rolling(365, min_periods=1).sum()
        else:
            annual_div = close_s * cfg["div_fallback"]

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
        out["dt"] = pd.to_datetime(out["date"], format="%Y%m%d")
        return out.reset_index(drop=True), None
    except Exception as ex:
        return None, str(ex)

# ──────────────────────────────────────────────────────────────
# 판정
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
    price_high  = window[peak_idx]
    ad_at_peak  = ad_lines[-(days_ago + 1)]
    last_close  = closes[-1];  last_ad = ad_lines[-1]
    price_low   = closes[-lookback:].min(); ad_low = ad_lines[-lookback:].min()
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
    ohlc      = pf[["dt","open","high","low","close"]].copy()
    ohlc["dn"] = ohlc["dt"].map(mdates.date2num)
    ohlc_vals  = ohlc[["dn","open","high","low","close"]].values
    days_ago = int(sig["peak_label"].split("일전")[0]) if "일전" in sig["peak_label"] else 0
    peak_dt  = pd.to_datetime(str(df["date"].iloc[-(days_ago + 1)]), format="%Y%m%d")
    peak_dn  = mdates.date2num(peak_dt)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), sharex=True,
                                   gridspec_kw={"height_ratios": [1.4, 1]},
                                   facecolor="#0e1117")
    for ax in (ax1, ax2):
        ax.set_facecolor("#0e1117"); ax.tick_params(colors="#aaaaaa")
        ax.spines[:].set_color("#333333")
    if MPL_OK:
        candlestick_ohlc(ax1, ohlc_vals, width=0.6, colorup="#26a69a", colordown="#ef5350", alpha=0.9)
    else:
        ax1.plot(pf["dt"], pf["close"].astype(float), color="#26a69a", linewidth=1.5)
    ax1.set_title(MARKET_CFG[market]["label"], color="#e0e0e0", fontsize=13)
    ax1.set_ylabel("Index", color="#aaaaaa"); ax1.grid(True, color="#1e2530", linewidth=0.5)
    ax1.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    ax1.axhline(y=sig["price_high"], color="orange", linestyle="--", linewidth=1.2, alpha=0.8,
                label=f"Peak {sig['price_high']:,.2f}")
    ax1.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", labelcolor="#e0e0e0")
    ax2.plot(pf["dt"], pf["ad_line"].astype(float), color="#1565c0", linewidth=1.8)
    ax2.set_ylabel("A/D Line", color="#aaaaaa"); ax2.set_title("A/D Line", color="#e0e0e0", fontsize=11)
    ax2.grid(True, color="#1e2530", linewidth=0.5)
    ax2.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    ax2.axhline(y=sig["ad_at_peak"], color="orange", linestyle="--", linewidth=1.2, alpha=0.8,
                label=f"A/D at Peak {sig['ad_at_peak']:,.0f}")
    ax2.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", labelcolor="#e0e0e0")
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate(rotation=30, ha="right")
    box_txt = (f"Peak: {sig['peak_label']}\nPrice vs Peak: {sig['price_off']:.2f}%\n"
               f"A/D vs Peak:   {sig['ad_off']:.2f}%\nGap:           {sig['gap']:.2f}%")
    ax1.text(0.01, 0.97, box_txt, transform=ax1.transAxes, va="top", ha="left",
             fontsize=10, color="white", family="monospace",
             bbox=dict(boxstyle="round,pad=0.5", facecolor=sig["color"], alpha=0.9))
    plt.tight_layout(pad=1.5)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig); buf.seek(0)
    return buf.read()

# ──────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="미장 브레드스 대시보드", page_icon="🇺🇸", layout="wide")
    st.title("🇺🇸 미국 시장 브레드스 대시보드")
    st.caption("NYSE / NASDAQ — 스탠 와인스태인 브레드스 분석")

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
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0, step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0, step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5, step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5, step=0.1)

    if not fetch_btn and "us_df_merged" not in st.session_state:
        st.info("👈 사이드바에서 마켓 선택 후 **데이터 불러오기** 버튼을 눌러주세요.")
        return

    if fetch_btn:
        if not PDR_OK:
            st.error("pandas-datareader 미설치: pip install pandas-datareader")
            return
        start_str = start_dt.strftime("%Y%m%d")
        end_str   = end_dt.strftime("%Y%m%d")
        try:
            with st.spinner("브레드스 수집 중…"):
                breadth_df = fetch_breadth(market, start_str, end_str)
            with st.spinner("지수 OHLC 수집 중…"):
                index_df   = fetch_index(market, start_str, end_str)
            df = breadth_df.merge(
                index_df[["date","open","high","low","close"]], on="date", how="inner"
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
        st.warning(f"데이터 부족: {len(df)}행"); return

    sig  = compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger)
    last = df.iloc[-1]
    tab1, tab2, tab3, tab4 = st.tabs(["📈 A/D Line", "⚡ MI 탄력지수", "🏔 NH-NL", "📊 P/D 비율"])

    # ══ TAB 1 ══
    with tab1:
        gc = "#00897b" if sig["gap"] >= 0 else "#c62828"
        ga = "▲" if sig["gap"] >= 0 else "▼"
        st.markdown(
            f'<div style="text-align:center;padding:6px 0 2px 0">'
            f'<span style="font-size:0.85em;color:#aaa">괴리 (A/D − 가격)</span><br>'
            f'<span style="font-size:2.6em;font-weight:900;color:{gc}">{ga} {sig["gap"]:+.2f}%</span>'
            f'<span style="font-size:0.8em;color:#aaa;margin-left:8px">기준: {sig["peak_label"]}</span></div>',
            unsafe_allow_html=True)
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("최근 날짜", pd.to_datetime(str(last["date"]), format="%Y%m%d").strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가", f"{float(last['close']):,.2f}")
        c3.metric("오늘 AD 차이",   f"{int(last['ad_diff']):+,}")
        c4.metric("가격 고점 대비", f"{sig['price_off']:.2f}%")
        c5.metric("A/D 고점 대비",  f"{sig['ad_off']:.2f}%")
        st.markdown(
            f'<div style="background:{sig["color"]};padding:12px 18px;border-radius:8px;margin:8px 0">'
            f'<b style="font-size:1.2em;color:white">{sig["verdict"]}</b>'
            f'&nbsp;&nbsp;<span style="color:#ffffffcc">{sig["note"]}</span></div>',
            unsafe_allow_html=True)
        try:
            st.image(make_chart_img(df, market, sig, chart_months), use_container_width=True)
        except Exception as e:
            st.error(f"차트 렌더링 실패: {e}")
        with st.expander("📋 원시 데이터"):
            show = df.copy()
            show["date"] = pd.to_datetime(show["date"].astype(str), format="%Y%m%d").dt.strftime("%Y-%m-%d")
            st.dataframe(show[["date","advances","declines","ad_diff","ad_line","close"]]
                         .sort_values("date", ascending=False).reset_index(drop=True),
                         use_container_width=True)

    # ══ TAB 2: MI ══
    with tab2:
        st.subheader("⚡ MI 탄력지수 (Momentum Index)")
        st.caption("스탠 와인스태인: 등락종목수 차이(AD)의 200일 롤링 평균. 0선 위=강세.")
        mi_w = st.slider("MA 기간", 50, 300, 200, step=10, key="us_mi")
        end_dt2   = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
        mask2     = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= end_dt2 - pd.DateOffset(months=chart_months)
        pf2       = df[mask2].copy(); pf2["dt"] = pd.to_datetime(pf2["date"].astype(str), format="%Y%m%d")
        ad_s      = pd.Series(df["ad_diff"].values.astype(float))
        mi_full   = ad_s.rolling(mi_w).mean()
        mi_plot   = mi_full.iloc[mask2.values].reset_index(drop=True)
        last_mi   = mi_full.iloc[-1]; prev_mi = mi_full.iloc[-2] if len(mi_full) >= 2 else last_mi
        if pd.isna(last_mi):              mv, mc = "⚪ 데이터 부족", "#757575"
        elif last_mi > 0 and last_mi > prev_mi: mv, mc = "🟢 강세 상승", "#2e7d32"
        elif last_mi > 0:                 mv, mc = "🟡 강세 둔화",  "#f9a825"
        elif last_mi < 0 and last_mi < prev_mi: mv, mc = "🔴 약세 하락", "#c62828"
        else:                             mv, mc = "🟠 약세 회복 중", "#ef6c00"
        m1,m2,m3 = st.columns(3)
        m1.metric(f"MI ({mi_w}일)", f"{last_mi:+.1f}" if not pd.isna(last_mi) else "N/A")
        m2.metric("전일 대비",      f"{last_mi-prev_mi:+.1f}" if not pd.isna(last_mi) else "N/A")
        m3.metric("판정", mv)
        fig_mi = go.Figure()
        fig_mi.add_trace(go.Bar(x=pf2["dt"], y=mi_plot,
            marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in mi_plot.fillna(0)],
            name=f"MI ({mi_w}일)", opacity=0.85))
        fig_mi.add_hline(y=0, line_color="gray", line_dash="dot", annotation_text="기준선(0)")
        fig_mi.update_layout(title=f"{market} MI 탄력지수 — AD {mi_w}일 롤링 평균",
                             template="plotly_dark", height=420, yaxis_title="MI")
        st.plotly_chart(fig_mi, use_container_width=True)
        if len(df) < mi_w:
            st.warning(f"⚠️ 데이터 {len(df)}일 — {mi_w}일 평균에 부족.")

    # ══ TAB 3: NH-NL ══
    with tab3:
        st.subheader("🏔 NH-NL (52주 신고가 - 신저가 종목 수)")
        st.caption("스탠 와인스태인: 매주 신고가 종목 수 - 신저가 종목 수 (주봉 집계).")
        if nhnl_df is not None and not nhnl_df.empty:
            end_dt3  = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d").max()
            mask3    = pd.to_datetime(nhnl_df["date"].astype(str), format="%Y%m%d") >= end_dt3 - pd.DateOffset(months=chart_months)
            pf3      = nhnl_df[mask3].copy(); pf3["dt"] = pd.to_datetime(pf3["date"].astype(str), format="%Y%m%d")
            ns       = pd.Series(nhnl_df["nhnl"].values.astype(float))
            nma      = ns.rolling(10).mean().iloc[mask3.values].reset_index(drop=True)
            ln       = int(ns.iloc[-1]); lh = int(nhnl_df["new_highs"].iloc[-1]); ll = int(nhnl_df["new_lows"].iloc[-1])
            nv       = ("🟢 강세" if ln>100 else "🟢 약한 강세" if ln>0 else "🔴 약세" if ln<-100 else "🟠 약한 약세")
            n1,n2,n3,n4 = st.columns(4)
            n1.metric("신고가 종목", f"{lh:,}"); n2.metric("신저가 종목", f"{ll:,}")
            n3.metric("NH-NL", f"{ln:+,}");    n4.metric("판정", nv)
            fig_n = go.Figure()
            fig_n.add_trace(go.Bar(x=pf3["dt"], y=pf3["nhnl"],
                marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in pf3["nhnl"]], name="NH-NL", opacity=0.8))
            fig_n.add_trace(go.Scatter(x=pf3["dt"], y=nma, line=dict(color="orange", width=1.5), name="10주 MA"))
            fig_n.add_hline(y=0, line_color="gray", line_dash="dot")
            fig_n.update_layout(title=f"{market} NH-NL (52주 신고가-신저가, 주봉)",
                                template="plotly_dark", height=420, yaxis_title="NH-NL 종목 수")
            st.plotly_chart(fig_n, use_container_width=True)
        else:
            st.warning("NH-NL 데이터를 가져오지 못했습니다. 데이터 불러오기를 다시 눌러주세요.")

    # ══ TAB 4: P/D ══
    with tab4:
        st.subheader("📊 P/D 비율 (Price ÷ Dividend)")
        st.caption("스탠 와인스태인: 지수 종가 ÷ 연간 배당금 (실제 배당금 시계열). 26↑=위험, 14~17=정상.")
        pd_ma_n        = st.slider("MA 기간 (주)", 4, 52, 13, key="us_pd_ma")
        pd_danger_high = st.number_input("위험 기준 (이상)", value=26.0, step=1.0, key="us_pd_dh")
        pd_normal_low  = st.number_input("정상 하단",        value=14.0, step=1.0, key="us_pd_nl")
        with st.spinner("P/D 데이터 로딩 중…"):
            pd_df, pd_err = fetch_pd(market, chart_months)
        if pd_err:
            st.error(f"P/D 오류: {pd_err}")
        elif pd_df is None or pd_df.empty:
            st.warning("P/D 데이터 없음.")
        else:
            end_pd   = pd_df["dt"].max()
            start_pd = end_pd - pd.DateOffset(months=chart_months)
            pf4      = pd_df[pd_df["dt"] >= start_pd].copy().reset_index(drop=True)
            prs      = pd_df["pd_ratio"]
            pma      = prs.rolling(pd_ma_n).mean()
            mask_pd  = pd_df["dt"] >= start_pd
            pma_plot = pma.iloc[mask_pd.values].reset_index(drop=True)
            last_pd  = prs.iloc[-1]; last_dy = pd_df["div_yield"].iloc[-1]; last_cl = pd_df["close"].iloc[-1]
            pv = ("🔴 위험" if not pd.isna(last_pd) and last_pd >= pd_danger_high else
                  "🟡 주의" if not pd.isna(last_pd) and last_pd >= pd_danger_high*0.85 else
                  "🟢 정상" if not pd.isna(last_pd) and last_pd >= pd_normal_low else
                  "🟢 저평가" if not pd.isna(last_pd) else "⚪ N/A")
            p1,p2,p3,p4 = st.columns(4)
            p1.metric("지수 종가",    f"{last_cl:,.2f}")
            p2.metric("연배당수익률", f"{last_dy*100:.2f}%")
            p3.metric("P/D 비율",     f"{last_pd:.1f}" if not pd.isna(last_pd) else "N/A")
            p4.metric("판정", pv)
            st.info("📌 P/D = 지수 종가 ÷ 연간 실제 배당금. **26↑=위험**, **14~17=정상** (스탠 와인스태인).")
            fig_pd = go.Figure()
            fig_pd.add_hrect(y0=pd_danger_high,
                y1=max(pf4["pd_ratio"].dropna().max()+2 if len(pf4["pd_ratio"].dropna())>0 else pd_danger_high+2, pd_danger_high+2),
                fillcolor="red", opacity=0.06, line_width=0, annotation_text="위험 구간", annotation_position="top left")
            fig_pd.add_hrect(y0=0, y1=pd_normal_low, fillcolor="teal", opacity=0.06, line_width=0,
                annotation_text="저평가 구간", annotation_position="bottom left")
            fig_pd.add_trace(go.Scatter(x=pf4["dt"], y=pf4["pd_ratio"],
                line=dict(color="#42a5f5", width=2), name="P/D 비율"))
            fig_pd.add_trace(go.Scatter(x=pf4["dt"], y=pma_plot,
                line=dict(color="orange", width=1.5, dash="dash"), name=f"P/D {pd_ma_n}주 MA"))
            fig_pd.add_hline(y=pd_danger_high, line_color="red",  line_dash="dash", annotation_text=f"위험({pd_danger_high:.0f})")
            fig_pd.add_hline(y=pd_normal_low,  line_color="teal", line_dash="dash", annotation_text=f"정상하단({pd_normal_low:.0f})")
            fig_pd.update_layout(title=f"{market} P/D 비율 (주봉, 실제 배당금 기반)",
                template="plotly_dark", height=420,
                legend=dict(orientation="h", y=1.05), yaxis_title="P/D 비율")
            st.plotly_chart(fig_pd, use_container_width=True)

if __name__ == "__main__":
    main()