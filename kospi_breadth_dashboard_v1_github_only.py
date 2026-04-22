#!/usr/bin/env python3
from __future__ import annotations

import io
import platform
import subprocess

import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="국장 브레드스 대시보드", page_icon="📊", layout="wide")

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

STATUS_MAP = {
    "BULLISH_CONFIRMATION":         ("상승 확인", "#2e7d32"),
    "BULLISH_DIVERGENCE":           ("A/D 미확인", "#c62828"),
    "BULLISH_DIVERGENCE_CANDIDATE": ("초기 경고", "#ef6c00"),
    "RECOVERY_IN_PROGRESS":         ("회복 진행", "#f9a825"),
    "DOWNSIDE_DIVERGENCE_CANDIDATE":("하락 다이버전스", "#00838f"),
    "NORMAL_WEAKNESS":              ("전반적 약세", "#455a64"),
    "NEUTRAL":                      ("중립", "#757575"),
}

def _setup_korean_font():
    try:
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
        sys_name = platform.system()
        if sys_name == "Darwin":
            plt.rcParams["font.family"] = "AppleGothic"
        elif sys_name == "Windows":
            plt.rcParams["font.family"] = "Malgun Gothic"
        else:
            nanum = [f.name for f in fm.fontManager.ttflist if "Nanum" in f.name]
            if not nanum:
                try:
                    subprocess.run(["apt-get", "install", "-y", "-q", "fonts-nanum"], check=True, capture_output=True)
                    fm._load_fontmanager(try_read_cache=False)
                except Exception:
                    pass
        plt.rcParams["axes.unicode_minus"] = False
    except Exception:
        pass

_setup_korean_font()

@st.cache_data(show_spinner=False, ttl=1800)
def _read_csv_url(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"CSV 로드 실패 ({r.status_code}): {url}")
    return pd.read_csv(io.StringIO(r.text), dtype={"date": str})

@st.cache_data(show_spinner=False, ttl=1800)
def load_from_github(market: str) -> pd.DataFrame:
    breadth = _read_csv_url(GITHUB_BREADTH[market])
    idx = _read_csv_url(GITHUB_INDEX[market])
    df = breadth.merge(idx[["date", "open", "high", "low", "close"]], on="date", how="inner").sort_values("date").reset_index(drop=True)
    df["dt"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
    return df.dropna(subset=["dt"]).reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=1800)
def load_nhnl_from_github(market: str) -> pd.DataFrame | None:
    r = requests.get(GITHUB_NHNL[market], timeout=20)
    if r.status_code != 200:
        return None
    df = pd.read_csv(io.StringIO(r.text), dtype={"date": str})
    if df.empty:
        return None
    rename_map = {}
    if "new_high" in df.columns and "new_highs" not in df.columns:
        rename_map["new_high"] = "new_highs"
    if "new_low" in df.columns and "new_lows" not in df.columns:
        rename_map["new_low"] = "new_lows"
    if "high_low_diff" in df.columns and "nhnl" not in df.columns:
        rename_map["high_low_diff"] = "nhnl"
    if rename_map:
        df = df.rename(columns=rename_map)
    if "new_highs" not in df.columns:
        df["new_highs"] = pd.NA
    if "new_lows" not in df.columns:
        df["new_lows"] = pd.NA
    if "nhnl" not in df.columns:
        return None
    df["dt"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
    return df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

def classify(price_off_high, ad_off_high, gap, price_off_low, ad_off_low,
             price_thr=2.0, ad_thr=3.0, gap_warn=1.5, gap_danger=2.5):
    ph = price_off_high >= -price_thr
    ah = ad_off_high >= -ad_thr
    pl = price_off_low <= price_thr
    al = ad_off_low <= ad_thr
    if ph and ah and gap >= -1.0:
        return "BULLISH_CONFIRMATION"
    if ph and gap <= -gap_danger:
        return "BULLISH_DIVERGENCE"
    if gap <= -gap_warn:
        return "BULLISH_DIVERGENCE_CANDIDATE"
    if gap < -1.0:
        return "RECOVERY_IN_PROGRESS"
    if pl and not al:
        return "DOWNSIDE_DIVERGENCE_CANDIDATE"
    if pl and al:
        return "NORMAL_WEAKNESS"
    return "NEUTRAL"

def compute_signals(df, lookback):
    closes = df["close"].values.astype(float)
    ad_lines = df["ad_line"].values.astype(float)
    window = closes[-lookback:]
    peak_idx = window.argmax()
    days_ago = lookback - 1 - peak_idx
    price_high = window[peak_idx]
    ad_at_peak = ad_lines[-(days_ago + 1)]
    price_low = closes[-lookback:].min()
    ad_low = ad_lines[-lookback:].min()
    last_close = closes[-1]
    last_ad = ad_lines[-1]
    price_off = (last_close - price_high) / abs(price_high) * 100 if price_high else float("nan")
    ad_off = (last_ad - ad_at_peak) / abs(ad_at_peak) * 100 if ad_at_peak else float("nan")
    gap = ad_off - price_off
    price_off_low = (last_close - price_low) / abs(price_low) * 100 if price_low else float("nan")
    ad_off_low = (last_ad - ad_low) / abs(ad_low) * 100 if ad_low else float("nan")
    status_key = classify(price_off, ad_off, gap, price_off_low, ad_off_low)
    verdict, color = STATUS_MAP[status_key]
    return {"gap": gap, "verdict": verdict, "color": color}

def local_extrema_points(df: pd.DataFrame, value_col: str, kind: str, lookback: int = 26):
    s = df[[value_col, "dt"]].tail(lookback).reset_index(drop=True)
    if len(s) < 5:
        return None
    vals = s[value_col].astype(float)
    idxs = []
    for i in range(1, len(s)-1):
        if kind == "high" and vals.iloc[i] >= vals.iloc[i-1] and vals.iloc[i] >= vals.iloc[i+1]:
            idxs.append(i)
        if kind == "low" and vals.iloc[i] <= vals.iloc[i-1] and vals.iloc[i] <= vals.iloc[i+1]:
            idxs.append(i)
    if len(idxs) >= 2:
        a, b = idxs[-2], idxs[-1]
        return (s.loc[a, "dt"], float(s.loc[a, value_col])), (s.loc[b, "dt"], float(s.loc[b, value_col]))
    return None

def add_trendline(fig, row, col, pts, color):
    if not pts:
        return
    (x0, y0), (x1, y1) = pts
    fig.add_trace(go.Scatter(x=[x0, x1], y=[y0, y1], mode="lines",
                             line=dict(color=color, width=1.5, dash="dash"),
                             hoverinfo="skip", showlegend=False), row=row, col=col)

def resample_weekly_index(df: pd.DataFrame) -> pd.DataFrame:
    return (df[["dt", "close"]].sort_values("dt").set_index("dt")
            .resample("W-FRI").last().dropna().reset_index())

def make_two_panel(top_x, top_y, bottom_x, bottom_y, top_label, bottom_label, top_trends=(), bottom_trends=()):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.52, 0.48], vertical_spacing=0.04)
    fig.add_trace(go.Scatter(x=top_x, y=top_y, mode="lines", line=dict(color="rgba(220,220,220,0.95)", width=1.8)),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=bottom_x, y=bottom_y, mode="lines", line=dict(color="#26a69a", width=2.0)),
                  row=2, col=1)
    for pts, color in top_trends:
        add_trendline(fig, 1, 1, pts, color)
    for pts, color in bottom_trends:
        add_trendline(fig, 2, 1, pts, color)
    x_min, x_max = min(top_x.min(), bottom_x.min()), max(top_x.max(), bottom_x.max())
    fig.update_xaxes(type="date", range=[x_min, x_max], tickformat="%Y-%m")
    fig.update_yaxes(title_text=top_label, row=1, col=1)
    fig.update_yaxes(title_text=bottom_label, row=2, col=1)
    fig.update_layout(template="plotly_dark", height=720, margin=dict(l=40, r=20, t=20, b=30),
                      hovermode="x unified", showlegend=False, xaxis_rangeslider_visible=False)
    return fig

def make_ad_chart(df: pd.DataFrame, chart_months: int):
    end_dt = df["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    pf = df[df["dt"] >= start_dt].copy().reset_index(drop=True)
    top_trends = [
        (local_extrema_points(pf, "close", "high"), "rgba(255,120,120,0.9)"),
        (local_extrema_points(pf, "close", "low"), "rgba(38,210,160,0.9)")
    ]
    bottom_trends = [
        (local_extrema_points(pf, "ad_line", "high"), "rgba(255,120,120,0.9)"),
        (local_extrema_points(pf, "ad_line", "low"), "rgba(38,210,160,0.9)")
    ]
    return make_two_panel(pf["dt"], pf["close"].astype(float), pf["dt"], pf["ad_line"].astype(float),
                          "지수", "A/D Line", top_trends, bottom_trends)

def make_mi_chart(df: pd.DataFrame, chart_months: int, mi_window: int):
    mi_full = pd.Series(df["ad_diff"].astype(float)).rolling(mi_window).mean()
    plot_df = df.copy()
    plot_df["mi"] = mi_full
    end_dt = plot_df["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    plot_df = plot_df[plot_df["dt"] >= start_dt].copy().reset_index(drop=True)
    fig = make_two_panel(
        plot_df["dt"], plot_df["close"].astype(float),
        plot_df["dt"], plot_df["mi"].astype(float),
        "지수", "MI",
        top_trends=[
            (local_extrema_points(plot_df, "close", "high"), "rgba(255,120,120,0.9)"),
            (local_extrema_points(plot_df, "close", "low"), "rgba(38,210,160,0.9)")
        ],
        bottom_trends=[
            (local_extrema_points(plot_df, "mi", "high"), "rgba(255,120,120,0.9)"),
            (local_extrema_points(plot_df, "mi", "low"), "rgba(38,210,160,0.9)")
        ],
    )
    fig.add_hline(y=0, row=2, col=1, line_color="rgba(180,180,180,0.7)", line_dash="dot")
    return fig, float(mi_full.iloc[-1]) if len(mi_full) else float("nan")

def make_nhnl_chart(df: pd.DataFrame, nhnl_df: pd.DataFrame, chart_months: int):
    idxw = resample_weekly_index(df)
    end_dt = nhnl_df["dt"].max()
    start_dt = end_dt - pd.DateOffset(months=chart_months)
    pf = nhnl_df[nhnl_df["dt"] >= start_dt].copy().reset_index(drop=True)
    idxw = idxw[(idxw["dt"] >= pf["dt"].min()) & (idxw["dt"] <= pf["dt"].max())].reset_index(drop=True)
    fig = make_two_panel(
        idxw["dt"], idxw["close"].astype(float),
        pf["dt"], pf["nhnl"].astype(float),
        "지수", "고점-저점 수치",
        top_trends=[
            (local_extrema_points(idxw, "close", "high"), "rgba(255,120,120,0.9)"),
            (local_extrema_points(idxw, "close", "low"), "rgba(38,210,160,0.9)")
        ],
        bottom_trends=[
            (local_extrema_points(pf, "nhnl", "high"), "rgba(255,120,120,0.9)"),
            (local_extrema_points(pf, "nhnl", "low"), "rgba(38,210,160,0.9)")
        ],
    )
    fig.add_hline(y=0, row=2, col=1, line_color="rgba(180,180,180,0.7)", line_dash="dot")
    return fig

def main():
    st.title("📊 국장 A/D Line 브레드스 대시보드")
    st.caption("GitHub Actions로 갱신된 CSV를 읽어 표시합니다.")

    with st.sidebar:
        st.header("⚙️ 설정")
        market = st.selectbox("마켓", ["KOSPI", "KOSDAQ"])
        chart_months = st.slider("차트 표시 기간 (월)", 1, 24, 6)
        lookback = st.slider("Lookback (일)", 20, 252, 126)
        mi_window = st.slider("MI 기간", 50, 300, 200, step=10)
        if st.button("🔄 GitHub 데이터 새로고침", width="stretch"):
            st.cache_data.clear()

    try:
        df = load_from_github(market)
        nhnl_df = load_nhnl_from_github(market)
    except Exception as e:
        st.error(f"GitHub 로드 실패: {e}")
        return

    st.success(f"GitHub 로드 완료 — {len(df)}일치 / 최신: {df['date'].iloc[-1]}")

    TAB_LABELS = ["📈 A/D Line", "⚡ 모멘텀", "🏔 NH-NL"]
    current_tab = st.session_state.get("active_tab", TAB_LABELS[0])
    if current_tab not in TAB_LABELS:
        current_tab = TAB_LABELS[0]
    if hasattr(st, "segmented_control"):
        active_tab = st.segmented_control("분석 탭", TAB_LABELS, selection_mode="single", default=current_tab)
    else:
        active_tab = st.radio("분석 탭", TAB_LABELS, index=TAB_LABELS.index(current_tab), horizontal=True)
    st.session_state["active_tab"] = active_tab

    if active_tab == "📈 A/D Line":
        sig = compute_signals(df, lookback)
        c1, c2, c3 = st.columns(3)
        c1.metric("최근 날짜", df["dt"].iloc[-1].strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가", f"{float(df['close'].iloc[-1]):,.2f}")
        c3.metric("오늘 AD 차이", f"{int(df['ad_diff'].iloc[-1]):+,}")
        st.plotly_chart(make_ad_chart(df, chart_months), width="stretch")

    elif active_tab == "⚡ 모멘텀":
        fig, last_mi = make_mi_chart(df, chart_months, mi_window)
        c1, c2 = st.columns(2)
        c1.metric("최근 날짜", df["dt"].iloc[-1].strftime("%Y-%m-%d"))
        c2.metric("MI", f"{last_mi:.1f}" if pd.notna(last_mi) else "N/A")
        st.plotly_chart(fig, width="stretch")

    else:
        st.subheader("🏔 고점-저점 수치")
        if nhnl_df is None or nhnl_df.empty:
            st.warning("GitHub의 NH-NL CSV가 아직 없습니다.")
            return
        last_row = nhnl_df.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("주간 새 고점 수", f"{int(last_row['new_highs'])}" if pd.notna(last_row["new_highs"]) else "N/A")
        c2.metric("주간 새 저점 수", f"{int(last_row['new_lows'])}" if pd.notna(last_row["new_lows"]) else "N/A")
        c3.metric("고점-저점 차이", f"{float(last_row['nhnl']):+.0f}")
        st.plotly_chart(make_nhnl_chart(df, nhnl_df, chart_months), width="stretch")

if __name__ == "__main__":
    main()
