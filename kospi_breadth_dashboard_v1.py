#!/usr/bin/env python3
from __future__ import annotations
"""
KOSPI / KOSDAQ Breadth Dashboard (Streamlit)
실행:
  pip install streamlit plotly pandas requests finance-datareader mplfinance matplotlib
  KRX_AUTH_KEY=your_key streamlit run kospi_breadth_dashboard.py
"""
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

import hashlib
import io
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
import pandas as pd
import plotly.graph_objects as go
import requests
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
    if resp_i.status_code != 200:
        raise RuntimeError(f"GitHub index CSV 없음 ({resp_i.status_code})\n{i_url}\n→ 로컬에서 update_and_push.sh 실행 후 push 해주세요.")
    idx = pd.read_csv(io.StringIO(resp_i.text), dtype={"date": str})

    df = breadth.merge(idx[["date","open","high","low","close"]], on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)
    return df

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
# 차트 — matplotlib (검증된 방식)
# ──────────────────────────────────────────────────────────────
def make_chart_img(df: pd.DataFrame, market: str, sig: dict,
                   chart_months: int) -> bytes:
    end_dt    = pd.to_datetime(df["date"].astype(str), format="%Y%m%d").max()
    start_dt  = end_dt - pd.DateOffset(months=chart_months)
    mask      = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") >= start_dt
    pf        = df[mask].copy().reset_index(drop=True)
    pf["dt"]  = pd.to_datetime(pf["date"].astype(str), format="%Y%m%d")

    # mplfinance용 OHLC (date_num, open, high, low, close)
    ohlc = pf[["dt", "open", "high", "low", "close"]].copy()
    ohlc["dn"] = ohlc["dt"].map(mdates.date2num)
    ohlc_vals  = ohlc[["dn", "open", "high", "low", "close"]].values

    # 고점 기준일
    days_ago = int(sig["peak_label"].split("일전")[0]) if "일전" in sig["peak_label"] else 0
    peak_dt  = pd.to_datetime(
        str(df["date"].iloc[-(days_ago + 1)]), format="%Y%m%d"
    )
    peak_dn  = mdates.date2num(peak_dt)

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 9), sharex=True,
        gridspec_kw={"height_ratios": [1.4, 1]},   # 비율 균형 (비교하기 쉽도록)
        facecolor="#0e1117",
    )
    for ax in (ax1, ax2):
        ax.set_facecolor("#0e1117")
        ax.tick_params(colors="#aaaaaa")
        ax.spines[:].set_color("#333333")
        ax.yaxis.label.set_color("#aaaaaa")

    if MPL_OK:
        candlestick_ohlc(ax1, ohlc_vals, width=0.6,
                         colorup="#26a69a", colordown="#ef5350", alpha=0.9)
    else:
        ax1.plot(pf["dt"], pf["close"].astype(float), color="#26a69a", linewidth=1.5)

    ax1.set_title(f"{market} Index", color="#e0e0e0", fontsize=13)
    ax1.set_ylabel("Index", color="#aaaaaa")
    ax1.grid(True, color="#1e2530", linewidth=0.5)
    # 수직선: 고점 날짜
    ax1.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    # 수평선: 고점 가격 — 캔들과 닿는 수준 확인용
    ax1.axhline(y=sig["price_high"], color="orange", linestyle="--",
                linewidth=1.2, alpha=0.8,
                label=f"Peak {sig['price_high']:,.2f}")
    ax1.legend(loc="upper left", fontsize=9,
               facecolor="#1a1a2e", labelcolor="#e0e0e0", framealpha=0.8)

    ax2.plot(pf["dt"], pf["ad_line"].astype(float),
             color="#1565c0", linewidth=1.8)
    ax2.set_ylabel("A/D Line", color="#aaaaaa")
    ax2.set_title("A/D Line", color="#e0e0e0", fontsize=11)
    ax2.grid(True, color="#1e2530", linewidth=0.5)
    # 수직선: 고점 날짜
    ax2.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    # 수평선: 고점일 당시 A/D 값 — A/D선과 닿는 수준 확인용
    ax2.axhline(y=sig["ad_at_peak"], color="orange", linestyle="--",
                linewidth=1.2, alpha=0.8,
                label=f"A/D at Peak {sig['ad_at_peak']:,.0f}")
    ax2.legend(loc="upper left", fontsize=9,
               facecolor="#1a1a2e", labelcolor="#e0e0e0", framealpha=0.8)

    # x축 포맷
    locator   = mdates.AutoDateLocator()
    formatter = mdates.DateFormatter("%Y-%m")
    ax2.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)
    fig.autofmt_xdate(rotation=30, ha="right")

    # 판정 박스 — 영어로만 표시 (한글 폰트 없는 환경 대비)
    box_txt = (f"Peak: {sig['peak_label']}\n"
               f"Price vs Peak: {sig['price_off']:.2f}%\n"
               f"A/D vs Peak:   {sig['ad_off']:.2f}%\n"
               f"Gap:           {sig['gap']:.2f}%")
    ax1.text(0.01, 0.97, box_txt, transform=ax1.transAxes,
             va="top", ha="left", fontsize=10,
             color="white", family="monospace",
             bbox=dict(boxstyle="round,pad=0.5", facecolor=sig["color"], alpha=0.9))

    plt.tight_layout(pad=1.5)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()

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

        # 데이터 소스 선택
        mode = st.radio("데이터 소스", ["☁️ GitHub (빠름)", "🔑 KRX API (직접 수집)"],
                        index=0, help="GitHub: 미리 push된 CSV를 읽음 (빠름)\nKRX API: 직접 수집 (느림, AUTH_KEY 필요)")

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

        fetch_btn = st.button("🔄 데이터 불러오기", type="primary", use_container_width=True)

        if mode == "🔑 KRX API (직접 수집)":
            st.caption("💡 새로 불러오고 싶으면 아래 캐시를 지우고 불러오세요.")

        st.divider()
        st.subheader("분석 파라미터")
        lookback     = st.slider("Lookback (일)",      20, 252, 126)
        chart_months = st.slider("차트 표시 기간 (월)", 1,  24,  6)
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0,  step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0,  step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5,  step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5,  step=0.1)

        if mode == "🔑 KRX API (직접 수집)":
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
        if mode == "☁️ GitHub (빠름)":
            try:
                with st.spinner("GitHub에서 CSV 읽는 중…"):
                    df = load_from_github(market)
                st.success(f"✅ GitHub에서 로드 완료 — {len(df)}일치 / 최신: {df['date'].iloc[-1]}")
            except Exception as e:
                st.error(f"GitHub 로드 실패: {e}")
                return
        else:
            # KRX API 모드
            if not auth_key:
                st.error("KRX AUTH_KEY를 입력해주세요.")
                return
            start_str = start_dt.strftime("%Y%m%d")
            end_str   = end_dt.strftime("%Y%m%d")
            cached = load_cache(market, start_str, end_str, 50000.0)
            if cached is not None:
                st.success(f"✅ 캐시에서 로드 ({market} {start_str}~{end_str})")
                df = cached
            else:
                try:
                    with st.spinner("지수 OHLC 수집 중…"):
                        index_df = fetch_index_ohlc(market, start_str, end_str)
                    breadth_df = build_breadth(auth_key, start_str, end_str, market, 50000.0)
                    df = breadth_df.merge(
                        index_df[["date","open","high","low","close"]],
                        on="date", how="inner"
                    ).sort_values("date").reset_index(drop=True)
                    save_cache(df, market, start_str, end_str, 50000.0)
                    st.success(f"✅ 수집 완료 — {len(df)}일치")
                except Exception as e:
                    st.error(f"데이터 수집 실패: {e}")
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
    last = df.iloc[-1]

    # ── 탭 구성 ──
    tab1, tab2, tab3, tab4 = st.tabs(["📈 A/D Line", "⚡ 모멘텀", "🏔 고점-저점(NH-NL)", "📊 P/D 비율"])

    # ══════════════════════════════════════════════
    # TAB 1: 기존 A/D Line 분석
    # ══════════════════════════════════════════════
    with tab1:
        gap_color = "#00897b" if sig["gap"] >= 0 else "#c62828"
        gap_arrow = "▲" if sig["gap"] >= 0 else "▼"
        st.markdown(
            f'<div style="text-align:center;padding:6px 0 2px 0">'
            f'<span style="font-size:0.85em;color:#aaaaaa">괴리 (A/D − 가격)</span><br>'
            f'<span style="font-size:2.6em;font-weight:900;color:{gap_color}">'
            f'{gap_arrow} {sig["gap"]:+.2f}%</span>'
            f'<span style="font-size:0.8em;color:#aaaaaa;margin-left:8px">'
            f'기준: {sig["peak_label"]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("최근 날짜",
                  pd.to_datetime(str(last["date"]), format="%Y%m%d").strftime("%Y-%m-%d"))
        c2.metric(f"{market} 종가", f"{float(last['close']):,.2f}")
        c3.metric("오늘 AD 차이",   f"{int(last['ad_diff']):+,}")
        c4.metric("가격 고점 대비", f"{sig['price_off']:.2f}%")
        c5.metric("A/D 고점 대비",  f"{sig['ad_off']:.2f}%")

        st.markdown(
            f'<div style="background:{sig["color"]};padding:12px 18px;border-radius:8px;margin:8px 0">'
            f'<b style="font-size:1.2em;color:white">{sig["verdict"]}</b>'
            f'&nbsp;&nbsp;<span style="color:#ffffffcc">{sig["note"]}</span>'
            f'&nbsp;&nbsp;<span style="color:#ffffffaa;font-size:0.9em">기준: {sig["peak_label"]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        try:
            img = make_chart_img(df, market, sig, chart_months)
            st.image(img, use_container_width=True)
        except Exception as e:
            st.error(f"차트 렌더링 실패: {e}")

        with st.expander("📋 원시 데이터 보기"):
            show = df.copy()
            show["date"] = pd.to_datetime(show["date"].astype(str), format="%Y%m%d").dt.strftime("%Y-%m-%d")
            st.dataframe(
                show[["date","advances","declines","unchanged",
                      "ad_diff","ad_line","close","breadth_thrust_ema10"]]
                .sort_values("date", ascending=False).reset_index(drop=True),
                use_container_width=True,
            )
            csv = show.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("📥 CSV 다운로드", csv,
                               f"{market}_breadth.csv", "text/csv")

    # ══════════════════════════════════════════════
    # TAB 2: MI 탄력지수 (스탠 와인스태인 책 정의)
    # ══════════════════════════════════════════════
    with tab2:
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
        st.plotly_chart(fig_mi, use_container_width=True)

        if len(df) < mi_window:
            st.warning(f"⚠️ 데이터 {len(df)}일 — {mi_window}일 MA 계산에 데이터가 부족합니다. "
                       f"수집 기간을 늘리거나 MA 기간을 줄여주세요.")

    # ══════════════════════════════════════════════
    # TAB 3: 고점-저점 수치 (NH-NL)
    # ══════════════════════════════════════════════
    with tab3:
        st.subheader("🏔 고점-저점 수치 (신고가 - 신저가 종목 수)")
        st.caption(
            "스탠 와인스태인 책 정의: 그 주 신고가 기록 종목 수 - 신저가 기록 종목 수 (주봉 집계). "
            "KRX 개별종목 일별 데이터 기반으로 52주 신고가/신저가 판별."
        )

        # KRX breadth 캐시에 개별종목 고가/저가 데이터가 있으면 계산
        # build_breadth()가 수집한 일별 advances/declines만 있고 종목별 가격은 없음
        # → GitHub CSV에 nh/nl 컬럼이 있으면 사용, 없으면 KRX API 재수집 안내

        if "new_highs" in df.columns and "new_lows" in df.columns:
            nhnl_s  = pd.Series((df["new_highs"] - df["new_lows"]).values.astype(float))
            nhnl_source = "KRX 데이터"
        else:
            # 대용: advances/(advances+declines) 비율 기반 weekly 집계
            # 주봉으로 리샘플: 주간 advances 합 - declines 합
            df_dt = df.copy()
            df_dt["dt"] = pd.to_datetime(df_dt["date"].astype(str), format="%Y%m%d")
            df_dt = df_dt.set_index("dt")
            weekly_adv  = df_dt["advances"].resample("W-FRI").sum()
            weekly_decl = df_dt["declines"].resample("W-FRI").sum()
            nhnl_weekly = (weekly_adv - weekly_decl).reset_index()
            nhnl_weekly.columns = ["dt", "nhnl"]
            nhnl_weekly = nhnl_weekly[nhnl_weekly["nhnl"].notna()]
            nhnl_source = "주봉 등락종목수 차이 (신고가/신저가 대용)"

            end_dt3   = nhnl_weekly["dt"].max()
            start_dt3 = end_dt3 - pd.DateOffset(months=chart_months)
            pf3       = nhnl_weekly[nhnl_weekly["dt"] >= start_dt3].copy().reset_index(drop=True)
            nhnl_plot = pf3["nhnl"]
            nhnl_ma   = nhnl_plot.rolling(4).mean()

            last_nhnl = nhnl_weekly["nhnl"].iloc[-1]
            nhnl_verdict = ("🟢 강세" if last_nhnl > 200 else
                            "🟢 약한 강세" if last_nhnl > 0 else
                            "🔴 약세" if last_nhnl < -200 else "🟠 약한 약세")

            st.info(
                f"💡 현재 데이터: **{nhnl_source}**\n\n"
                "KRX 개별종목 52주 신고가/신저가 데이터는 "
                "GitHub CSV에 `new_highs`, `new_lows` 컬럼을 추가하면 정확하게 표시됩니다."
            )

            h1, h2, h3 = st.columns(3)
            h1.metric("주간 NH-NL", f"{int(last_nhnl):+,}")
            h2.metric("판정", nhnl_verdict)
            h3.metric("기준", nhnl_source)

            fig_hl = go.Figure()
            fig_hl.add_trace(go.Bar(
                x=pf3["dt"], y=nhnl_plot,
                marker_color=[("#26a69a" if v >= 0 else "#ef5350") for v in nhnl_plot],
                name="주봉 NH-NL", opacity=0.8
            ))
            fig_hl.add_trace(go.Scatter(
                x=pf3["dt"], y=nhnl_ma,
                line=dict(color="orange", width=1.5),
                name="4주 MA"
            ))
            fig_hl.add_hline(y=0, line_color="gray", line_dash="dot")
            fig_hl.update_layout(
                title=f"{market} 고점-저점 수치 (주봉 등락종목수 차이)",
                template="plotly_dark", height=420,
                yaxis_title="NH-NL"
            )
            st.plotly_chart(fig_hl, use_container_width=True)

    # ══════════════════════════════════════════════
    # TAB 4: P/D 비율 (스탠 와인스태인 책 정의)
    # ══════════════════════════════════════════════
    with tab4:
        st.subheader("📊 P/D 비율 (Price ÷ Dividend)")
        st.caption(
            "스탠 와인스태인 책 정의: 지수 종가 ÷ 배당금 추정치 (종가 × 배당수익률). "
            "26 이상 = 위험 구간 / 14~17 = 정상 / 낮을수록 저평가"
        )

        col_left, col_right = st.columns(2)
        with col_left:
            pd_market = st.selectbox("미국 지수", ["다우존스 (^DJI)", "S&P500 (^GSPC)", "나스닥 (^IXIC)"],
                                     key="pd_mkt")
        with col_right:
            pd_kr_market = st.selectbox("국내 지수", ["KOSPI", "KOSDAQ"], key="pd_kr_mkt")
        pd_symbol_map = {
            "다우존스 (^DJI)":  "DJI",
            "S&P500 (^GSPC)":  "S&P500",
            "나스닥 (^IXIC)":   "IXIC",
        }
        pd_symbol = pd_symbol_map[pd_market]
        pd_ma_n = st.slider("MA 기간 (주)", 4, 52, 13, key="pd_ma")

        pd_danger_high = st.number_input("위험 기준 (이상)", value=26.0, step=1.0, key="pd_dh")
        pd_normal_low  = st.number_input("정상 하단 기준", value=14.0, step=1.0, key="pd_nl")

        @st.cache_data(show_spinner=False, ttl=86400)
        def fetch_pd_data(symbol: str, months: int):
            """
            스탠 와인스태인 P/D 비율 계산.
            책 정의: 다우지수 종가 ÷ 구성 기업 연간 배당금 합계.
            배당금 = 지수 종가 × 연간 배당수익률(%).
            P/D = 종가 / (종가 × div_yield) = 1 / div_yield.
            yfinance로 배당수익률 시계열을 가져와 계산.
            """
            if not FDR_OK:
                return None, "finance-datareader 미설치"
            try:
                import yfinance as yf
            except ImportError:
                # yfinance 없으면 고정 배당수익률 사용
                yf = None

            try:
                end_d   = datetime.today()
                start_d = end_d - timedelta(days=max(365 * 5, months * 35))
                # FDR로 주봉 종가
                sym_fdr = {"DJI": "DJI", "S&P500": "S&P500", "IXIC": "IXIC"}.get(symbol, symbol)
                raw = fdr.DataReader(sym_fdr,
                                     start_d.strftime("%Y-%m-%d"),
                                     end_d.strftime("%Y-%m-%d"))
                if raw.empty:
                    return None, f"{sym_fdr} 데이터 없음"
                raw.index = pd.to_datetime(raw.index)
                raw.columns = [str(c).strip().title() for c in raw.columns]
                close_col = next((c for c in raw.columns if c.lower() in ("close", "adj close")), None)
                if not close_col:
                    return None, f"종가 컬럼 없음: {list(raw.columns)}"
                # 주봉 리샘플 (금요일 종가)
                weekly = raw[[close_col]].resample("W-FRI").last().dropna()
                weekly.columns = ["close"]

                # yfinance로 배당수익률 시계열 시도
                div_yield_series = None
                if yf is not None:
                    try:
                        yf_sym = {"DJI": "^DJI", "S&P500": "^GSPC", "IXIC": "^IXIC"}.get(symbol, "^DJI")
                        ticker = yf.Ticker(yf_sym)
                        info   = ticker.info
                        # trailingAnnualDividendYield: 연간 배당수익률 (소수, e.g. 0.018)
                        dy_val = info.get("trailingAnnualDividendYield") or info.get("dividendYield")
                        if dy_val and dy_val > 0:
                            div_yield_series = float(dy_val)  # 단일 값 — 최근 수익률로 전체 적용
                    except Exception:
                        div_yield_series = None

                # 수익률 없으면 역사적 평균값 사용
                if div_yield_series is None:
                    fallback = {"DJI": 0.020, "S&P500": 0.018, "IXIC": 0.007}
                    div_yield_series = fallback.get(symbol, 0.018)

                # 배당금 추정 = 종가 × 연간 배당수익률
                weekly["div_yield"]    = div_yield_series
                weekly["dividend_est"] = weekly["close"] * weekly["div_yield"]
                # P/D = 종가 ÷ 배당금
                weekly["pd_ratio"] = weekly["close"] / weekly["dividend_est"].replace(0, float("nan"))

                weekly = weekly.reset_index()
                weekly.columns = ["date", "close", "div_yield", "dividend_est", "pd_ratio"]
                return weekly, None
            except Exception as e:
                return None, str(e)

        with st.spinner("P/D 데이터 로딩 중…"):
            pd_df, pd_err = fetch_pd_data(pd_symbol, chart_months)

        if pd_err:
            st.error(f"P/D 데이터 오류: {pd_err}")
        elif pd_df is None or pd_df.empty:
            st.warning("P/D 데이터를 가져오지 못했습니다.")
        else:
            end_pd   = pd_df["date"].max()
            start_pd = end_pd - pd.DateOffset(months=chart_months)
            pf4 = pd_df[pd_df["date"] >= start_pd].copy().reset_index(drop=True)

            pd_ratio_s  = pd_df["pd_ratio"]
            pd_ma_s     = pd_ratio_s.rolling(pd_ma_n).mean()
            pd_plot     = pf4["pd_ratio"]
            pd_ma_plot  = pd_ratio_s.rolling(pd_ma_n).mean().iloc[
                            pd_df["date"] >= start_pd
                          ].reset_index(drop=True)

            last_pd    = pd_ratio_s.iloc[-1]
            last_pd_ma = pd_ma_s.iloc[-1]
            last_close = pd_df["close"].iloc[-1]
            last_div   = pd_df["dividend_est"].iloc[-1]

            pd_verdict = ("🔴 위험 — 과대평가 구간"  if last_pd >= pd_danger_high else
                          "🟡 주의 — 고평가 근접"     if last_pd >= pd_danger_high * 0.85 else
                          "🟢 정상 구간"              if last_pd >= pd_normal_low else
                          "🟢 저평가 — 매수 유리")
            pd_color   = ("#c62828" if last_pd >= pd_danger_high else
                          "#ef6c00" if last_pd >= pd_danger_high * 0.85 else
                          "#2e7d32")

            last_div_yield = pd_df["div_yield"].iloc[-1]

            p1, p2, p3, p4_col = st.columns(4)
            p1.metric("지수 종가",    f"{last_close:,.2f}")
            p2.metric("연배당수익률", f"{last_div_yield*100:.2f}%")
            p3.metric("P/D 비율",     f"{last_pd:.1f}" if not pd.isna(last_pd) else "N/A")
            p4_col.metric("판정",     pd_verdict)

            st.info(
                "📌 **스탠 와인스태인 P/D 계산법**: 배당금(D) = 지수 종가 × 연간 배당수익률. "
                "P/D = 지수 종가 ÷ 배당금. "
                "**26 이상 = 위험**, **14~17 = 정상 구간** (책 기준)."
            )

            fig_pd = go.Figure()
            fig_pd.add_hrect(
                y0=pd_danger_high, y1=max(pd_plot.max(skipna=True) + 2, pd_danger_high + 2),
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
                line=dict(color="#42a5f5", width=2),
                name="P/D 비율"
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
                title=f"{pd_market} P/D 비율 (Price ÷ Dividend 추정, 주봉)",
                template="plotly_dark", height=420,
                legend=dict(orientation="h", y=1.05),
                yaxis_title="P/D 비율"
            )
            st.plotly_chart(fig_pd, use_container_width=True)

        # ── 국장 P/D ──────────────────────────────────
        st.divider()
        st.subheader(f"📊 {pd_kr_market} P/D 비율")

        @st.cache_data(show_spinner=False, ttl=86400)
        def fetch_pd_data_kr(kr_symbol: str, months: int):
            """KOSPI/KOSDAQ P/D 계산 (스탠 와인스태인 방식 동일 적용)
            배당금 = 지수 종가 × 연간 배당수익률 / KOSPI 역사적 평균 배당수익률 사용"""
            if not FDR_OK:
                return None, "finance-datareader 미설치"
            try:
                import yfinance as yf
            except ImportError:
                yf = None
            try:
                end_d   = datetime.today()
                start_d = end_d - timedelta(days=max(365 * 5, months * 35))
                sym_fdr = {"KOSPI": "KS11", "KOSDAQ": "KQ11"}.get(kr_symbol, "KS11")
                raw = fdr.DataReader(sym_fdr,
                                     start_d.strftime("%Y-%m-%d"),
                                     end_d.strftime("%Y-%m-%d"))
                if raw.empty:
                    return None, f"{sym_fdr} 데이터 없음"
                raw.index = pd.to_datetime(raw.index)
                raw.columns = [str(c).strip().title() for c in raw.columns]
                close_col = next((c for c in raw.columns if c.lower() in ("close", "adj close")), None)
                if not close_col:
                    return None, f"종가 컬럼 없음: {list(raw.columns)}"
                weekly = raw[[close_col]].resample("W-FRI").last().dropna()
                weekly.columns = ["close"]

                # yfinance로 배당수익률 시도
                div_yield_val = None
                if yf is not None:
                    try:
                        yf_sym = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11"}.get(kr_symbol, "^KS11")
                        ticker = yf.Ticker(yf_sym)
                        info   = ticker.info
                        dy_val = info.get("trailingAnnualDividendYield") or info.get("dividendYield")
                        if dy_val and dy_val > 0:
                            div_yield_val = float(dy_val)
                    except Exception:
                        div_yield_val = None

                # 없으면 역사적 평균: KOSPI ≈ 2.5%, KOSDAQ ≈ 0.8%
                if div_yield_val is None:
                    fallback_kr = {"KOSPI": 0.025, "KOSDAQ": 0.008}
                    div_yield_val = fallback_kr.get(kr_symbol, 0.025)

                weekly["div_yield"]    = div_yield_val
                weekly["dividend_est"] = weekly["close"] * weekly["div_yield"]
                weekly["pd_ratio"]     = weekly["close"] / weekly["dividend_est"].replace(0, float("nan"))
                weekly = weekly.reset_index()
                weekly.columns = ["date", "close", "div_yield", "dividend_est", "pd_ratio"]
                return weekly, None
            except Exception as e:
                return None, str(e)

        with st.spinner(f"{pd_kr_market} P/D 로딩 중…"):
            pd_kr_df, pd_kr_err = fetch_pd_data_kr(pd_kr_market, chart_months)

        if pd_kr_err:
            st.error(f"국장 P/D 오류: {pd_kr_err}")
        elif pd_kr_df is None or pd_kr_df.empty:
            st.warning("국장 P/D 데이터를 가져오지 못했습니다.")
        else:
            end_kr   = pd_kr_df["date"].max()
            start_kr = end_kr - pd.DateOffset(months=chart_months)
            pf4_kr   = pd_kr_df[pd_kr_df["date"] >= start_kr].copy().reset_index(drop=True)
            kr_ratio_s   = pd_kr_df["pd_ratio"]
            kr_ma_s      = kr_ratio_s.rolling(pd_ma_n).mean()
            kr_plot      = pf4_kr["pd_ratio"]
            kr_ma_plot   = kr_ratio_s.rolling(pd_ma_n).mean().iloc[
                             pd_kr_df["date"] >= start_kr
                           ].reset_index(drop=True)

            last_kr_pd  = kr_ratio_s.iloc[-1]
            last_kr_dy  = pd_kr_df["div_yield"].iloc[-1]
            last_kr_cls = pd_kr_df["close"].iloc[-1]

            kr_verdict = ("🔴 위험"   if not pd.isna(last_kr_pd) and last_kr_pd >= pd_danger_high else
                          "🟡 주의"   if not pd.isna(last_kr_pd) and last_kr_pd >= pd_danger_high * 0.85 else
                          "🟢 정상"   if not pd.isna(last_kr_pd) and last_kr_pd >= pd_normal_low else
                          "🟢 저평가" if not pd.isna(last_kr_pd) else "⚪ N/A")

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("지수 종가",    f"{last_kr_cls:,.2f}")
            k2.metric("연배당수익률", f"{last_kr_dy*100:.2f}%")
            k3.metric("P/D 비율",    f"{last_kr_pd:.1f}" if not pd.isna(last_kr_pd) else "N/A")
            k4.metric("판정",         kr_verdict)

            fig_kr_pd = go.Figure()
            fig_kr_pd.add_hrect(
                y0=pd_danger_high,
                y1=max(kr_plot.dropna().max() + 2 if len(kr_plot.dropna()) > 0 else pd_danger_high + 2,
                       pd_danger_high + 2),
                fillcolor="red", opacity=0.06, line_width=0,
                annotation_text="위험 구간", annotation_position="top left"
            )
            fig_kr_pd.add_hrect(
                y0=0, y1=pd_normal_low,
                fillcolor="teal", opacity=0.06, line_width=0,
                annotation_text="저평가 구간", annotation_position="bottom left"
            )
            fig_kr_pd.add_trace(go.Scatter(
                x=pf4_kr["date"], y=kr_plot,
                line=dict(color="#ef9a9a", width=2),
                name=f"{pd_kr_market} P/D",
                connectgaps=False
            ))
            fig_kr_pd.add_trace(go.Scatter(
                x=pf4_kr["date"], y=kr_ma_plot,
                line=dict(color="orange", width=1.5, dash="dash"),
                name=f"P/D {pd_ma_n}주 MA"
            ))
            fig_kr_pd.add_hline(y=pd_danger_high, line_color="red",  line_dash="dash",
                                annotation_text=f"위험({pd_danger_high:.0f})")
            fig_kr_pd.add_hline(y=pd_normal_low,  line_color="teal", line_dash="dash",
                                annotation_text=f"정상하단({pd_normal_low:.0f})")
            fig_kr_pd.update_layout(
                title=f"{pd_kr_market} P/D 비율 (주봉 등락률 기반)",
                template="plotly_dark", height=400,
                legend=dict(orientation="h", y=1.05),
                yaxis_title="P/D 비율"
            )
            st.plotly_chart(fig_kr_pd, use_container_width=True)


if __name__ == "__main__":
    main()
