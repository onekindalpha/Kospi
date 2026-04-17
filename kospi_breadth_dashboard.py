#!/usr/bin/env python3
"""
KOSPI / KOSDAQ Breadth Dashboard (Streamlit)
실행:
  pip install streamlit plotly pandas requests finance-datareader mplfinance matplotlib
  KRX_AUTH_KEY=your_key streamlit run kospi_breadth_dashboard.py
"""
from __future__ import annotations

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
    sys = platform.system()
    if sys == "Darwin":
        plt.rcParams["font.family"] = "AppleGothic"
    elif sys == "Windows":
        plt.rcParams["font.family"] = "Malgun Gothic"
    else:
        nanum = [f.name for f in fm.fontManager.ttflist if "Nanum" in f.name]
        if nanum:
            plt.rcParams["font.family"] = nanum[0]
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

    ax1.set_title(f"{market} 지수 (캔들)", color="#e0e0e0", fontsize=13)
    ax1.set_ylabel("지수", color="#aaaaaa")
    ax1.grid(True, color="#1e2530", linewidth=0.5)
    # 수직선: 고점 날짜
    ax1.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    # 수평선: 고점 가격 — 캔들과 닿는 수준 확인용
    ax1.axhline(y=sig["price_high"], color="orange", linestyle="--",
                linewidth=1.2, alpha=0.8,
                label=f"고점 {sig['price_high']:,.2f}")
    ax1.legend(loc="upper left", fontsize=9,
               facecolor="#1a1a2e", labelcolor="#e0e0e0", framealpha=0.8)

    ax2.plot(pf["dt"], pf["ad_line"].astype(float),
             color="#1565c0", linewidth=1.8)
    ax2.set_ylabel("A/D Line", color="#aaaaaa")
    ax2.grid(True, color="#1e2530", linewidth=0.5)
    # 수직선: 고점 날짜
    ax2.axvline(peak_dn, color="orange", linestyle=":", linewidth=1.2, alpha=0.6)
    # 수평선: 고점일 당시 A/D 값 — A/D선과 닿는 수준 확인용
    ax2.axhline(y=sig["ad_at_peak"], color="orange", linestyle="--",
                linewidth=1.2, alpha=0.8,
                label=f"고점일 A/D {sig['ad_at_peak']:,.0f}")
    ax2.legend(loc="upper left", fontsize=9,
               facecolor="#1a1a2e", labelcolor="#e0e0e0", framealpha=0.8)

    # x축 포맷
    locator   = mdates.AutoDateLocator()
    formatter = mdates.DateFormatter("%Y-%m")
    ax2.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)
    fig.autofmt_xdate(rotation=30, ha="right")

    # 판정 박스
    box_txt = (f"{sig['verdict']}\n{sig['note']}\n"
               f"─────────────────\n"
               f"기준고점: {sig['peak_label']}\n"
               f"가격 고점 대비: {sig['price_off']:.2f}%\n"
               f"A/D 고점 대비: {sig['ad_off']:.2f}%\n"
               f"괴리: {sig['gap']:.2f}%")
    ax1.text(0.01, 0.97, box_txt, transform=ax1.transAxes,
             va="top", ha="left", fontsize=10,
             color="white",
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
        auth_key = st.text_input("KRX AUTH_KEY",
                                 value=os.environ.get("KRX_AUTH_KEY", ""),
                                 type="password")
        market   = st.selectbox("마켓", ["KOSPI", "KOSDAQ"])
        c1, c2   = st.columns(2)
        today    = datetime.today()
        start_dt = c1.date_input("시작일", value=today - timedelta(days=730))
        end_dt   = c2.date_input("종료일", value=today)

        fetch_btn  = st.button("🔄 데이터 불러오기", type="primary", use_container_width=True)

        st.divider()
        st.subheader("분석 파라미터")
        lookback     = st.slider("Lookback (일)",      20, 252, 126)
        chart_months = st.slider("차트 표시 기간 (월)", 1,  24,  6)
        base_value   = st.number_input("A/D Line 시작값", value=50000.0, step=1000.0)
        with st.expander("임계값 세부 설정"):
            price_thr  = st.number_input("가격 고점 근접 기준 %", value=2.0,  step=0.1)
            ad_thr     = st.number_input("A/D 고점 근접 기준 %",  value=3.0,  step=0.1)
            gap_warn   = st.number_input("경고 괴리 기준 %",       value=1.5,  step=0.1)
            gap_danger = st.number_input("위험 괴리 기준 %",       value=2.5,  step=0.1)

        # ── 캐시 관리 ──
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
        st.info("👈 사이드바에서 설정 후 **데이터 불러오기** 버튼을 눌러주세요.")
        st.markdown("""
**필요한 패키지:**
```bash
pip install streamlit plotly pandas requests finance-datareader mplfinance matplotlib
```
**실행:**
```bash
KRX_AUTH_KEY=your_key streamlit run kospi_breadth_dashboard.py
```
        """)
        return

    if fetch_btn:
        if not auth_key:
            st.error("KRX AUTH_KEY를 입력해주세요.")
            return

        start_str = start_dt.strftime("%Y%m%d")
        end_str   = end_dt.strftime("%Y%m%d")

        # ── 캐시 먼저 확인 ──
        cached = load_cache(market, start_str, end_str, base_value)
        if cached is not None:
            st.success(f"✅ 캐시에서 로드했습니다 ({market} {start_str}~{end_str})")
            df = cached
        else:
            try:
                with st.spinner("지수 OHLC 수집 중…"):
                    index_df = fetch_index_ohlc(market, start_str, end_str)
                breadth_df = build_breadth(auth_key, start_str, end_str, market, base_value)
                df = breadth_df.merge(
                    index_df[["date", "open", "high", "low", "close"]],
                    on="date", how="inner"
                ).sort_values("date").reset_index(drop=True)
                save_cache(df, market, start_str, end_str, base_value)
                st.success(f"✅ 수집 완료 — {len(df)}일치 데이터 저장됨")
            except Exception as e:
                st.error(f"데이터 수집 실패: {e}")
                return

        st.session_state["df_merged"] = df

    # ── 차트 및 판정 출력 ───────────────────────────
    # 슬라이더/파라미터는 항상 현재 사이드바 값 사용 (버튼 불필요)
    df = st.session_state["df_merged"]

    if len(df) < lookback:
        st.warning(f"데이터 부족: {len(df)}행 (lookback={lookback})")
        return

    # 항상 현재 사이드바 값으로 계산
    sig  = compute_signals(df, lookback, price_thr, ad_thr, gap_warn, gap_danger)
    last = df.iloc[-1]

    # ── 상단 메트릭 — 괴리를 가장 크게 강조 ──
    # 괴리 색상: + = 청록(선행/좋음), - = 빨강(지연/나쁨)
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

    # ── 판정 박스 ──
    st.markdown(
        f'<div style="background:{sig["color"]};padding:12px 18px;border-radius:8px;margin:8px 0">'
        f'<b style="font-size:1.2em;color:white">{sig["verdict"]}</b>'
        f'&nbsp;&nbsp;<span style="color:#ffffffcc">{sig["note"]}</span>'
        f'&nbsp;&nbsp;<span style="color:#ffffffaa;font-size:0.9em">기준: {sig["peak_label"]}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── 차트 (matplotlib PNG) ──
    try:
        img = make_chart_img(df, market, sig, chart_months)
        st.image(img, use_container_width=True)
    except Exception as e:
        st.error(f"차트 렌더링 실패: {e}")

    # ── 원시 데이터 ──
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


if __name__ == "__main__":
    main()
