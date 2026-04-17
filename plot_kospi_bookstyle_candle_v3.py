import argparse
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc


# ASCII-only labels on the PNG to avoid font-garbling issues across environments.
TITLE_TMPL = "KOSPI + Advance-Decline Line (Last {months}M, Candles)"
Y1_LABEL = "KOSPI"
Y2_LABEL = "A/D Line"

STATUS_MAP = {
    "BULLISH_CONFIRMATION": ("Bullish confirmation", "Price and A/D are both near prior highs", "#2e7d32"),
    "BULLISH_DIVERGENCE_CANDIDATE": ("Bullish divergence candidate", "Price is recovering faster than A/D", "#ef6c00"),
    "BULLISH_DIVERGENCE": ("Bullish divergence", "Price is near highs but A/D is clearly lagging", "#c62828"),
    "RECOVERY_IN_PROGRESS": ("Recovery in progress", "Price is re-attacking highs but breadth is not confirmed", "#616161"),
    "DOWNSIDE_DIVERGENCE_CANDIDATE": ("Downside divergence candidate", "Price is near lows but A/D is not confirming a new low", "#00838f"),
    "NORMAL_WEAKNESS": ("Normal weakness", "Price and A/D are both near lows", "#455a64"),
    "NEUTRAL": ("Neutral", "No decisive signal", "#757575"),
}


def parse_yyyymmdd(series):
    return pd.to_datetime(
        series.astype(str).str.replace(r"\.0$", "", regex=True),
        format="%Y%m%d",
        errors="coerce"
    )


def classify(price_off_high, ad_off_high, gap_off_high, price_off_low, ad_off_low,
             price_thr, ad_thr, gap_warn, gap_danger):
    price_near_high = price_off_high <= price_thr
    ad_near_high = ad_off_high <= ad_thr
    price_near_low = price_off_low <= price_thr
    ad_near_low = ad_off_low <= ad_thr

    if price_near_high and ad_near_high and gap_off_high <= 1.0:
        return "BULLISH_CONFIRMATION"
    if price_near_high and gap_off_high >= gap_danger:
        return "BULLISH_DIVERGENCE"
    if gap_off_high >= gap_warn:
        return "BULLISH_DIVERGENCE_CANDIDATE"
    if gap_off_high > 1.0:
        return "RECOVERY_IN_PROGRESS"
    if price_near_low and not ad_near_low:
        return "DOWNSIDE_DIVERGENCE_CANDIDATE"
    if price_near_low and ad_near_low:
        return "NORMAL_WEAKNESS"
    return "NEUTRAL"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--breadth", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--lookback", type=int, default=126)
    ap.add_argument("--price-thr", type=float, default=2.0)
    ap.add_argument("--ad-thr", type=float, default=3.0)
    ap.add_argument("--gap-warn", type=float, default=1.5)
    ap.add_argument("--gap-danger", type=float, default=2.5)
    ap.add_argument("--months", type=int, default=6)
    ap.add_argument("--png", required=True)
    args = ap.parse_args()

    breadth = pd.read_csv(args.breadth)
    if "date" not in breadth.columns:
        raise ValueError(f"breadth csv missing date column: {list(breadth.columns)}")
    breadth["date"] = parse_yyyymmdd(breadth["date"])
    breadth = breadth.dropna(subset=["date"]).sort_values("date").copy()

    index_df = pd.read_csv(args.index)
    need_cols = ["date", "open", "high", "low", "close"]
    miss = [c for c in need_cols if c not in index_df.columns]
    if miss:
        raise ValueError(f"index csv missing columns: {miss}; actual={list(index_df.columns)}")

    index_df["date"] = parse_yyyymmdd(index_df["date"])
    for c in ["open", "high", "low", "close"]:
        index_df[c] = pd.to_numeric(index_df[c], errors="coerce")
    index_df = index_df.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").copy()

    df = breadth.merge(
        index_df[["date", "open", "high", "low", "close"]],
        on="date",
        how="inner"
    ).sort_values("date").copy()

    print(f"[INFO] breadth rows = {len(breadth)}")
    print(f"[INFO] index rows   = {len(index_df)}")
    print(f"[INFO] merged rows  = {len(df)}")
    print(f"[INFO] breadth last = {breadth['date'].max().date()}")
    print(f"[INFO] index last   = {index_df['date'].max().date()}")
    if len(df) > 0:
        print(f"[INFO] merged range = {df['date'].min().date()} .. {df['date'].max().date()}")
        if breadth["date"].max() != index_df["date"].max():
            print("[WARN] breadth/index latest dates differ. Chart uses the latest COMMON date.")

    if len(df) < args.lookback:
        raise ValueError(f"merged rows too small: {len(df)}")

    last = df.iloc[-1]

    price_high = df["close"].rolling(args.lookback, min_periods=args.lookback).max().iloc[-1]
    ad_high = df["ad_line"].rolling(args.lookback, min_periods=args.lookback).max().iloc[-1]
    price_low = df["close"].rolling(args.lookback, min_periods=args.lookback).min().iloc[-1]
    ad_low = df["ad_line"].rolling(args.lookback, min_periods=args.lookback).min().iloc[-1]

    price_off_high_pct = (price_high - last["close"]) / price_high * 100 if price_high else float("nan")
    ad_off_high_pct = (ad_high - last["ad_line"]) / ad_high * 100 if ad_high else float("nan")
    gap_off_high_pct = ad_off_high_pct - price_off_high_pct

    price_off_low_pct = (last["close"] - price_low) / price_low * 100 if price_low else float("nan")
    ad_off_low_pct = (last["ad_line"] - ad_low) / ad_low * 100 if ad_low else float("nan")

    status_key = classify(
        price_off_high_pct, ad_off_high_pct, gap_off_high_pct,
        price_off_low_pct, ad_off_low_pct,
        args.price_thr, args.ad_thr, args.gap_warn, args.gap_danger
    )
    verdict, note, box_color = STATUS_MAP[status_key]

    print("=" * 72)
    print(f"Latest common date: {last['date'].date()}")
    print(f"KOSPI close: {last['close']:.2f}")
    print(f"A/D line: {last['ad_line']:.1f}")
    print(f"A/D diff: {last['ad_diff']:.1f}")
    print(f"Breadth thrust EMA10: {last['breadth_thrust_ema10']:.6f}")
    print("-" * 72)
    print(f"Price off high %: {price_off_high_pct:.2f}")
    print(f"A/D off high %:   {ad_off_high_pct:.2f}")
    print(f"A/D lag gap %:    {gap_off_high_pct:.2f}")
    print(f"Price off low %:  {price_off_low_pct:.2f}")
    print(f"A/D off low %:    {ad_off_low_pct:.2f}")
    print(f"Status: {verdict}")
    print(f"Note:   {note}")
    print("=" * 72)

    end_date = df["date"].max()
    start_date = end_date - pd.DateOffset(months=args.months)
    plot_df = df[df["date"] >= start_date].copy()

    print(f"[INFO] plot rows  = {len(plot_df)}")
    print(f"[INFO] plot range = {plot_df['date'].min().date()} .. {plot_df['date'].max().date()}")

    ohlc = plot_df[["date", "open", "high", "low", "close"]].copy()
    ohlc["date_num"] = ohlc["date"].map(mdates.date2num)
    ohlc = ohlc[["date_num", "open", "high", "low", "close"]]

    fig, axes = plt.subplots(
        2, 1, figsize=(15, 8), sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1]}
    )
    ax1, ax2 = axes

    candlestick_ohlc(
        ax1,
        ohlc.values,
        width=0.6,
        colorup="green",
        colordown="red",
        alpha=0.9
    )
    ax1.set_title(TITLE_TMPL.format(months=args.months))
    ax1.set_ylabel(Y1_LABEL)
    ax1.grid(True, alpha=0.3)

    ax2.plot(plot_df["date"], plot_df["ad_line"], linewidth=1.5)
    ax2.set_ylabel(Y2_LABEL)
    ax2.grid(True, alpha=0.3)

    locator = mdates.AutoDateLocator()
    formatter = mdates.DateFormatter("%Y-%m")
    ax2.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)

    left_box = (
        f"{last['date'].date()}\\n"
        f"Status: {verdict}\\n"
        f"Note: {note}\\n"
        f"Price off high: {price_off_high_pct:.2f}%\\n"
        f"A/D off high: {ad_off_high_pct:.2f}%\\n"
        f"A/D lag gap: {gap_off_high_pct:.2f}%"
    )
    ax1.text(
        0.01, 0.02, left_box,
        transform=ax1.transAxes,
        va="bottom", ha="left",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.92)
    )

    ax2.text(
        0.98, 0.92, f"{verdict}\\n{note}",
        transform=ax2.transAxes,
        ha="right", va="top",
        color="white", fontsize=11,
        bbox=dict(boxstyle="round,pad=0.45", facecolor=box_color, alpha=0.96)
    )

    fig.autofmt_xdate()
    plt.tight_layout()

    out_path = Path(args.png)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=160, bbox_inches="tight")
    print(f"saved chart: {out_path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
