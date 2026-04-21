#!/bin/bash
# 매일 장 마감 후 실행: 브레드스 + 지수 CSV 업데이트 후 GitHub push
# 실행: ./update_and_push.sh

cd /Users/velocitygoal/Public/kospi

export KRX_AUTH_KEY=6B1C3BAAFE1142C884CEB9A2599001E7CCD4E35B

TODAY=$(date +%Y%m%d)
mkdir -p data

# ── KOSPI 브레드스 ──
KOSPI_CSV="data/kospi_breadth.csv"
if [ -f "$KOSPI_CSV" ]; then
    LAST=$(tail -1 $KOSPI_CSV | cut -d',' -f1 | tr -d '\r')
    START=$(date -j -f "%Y%m%d" -v+1d "$LAST" +%Y%m%d 2>/dev/null || date -d "$LAST + 1 day" +%Y%m%d)
    echo "KOSPI 브레드스: $START ~ $TODAY (이어받기)"
else
    START=$(date -j -v-2y +%Y%m%d 2>/dev/null || date -d "2 years ago" +%Y%m%d)
    echo "KOSPI 브레드스: $START ~ $TODAY (처음 수집)"
fi
python krx_breadth_openapi_exact_v4.py --market KOSPI --start $START --end $TODAY --out $KOSPI_CSV \
    && echo "KOSPI 브레드스 저장 완료" \
    || echo "[SKIP] KOSPI 브레드스: 새 데이터 없음 (오늘 휴장이거나 아직 미반영)"

# ── KOSDAQ 브레드스 ──
KOSDAQ_CSV="data/kosdaq_breadth.csv"
if [ -f "$KOSDAQ_CSV" ]; then
    LAST=$(tail -1 $KOSDAQ_CSV | cut -d',' -f1 | tr -d '\r')
    START=$(date -j -f "%Y%m%d" -v+1d "$LAST" +%Y%m%d 2>/dev/null || date -d "$LAST + 1 day" +%Y%m%d)
    echo "KOSDAQ 브레드스: $START ~ $TODAY (이어받기)"
else
    START=$(date -j -v-2y +%Y%m%d 2>/dev/null || date -d "2 years ago" +%Y%m%d)
    echo "KOSDAQ 브레드스: $START ~ $TODAY (처음 수집)"
fi
python krx_breadth_openapi_exact_v4.py --market KOSDAQ --start $START --end $TODAY --out $KOSDAQ_CSV \
    && echo "KOSDAQ 브레드스 저장 완료" \
    || echo "[SKIP] KOSDAQ 브레드스: 새 데이터 없음 (오늘 휴장이거나 아직 미반영)"

# ── KOSPI 지수 OHLC ──
echo "KOSPI 지수 OHLC 수집 중..."
python fetch_kospi_index_ohlc_quick.py --start 20230101 --end $TODAY --out data/kospi_index.csv

# ── KOSDAQ 지수 OHLC ──
echo "KOSDAQ 지수 OHLC 수집 중..."
# KQ11 사용 (KOSDAQ 지수)
python -c "
import sys, pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
df = fdr.DataReader('KQ11', '20230101', '$(date +%Y-%m-%d)')
if df.empty: raise RuntimeError('KQ11 empty')
out = df.reset_index()[['Date','Open','High','Low','Close']].copy()
out.columns = ['date','open','high','low','close']
out['date'] = pd.to_datetime(out['date']).dt.strftime('%Y%m%d')
out = out[out['date'] <= '$TODAY']
out.to_csv('data/kosdaq_index.csv', index=False, encoding='utf-8-sig')
print('saved data/kosdaq_index.csv, last:', out['date'].iloc[-1])
"

# ── GitHub push ──
echo ""
echo "GitHub push 중..."
git add data/
git commit -m "data: $TODAY 업데이트" || echo "변경 없음 (커밋 스킵)"
git push origin main

echo ""
echo "✅ 완료! ($TODAY)"
