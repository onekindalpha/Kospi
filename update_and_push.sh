#!/bin/bash
# 매일 장 마감 후 실행: 브레드스 + 지수 CSV 업데이트 후 GitHub push
# 실행:
#   ./update_and_push.sh
#
# KOSDAQ 브레드스 승인 후 실행:
#   ENABLE_KOSDAQ_BREADTH=1 ./update_and_push.sh

set -u

cd /Users/velocitygoal/Public/kospi || exit 1

# ── KRX 인증키 확인 ─────────────────────────────────────────
# 절대 여기 파일에 인증키를 직접 쓰지 말 것.
# 로컬 실행 전:
#   export KRX_AUTH_KEY="발급받은_키"
if [ -z "${KRX_AUTH_KEY:-}" ]; then
    echo "[ERROR] KRX_AUTH_KEY not set"
    echo "실행 전 아래처럼 설정:"
    echo '  export KRX_AUTH_KEY="발급받은_키"'
    exit 1
fi

TODAY=$(date +%Y%m%d)
mkdir -p data

# macOS / Linux 날짜 호환 함수
next_day() {
    local ymd="$1"
    date -j -f "%Y%m%d" -v+1d "$ymd" +%Y%m%d 2>/dev/null || date -d "$ymd + 1 day" +%Y%m%d
}

one_year_ago() {
    date -j -v-1y +%Y%m%d 2>/dev/null || date -d "1 year ago" +%Y%m%d
}

# CSV 마지막 날짜 다음날부터 시작, 없으면 1년치
calc_start() {
    local csv="$1"

    if [ -f "$csv" ] && [ "$(wc -l < "$csv")" -gt 1 ]; then
        local last
        last=$(tail -1 "$csv" | cut -d',' -f1 | tr -d '\r')
        next_day "$last"
    else
        one_year_ago
    fi
}

# ── KOSPI 브레드스 ──────────────────────────────────────────
KOSPI_CSV="data/kospi_breadth.csv"
START=$(calc_start "$KOSPI_CSV")

if [ -f "$KOSPI_CSV" ] && [ "$(wc -l < "$KOSPI_CSV")" -gt 1 ]; then
    echo "KOSPI 브레드스: $START ~ $TODAY (이어받기)"
else
    echo "KOSPI 브레드스: $START ~ $TODAY (처음 수집, 1년치)"
fi

python krx_breadth_openapi_exact_v4.py \
    --market KOSPI \
    --start "$START" \
    --end "$TODAY" \
    --out "$KOSPI_CSV" \
    && echo "KOSPI 브레드스 저장 완료" \
    || echo "[SKIP] KOSPI 브레드스: 새 데이터 없음 또는 수집 실패"

# ── KOSDAQ 브레드스 ─────────────────────────────────────────
# 승인 전에는 ENABLE_KOSDAQ_BREADTH=1을 주지 말 것.
# 필요한 승인 API:
#   코스닥 일별매매정보 / ksq_bydd_trd
KOSDAQ_CSV="data/kosdaq_breadth.csv"
START=$(calc_start "$KOSDAQ_CSV")

if [ -f "$KOSDAQ_CSV" ] && [ "$(wc -l < "$KOSDAQ_CSV")" -gt 1 ]; then
    echo "KOSDAQ 브레드스: $START ~ $TODAY (이어받기)"
else
    echo "KOSDAQ 브레드스: $START ~ $TODAY (처음 수집, 1년치)"
fi

if [ "${ENABLE_KOSDAQ_BREADTH:-0}" = "1" ]; then
    python krx_breadth_openapi_exact_v4.py \
        --market KOSDAQ \
        --start "$START" \
        --end "$TODAY" \
        --out "$KOSDAQ_CSV" \
        && echo "KOSDAQ 브레드스 저장 완료" \
        || echo "[SKIP] KOSDAQ 브레드스 실패: ksq_bydd_trd 승인/권한/날짜 확인"
else
    echo "[SKIP] KOSDAQ 브레드스: ksq_bydd_trd 승인 대기 중"
    echo "승인 후 실행:"
    echo "  ENABLE_KOSDAQ_BREADTH=1 ./update_and_push.sh"
fi

# ── KOSPI 지수 OHLC ─────────────────────────────────────────
echo "KOSPI 지수 OHLC 수집 중..."
python fetch_kospi_index_ohlc_quick.py \
    --start 20230101 \
    --end "$TODAY" \
    --out data/kospi_index.csv \
    || echo "[WARN] KOSPI 지수 OHLC 수집 실패"

# ── KOSDAQ 지수 OHLC ────────────────────────────────────────
echo "KOSDAQ 지수 OHLC 수집 중..."
python -c "
import pandas as pd
import FinanceDataReader as fdr

today = '$TODAY'
df = fdr.DataReader('KQ11', '20230101', '$(date +%Y-%m-%d)')
if df.empty:
    raise RuntimeError('KQ11 empty')

out = df.reset_index()[['Date','Open','High','Low','Close']].copy()
out.columns = ['date','open','high','low','close']
out['date'] = pd.to_datetime(out['date']).dt.strftime('%Y%m%d')
out = out[out['date'] <= today]

if out.empty:
    raise RuntimeError('KOSDAQ index output empty')

out.to_csv('data/kosdaq_index.csv', index=False, encoding='utf-8-sig')
print('saved data/kosdaq_index.csv, last:', out['date'].iloc[-1])
" || echo "[WARN] KOSDAQ 지수 OHLC 수집 실패"

# ── GitHub push ─────────────────────────────────────────────
echo ""
echo "GitHub push 중..."
git add data/

if git diff --cached --quiet; then
    echo "변경 없음 (커밋 스킵)"
else
    git commit -m "data: $TODAY 업데이트"
    git pull --rebase origin main && git push origin main || echo "[WARN] push 실패 — git status 확인"
fi

echo ""
echo "✅ 완료! ($TODAY)"