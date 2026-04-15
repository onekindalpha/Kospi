# Lynch-style JSON templates

이 폴더에는 하이닉스/삼전/삼전기에서 하던 방식처럼 표를 출력하기 위한 템플릿이 들어 있다.

## 파일 목록
- coin_template.json
- uvv_template.json
- o_template.json
- fiat_template.json
- generate_lynch_tables.py

## 사용법
1. JSON 안의 숫자를 최신 값으로 채운다.
2. 터미널에서 아래처럼 실행한다.

python generate_lynch_tables.py coin_template.json uvv_template.json o_template.json fiat_template.json

3. 같은 폴더에 .md 파일이 생성된다.
   - 회사형(COIN/UVV/O)은 하이닉스/삼전식 표
   - ETF형(FIAT)은 ETF 전용 표

## 데이터 출처 권장
- 미국 회사주(COIN/UVV/O)
  - 재무제표: SEC EDGAR 10-K / 10-Q / Company Facts
  - 배당: 회사 IR dividend history
  - 현재가: 브로커/시세 API
- ETF(FIAT)
  - 공식 YieldMax 페이지
  - SEC prospectus
  - 시세/NAV 데이터

## 자동 채움 스크립트
- `autofill_us_data.py`

예시:
```bash
python autofill_us_data.py coin_template.json uvv_template.json o_template.json fiat_template.json
python generate_lynch_tables.py coin_template_autofilled.json uvv_template_autofilled.json o_template_autofilled.json fiat_template_autofilled.json
```

설명:
- 회사형(COIN/UVV/O)은 SEC Company Facts + Yahoo quote 기준으로 채움
- ETF형(FIAT)은 Yahoo quote + 공식 YieldMax 페이지를 best effort로 채움
- 일부 값은 공시 구조/사이트 구조에 따라 0으로 남을 수 있으니 검산 필요
# Kospi
