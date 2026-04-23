# 실데이터 수집 스모크런

이 문서는 OpenDART API 키를 로컬 환경변수로 설정한 뒤, 소수 기업만 대상으로 `collect-analysis`를 실행해 수집 파이프라인을 점검하는 절차를 기록합니다.

## 목적

- 실제 OpenDART 응답이 현재 파서와 계정 매핑에 맞는지 확인합니다.
- 일부 연도, 일부 보고서 코드, 일부 지표가 비어 있을 때 `coverage-report.json`으로 누락 지점을 확인합니다.
- API 요청 실패가 전체 실행을 중단하지 않고 `collection-errors.json`에 기록되는지 확인합니다.

## 준비

API 키는 저장소에 커밋하지 않고 PowerShell 환경변수로만 설정합니다.

```powershell
$env:OPENDART_API_KEY="발급받은_키"
$env:PYTHONPATH="src"
```

회사 고유번호는 `company-master` 산출물에서 가져오거나 직접 텍스트 파일로 만들 수 있습니다. 예시는 삼성전자 고유번호만 사용합니다.

```powershell
New-Item -ItemType Directory -Force -Path data | Out-Null
Set-Content -Encoding UTF8 -Path data/smoke-corp-codes.txt -Value "00126380"
```

## 1개 기업 1년 빠른 확인

가장 먼저 요청 수가 적은 1년치로 확인합니다.

```powershell
python -m show_me_the_per.cli collect-analysis `
  --corp-code-file data/smoke-corp-codes.txt `
  --year-from 2025 `
  --year-to 2025 `
  --fs-div CFS `
  --output-dir data/smoke-analysis-1y `
  --threshold-percent 20 `
  --recent-annual-periods 1 `
  --recent-quarterly-periods 1
```

생성되는 파일은 다음과 같습니다.

- `data/smoke-analysis-1y/financial-statements.json`
- `data/smoke-analysis-1y/financial-period-values.json`
- `data/smoke-analysis-1y/growth-metrics.json`
- `data/smoke-analysis-1y/coverage-report.json`
- `data/smoke-analysis-1y/collection-errors.json`

확인 기준은 다음과 같습니다.

- `collection-errors.json`의 `summary.errors`가 `0`인지 확인합니다.
- `coverage-report.json`에서 요청한 기업의 `missing_report_codes`, `missing_business_years`를 확인합니다.
- `coverage-report.json`의 지표별 `missing_annual_years`, `missing_quarter_periods`를 확인합니다.

같은 입력을 브라우저에서 확인하려면 FastAPI 화면을 실행합니다.

```powershell
python -m show_me_the_per.cli web --host 127.0.0.1 --port 8000
```

브라우저에서 `http://127.0.0.1:8000`을 열고 기업 이름 `삼성전자`, 조회 연수 `1`, 기준 연도 `2025`, 재무제표 `연결`을 입력합니다. 결과 화면에서 연간 금액, 분기 금액, 성장률 필터 결과, 성장률 차트를 확인합니다.

## 최근 10년 확인

1년 스모크런이 성공하면 최근 10년 이상 기간을 실행합니다.

```powershell
python -m show_me_the_per.cli collect-analysis `
  --corp-code-file data/smoke-corp-codes.txt `
  --year-from 2015 `
  --year-to 2025 `
  --fs-div CFS `
  --output-dir data/smoke-analysis-10y `
  --database data/show-me-the-per.sqlite3 `
  --threshold-percent 20 `
  --recent-annual-periods 3 `
  --recent-quarterly-periods 12
```

일부 요청이 실패하더라도 기본값은 계속 진행입니다. 실패 즉시 멈추고 원인을 바로 보고 싶을 때만 `--fail-fast`를 추가합니다.

```powershell
python -m show_me_the_per.cli collect-analysis `
  --corp-code-file data/smoke-corp-codes.txt `
  --year-from 2025 `
  --year-to 2025 `
  --fail-fast `
  --output-dir data/smoke-analysis-fail-fast
```

## 산출물 관리

`data/`는 로컬 산출물 전용이며 `.gitignore`에 포함되어 있습니다. API 키, 원천 응답, 분석 JSON, 이후 생성할 DB 파일은 저장소에 커밋하지 않습니다.

## DB 저장 확인

스모크런에서 `--database`를 지정하지 않았다면 기존 산출물을 나중에 DB에 적재할 수 있습니다.

```powershell
python -m show_me_the_per.cli analysis-to-db `
  --input-dir data/smoke-analysis-10y `
  --database data/show-me-the-per.sqlite3 `
  --summary-output data/db-summary.json
```

DB 테이블별 row 수는 다음 명령으로 다시 확인합니다.

```powershell
python -m show_me_the_per.cli database-summary `
  --database data/show-me-the-per.sqlite3 `
  --output data/db-summary.json
```

성장률 필터 결과를 DB에서 바로 랭킹으로 조회합니다.

```powershell
python -m show_me_the_per.cli rank-growth-from-db `
  --database data/show-me-the-per.sqlite3 `
  --growth-metric revenue `
  --growth-series-type annual_yoy `
  --output data/db-growth-ranking.json
```

스모크런처럼 기업 수가 적고 필터를 통과한 기업이 없을 수 있는 경우에는 `--include-failed-growth`를 붙여 전체 결과를 확인합니다.

```powershell
python -m show_me_the_per.cli rank-growth-from-db `
  --database data/show-me-the-per.sqlite3 `
  --growth-metric revenue `
  --growth-series-type annual_yoy `
  --include-failed-growth `
  --output data/db-growth-ranking-all.json
```

회사별 성장률 숫자와 차트는 HTML 리포트로 생성합니다.

```powershell
python -m show_me_the_per.cli company-growth-report `
  --database data/show-me-the-per.sqlite3 `
  --corp-code 00126380 `
  --recent-years 10 `
  --output data/samsung-growth-report.html
```

여러 회사를 성장률 조건으로 비교할 때는 HTML 랭킹 리포트를 생성합니다.

```powershell
python -m show_me_the_per.cli growth-ranking-report `
  --database data/show-me-the-per.sqlite3 `
  --growth-metric revenue `
  --growth-series-type annual_yoy `
  --limit 50 `
  --output data/growth-ranking-report.html
```
