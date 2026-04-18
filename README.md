# ShowMeThePER

한국 상장기업의 재무제표와 성장률 지표를 수집하고 보여주는 프로젝트입니다.

## 현재 구현 범위

첫 개발 단계에서는 상장기업 목록과 OpenDART 기업 고유번호를 매칭하는 기반을 제공합니다.

- KRX 상장종목정보 API 응답 파싱
- OpenDART 고유번호 ZIP/XML 응답 파싱
- KRX 단축코드와 OpenDART `stock_code` 기반 매칭
- 매칭 성공, 미매칭, 중복 후보 분리
- 기업 마스터 JSON, CSV, Markdown 리포트 출력
- OpenDART 다중회사 주요 재무계정 응답 파싱
- OpenDART 주요 재무계정 row를 연간 성장률 계산용 기간값으로 정규화
- 보고서별 누적값에서 분기별 표준 재무값 산출
- 연간 YoY, 분기 YoY, 최근 4분기 합산 YoY 성장률 계산
- 최근 N개 성장률이 모두 기준 이상인지 판정하는 기본 성장률 필터
- 여러 연도와 보고서 코드를 한 번에 수집해 분석 산출물과 커버리지 리포트 출력
- 수집 요청 일부가 실패해도 계속 진행하고 오류 리포트 출력
- 분석 산출물을 SQLite DB에 저장하고 요약 조회
- SQLite DB에 저장된 성장률 필터 결과로 성장률 랭킹 조회
- SQLite DB에 저장된 회사별 성장률을 정적 HTML 리포트로 출력
- SQLite DB에 저장된 성장률 필터 결과를 정적 HTML 랭킹 리포트로 출력
- FastAPI 브라우저 화면에서 요청 기업의 N년치 재무제표 수집과 성장률 분석 실행
- 성장률, PER, PBR, ROE 기반 순위 JSON 출력
- API 키 없이 실행 가능한 단위 테스트

## 브라우저 UI

OpenDART API 키를 환경변수로 설정한 뒤 FastAPI 서버를 실행하면 브라우저에서 기업 이름과 조회 연수를 입력해 바로 분석할 수 있습니다.

```powershell
$env:OPENDART_API_KEY="..."
$env:PYTHONPATH="src"
python -m show_me_the_per.cli web --host 127.0.0.1 --port 8000
```

브라우저에서 `http://127.0.0.1:8000`을 열고 기업 이름, 조회 연수, 기준 연도, 재무제표 구분을 입력합니다. 기업 이름 대신 종목코드나 OpenDART 고유번호도 사용할 수 있습니다. 결과 화면에는 연간 금액, 분기 금액, 성장률 필터 결과, 성장률 차트가 표시됩니다.

## CLI

환경 변수 또는 인자로 API 키를 넘겨 기업 마스터 파일을 생성할 수 있습니다.

```powershell
$env:KRX_SERVICE_KEY="..."
$env:OPENDART_API_KEY="..."
$env:PYTHONPATH="src"
python -m show_me_the_per.cli company-master `
  --output data/company-master.json `
  --matched-csv data/company-master-matched.csv `
  --unmatched-csv data/company-master-unmatched.csv `
  --ambiguous-json data/company-master-ambiguous.json `
  --report data/company-master-report.md
```

하위 호환을 위해 `company-master` 서브커맨드는 생략할 수 있습니다.

OpenDART 주요 재무계정도 기업 고유번호 기준으로 수집할 수 있습니다.

```powershell
$env:OPENDART_API_KEY="..."
$env:PYTHONPATH="src"
python -m show_me_the_per.cli financial-statements `
  --corp-code 00126380 `
  --business-year 2025 `
  --report-code 11011 `
  --fs-div CFS `
  --output data/financial-statements.json
```

수집한 주요 재무계정 row에서 매출, 영업이익, 순이익의 연간 기간값을 만들 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli financial-period-values `
  --input data/financial-statements.json `
  --output data/financial-period-values.json
```

정규화된 기간별 재무값 JSON에서 성장률 지표와 기본 필터 결과를 계산할 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli growth-metrics `
  --input data/financial-period-values.json `
  --output data/growth-metrics.json `
  --threshold-percent 20 `
  --recent-annual-periods 3 `
  --recent-quarterly-periods 12
```

입력 JSON은 다음처럼 `values` 배열을 사용합니다.

```json
{
  "values": [
    {
      "corp_code": "00126380",
      "metric": "revenue",
      "period_type": "annual",
      "fiscal_year": 2024,
      "amount": "300000000000000"
    },
    {
      "corp_code": "00126380",
      "metric": "revenue",
      "period_type": "quarter",
      "fiscal_year": 2025,
      "fiscal_quarter": 1,
      "amount": "79000000000000"
    }
  ]
}
```

성장률 결과와 선택적인 PER/PBR/ROE JSON을 조합해 순위를 만들 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli rank-companies `
  --growth-input data/growth-metrics.json `
  --valuation-input data/valuation-metrics.json `
  --output data/rankings.json `
  --growth-metric revenue `
  --growth-series-type annual_yoy `
  --max-per 15 `
  --max-pbr 1 `
  --min-roe 20 `
  --rank-valuation-by roe
```

PER/PBR/ROE 입력 JSON은 다음처럼 `companies` 배열을 사용합니다.

```json
{
  "companies": [
    {
      "corp_code": "00126380",
      "corp_name": "삼성전자",
      "stock_code": "005930",
      "per": "12.5",
      "pbr": "1.2",
      "roe": "18.4"
    }
  ]
}
```

여러 기업과 여러 연도를 한 번에 수집한 뒤 표준 기간값, 성장률, 커버리지 리포트까지 생성할 수 있습니다. 기본 보고서 코드는 1분기, 반기, 3분기, 사업보고서입니다.

```powershell
$env:OPENDART_API_KEY="..."
$env:PYTHONPATH="src"
python -m show_me_the_per.cli collect-analysis `
  --corp-code-file data/company-master.json `
  --year-from 2015 `
  --year-to 2025 `
  --fs-div CFS `
  --output-dir data/analysis `
  --database data/show-me-the-per.sqlite3 `
  --threshold-percent 20 `
  --recent-annual-periods 3 `
  --recent-quarterly-periods 12
```

`collect-analysis`는 다음 파일을 생성합니다.

- `financial-statements.json`: OpenDART 주요 재무계정 원천 row
- `financial-period-values.json`: 연간/분기별 표준 재무값
- `growth-metrics.json`: 연간 YoY, 분기 YoY, 최근 4분기 합산 YoY 성장률과 기본 필터 결과
- `coverage-report.json`: 기업별 수집 연도, 보고서 코드, 지표별 연간/분기 데이터 확보 여부, 성장률 필터 결과
- `collection-errors.json`: 수집 요청별 실패 연도, 보고서 코드, 오류 유형과 메시지

일부 OpenDART 요청이 실패하면 기본적으로 나머지 요청을 계속 진행하고 오류를 `collection-errors.json`에 기록합니다. 실패 즉시 중단하려면 `--fail-fast`를 추가합니다.

실제 API 키로 빠르게 점검하는 절차는 [실데이터 수집 스모크런](docs/smoke-run.md)에 정리되어 있습니다.

이미 만들어둔 분석 산출물 디렉터리를 SQLite DB에 저장할 수도 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli analysis-to-db `
  --input-dir data/smoke-analysis-10y `
  --database data/show-me-the-per.sqlite3 `
  --summary-output data/db-summary.json
```

DB 요약은 다음 명령으로 확인할 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli database-summary `
  --database data/show-me-the-per.sqlite3 `
  --output data/db-summary.json
```

DB에 저장된 성장률 필터 결과로 랭킹을 조회할 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli rank-growth-from-db `
  --database data/show-me-the-per.sqlite3 `
  --growth-metric revenue `
  --growth-series-type annual_yoy `
  --output data/db-growth-ranking.json
```

기본값은 성장률 필터를 통과한 결과만 대상으로 삼습니다. 통과하지 못한 결과까지 함께 비교하려면 `--include-failed-growth`를 추가합니다.

SQLite DB에는 원천 주요 재무계정 row, 표준 기간값, 성장률 포인트, 성장률 필터 결과, 수집 오류가 저장됩니다. `data/`와 DB 파일은 로컬 산출물로 보고 `.gitignore`에 포함합니다.

회사별 성장률 숫자와 차트는 정적 HTML 리포트로 확인할 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli company-growth-report `
  --database data/show-me-the-per.sqlite3 `
  --corp-code 00126380 `
  --recent-years 10 `
  --output data/samsung-growth-report.html
```

DB에 저장된 성장률 필터 결과를 여러 회사 기준으로 비교하는 정적 HTML 랭킹 리포트도 생성할 수 있습니다.

```powershell
$env:PYTHONPATH="src"
python -m show_me_the_per.cli growth-ranking-report `
  --database data/show-me-the-per.sqlite3 `
  --growth-metric revenue `
  --growth-series-type annual_yoy `
  --limit 50 `
  --output data/growth-ranking-report.html
```

## 테스트

```powershell
$env:PYTHONPATH="src"
python -m unittest discover -s tests -v
```
