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
- 연간 YoY, 분기 YoY, 최근 4분기 합산 YoY 성장률 계산
- 최근 N개 성장률이 모두 기준 이상인지 판정하는 기본 성장률 필터
- API 키 없이 실행 가능한 단위 테스트

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

## 테스트

```powershell
$env:PYTHONPATH="src"
python -m unittest discover -s tests -v
```
