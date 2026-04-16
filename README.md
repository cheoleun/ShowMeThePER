# ShowMeThePER

한국 상장기업의 재무제표와 성장률 지표를 수집하고 보여주는 프로젝트입니다.

## 현재 구현 범위

첫 개발 단계에서는 상장기업 목록과 OpenDART 기업 고유번호를 매칭하는 기반을 제공합니다.

- KRX 상장종목정보 API 응답 파싱
- OpenDART 고유번호 ZIP/XML 응답 파싱
- KRX 단축코드와 OpenDART `stock_code` 기반 매칭
- 매칭 성공, 미매칭, 중복 후보 분리
- API 키 없이 실행 가능한 단위 테스트

## CLI

환경 변수 또는 인자로 API 키를 넘겨 기업 마스터 JSON을 생성할 수 있습니다.

```powershell
$env:KRX_SERVICE_KEY="..."
$env:OPENDART_API_KEY="..."
$env:PYTHONPATH="src"
python -m show_me_the_per.cli --output data/company-master.json
```

## 테스트

```powershell
$env:PYTHONPATH="src"
python -m unittest discover -s tests -v
```
