# ShowMeThePER

한국 상장기업의 재무 데이터를 수집하고, 브라우저에서 분석/비교/필터링할 수 있는 프로젝트입니다.

현재 프로젝트는 다음 흐름을 중심으로 동작합니다.

- `재무정보`: 단일 기업 재무와 성장률 보기
- `VS 기업비교`: 두 기업 비교
- `기업필터`: 성장률 조건에 맞는 기업 찾기
- `DB 업데이트`: 회사 목록 동기화, 배치 수집, 진단, 키 관리

## 빠른 시작

상세 가이드는 아래 문서를 먼저 보는 것을 권장합니다.

- [처음 사용 가이드](docs/getting-started.md)

가장 빠른 실행 순서는 아래와 같습니다.

### 1. 저장소 받기

```powershell
git clone https://github.com/cheoleun/ShowMeThePER.git
cd ShowMeThePER
```

### 2. Python 환경 준비

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .
```

또는 conda를 사용해도 됩니다.

```powershell
conda create -n showmetheper python=3.11 -y
conda activate showmetheper
python -m pip install --upgrade pip
pip install -e .
```

### 3. API 키 설정

프로젝트는 두 종류의 키를 사용합니다.

- `OPENDART_API_KEY`: 재무제표 수집용
- `KRX_SERVICE_KEY`: 상장사 목록/전일 종가/시가총액 조회용

PowerShell 예시:

```powershell
$env:OPENDART_API_KEY="발급받은_OpenDART_키"
$env:KRX_SERVICE_KEY="발급받은_KRX_서비스키"
```

발급 방법은 [처음 사용 가이드](docs/getting-started.md)에 정리되어 있습니다.

### 4. 웹 서버 실행

```powershell
python -m show_me_the_per.cli web --host 127.0.0.1 --port 8000
```

또는 설치된 콘솔 스크립트를 사용할 수 있습니다.

```powershell
show-me-the-per web --host 127.0.0.1 --port 8000
```

브라우저에서 열기:

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

## 현재 구현 범위

현재 기준으로 구현되어 있는 주요 기능은 아래와 같습니다.

### 웹 UI

- FastAPI 기반 브라우저 UI
- 기업명 기준 조회
- DB 기준 재무정보 렌더링
- 단일 기업 재무 차트
- 두 기업 비교 화면
- 기업필터 화면
- DB 업데이트 운영 화면

### 재무/성장률

- 연간 / 분기 / 최근 4분기 누적 금액 표시
- YoY / QoQ 성장률 계산
- 최근 구간 요약
- 매출 / 영업이익 / 순이익 / EPS 표시
- 전일 종가 / 시가총액 / 시장 구분 표시

### 기업필터

- 시장, 기준 연도, 재무제표 구분, 정렬, 표시 개수 설정
- 성장률 조건을 아래 축으로 선택 가능
  - 연간 YoY
  - 분기 YoY
  - 분기 QoQ
  - 최근 4분기 누적 YoY
- 각 조건마다
  - 적용 여부
  - 최근 몇 년 / 몇 분기
  - 기준 성장률 %
  를 별도로 지정 가능
- 조건을 모두 만족한 기업만 결과에 표시

### DB 업데이트

- 회사 목록 동기화
- 배치 작업 생성 / 일시정지 / 이어받기 / 실패만 재시도
- KRX 연결 점검
- OpenDART 키 관리
- 전체 DB 초기화
- 실패/건너뜀 원인 요약 표시

### CLI

다음 CLI 명령이 있습니다.

- `company-master`
- `collect-analysis`
- `analysis-to-db`
- `database-summary`
- `rank-growth-from-db`
- `company-growth-report`
- `growth-ranking-report`
- `rank-companies`
- `web`

웹 UI가 주 사용 경로이지만, 배치/보고서/DB 요약은 CLI로도 실행할 수 있습니다.

## 탭 설명

### 재무정보

단일 기업의 재무 흐름을 보는 화면입니다.

- 최근 구간 요약
- 연간/분기/최근 4분기 누적 차트
- 성장률 상세
- EPS, 전일 종가, 시가총액, 시장 구분

이 화면은 DB를 먼저 조회하고, 데이터가 없거나 부족하면 OpenDART에서 수집한 뒤 DB에 저장하고 다시 DB 기준으로 렌더링합니다.

### VS 기업비교

두 기업의 재무 흐름을 같은 기준으로 비교합니다.

- 연간/분기/최근 4분기 누적 비교
- 성장률 비교
- 회사별 요약 정보

### 기업필터

성장률 조건에 맞는 기업 리스트를 찾는 화면입니다.

- 탭 진입 시 자동 조회하지 않음
- `조회` 버튼을 눌렀을 때만 결과를 계산
- 결과는 “조건에 맞는 기업” 리스트로 표시

### DB 업데이트

운영 전용 화면입니다.

- 회사 목록 동기화
- 재무 데이터 배치 수집
- KRX 연결 점검
- OpenDART 키 관리
- DB 초기화

## 데이터 저장 방식

이 프로젝트는 웹 화면에서도 DB를 적극적으로 사용합니다.

- 재무정보/비교 화면은 DB를 기준 저장소로 사용
- OpenDART에서 새 데이터를 가져오더라도 먼저 DB에 반영한 뒤 DB 기준으로 다시 렌더링
- 기업필터도 DB에 저장된 성장률 포인트를 기준으로 계산

### 기본 DB 위치

Windows 기본 경로:

- `%LOCALAPPDATA%\\show-me-the-per-cfs.sqlite3`
- `%LOCALAPPDATA%\\show-me-the-per-ofs.sqlite3`
- `%LOCALAPPDATA%\\show-me-the-per-all.sqlite3`
- `%LOCALAPPDATA%\\show-me-the-per-settings.sqlite3`

원하면 캐시 디렉터리를 직접 지정할 수 있습니다.

```powershell
$env:SHOW_ME_THE_PER_WEB_CACHE_DIR="C:\\my-cache\\show-me-the-per"
```

## API 키와 진단

### OpenDART

재무제표 수집에 사용합니다.

- 환경변수: `OPENDART_API_KEY`
- 또는 `DB 업데이트 > 데이터/API 설정`에서 로컬 저장 키 관리 가능

OpenDART 요청 제한 또는 키 오류가 발생하면 배치 작업은 `blocked` 상태로 멈추고, 키를 바꾼 뒤 `이어받기` 할 수 있습니다.

### KRX

상장사 목록과 전일 시세 정보를 위해 사용합니다.

- 환경변수: `KRX_SERVICE_KEY`

관련 API:

- [금융위원회_KRX상장종목정보](https://www.data.go.kr/data/15094775/openapi.do)
- [금융위원회_주식시세정보](https://www.data.go.kr/data/15094808/openapi.do)

문제가 있으면 `DB 업데이트 > KRX 연결 점검 실행`부터 확인하는 것이 좋습니다.

## 권장 사용 순서

실제 사용 흐름은 아래 순서를 권장합니다.

1. API 키 준비
2. 웹 서버 실행
3. `DB 업데이트`로 이동
4. `데이터/API 설정` 확인
5. `KRX 연결 점검 실행`
6. `회사 목록 동기화`
7. `새 작업 시작`
8. 배치 수집 진행
9. `재무정보`, `VS 기업비교`, `기업필터` 사용

## 테스트

전체 테스트:

```powershell
python -m unittest discover -s tests -v
```

웹 테스트만:

```powershell
python -m unittest discover -s tests -p "test_web.py" -v
```

## 문서

- [처음 사용 가이드](docs/getting-started.md)
- [요구사항 문서](docs/requirements.md)
- [데이터 수집 전략](docs/data-strategy.md)
- [스모크 런 가이드](docs/smoke-run.md)

## 참고

이 프로젝트는 현재 웹 UI와 로컬 DB 중심으로 동작합니다.  
README는 현재 구현 상태에 맞춰 정리했으며, 더 자세한 운영 절차와 키 발급 방법은 [처음 사용 가이드](docs/getting-started.md)를 기준으로 보시면 됩니다.
