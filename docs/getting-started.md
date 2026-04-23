# ShowMeThePER 처음 사용 가이드

이 문서는 `ShowMeThePER` 저장소를 처음 받아서 실행하는 사람을 위한 빠른 시작 가이드입니다.

다룹니다:

- 개발 환경 준비
- `OpenDART`, `KRX` 키 발급
- 웹 서버 실행
- 첫 사용 순서
- 각 메뉴 설명
- 자주 겪는 문제

기준 날짜: 2026-04-23  
공식 안내 링크:

- OpenDART 인증키 신청: [opendart.fss.or.kr](https://opendart.fss.or.kr/uss/umt/EgovMberInsertView.do)
- OpenDART 고유번호 가이드: [corpCode.xml 개발가이드](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018)
- KRX 상장종목정보: [data.go.kr 15094775](https://www.data.go.kr/data/15094775/openapi.do)
- KRX 주식시세정보: [data.go.kr 15094808](https://www.data.go.kr/data/15094808/openapi.do)

## 1. 프로젝트 개요

이 프로젝트는 한국 상장기업의 재무 데이터를 수집하고, 다음 화면으로 보여줍니다.

- `재무정보`: 단일 기업의 연간/분기 재무와 성장률
- `VS 기업비교`: 두 기업 비교
- `기업필터`: 조건에 맞는 기업 리스트
- `DB 업데이트`: 회사 목록 동기화, 배치 수집, 키 설정, 진단

핵심 데이터 소스는 다음 두 가지입니다.

- `OpenDART`: 기업 재무제표 수집
- `KRX / 공공데이터포털`: 상장종목 목록, 전일 종가, 시가총액, 시장 구분

## 2. 권장 개발 환경

### 최소 요구사항

- Windows PowerShell 기준 설명
- Python `3.9+`
- 인터넷 연결
- OpenDART API 키
- KRX 서비스 키

### 권장 버전

- Python `3.11`
- `pip install -e .` 방식

## 3. 저장소 받기

```powershell
git clone https://github.com/cheoleun/ShowMeThePER.git
cd ShowMeThePER
```

## 4. Python 환경 만들기

### 방법 A. venv 사용

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .
```

### 방법 B. anaconda / conda 사용

```powershell
conda create -n showmetheper python=3.11 -y
conda activate showmetheper
python -m pip install --upgrade pip
pip install -e .
```

설치되는 주요 의존성은 현재 아래와 같습니다.

- `fastapi`
- `httpx`
- `uvicorn[standard]`

## 5. API 키 준비

### 5-1. OpenDART 키 발급

프로젝트의 재무 데이터 수집에는 `OpenDART` 키가 필요합니다.

발급 경로:

1. [OpenDART 인증키 신청 페이지](https://opendart.fss.or.kr/uss/umt/EgovMberInsertView.do) 접속
2. 회원가입 / 로그인
3. 인증키 신청
4. 발급 완료 후 키 확인

공식 안내상:

- 개인 회원은 계정 신청 완료 후 즉시 발급
- 기업 회원은 담당자 승인 후 발급

프로젝트에서 사용하는 환경변수 이름:

- `OPENDART_API_KEY`

### 5-2. KRX 키 발급

프로젝트의 회사 목록 동기화와 전일 시세 정보에는 `공공데이터포털` 서비스 키가 필요합니다.

활용신청 권장 대상:

1. [금융위원회_KRX상장종목정보](https://www.data.go.kr/data/15094775/openapi.do)
2. [금융위원회_주식시세정보](https://www.data.go.kr/data/15094808/openapi.do)

권장 순서:

1. [공공데이터포털](https://www.data.go.kr/) 로그인
2. 위 두 API 페이지에서 각각 `활용신청`
3. `마이페이지 > 오픈API > 활용신청현황`에서 상태 확인
4. 발급된 서비스키 확인

프로젝트에서 사용하는 환경변수 이름:

- `KRX_SERVICE_KEY`

주의:

- 실사용 기준으로 위 **두 API 모두 활용신청**해 두는 것이 안전합니다.
- 회사 목록 동기화는 `KRX상장종목정보`
- 전일 종가/시가총액은 `주식시세정보`
  를 사용합니다.

## 6. 환경변수 설정

### 현재 PowerShell 세션에만 설정

```powershell
$env:OPENDART_API_KEY="발급받은_OpenDART_키"
$env:KRX_SERVICE_KEY="발급받은_KRX_서비스키"
```

### 예시

```powershell
$env:OPENDART_API_KEY="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
$env:KRX_SERVICE_KEY="yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
```

## 7. 실행 방법

루트 디렉터리에서 실행합니다.

### 방법 A. 모듈 실행

```powershell
python -m show_me_the_per.cli web --host 127.0.0.1 --port 8000
```

### 방법 B. 설치된 스크립트 실행

```powershell
show-me-the-per web --host 127.0.0.1 --port 8000
```

브라우저에서 열기:

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

## 8. 처음 사용할 때 추천 순서

### 가장 추천하는 순서

1. 웹 서버 실행
2. 브라우저에서 `/db-update`로 이동
3. `데이터/API 설정` 확인
4. `KRX 연결 점검 실행`
5. `회사 목록 동기화`
6. `새 작업 시작`
7. 배치 수집 완료 후 `기업필터`, `재무정보`, `VS 기업비교` 사용

### 최소한의 빠른 사용 순서

배치 수집 없이 단일 기업부터 보고 싶다면:

1. 서버 실행
2. 메인 화면에서 기업명 입력
3. `재무정보`에서 조회

이 경우에도 OpenDART 키는 필요합니다.

## 9. 각 메뉴 설명

## 9-1. 재무정보

단일 기업의 재무 흐름을 보는 화면입니다.

입력:

- 기업명
- 조회 기간
- 기준 연도
- 재무제표 구분(`연결`, `별도`, `전체`)

보여주는 내용:

- 최근 구간 요약
- 연간/분기/최근 4분기 누적 금액 차트
- 매출 / 영업이익 / 순이익 / EPS
- YoY / QoQ 성장률
- 전일 종가 / 시가총액 / 시장 구분

동작 방식:

- 먼저 DB를 조회합니다
- 데이터가 없거나 부족하면 OpenDART에서 가져옵니다
- 가져온 데이터는 DB에 저장한 뒤, **DB 기준으로 다시 읽어 화면에 표시**합니다

## 9-2. VS 기업비교

두 기업의 재무 흐름을 비교하는 화면입니다.

입력:

- 비교할 두 기업
- 조회 기간
- 기준 연도
- 재무제표 구분

보여주는 내용:

- 두 기업의 요약 지표
- 연간 / 분기 / 최근 4분기 누적 비교 차트
- 성장률 비교

## 9-3. 기업필터

조건에 맞는 기업 리스트를 찾는 화면입니다.

주요 입력:

- 시장(`전체`, `KOSPI`, `KOSDAQ`)
- 기준 연도
- 재무제표 구분
- 정렬
- 표시 개수

성장률 조건 선택:

- `연간 YoY`
- `분기 YoY`
- `분기 QoQ`
- `최근 4분기 누적 YoY`

각 셀에서 설정할 수 있는 값:

- 적용 여부
- 최근 몇 년 / 몇 분기
- 기준 성장률 %

예시:

- `연간 YoY - 매출`: 최근 3년, 기준 15%
- `분기 YoY - 영업이익`: 최근 8분기, 기준 20%
- `분기 QoQ - 순이익`: 최근 6분기, 기준 10%

동작 방식:

- 이 페이지는 탭 진입 시 자동 조회하지 않습니다
- **`조회` 버튼을 눌렀을 때만** 결과를 계산합니다
- 선택한 조건은 **모두 충족**해야 통과입니다

결과:

- `조건에 맞는 기업`
- 선택 조건별 최소 성장률
- 기업 상세 페이지 이동 링크

## 9-4. DB 업데이트

운영 메뉴입니다. 데이터 수집과 진단은 이 페이지에서 관리합니다.

구성:

- `데이터/API 설정`
- `KRX 연결 점검 실행`
- `회사 목록 동기화`
- `새 작업 시작`
- `일시정지`
- `이어받기`
- `실패만 재시도`
- `전체 DB 초기화`

### 데이터/API 설정

여기서 OpenDART 키를 로컬 DB에 저장하고 전환할 수 있습니다.

용도:

- OpenDART 일일 한도 초과 시 다른 키로 전환
- 환경변수 대신 로컬 설정 DB에 키 저장

### KRX 연결 점검 실행

다음을 점검합니다.

- 회사 목록 API
- 시세 API
- 현재 서버가 실제로 보고 있는 KRX 키 상태

문제가 있을 때 먼저 눌러보면 좋습니다.

### 회사 목록 동기화

상장사 목록을 DB에 넣는 단계입니다.

설명:

- 대상 회사 목록을 준비하는 단계
- 배치 수집 전에 먼저 한 번 필요
- 현재 구현은 **서울 기준 하루 1번만** 실제 동기화합니다
- 같은 날 다시 누르면 `오늘 이미 동기화됨`으로 처리됩니다

### 새 작업 시작

실제 재무 데이터 배치 수집 작업(job)을 만듭니다.

설정:

- 대상 범위(`전체`, `KOSPI`, `KOSDAQ`)
- 재무제표 구분
- 연도 범위
- 배치 크기

배치 크기 의미:

- 한 번의 배치 실행에서 처리할 회사 수
- 예: `25`면 한 번에 25개 회사 처리

### 일시정지 / 이어받기 / 실패만 재시도

- `일시정지`: 현재 작업을 멈춤
- `이어받기`: 멈춘 job의 남은 회사부터 계속 진행
- `실패만 재시도`: 실패한 회사만 다시 `pending`으로 바꿔 재실행

### 전체 DB 초기화

로컬 캐시 DB를 비우는 기능입니다.

삭제 대상:

- 재무 데이터
- 성장률 데이터
- 회사 목록
- 시세 / 시가총액 캐시
- 배치 작업 상태

주의:

- 현재 로컬 환경의 캐시가 전부 초기화됩니다

## 10. DB와 설정 파일 위치

기본 저장 위치는 repo 내부가 아니라 사용자 로컬 캐시 경로입니다.

Windows 기본 위치:

- `%LOCALAPPDATA%\show-me-the-per-cfs.sqlite3`
- `%LOCALAPPDATA%\show-me-the-per-ofs.sqlite3`
- `%LOCALAPPDATA%\show-me-the-per-all.sqlite3`
- `%LOCALAPPDATA%\show-me-the-per-settings.sqlite3`

환경변수로 캐시 디렉터리 변경 가능:

```powershell
$env:SHOW_ME_THE_PER_WEB_CACHE_DIR="C:\\my-cache\\show-me-the-per"
```

그러면 위 DB 파일들이 해당 디렉터리 아래에 생성됩니다.

## 11. 테스트 실행

전체 테스트:

```powershell
python -m unittest discover -s tests -v
```

웹 테스트만:

```powershell
python -m unittest discover -s tests -p "test_web.py" -v
```

## 12. 자주 겪는 문제

## OpenDART 키 오류

예:

- `010 등록되지 않은 인증키입니다`
- `011 사용불가 키입니다`

확인:

- OpenDART 키가 맞는지
- 서버를 다시 띄운 PowerShell 세션에 환경변수가 들어갔는지
- `DB 업데이트 > 데이터/API 설정`에서 다른 키가 활성화돼 있지는 않은지

## OpenDART 요청 제한 초과

예:

- `020 요청 제한을 초과하였습니다`

현재 동작:

- 배치 작업은 `blocked` 상태로 멈춥니다
- 현재 회사는 `pending`으로 남습니다
- 다른 OpenDART 키로 바꾼 뒤 `이어받기` 하면 됩니다

## KRX 401 / 403

확인 순서:

1. `DB 업데이트 > KRX 연결 점검 실행`
2. `KRX_SERVICE_KEY` 확인
3. 공공데이터포털에서 아래 두 API 활용신청 상태 확인
   - [KRX상장종목정보](https://www.data.go.kr/data/15094775/openapi.do)
   - [주식시세정보](https://www.data.go.kr/data/15094808/openapi.do)

## 기업필터가 느릴 때

현재 `기업필터`는 기본적으로 표시 개수를 제한합니다.  
그래도 느리면:

- 표시 개수 줄이기
- 조건을 더 좁히기
- 먼저 `/db-update`에서 배치 수집을 충분히 진행하기

## 13. 추천 운영 방법

실사용 기준으로는 아래 흐름을 추천합니다.

1. OpenDART / KRX 키 준비
2. `/db-update`에서 연결 점검
3. 회사 목록 동기화
4. 3년 또는 10년 기준으로 배치 수집
5. `재무정보`, `VS 기업비교`, `기업필터` 활용
6. OpenDART 한도 초과 시 로컬 저장 키 전환 후 이어받기

---

필요하면 다음 단계로 문서를 더 나눌 수 있습니다.

- `운영 가이드`
- `API 키 문제 해결 가이드`
- `기업필터 사용 예시 모음`
- `배치 업데이트 운영 팁`
