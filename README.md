# 🔍 오즈키즈 키워드 검색수 대시보드

유아동 패션 키워드의 주간 검색수를 트래킹하고, 연간 트렌드를 분석하는 Streamlit 대시보드입니다.

---

## 📁 프로젝트 구조

```
keyword-dashboard/
├── app.py                  ← Streamlit 대시보드 (메인)
├── config.py               ← API 키 & 설정
├── naver_api.py            ← 네이버 검색광고 + 데이터랩 API
├── google_sheets.py        ← Google Sheets 읽기/쓰기
├── ad_rank_parser.py       ← 광고 순위 엑셀 파서
├── fetch_weekly_data.py    ← 주간 자동 수집 스크립트
├── keywords.xlsx           ← 관리 키워드 목록
├── keywords_meta.csv       ← 키워드 필터 태그 (직접 작성)
├── requirements.txt        ← 패키지 목록
├── .env.example            ← 환경변수 템플릿
├── run_weekly.bat          ← Windows 작업 스케줄러용
└── .github/workflows/
    └── weekly.yml          ← GitHub Actions 자동화
```

---

## 🚀 설치 & 실행 (5단계)

### 1단계: Python 환경 설정

```bash
# 프로젝트 폴더에서
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux

pip install -r requirements.txt
```

### 2단계: API 키 발급

#### (A) 네이버 검색광고 API
1. [네이버 검색광고 센터](https://searchad.naver.com) 로그인
2. `도구` → `API 사용 관리` → API 키 발급
3. 필요한 값: `API LICENSE`, `SECRET KEY`, `CUSTOMER ID`

#### (B) 네이버 개발자 센터 API
1. [네이버 개발자 센터](https://developers.naver.com) 로그인
2. `내 애플리케이션` → 새 앱 등록
3. 사용할 API: `데이터랩`, `검색 (쇼핑)`
4. 필요한 값: `Client ID`, `Client Secret`

#### (C) Google Sheets API
1. [Google Cloud Console](https://console.cloud.google.com) 접속
2. 새 프로젝트 생성
3. `API 및 서비스` → `라이브러리` → `Google Sheets API` 활성화
4. `서비스 계정` 만들기 → JSON 키 다운로드 → `credentials.json`으로 저장
5. **중요**: Google Sheet를 서비스 계정 이메일에 공유 (편집자 권한)

### 3단계: 환경변수 설정

```bash
cp .env.example .env
# .env 파일을 열어서 실제 API 키 값 입력
```

### 4단계: 첫 데이터 수집

```bash
python fetch_weekly_data.py
```

### 5단계: 대시보드 실행

```bash
streamlit run app.py
```
브라우저에서 `http://localhost:8501` 으로 접속됩니다.

---

## 🔄 자동화 설정

### 방법 A: GitHub Actions (추천)

1. 이 프로젝트를 GitHub 저장소에 push
2. `Settings` → `Secrets and variables` → `Actions` → 아래 시크릿 추가:

| Secret Name | 값 |
|---|---|
| `NAVER_AD_API_LICENSE` | 검색광고 API 라이선스 |
| `NAVER_AD_SECRET_KEY` | 검색광고 시크릿 키 |
| `NAVER_AD_CUSTOMER_ID` | 검색광고 고객 ID |
| `NAVER_CLIENT_ID` | 네이버 클라이언트 ID |
| `NAVER_CLIENT_SECRET` | 네이버 클라이언트 시크릿 |
| `GOOGLE_CREDENTIALS_JSON` | credentials.json 내용 전체 (JSON 문자열) |

3. 매주 월요일 오전 6시(KST)에 자동 실행됩니다.
4. 수동 실행: `Actions` 탭 → `Run workflow`

### 방법 B: Windows 작업 스케줄러

1. `Win + R` → `taskschd.msc`
2. `기본 작업 만들기`
3. 트리거: 매주 월요일 06:00
4. 동작: `프로그램 시작` → `run_weekly.bat` 경로 지정

---

## 📊 대시보드 기능

| 탭 | 기능 |
|---|---|
| 📈 주간 검색수 | 이번 주 검색수, 지난 주 대비 변화율, 급상승/급하락 하이라이트, 키워드별 추이 그래프 |
| 📊 연간 트렌드 | 데이터랩 비율 × 실제 검색수 결합, 올해 vs 작년 비교 그래프 |
| 🏆 광고 순위 | 네이버 검색광고 리포트 엑셀 업로드 → 파워링크/쇼핑검색 순위 파싱 |
| ⚙️ 데이터 관리 | 수동 수집 실행, 키워드 메타 정보 확인, API 연결 상태 |

### 필터
- **계절**: 봄, 여름, 가을, 겨울
- **복종**: 의류, 신발, 잡화
- **성별**: 남, 여, 공용
- **키워드 검색**: 직접 입력

> 필터를 사용하려면 `keywords_meta.csv`를 작성해야 합니다.

---

## 📝 keywords_meta.csv 작성 방법

```csv
keyword,계절,복종,성별,카테고리
아기우산,여름,잡화,공용,
유아우산,여름,잡화,공용,
키즈운동화,봄/가을,신발,공용,
여아원피스,여름,의류,여,
```

- **계절**: 봄, 여름, 가을, 겨울 (복수: `봄/가을`)
- **복종**: 의류, 신발, 잡화
- **성별**: 남, 여, 공용
- **카테고리**: 빈칸 → 첫 실행 시 네이버 쇼핑 API로 자동 채움

---

## ⚠️ 알려진 제한

- 네이버 데이터랩 API: 일일 1,000건, 한 번에 5개 키워드
- 쇼핑 오가닉 순위 / 블로그 순위: 공식 API 미제공 (향후 추가 가능)
- Google Sheets 무료 한도: 분당 60회 읽기, 분당 60회 쓰기
