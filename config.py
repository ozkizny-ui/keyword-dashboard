"""
설정 파일 - API 키와 프로젝트 설정
우선순위: 환경변수(os.getenv) → st.secrets (Streamlit Cloud) → 기본값
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _secret(key: str, default: str = "") -> str:
    """os.getenv 우선, 없으면 st.secrets에서 읽기 (CLI 환경에서도 안전)"""
    val = os.getenv(key)
    if val:
        return val
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return default


# ──────────────────────────────────────────────
# 네이버 검색광고 API (키워드 검색수 조회)
# 발급: https://searchad.naver.com → 도구 → API 사용 관리
# ──────────────────────────────────────────────
NAVER_AD_API_LICENSE = _secret("NAVER_AD_API_LICENSE")
NAVER_AD_SECRET_KEY = _secret("NAVER_AD_SECRET_KEY")
NAVER_AD_CUSTOMER_ID = _secret("NAVER_AD_CUSTOMER_ID")
NAVER_AD_BASE_URL = "https://api.searchad.naver.com"

# ──────────────────────────────────────────────
# 네이버 데이터랩 API (트렌드 비율 조회)
# 발급: https://developers.naver.com → 내 애플리케이션
# ──────────────────────────────────────────────
NAVER_CLIENT_ID = _secret("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = _secret("NAVER_CLIENT_SECRET")
NAVER_DATALAB_URL = "https://openapi.naver.com/v1/datalab/search"

# ──────────────────────────────────────────────
# Google Sheets 설정
# 발급: https://console.cloud.google.com → 서비스 계정 키
# ──────────────────────────────────────────────
GOOGLE_CREDENTIALS_FILE = _secret("GOOGLE_CREDENTIALS_FILE", "credentials.json")
SPREADSHEET_ID = "1uD-2gHghytC-Gb4ryEWGmhtjeFqGcTY59M90sxrASyI"
SHEET_NAME_WEEKLY = "주간검색수"
SHEET_NAME_TREND = "연간트렌드"
SHEET_NAME_RANK = "광고순위"
SHEET_NAME_RANK_SHOPPING = "쇼핑검색순위"
SHEET_NAME_RANK_POWERLINK = "파워링크순위"
SHEET_NAME_RANK_BLOG = "블로그순위"

# ──────────────────────────────────────────────
# 프로젝트 설정
# ──────────────────────────────────────────────
BRAND_STORE_NAME = "오즈키즈"
BRAND_STORE_URL = "https://brand.naver.com/ozkiz"
KEYWORDS_FILE = "keywords.xlsx"
KEYWORDS_META_FILE = "keywords_meta.csv"

# 데이터랩 API 제한: 한 번에 5개 키워드, 일일 1,000건
DATALAB_BATCH_SIZE = 5
DATALAB_DELAY_SEC = 0.5  # 호출 간 딜레이 (초)

# 검색광고 API: 한 번에 5개 키워드 (URL 길이 제한 방지)
AD_API_BATCH_SIZE = 5

# 주간 변화율 알림 기준 (%)
CHANGE_ALERT_THRESHOLD = 30
